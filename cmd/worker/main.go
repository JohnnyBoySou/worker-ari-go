// Comando worker: o worker ARI em Go.
//
// Sidecar 1:1 do Asterisk do shard — dono único do app Stasis `connect-<shardId>`
// e de todo o controle REST daquele nó.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/lai/worker-ari/internal/ari"
	"github.com/lai/worker-ari/internal/commands"
	"github.com/lai/worker-ari/internal/config"
	"github.com/lai/worker-ari/internal/drain"
	"github.com/lai/worker-ari/internal/httpapi"
	"github.com/lai/worker-ari/internal/logx"
	"github.com/lai/worker-ari/internal/metrics"
	"github.com/lai/worker-ari/internal/orchestrator"
	"github.com/lai/worker-ari/internal/publisher"
	"github.com/lai/worker-ari/internal/recordings"
	"github.com/lai/worker-ari/internal/shard"
	"github.com/lai/worker-ari/internal/store"
	"github.com/redis/go-redis/v9"
)

func main() {
	if err := run(); err != nil {
		logx.Error("worker.fatal", "err", err.Error())
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	// O subject é `ari.cmd.<shardId>`: NATS sem shard não faz sentido. Falhar no
	// boot é melhor que subir escutando um endereço que ninguém publica.
	if cfg.ShardID == "" {
		return errors.New("SHARD_ID é obrigatório neste worker (o subject é ari.cmd.<shardId>)")
	}

	ctx, parar := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer parar()
	boot, cancelBoot := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelBoot()

	pg, err := store.NewPostgres(boot, cfg.DatabaseURL, cfg.PGMaxConns)
	if err != nil {
		return err
	}
	defer pg.Close()

	ropt, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return err
	}
	rdb := redis.NewClient(ropt)
	defer func() { _ = rdb.Close() }()

	var up orchestrator.Uploader
	if cfg.S3.Enabled() {
		up = recordings.New(cfg.S3.AccessKeyID, cfg.S3.SecretAccessKey, cfg.S3.Region, cfg.S3.Endpoint, cfg.S3.Bucket)
		logx.Info("worker.s3_enabled", "bucket", cfg.S3.Bucket, "endpoint", cfg.S3.Endpoint, "region", cfg.S3.Region)
	} else {
		logx.Warn("worker.s3_disabled", "reason", "AWS_S3_BUCKET_NAME/AWS_ACCESS_KEY_ID ausentes — gravações só no Asterisk")
	}

	m := metrics.New()
	a := ari.New(ari.Options{
		BaseURL: cfg.AriURL, Username: cfg.AriUser, Password: cfg.AriPassword,
		App: cfg.AriApp, AppCheck: cfg.AppCheck, MaxConns: cfg.AriMaxConns,
	})
	// O cliente do orquestrador fala com o back e com o voice-talk — dois hosts,
	// e o notifyBackFinalized dispara uma requisição por chamada ENCERRADA. Com o
	// transporte padrão (2 conexões ociosas por host), um pico de encerramentos
	// vira handshake TCP por chamada; ver ari.NewTransport.
	httpOut := &http.Client{Timeout: 10 * time.Second, Transport: ari.NewTransport(cfg.AriMaxConns)}
	orq := orchestrator.New(a, pg, publisher.New(rdb, cfg.EventsChannel), up, m, orchestrator.Options{
		AriApp: cfg.AriApp, TrunkEndpoint: cfg.TrunkEndpoint, RingTimeout: cfg.RingTimeout,
		MaxCalls: cfg.MaxCalls, RecordingDownloadAttempts: cfg.RecordingDownloadAttempts,
		AIAudiosocketAddr:   cfg.AIAudiosocketAddr,
		VoiceTalkControlURL: cfg.VoiceTalkControlURL, VoiceTalkToken: cfg.VoiceTalkToken,
		BackURL: cfg.BackURL, WorkerAPIKey: cfg.WorkerAPIKey,
		RecordingConcurrency: cfg.RecordingConcurrency,
		HTTPClient:           httpOut,
	})

	// Eventos do Stasis. "open" dispara a cada (re)conexão -> reidrata (MERGE:
	// chamadas já rastreadas ficam intocadas).
	// CADA EVENTO EM SUA GOROUTINE.
	//
	// O handler roda dentro do laco de leitura do WebSocket: chamado direto, ele
	// BLOQUEIA a leitura do proximo evento ate terminar. E um `onStasisStart` de
	// saida faz ~7 idas ao ARI (answer, 2x getvar, createBridge, addChannel,
	// originate, record) mais uma escrita no banco — em serie, isso limita o
	// worker a ~20 eventos/s, e o originate da perna B de todas as chamadas
	// seguintes fica na fila.
	//
	// Medido: a 150 simultaneas so 59 completavam; o SIPp do trunk recebia 59
	// INVITEs em vez de 150, com a CPU do Asterisk em 33%. Nao era saturacao,
	// era serializacao.
	//
	// O original em TS ja era assim: `void this.onStasisStart(ev)` e
	// fire-and-forget, e o event loop segue para o proximo evento. A goroutine
	// restaura essa semantica — e e exatamente para isto que o estado do
	// orquestrador vive atras de mutex (ver internal/orchestrator/state.go).
	a.On("StasisStart", func(_ string, p []byte) {
		var ev ari.StasisStart
		if json.Unmarshal(p, &ev) == nil {
			go orq.OnStasisStart(ctx, ev)
		}
	})
	a.On("ChannelDestroyed", func(_ string, p []byte) {
		var ev ari.ChannelDestroyed
		if json.Unmarshal(p, &ev) == nil {
			go orq.OnChannelDestroyed(ctx, ev)
		}
	})
	a.On("open", func(string, []byte) { orq.Rehydrate(ctx) })

	go a.Run(ctx)

	// Estado de drenagem: lido pelo registro de shards, pelo /health e pelo
	// /metrics. Uma variável só, para as três respostas não poderem divergir.
	var draining atomic.Bool
	aceitando := func() bool { return !draining.Load() && orq.ActiveCalls() < cfg.MaxCalls }

	reg := shard.NewRegistry(shard.RegistryOptions{
		Redis: rdb, Key: cfg.ShardRegistryKey, ShardID: cfg.ShardID,
		AriApp: cfg.AriApp, Queue: cfg.CommandSubject(), MaxCalls: cfg.MaxCalls,
		Heartbeat: cfg.ShardHeartbeat, TTL: cfg.ShardTTL,
		ActiveCalls: orq.ActiveCalls, Accepting: aceitando,
	})
	reg.Start(ctx)

	cons, err := commands.Connect(ctx, cfg.NatsURL, cfg.ShardID, orq, 5*time.Second, cfg.CmdParticoes)
	if err != nil {
		return err
	}

	srv := &http.Server{
		Addr: ":" + cfg.MetricsPort,
		Handler: httpapi.Handler(httpapi.Deps{
			AppAliveAt: a.AppAliveAt,
			AppCheck:   func() time.Duration { return cfg.AppCheck },
			RenderMetrics: func() string {
				return m.Render(metrics.Gauges{
					ActiveCalls:        orq.ActiveCalls(),
					RecordingsInFlight: orq.RecordingsInFlight(),
					RecordingsMax:      orq.RecordingsMax(),
				}, &metrics.ShardGauges{
					ShardID: cfg.ShardID, MaxCalls: cfg.MaxCalls,
					Accepting: aceitando(), Draining: draining.Load(),
				})
			},
			Shard: func() httpapi.ShardStatus {
				return httpapi.ShardStatus{
					ShardID: cfg.ShardID, AriApp: cfg.AriApp, Queue: cfg.CommandSubject(),
					ActiveCalls: orq.ActiveCalls(), MaxCalls: cfg.MaxCalls,
					Accepting: aceitando(), Draining: draining.Load(),
				}
			},
		}),
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logx.Error("http.caiu", "err", err.Error())
		}
	}()

	// O nome do app TEM que ser idêntico ao ARI_APP do worker-asterisk deste nó
	// (que o usa também como contexto do dialplan). Logar os nomes derivados no
	// boot permite conferir com um grep, em vez de descobrir pelo sintoma —
	// "comando aceito, chamada nunca montada".
	// O orçamento de sessões no boot permite conferir com um grep que o worker e
	// o `sessionlimit` do Asterisk deste nó estão falando do mesmo número — em
	// vez de descobrir pelo sintoma, que é chamada que não monta sem erro.
	logx.Info("worker.boot",
		"shardId", cfg.ShardID, "ariApp", cfg.AriApp, "subject", cfg.CommandSubject(),
		"ari", cfg.AriURL, "maxCalls", cfg.MaxCalls, "port", cfg.MetricsPort,
		"ariMaxConns", cfg.AriMaxConns, "recordingConcurrency", cfg.RecordingConcurrency)

	<-ctx.Done()

	// ENCERRAMENTO COM DRENAGEM. A ordem importa e cada passo tem um motivo:
	//   1. marca draining  -> /health e /metrics já contam a verdade
	//   2. sai do registro -> o produtor para de escolher este shard AGORA, em
	//                         vez de esperar o TTL vencer mandando comando para
	//                         um nó que está de saída
	//   3. pausa o consumo -> para de puxar comando novo
	//   4. espera as ativas -> o passo que preserva as chamadas em curso
	//   5. fecha o resto
	draining.Store(true)
	logx.Info("worker.drenando", "ativas", orq.ActiveCalls())

	desligar := context.Background()
	reg.Deregister(desligar)
	cons.Pause()

	r := drain.Wait(desligar, orq.ActiveCalls, drain.Options{
		Timeout: cfg.DrainTimeout, Poll: cfg.DrainPoll,
		OnProgress: func(restantes int, decorrido time.Duration) {
			logx.Info("worker.drenando_progresso", "restantes", restantes, "decorridoMs", decorrido.Milliseconds())
		},
	})
	if r.Drained {
		logx.Info("worker.drenado", "decorridoMs", r.Decorrido.Milliseconds())
	} else {
		// Quantas conversas foram cortadas é informação de incidente: tem que
		// estar no log, não só no gráfico.
		logx.Warn("worker.drenagem_estourou", "restantes", r.Restantes, "timeoutMs", cfg.DrainTimeout.Milliseconds())
	}

	cons.Close()
	sh, cancel := context.WithTimeout(desligar, 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(sh)
	return nil
}
