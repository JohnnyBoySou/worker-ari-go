// Package config são as envs do worker. Mesmo contrato do worker em TypeScript
// (src/config.ts) para os dois poderem rodar com o MESMO .env durante a migração
// — se os nomes divergissem, um nó em Go e um em TS lado a lado teriam
// comportamentos diferentes sem que nada acusasse.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/lai/worker-ari/internal/ari"
	"github.com/lai/worker-ari/internal/commands"
	"github.com/lai/worker-ari/internal/orchestrator"
	"github.com/lai/worker-ari/internal/shard"
)

type Config struct {
	AriURL      string
	AriUser     string
	AriPassword string
	AriAppBase  string
	ShardID     string
	AriApp      string // AriAppBase + "-" + ShardID (ou só a base)

	TrunkEndpoint string
	RingTimeout   int
	AppCheck      time.Duration

	RecordingDownloadAttempts int

	DatabaseURL string
	RedisURL    string
	NatsURL     string

	CommandSubjectBase string
	EventsChannel      string
	// CmdParticoes é a concorrência do consumo de comandos, particionada por
	// callId (ver internal/commands.Connect).
	CmdParticoes int
	// AriMaxConns é o TETO de conexões simultâneas com o ARI deste nó, derivado
	// do sessionlimit do Asterisk menos a reserva.
	AriMaxConns int
	// RecordingConcurrency é quantas dessas conexões podem estar em gravação.
	RecordingConcurrency int
	// PGMaxConns é o teto do pool do Postgres. <= 0 deixa o default do pgx.
	PGMaxConns int

	ShardRegistryKey string
	ShardHeartbeat   time.Duration
	ShardTTL         time.Duration

	MaxCalls     int
	DrainTimeout time.Duration
	DrainPoll    time.Duration
	MetricsPort  string

	BackURL      string
	WorkerAPIKey string

	AIAudiosocketAddr   string
	VoiceTalkControlURL string
	VoiceTalkToken      string

	S3 S3Config
}

type S3Config struct {
	AccessKeyID     string
	SecretAccessKey string
	Region          string
	Endpoint        string
	Bucket          string
}

func (s S3Config) Enabled() bool { return s.Bucket != "" && s.AccessKeyID != "" }

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func num(k string, def int) int {
	v, err := strconv.Atoi(env(k, ""))
	if err != nil || v == 0 {
		return def
	}
	return v
}

func ms(k string, def time.Duration) time.Duration {
	v, err := strconv.Atoi(env(k, ""))
	if err != nil {
		return def
	}
	return time.Duration(v) * time.Millisecond
}

func Load() (Config, error) {
	// SHARD_ID inválido é erro de boot, não default silencioso: um nome de app
	// torto faz o Asterisk mandar Stasis para um app que ninguém escuta.
	shardID, err := shard.Resolve(os.Getenv("SHARD_ID"))
	if err != nil {
		return Config{}, err
	}
	base := env("ASTERISK_ARI_APP", "connect")

	// ORÇAMENTO DE SESSÕES HTTP DO ASTERISK. O `sessionlimit` do http.conf do nó
	// é um teto DURO e compartilhado: passando dele o Asterisk recusa a conexão,
	// e um originate recusado é uma chamada que não monta. Este valor tem que
	// bater com o que o worker-asterisk renderiza — se lá for menor, o teto que
	// vale é o de lá. Ver ari.DefaultSessionLimit.
	sessionLimit := num("ARI_HTTP_SESSION_LIMIT", ari.DefaultSessionLimit)
	ariMaxConns := sessionLimit - ari.ReservaSessoes
	if ariMaxConns <= 0 {
		ariMaxConns = ari.DefaultMaxConns
	}

	c := Config{
		// Como sidecar, o bind é 127.0.0.1:8088 do PRÓPRIO nó — o worker não fala
		// com o ARI de outro shard.
		AriURL:      env("ASTERISK_ARI_URL", "http://127.0.0.1:8088"),
		AriUser:     env("ASTERISK_ARI_USERNAME", ""),
		AriPassword: env("ASTERISK_ARI_PASSWORD", ""),
		AriAppBase:  base,
		ShardID:     shardID,
		// O nome do app e o do subject derivam do MESMO SHARD_ID: envs
		// independentes permitiriam o worker escutar um app e consumir os
		// comandos de outro shard.
		AriApp: shard.Name(base, shardID, "-"),

		TrunkEndpoint: env("SIP_TRUNK_ENDPOINT", "trunk"),
		RingTimeout:   num("CALL_RING_TIMEOUT_SECONDS", 45),
		// 30s é barato — um GET local no Asterisk — e detecta em meio minuto o
		// socket meio-aberto que em 18/08/2026 deixou a telefonia parada 3 dias.
		AppCheck: ms("ARI_APP_CHECK_MS", 30*time.Second),

		// A gravação só é FINALIZADA depois que a bridge cai; em gravações longas
		// isso passa de 1,5s. Backoff exponencial capado em 4s.
		RecordingDownloadAttempts: num("RECORDING_DOWNLOAD_ATTEMPTS", 8),

		DatabaseURL: os.Getenv("DATABASE_URL"),
		RedisURL:    env("REDIS_URL", "redis://127.0.0.1:6379"),
		NatsURL:     env("NATS_URL", "nats://127.0.0.1:4222"),

		CommandSubjectBase: env("ARI_COMMAND_SUBJECT", "ari.cmd"),
		EventsChannel:      env("ARI_EVENTS_CHANNEL", "ari.call-events"),

		// 16 partições contra 6-11 comandos/s por shard é folga deliberada: o que
		// se compra aqui não é vazão, é impedir que um comando lento segure os
		// outros (ver internal/commands.Connect).
		CmdParticoes: num("ARI_CMD_PARTITIONS", commands.DefaultParticoes),
		AriMaxConns:  num("ARI_MAX_CONNS", ariMaxConns),
		// Das conexões acima, quantas podem estar presas em gravação — arquivo de
		// MBs, que segura a sessão por segundos. O resto fica para o controle de
		// chamadas, que é o que não pode esperar.
		RecordingConcurrency: num("RECORDING_CONCURRENCY", orchestrator.DefaultRecordingConcurrency),
		// O pool do pgx tem default max(4, NumCPU) — num sidecar de 2 vCPUs, 4
		// conexões para as ~4 escritas de cada chamada. Com goroutine por evento,
		// um pico de finalizações vira fila no pool antes de virar fila no banco.
		PGMaxConns: num("PG_MAX_CONNS", 20),

		ShardRegistryKey: env("ARI_SHARD_REGISTRY_KEY", "ari:shards"),
		ShardHeartbeat:   ms("ARI_SHARD_HEARTBEAT_MS", 5*time.Second),
		// Três batidas de folga: um blip de Redis não pode tirar um nó saudável
		// do roteamento.
		ShardTTL: ms("ARI_SHARD_TTL_MS", 15*time.Second),

		// Passar do teto não dá erro, dá áudio picotado em TODAS as chamadas do
		// nó. Recusar a 1001ª é mais barato que picotar mil.
		MaxCalls: num("ARI_MAX_CALLS", 1000),
		// Tem que ser MAIOR que a duração típica da chamada, e o terminating:wait
		// do lifecycle hook do ASG tem que ser >= a este valor.
		DrainTimeout: ms("ARI_DRAIN_TIMEOUT_MS", 5*time.Minute),
		DrainPoll:    ms("ARI_DRAIN_POLL_MS", time.Second),
		MetricsPort:  env("PORT", "8090"),

		BackURL:      env("BACK_URL", ""),
		WorkerAPIKey: env("WORKER_API_KEY", ""),

		AIAudiosocketAddr:   env("AI_AUDIOSOCKET_ADDR", ""),
		VoiceTalkControlURL: env("VOICE_TALK_CONTROL_URL", ""),
		VoiceTalkToken:      env("VOICE_TALK_TOKEN", ""),

		S3: S3Config{
			AccessKeyID:     env("AWS_ACCESS_KEY_ID", ""),
			SecretAccessKey: env("AWS_SECRET_ACCESS_KEY", ""),
			Region:          env("AWS_DEFAULT_REGION", "us-east-1"),
			Endpoint:        env("AWS_ENDPOINT_URL", ""),
			Bucket:          env("AWS_S3_BUCKET_NAME", ""),
		},
	}
	if c.DatabaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL ausente")
	}
	return c, nil
}

// CommandSubject é `ari.cmd.<shardId>`. Sem shard não há subject: o modo
// single-node legado consome pelo transporte antigo.
func (c Config) CommandSubject() string { return shard.Name(c.CommandSubjectBase, c.ShardID, ".") }
