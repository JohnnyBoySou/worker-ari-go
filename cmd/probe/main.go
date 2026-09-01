// Comando probe: valida o client ARI contra um Asterisk REAL, sem tocar em
// chamada nenhuma.
//
// POR QUE ELE EXISTE. O worker completo exige Postgres, NATS e Redis, e originar
// uma chamada custa dinheiro e toca o telefone de alguém. Mas a parte do port que
// nunca falou com um Asterisk de verdade é justamente a mais delicada: o REST, o
// handshake do WebSocket de eventos e o watchdog do app Stasis. Esta sonda
// exercita os três e mais nada.
//
// SEGURANÇA. Ela conecta num app Stasis PRÓPRIO (`connect-<SHARD_ID>`), nunca no
// `connect` de produção. Dois processos no mesmo app fazem o Asterisk distribuir
// os eventos entre eles: metade das chamadas ao vivo ficaria sem dono. O
// SHARD_ID é obrigatório aqui por isso — não há default que possa dar errado.
//
//	SHARD_ID=probe ASTERISK_ARI_URL=https://ari.exemplo \
//	ASTERISK_ARI_USERNAME=... ASTERISK_ARI_PASSWORD=... go run ./cmd/probe
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/lai/worker-ari/internal/ari"
	"github.com/lai/worker-ari/internal/shard"
)

func main() {
	// Carrega .env se existir: a senha do ARI não deve passar por linha de
	// comando (fica no histórico do shell e na lista de processos).
	carregarEnv(".env")

	base := env("ASTERISK_ARI_APP", "connect")
	shardID, err := shard.Resolve(os.Getenv("SHARD_ID"))
	if err != nil {
		fatal("%v", err)
	}
	if shardID == "" {
		fatal("SHARD_ID é obrigatório.\n"+
			"Sem ele a sonda conectaria no app %q — o MESMO do worker de produção — e o\n"+
			"Asterisk passaria a dividir os eventos entre os dois. Metade das chamadas ao\n"+
			"vivo ficaria sem dono. Use algo como SHARD_ID=probe.", base)
	}
	app := shard.Name(base, shardID, "-")
	url := env("ASTERISK_ARI_URL", "http://127.0.0.1:8088")
	dur := envDur("PROBE_SECONDS", 25)

	fmt.Printf("ARI   : %s\napp   : %s  (nunca %q)\njanela: %s\n\n", url, app, base, dur)

	c := ari.New(ari.Options{
		BaseURL:  url,
		Username: env("ASTERISK_ARI_USERNAME", ""),
		Password: env("ASTERISK_ARI_PASSWORD", ""),
		App:      app,
		// Curto de propósito: queremos VER o watchdog rodar dentro da janela.
		AppCheck:     8 * time.Second,
		ReconnectMin: time.Second,
		ReconnectMax: 5 * time.Second,
	})

	ctx, parar := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer parar()

	var abriu, fechou, eventos atomic.Int32
	c.On("open", func(string, []byte) {
		abriu.Add(1)
		fmt.Printf("  [%s] WebSocket ABERTO no app %s\n", hora(), app)
	})
	c.On("close", func(string, []byte) {
		fechou.Add(1)
		fmt.Printf("  [%s] WebSocket FECHOU\n", hora())
	})
	// subscribeAll=true: todo evento do app passa por aqui. Como ninguém roteia
	// para este app, o normal é não ver nenhum — e isso é o resultado esperado.
	for _, t := range []string{"StasisStart", "ChannelDestroyed", "ChannelStateChange"} {
		tipo := t
		c.On(tipo, func(_ string, p []byte) {
			eventos.Add(1)
			fmt.Printf("  [%s] evento %s: %s\n", hora(), tipo, resumo(p))
		})
	}

	go c.Run(ctx)

	// 1) REST: o Asterisk responde e quem somos nós?
	fmt.Println("1. REST — GET /ari/asterisk/info")
	if !checarInfo(ctx, url) {
		os.Exit(1)
	}

	// 2) Registro do app: é o que o watchdog consulta. 404 aqui significa que o
	//    WebSocket não registrou o app — o estado exato do incidente de 18/08.
	fmt.Println("\n2. Registro do app Stasis (o que o watchdog checa)")
	time.Sleep(2 * time.Second)
	if reg, conclusivo := c.CheckApp(ctx); reg {
		fmt.Printf("   OK  app %q registrado no Asterisk\n", app)
	} else if conclusivo {
		fmt.Printf("   FALHA  404: o app %q NÃO está registrado (socket meio-aberto)\n", app)
	} else {
		fmt.Println("   INCONCLUSIVO  erro ambíguo — por desenho, isto NÃO derruba o socket")
	}

	// 3) Estado vivo do Asterisk. Somente leitura.
	fmt.Println("\n3. Estado atual (somente leitura)")
	if chs, err := c.ListChannels(ctx); err != nil {
		fmt.Printf("   canais : ERRO %v\n", err)
	} else {
		fmt.Printf("   canais : %d ativos\n", len(chs))
		for i, ch := range chs {
			if i == 5 {
				fmt.Printf("            ... e mais %d\n", len(chs)-5)
				break
			}
			fmt.Printf("            %s  state=%s exten=%s\n", ch.ID, ch.State, ch.Dialplan.Exten)
		}
	}
	if brs, err := c.ListBridges(ctx); err != nil {
		fmt.Printf("   bridges: ERRO %v\n", err)
	} else {
		fmt.Printf("   bridges: %d ativas\n", len(brs))
	}

	// 4) Mantém o socket aberto: o watchdog roda a cada 8s e qualquer queda
	//    aparece como close/open. É o teste de estabilidade que só o tempo faz.
	fmt.Printf("\n4. Segurando a conexão por %s (watchdog a cada 8s)\n", dur)
	select {
	case <-ctx.Done():
	case <-time.After(dur):
	}

	fmt.Printf("\n=== resumo ===\n")
	fmt.Printf("aberturas do WebSocket : %d  (1 = estável; >1 = reconectou)\n", abriu.Load())
	fmt.Printf("quedas                 : %d\n", fechou.Load())
	fmt.Printf("eventos recebidos      : %d  (0 é o esperado: ninguém roteia para %s)\n", eventos.Load(), app)
	if ultimo := c.AppAliveAt(); !ultimo.IsZero() {
		fmt.Printf("última prova de vida   : há %s\n", time.Since(ultimo).Round(time.Second))
	}
	if abriu.Load() == 1 && fechou.Load() == 0 {
		fmt.Println("\nOK: client ARI validado contra o Asterisk real.")
	} else {
		fmt.Println("\nATENÇÃO: a conexão oscilou — ver as linhas de close/open acima.")
	}
}

func checarInfo(ctx context.Context, url string) bool {
	// Usa o próprio caminho REST do client via um GET direto, para separar
	// "credencial errada" de "app não registrado" no diagnóstico.
	c := ari.New(ari.Options{
		BaseURL: url, Username: env("ASTERISK_ARI_USERNAME", ""),
		Password: env("ASTERISK_ARI_PASSWORD", ""), App: "info-probe",
	})
	if _, conclusivo := c.CheckApp(ctx); !conclusivo {
		fmt.Println("   FALHA  não deu para falar com o ARI (credencial? rede? URL?)")
		return false
	}
	fmt.Println("   OK  o ARI respondeu e autenticou")
	return true
}

func resumo(p []byte) string {
	var m map[string]any
	if json.Unmarshal(p, &m) != nil {
		return string(p[:min(120, len(p))])
	}
	b, _ := json.Marshal(m["channel"])
	return string(b[:min(160, len(b))])
}

func hora() string { return time.Now().Format("15:04:05") }

// carregarEnv lê um .env simples (KEY=VALUE, # comenta) SEM sobrescrever o que
// já veio do ambiente — variável explícita na chamada vence o arquivo.
func carregarEnv(caminho string) {
	b, err := os.ReadFile(caminho)
	if err != nil {
		return
	}
	for _, linha := range strings.Split(string(b), "\n") {
		linha = strings.TrimSpace(linha)
		if linha == "" || strings.HasPrefix(linha, "#") {
			continue
		}
		k, v, ok := strings.Cut(linha, "=")
		if !ok {
			continue
		}
		k = strings.TrimSpace(k)
		v = strings.Trim(strings.TrimSpace(v), `"'`)
		if _, existe := os.LookupEnv(k); !existe {
			_ = os.Setenv(k, v)
		}
	}
	fmt.Printf("(.env carregado de %s)\n", caminho)
}

func env(k, d string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return d
}

func envDur(k string, d int) time.Duration {
	var n int
	if _, err := fmt.Sscanf(env(k, ""), "%d", &n); err == nil && n > 0 {
		return time.Duration(n) * time.Second
	}
	return time.Duration(d) * time.Second
}

func fatal(f string, a ...any) {
	fmt.Fprintf(os.Stderr, "erro: "+f+"\n", a...)
	os.Exit(1)
}
