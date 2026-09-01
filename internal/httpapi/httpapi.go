// Package httpapi é o HTTP mínimo de observabilidade (/health e /metrics).
//
// Port de src/http.ts. Vive fora do main porque lá a decisão de saúde nasceria
// ao lado da conexão do Postgres, do Redis e do WebSocket do ARI — testá-la
// exigiria subir o worker inteiro, então ela nunca seria testada. É justamente a
// regra que, mal resolvida, fez o incidente de 18/08/2026 passar 3 dias
// despercebido.
package httpapi

import (
	"encoding/json"
	"net/http"
	"time"
)

type ShardStatus struct {
	ShardID     string `json:"shardId"` // "" = modo single-node legado
	AriApp      string `json:"ariApp"`
	Queue       string `json:"queue"`
	ActiveCalls int    `json:"activeCalls"`
	MaxCalls    int    `json:"maxCalls"`
	Accepting   bool   `json:"accepting"`
	Draining    bool   `json:"draining"`
}

type Deps struct {
	// AppAliveAt é o carimbo da última confirmação de que o Asterisk enxerga o
	// nosso app Stasis. Zero = nunca confirmado.
	AppAliveAt func() time.Time
	// AppCheck é o período do watchdog. <= 0 desliga a checagem.
	AppCheck func() time.Duration
	// RenderMetrics devolve o corpo já pronto do /metrics.
	RenderMetrics func() string
	// Shard é opcional: sem ele o /health é o de antes.
	Shard func() ShardStatus
	Now   func() time.Time
}

// StasisHealthy: o worker só é ÚTIL enquanto o Asterisk enxerga o app Stasis.
// Sem isso ele consome comandos e origina por REST, mas nenhum evento volta — a
// ligação nunca é montada nem finalizada.
//
// Duas decisões deliberadas:
//
//   - Watchdog desligado (check <= 0) responde SAUDÁVEL. Não há carimbo para
//     julgar, e reprovar todo mundo seria alarme falso — pior que alarme nenhum.
//     É o modo usado em teste local sem Asterisk.
//   - A janela é check*3. Um ciclo perdido pode ser um blip de rede; três
//     seguidos são um padrão. Estreitar isso troca um bug por outro: liveness que
//     oscila faz o supervisor reiniciar um worker saudável, e reiniciar derruba o
//     WebSocket do Stasis no meio das chamadas em curso.
func StasisHealthy(aliveAt time.Time, check time.Duration, now time.Time) bool {
	if check <= 0 {
		return true
	}
	return !aliveAt.IsZero() && now.Sub(aliveAt) < check*3
}

func Handler(d Deps) http.Handler {
	now := d.Now
	if now == nil {
		now = time.Now
	}
	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		aliveAt := d.AppAliveAt()
		ok := StasisHealthy(aliveAt, d.AppCheck(), now())
		body := map[string]any{
			"status":     map[bool]string{true: "ok", false: "degraded"}[ok],
			"stasis":     ok,
			"appAliveAt": aliveAt.UnixMilli(),
		}
		if aliveAt.IsZero() {
			body["appAliveAt"] = 0
		}
		if d.Shard != nil {
			body["shard"] = d.Shard()
		}
		// 503 quando degradado: o supervisor precisa distinguir "de pé" de "de pé
		// e inútil". O {"status":"ok"} incondicional é o que escondeu o incidente.
		//
		// DRENANDO CONTINUA 200. É tentador devolver 503 para "tirar do
		// balanceador", mas quem tira este nó do roteamento é a saída do registro
		// de shards, e o /health aqui é o healthcheck do SUPERVISOR: reprovar
		// durante a drenagem o faria MATAR o processo no meio das chamadas que a
		// drenagem existe justamente para preservar.
		w.Header().Set("content-type", "application/json")
		if !ok {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(body)
	})

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("content-type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = w.Write([]byte(d.RenderMetrics()))
	})

	return mux
}
