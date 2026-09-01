// Package metrics são os contadores in-memory do worker, sem dependência
// externa. Port de src/metrics.ts.
//
// São monotônicos e reiniciam a cada restart: isto é observabilidade operacional
// (taxa de falha de originate, distribuição de desfecho), não billing. O gauge de
// chamadas ATIVAS não vive aqui — o orquestrador já rastreia isso, e o /metrics
// compõe o valor a partir dele (fonte única, sem bookkeeping duplo).
package metrics

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

type Registry struct {
	mu       sync.Mutex
	counters map[string]int64
}

var ordem = []string{"originate_ok", "originate_fail", "call_completed", "call_no_answer", "call_failed"}

func New() *Registry {
	c := map[string]int64{}
	for _, k := range ordem {
		c[k] = 0
	}
	return &Registry{counters: c}
}

func (r *Registry) inc(k string) {
	r.mu.Lock()
	r.counters[k]++
	r.mu.Unlock()
}

func (r *Registry) OriginateOK()   { r.inc("originate_ok") }
func (r *Registry) OriginateFail() { r.inc("originate_fail") }

// Finalized contabiliza o desfecho final. Status desconhecido é IGNORADO — não
// inventa contador nem soma no balde errado: métrica errada é pior que métrica
// faltando, porque ninguém desconfia dela.
func (r *Registry) Finalized(status string) {
	switch status {
	case "COMPLETED":
		r.inc("call_completed")
	case "NO_ANSWER":
		r.inc("call_no_answer")
	case "FAILED":
		r.inc("call_failed")
	}
}

func (r *Registry) Snapshot() map[string]int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]int64, len(r.counters))
	for k, v := range r.counters {
		out[k] = v
	}
	return out
}

// ShardGauges são os gauges do modo shardeado. Omitidos, a saída é a de antes.
type ShardGauges struct {
	ShardID   string
	MaxCalls  int
	Accepting bool
	Draining  bool
}

// Render devolve o corpo do /metrics no formato de exposição do Prometheus.
//
// `shard` é opcional: sem ele a saída é a do modo single-node. Com ele, entram
// `accepting` e `draining` — sem essas duas, SATURAÇÃO e DRENAGEM ficam
// indistinguíveis de "sem demanda" no gráfico de chamadas ativas.
//
// O shardId sai como info-metric rotulado, não como rótulo em toda métrica:
// identidade de alvo é trabalho do service discovery do Prometheus, e duplicá-la
// em cada linha só engorda a série temporal.
func (r *Registry) Render(activeCalls int, shard *ShardGauges) string {
	var b strings.Builder
	fmt.Fprintf(&b, "ari_worker_active_calls %d\n", activeCalls)
	snap := r.Snapshot()
	chaves := make([]string, 0, len(snap))
	for k := range snap {
		chaves = append(chaves, k)
	}
	sort.Strings(chaves)
	for _, k := range chaves {
		fmt.Fprintf(&b, "ari_worker_%s %d\n", k, snap[k])
	}
	if shard != nil {
		fmt.Fprintf(&b, "ari_worker_max_calls %d\n", shard.MaxCalls)
		fmt.Fprintf(&b, "ari_worker_accepting %d\n", boolInt(shard.Accepting))
		fmt.Fprintf(&b, "ari_worker_draining %d\n", boolInt(shard.Draining))
		fmt.Fprintf(&b, "ari_worker_shard_info{shard=%q} 1\n", escapeLabel(shard.ShardID))
	}
	return b.String()
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

// O shardId já é validado na entrada (shard.Resolve só aceita [a-z0-9-]), mas o
// /metrics não pode depender disso: um valor com aspas ou barra quebraria o
// parser do Prometheus e derrubaria a coleta do alvo INTEIRO, não só desta linha.
func escapeLabel(v string) string {
	v = strings.ReplaceAll(v, `\`, `\\`)
	v = strings.ReplaceAll(v, `"`, `\"`)
	return strings.ReplaceAll(v, "\n", `\n`)
}
