package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// Port de test/http-health.test.ts.
//
// Em 18/08/2026 o /health devolvia {"status":"ok"} incondicional enquanto o
// Asterisk já não enxergava o app Stasis. Ninguém foi avisado por 3 dias. O que
// se testa aqui é a regra que fecha esse buraco: saúde é o Asterisk ter
// confirmado o app RECENTEMENTE, não o processo estar de pé.

const check = 30 * time.Second

var agora = time.Unix(1_700_000_000, 0)

func handler(aliveAt time.Time, c time.Duration, shard func() ShardStatus) http.Handler {
	return Handler(Deps{
		AppAliveAt:    func() time.Time { return aliveAt },
		AppCheck:      func() time.Duration { return c },
		RenderMetrics: func() string { return "ari_worker_active_calls 0\n" },
		Shard:         shard,
		Now:           func() time.Time { return agora },
	})
}

func get(h http.Handler, path string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", path, nil))
	return rec
}

func TestStasisHealthy(t *testing.T) {
	if !StasisHealthy(agora.Add(-5*time.Second), check, agora) {
		t.Fatal("carimbo recente é saudável")
	}
	// Estado de boot antes da primeira checagem, e também o de um worker que
	// subiu mas nunca conseguiu falar com o Asterisk. Os dois merecem 503: quem
	// nunca provou vida não deve ser considerado pronto.
	if StasisHealthy(time.Time{}, check, agora) {
		t.Fatal("nunca carimbado NÃO é saudável")
	}
	// Um ciclo perdido é blip; três seguidos são padrão.
	if !StasisHealthy(agora.Add(-2*check), check, agora) {
		t.Fatal("2 ciclos ainda é saudável")
	}
	if StasisHealthy(agora.Add(-4*check), check, agora) {
		t.Fatal("4 ciclos não é saudável")
	}
	// Watchdog desligado responde saudável: não há carimbo para julgar, e
	// reprovar todo mundo seria alarme falso.
	if !StasisHealthy(time.Time{}, 0, agora) {
		t.Fatal("watchdog desligado responde saudável")
	}
}

func TestHealthDevolve503QuandoDegradado(t *testing.T) {
	rec := get(handler(time.Time{}, check, nil), "/health")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, queria 503", rec.Code)
	}
	var body map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if body["status"] != "degraded" || body["stasis"] != false {
		t.Fatalf("corpo = %v", body)
	}
}

func TestHealthOKQuandoSaudavel(t *testing.T) {
	rec := get(handler(agora.Add(-time.Second), check, nil), "/health")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestDrenandoContinua200(t *testing.T) {
	// Reprovar durante a drenagem faria o supervisor matar o processo no meio
	// das chamadas que a drenagem existe para preservar. O estado vai no corpo,
	// para quem monitora, sem virar sentença de morte.
	rec := get(handler(agora, check, func() ShardStatus {
		return ShardStatus{ShardID: "a1", Draining: true, Accepting: false, ActiveCalls: 42}
	}), "/health")
	if rec.Code != http.StatusOK {
		t.Fatalf("drenando devia continuar 200, veio %d", rec.Code)
	}
	var body struct {
		Shard ShardStatus `json:"shard"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if !body.Shard.Draining || body.Shard.ActiveCalls != 42 {
		t.Fatalf("o estado de drenagem tem que aparecer no corpo: %+v", body.Shard)
	}
}

func TestSemShardOCorpoNaoTemOCampo(t *testing.T) {
	rec := get(handler(agora, check, nil), "/health")
	var body map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if _, tem := body["shard"]; tem {
		t.Fatal("modo single-node não deve emitir shard")
	}
}

func TestMetricsTemContentTypeDoPrometheus(t *testing.T) {
	rec := get(handler(agora, check, nil), "/metrics")
	if ct := rec.Header().Get("content-type"); ct != "text/plain; version=0.0.4; charset=utf-8" {
		t.Fatalf("content-type = %q", ct)
	}
}
