package metrics

import (
	"strings"
	"testing"
)

func TestContadoresSeparados(t *testing.T) {
	r := New()
	r.OriginateOK()
	r.OriginateOK()
	r.OriginateFail()
	s := r.Snapshot()
	if s["originate_ok"] != 2 || s["originate_fail"] != 1 {
		t.Fatalf("%+v", s)
	}
}

func TestFinalizedRoteiaCadaDesfecho(t *testing.T) {
	r := New()
	r.Finalized("COMPLETED")
	r.Finalized("NO_ANSWER")
	r.Finalized("FAILED")
	s := r.Snapshot()
	if s["call_completed"] != 1 || s["call_no_answer"] != 1 || s["call_failed"] != 1 {
		t.Fatalf("%+v", s)
	}
}

func TestStatusDesconhecidoNaoInventaContador(t *testing.T) {
	// RINGING não é desfecho. Um status novo no futuro deve ser ignorado em
	// silêncio, não somar no balde errado.
	r := New()
	antes := r.Snapshot()
	r.Finalized("RINGING")
	r.Finalized("")
	r.Finalized("completed") // minúscula não é o contrato
	depois := r.Snapshot()
	for k, v := range antes {
		if depois[k] != v {
			t.Fatalf("contador %s mudou", k)
		}
	}
}

func TestSnapshotECopia(t *testing.T) {
	r := New()
	s := r.Snapshot()
	s["originate_ok"] = 999
	if r.Snapshot()["originate_ok"] == 999 {
		t.Fatal("snapshot tem que ser cópia")
	}
}

func TestRenderSemShardEOFormatoAntigo(t *testing.T) {
	// Compatibilidade: sem shard, a saída é a do modo single-node.
	out := New().Render(Gauges{ActiveCalls: 4}, nil)
	if !strings.Contains(out, "ari_worker_active_calls 4") {
		t.Fatal("gauge de ativas ausente")
	}
	if strings.Contains(out, "shard_info") {
		t.Fatal("sem shard não pode emitir shard_info")
	}
	if !strings.HasSuffix(out, "\n") {
		t.Fatal("parser do Prometheus rejeita a última linha sem \\n")
	}
	for _, l := range strings.Split(strings.TrimRight(out, "\n"), "\n") {
		if !strings.HasPrefix(l, "ari_worker_") {
			t.Fatalf("linha fora do prefixo: %q", l)
		}
	}
}

func TestRenderComShardDistingueSaturacaoDeDrenagem(t *testing.T) {
	// Sem accepting/draining, um nó cheio e um nó drenando são indistinguíveis
	// de "sem demanda" no gráfico de chamadas ativas.
	out := New().Render(Gauges{ActiveCalls: 1000}, &ShardGauges{ShardID: "a1", MaxCalls: 1000, Accepting: false, Draining: true})
	for _, esperado := range []string{
		"ari_worker_max_calls 1000",
		"ari_worker_accepting 0",
		"ari_worker_draining 1",
		`ari_worker_shard_info{shard="a1"} 1`,
	} {
		if !strings.Contains(out, esperado) {
			t.Fatalf("faltou %q em:\n%s", esperado, out)
		}
	}
}

func TestRenderEscapaORotulo(t *testing.T) {
	// Um rótulo com aspas quebraria o parser e derrubaria a coleta do alvo
	// inteiro, não só desta linha.
	out := New().Render(Gauges{}, &ShardGauges{ShardID: `a"1`})
	if strings.Contains(out, `shard="a"1"`) {
		t.Fatalf("rótulo não escapado: %s", out)
	}
}
