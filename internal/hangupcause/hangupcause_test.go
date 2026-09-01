package hangupcause

import "testing"

func c(v int) *int { return &v }

// Port do test/hangup-cause.test.ts. A tabela é o contrato com o front
// (CALL_FAILURE_LABELS): mudar um failureReason aqui sem mudar lá faz a tela
// mostrar o desfecho errado — que é pior que não mostrar nada, porque ninguém
// desconfia.
func TestClassify(t *testing.T) {
	casos := []struct {
		nome   string
		cause  *int
		status string
		reason string
	}{
		{"congestionamento do carrier (503)", c(34), "FAILED", "carrier_congestion"},
		{"congestionamento de equipamento", c(42), "FAILED", "carrier_congestion"},
		{"rede fora do ar", c(38), "FAILED", "carrier_unavailable"},
		{"falha temporária", c(41), "FAILED", "carrier_unavailable"},
		{"número inexistente", c(1), "FAILED", "invalid_number"},
		{"número mudou", c(22), "FAILED", "invalid_number"},
		{"ocupado", c(17), "NO_ANSWER", "busy"},
		{"rejeitada", c(21), "NO_ANSWER", "rejected"},
		{"desligou no toque", c(16), "NO_ANSWER", "no_answer"},
		{"não atendeu", c(19), "NO_ANSWER", "no_answer"},
	}
	for _, tc := range casos {
		t.Run(tc.nome, func(t *testing.T) {
			got := Classify(tc.cause)
			if got.Status != tc.status || got.FailureReason != tc.reason {
				t.Fatalf("cause=%d -> %+v, queria {%s %s}", *tc.cause, got, tc.status, tc.reason)
			}
		})
	}
}

func TestSemCausaNaoInventaFalha(t *testing.T) {
	// terminate manual chega sem causa. Inventar um failureReason aqui faria o
	// front acusar falha do carrier num encerramento normal do operador.
	for _, cause := range []*int{nil, c(0), c(999)} {
		got := Classify(cause)
		if got.Status != "NO_ANSWER" || got.FailureReason != "" {
			t.Fatalf("cause=%v devia ser NO_ANSWER puro, veio %+v", cause, got)
		}
	}
}

func TestSeparacaoCulpaProvedorVsCliente(t *testing.T) {
	// A distinção que justifica a tabela existir: congestionamento do carrier é
	// FAILED (nossa/provedor), ocupado é NO_ANSWER (do cliente). Colapsar os dois
	// em NO_ANSWER esconderia degradação do tronco atrás de "ninguém atendeu".
	if Classify(c(34)).Status != "FAILED" {
		t.Fatal("congestionamento tem que ser FAILED")
	}
	if Classify(c(17)).Status != "NO_ANSWER" {
		t.Fatal("ocupado tem que ser NO_ANSWER")
	}
}
