package drain

import (
	"context"
	"testing"
	"time"
)

// Relógio falso que avança a cada sleep — um teste de timeout que espera de
// verdade é um teste que ninguém roda.
type relogio struct{ t time.Duration }

func (r *relogio) now() time.Time                           { return time.Unix(0, 0).Add(r.t) }
func (r *relogio) sleep(_ context.Context, d time.Duration) { r.t += d }

func opts(r *relogio, timeout, poll time.Duration) Options {
	return Options{Timeout: timeout, Poll: poll, Now: r.now, Sleep: r.sleep}
}

func TestSemChamadaRetornaNaHora(t *testing.T) {
	r := &relogio{}
	got := Wait(context.Background(), func() int { return 0 }, opts(r, 5*time.Minute, time.Second))
	if !got.Drained || got.Decorrido != 0 {
		t.Fatalf("%+v", got)
	}
}

func TestEsperaAsChamadasCairem(t *testing.T) {
	r := &relogio{}
	ativas := 3
	got := Wait(context.Background(), func() int { v := ativas; ativas--; return v },
		opts(r, 5*time.Minute, time.Second))
	if !got.Drained || got.Decorrido != 3*time.Second {
		t.Fatalf("%+v", got)
	}
}

func TestTimeoutNaoEErroEReportaQuantasFicaram(t *testing.T) {
	// O nó vai embora de qualquer jeito: a decisão já foi tomada pelo ASG.
	// Devolver erro aqui só trocaria um encerramento ordenado por um stack
	// trace. Quantas conversas foram cortadas é informação de incidente.
	r := &relogio{}
	got := Wait(context.Background(), func() int { return 7 }, opts(r, 10*time.Second, time.Second))
	if got.Drained || got.Restantes != 7 || got.Decorrido != 10*time.Second {
		t.Fatalf("%+v", got)
	}
}

func TestNaoDormeAlemDoPrazo(t *testing.T) {
	// Com Poll maior que o que sobra, dormir o ciclo inteiro passaria do prazo
	// que o lifecycle hook do ASG está contando — e a AWS mataria a instância no
	// meio do último sono.
	r := &relogio{}
	got := Wait(context.Background(), func() int { return 1 }, opts(r, 2500*time.Millisecond, 10*time.Second))
	if got.Decorrido != 2500*time.Millisecond {
		t.Fatalf("decorrido = %v", got.Decorrido)
	}
}
