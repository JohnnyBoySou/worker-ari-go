// Package drain espera as chamadas em curso terminarem antes de encerrar.
//
// Port de src/drain.ts. O encerramento antigo fechava tudo e saía na hora — num
// VPS fixo era aceitável, porque deploy era raro. Num ASG de ~50-100 nós o
// scale-in acontece sozinho o dia inteiro, e sair na hora DERRUBA toda chamada
// viva do nó: com ~1000 chamadas por nó, cada scale-in cortaria mil conversas.
//
// A ordem correta do encerramento (ver cmd/worker) é:
//  1. sair do registro de shards -> o produtor para de mandar chamada NOVA
//  2. parar de consumir comandos -> para de puxar comando novo
//  3. ESPERAR as ativas caírem   -> é o que este pacote faz
//  4. fechar ARI/Redis/Postgres e sair
//
// O timeout tem que ser MAIOR que a duração típica de uma chamada, senão o passo
// 3 vira teatro. O `terminating:wait` do lifecycle hook do ASG precisa ser >= a
// ele, senão a AWS mata a instância antes de o worker terminar.
package drain

import (
	"context"
	"time"
)

type Options struct {
	Timeout    time.Duration
	Poll       time.Duration
	Now        func() time.Time
	Sleep      func(context.Context, time.Duration)
	OnProgress func(restantes int, decorrido time.Duration)
}

type Result struct {
	Drained   bool // true = chegou a zero; false = estourou o timeout
	Restantes int
	Decorrido time.Duration
}

// Wait espera activeCalls chegar a zero.
//
// Devolve o resultado em vez de erro: estourar o timeout NÃO é falha, é uma
// decisão operacional já tomada (o nó vai embora de qualquer jeito). Quem chama
// decide o que logar — e o número de chamadas sacrificadas é informação de
// incidente, então precisa sair no log.
func Wait(ctx context.Context, activeCalls func() int, o Options) Result {
	now := o.Now
	if now == nil {
		now = time.Now
	}
	sleep := o.Sleep
	if sleep == nil {
		sleep = func(ctx context.Context, d time.Duration) {
			t := time.NewTimer(d)
			defer t.Stop()
			select {
			case <-ctx.Done():
			case <-t.C:
			}
		}
	}
	inicio := now()
	for {
		restantes := activeCalls()
		decorrido := now().Sub(inicio)
		if restantes <= 0 {
			return Result{Drained: true, Decorrido: decorrido}
		}
		if decorrido >= o.Timeout {
			return Result{Restantes: restantes, Decorrido: decorrido}
		}
		if o.OnProgress != nil {
			o.OnProgress(restantes, decorrido)
		}
		// Não dormir além do que resta do prazo: com Poll grande, esperar o ciclo
		// inteiro passaria do timeout que o lifecycle hook está contando.
		espera := o.Poll
		if resta := o.Timeout - decorrido; espera > resta {
			espera = resta
		}
		sleep(ctx, espera)
	}
}
