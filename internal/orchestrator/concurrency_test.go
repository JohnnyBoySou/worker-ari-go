package orchestrator

import (
	"fmt"
	"sync"
	"testing"

	"github.com/lai/worker-ari/internal/ari"
)

// O TESTE QUE JUSTIFICA state.go.
//
// O worker em TypeScript é single-threaded: o event loop serializa os handlers e
// os `Map` só podiam ser observados entre `await`s — por isso o original não tem
// um único lock. Aqui, o consumidor de comandos e os eventos do ARI rodam em
// goroutines CONCORRENTES, e acesso concorrente a map em Go não é um bug sutil
// que aparece em produção: é panic do runtime, na hora.
//
// Rodar com -race é o que dá sentido a estes testes (ver `go test -race ./...`,
// que é o alvo do CI). Sem o -race eles passariam mesmo com o estado desprotegido.
func TestSemCorridaEntreComandosEEventos(t *testing.T) {
	a, s := novoAri(), novoStore()
	a.vars["OUTBOUND_TARGET"] = "1842"
	var n int
	var mu sync.Mutex
	c := orq(a, s, Options{NewID: func() string {
		mu.Lock()
		defer mu.Unlock()
		n++
		return fmt.Sprintf("id%d", n)
	}})

	const chamadas = 40
	var wg sync.WaitGroup

	// Comandos chegando pelo NATS...
	for i := 0; i < chamadas; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("call_%d", i)
			_ = c.StartOutbound(ctx, cmdOut(id), false)
			c.OnStasisStart(ctx, ari.StasisStart{
				Args: []string{"outbound", id}, Channel: ari.Channel{ID: id + "-A"}})
		}(i)
	}

	// ...enquanto eventos do ARI chegam pelo WebSocket, para as MESMAS chamadas.
	for i := 0; i < chamadas; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("call_%d", i)
			c.OnStasisStart(ctx, ari.StasisStart{
				Args: []string{"legB", id}, Channel: ari.Channel{ID: id + "-B"}})
			c.OnChannelDestroyed(ctx, ari.ChannelDestroyed{Channel: ari.Channel{ID: id + "-B"}})
		}(i)
	}

	// ...e a reidratação varrendo o estado ao mesmo tempo (um blip de WebSocket
	// durante o pico é exatamente quando isso acontece).
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.Rehydrate(ctx)
			_ = c.ActiveCalls()
		}()
	}

	wg.Wait()
}

// A atomicidade que o event loop dava de graça.
func TestBridgeEIndiceNuncaDivergem(t *testing.T) {
	// No TS, `bridges.set` e `indexBridge` são coladas (sem await entre elas),
	// então nenhum handler jamais observou uma sem a outra. Em Go isso só
	// continua verdade porque são a MESMA seção crítica — daí setBridge e
	// removerBridge fazerem as duas coisas sob um lock só.
	//
	// Se divergissem, o onChannelDestroyed acharia o canal no índice mas não o
	// ref (ou o contrário) e a chamada nunca seria finalizada.
	a, s := novoAri(), novoStore()
	c := orq(a, s, Options{})

	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		id := fmt.Sprintf("call_%d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.s.setBridge(id, bridgeRef{
				bridgeID: "b" + id, sellerChannelID: id + "-A", leadChannelID: id + "-B",
			})
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Lê pelo índice: ou acha os dois, ou não acha nada. Nunca meio.
			if callID, ref, ok := c.s.porCanal(id + "-A"); ok {
				if callID != id || ref.bridgeID != "b"+id {
					t.Errorf("índice aponta para ref errado: %s -> %+v", callID, ref)
				}
			}
		}()
	}
	wg.Wait()

	// Depois de remover, nem ref nem índice sobrevivem.
	for i := 0; i < 200; i++ {
		id := fmt.Sprintf("call_%d", i)
		c.s.removerBridge(id)
	}
	for i := 0; i < 200; i++ {
		id := fmt.Sprintf("call_%d", i)
		if _, _, ok := c.s.porCanal(id + "-A"); ok {
			t.Fatalf("índice vazou para %s — entrada morta que nunca é limpa", id)
		}
	}
}

func TestGaugeDeAtivasNaoVazaSobConcorrencia(t *testing.T) {
	// callOrg é o gauge. Um vazamento faz o nó se declarar mais cheio do que
	// está e, no limite, parar de aceitar chamada para sempre — sem nada acusar,
	// porque a métrica que denunciaria É a que vazou.
	a, s := novoAri(), novoStore()
	a.vars["OUTBOUND_TARGET"] = "1842"
	c := orq(a, s, Options{})

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("call_%d", i)
			_ = c.StartOutbound(ctx, cmdOut(id), false)
			c.OnStasisStart(ctx, ari.StasisStart{
				Args: []string{"outbound", id}, Channel: ari.Channel{ID: id + "-A"}})
			_ = c.Terminate(ctx, id)
		}(i)
	}
	wg.Wait()

	if got := c.ActiveCalls(); got != 0 {
		t.Fatalf("gauge vazou: %d chamadas ativas depois de encerrar todas", got)
	}
}
