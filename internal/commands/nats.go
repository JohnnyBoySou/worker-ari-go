// Package commands consome comandos de chamada por NATS JetStream, no subject
// `ari.cmd.<shardId>`.
//
// POR QUE NATS E NÃO KAFKA: o que trafega aqui é COMANDO endereçado a UM nó —
// caixa postal, não log. O conjunto de shards é um ASG elástico (50 -> 100 nós
// sozinho) e o número de partições de um tópico Kafka é fixo na criação:
// aumentar quebra o mapeamento chave->partição, diminuir é impossível. Um subject
// é só um nome. Volume não é argumento para nenhum dos dois — a 100k são 6 a 11
// comandos por segundo POR SHARD.
package commands

import (
	"context"
	"encoding/json"
	"hash/fnv"
	"sync"
	"time"

	"github.com/lai/worker-ari/internal/logx"
	"github.com/lai/worker-ari/internal/orchestrator"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const Stream = "ARI_CMD"

func Subject(shardID string) string { return "ari.cmd." + shardID }

// Durable é o nome do consumidor durável do shard. Estável entre restarts: é o
// que faz o JetStream REENTREGAR o que este nó não confirmou antes de cair.
func Durable(shardID string) string { return "shard-" + shardID }

// Envelope é o contrato com o produtor (mini-back/internal/commands).
type Envelope struct {
	Name    string          `json:"name"`
	CallID  string          `json:"callId"`
	ShardID string          `json:"shardId"`
	Data    json.RawMessage `json:"data"`
}

// Disposicao é o que fazer com a mensagem depois de processada.
type Disposicao string

const (
	Ack  Disposicao = "ack"
	Nak  Disposicao = "nak"
	Term Disposicao = "term"
)

func Parse(b []byte) (Envelope, bool) {
	var e Envelope
	if json.Unmarshal(b, &e) != nil || e.Name == "" || e.CallID == "" {
		return Envelope{}, false
	}
	return e, true
}

// Despachar executa um comando e diz o que fazer com a mensagem.
//
// As três disposições existem porque as três falhas são diferentes:
//
//   - Ack  — executado. Some da fila (retenção WorkQueue).
//   - Nak  — falha TRANSITÓRIA (Asterisk fora do ar, timeout). Reentregar.
//   - Term — falha PERMANENTE: envelope corrompido, comando desconhecido, ou
//     endereçado a OUTRO shard. Reentregar não muda nada, e sem o Term a mensagem
//     gira até o max_deliver ocupando o consumidor e escondendo os comandos bons
//     atrás dela.
func Despachar(ctx context.Context, env Envelope, ok bool, shardID string, o *orchestrator.Orchestrator) Disposicao {
	if !ok {
		logx.Warn("nats.envelope_invalido")
		return Term
	}
	// Endereçamento errado é bug de roteamento do PRODUTOR, não falha
	// transitória. Executar assim mesmo seria pior: este nó montaria uma chamada
	// que o produtor acha que vive em outro shard, e o terminate nunca chegaria
	// aqui.
	if env.ShardID != "" && env.ShardID != shardID {
		logx.Warn("nats.shard_errado", "callId", env.CallID, "destino", env.ShardID, "aqui", shardID)
		return Term
	}

	var err error
	switch env.Name {
	case "startOutbound":
		var cmd orchestrator.StartOutbound
		if json.Unmarshal(env.Data, &cmd) != nil {
			return Term
		}
		err = o.StartOutbound(ctx, cmd, false)
	case "startAiCall":
		var cmd orchestrator.StartAiCall
		if json.Unmarshal(env.Data, &cmd) != nil {
			return Term
		}
		err = o.StartAiCall(ctx, cmd)
	case "takeover":
		err = o.Takeover(ctx, env.CallID)
	case "terminate":
		err = o.Terminate(ctx, env.CallID)
	case "backfillRecordings":
		var cmd struct {
			Limit int `json:"limit"`
		}
		_ = json.Unmarshal(env.Data, &cmd)
		_, err = o.BackfillRecordings(ctx, cmd.Limit)
	default:
		logx.Warn("nats.comando_desconhecido", "name", env.Name, "callId", env.CallID)
		return Term
	}
	if err != nil {
		logx.Warn("nats.comando_falhou", "name", env.Name, "callId", env.CallID, "err", err.Error())
		return Nak
	}
	return Ack
}

// DefaultParticoes é a concorrência do consumo. Ver Connect.
const DefaultParticoes = 16

// capFila é o buffer de cada partição. Pequeno de propósito: com o AckWait de
// 30s, mensagem parada em fila longa seria REENTREGUE pelo JetStream enquanto
// ainda espera a vez. Encher a fila bloqueia o handler — que é a contrapressão
// correta, e só acontece se uma partição estiver de fato saturada.
const capFila = 32

type Consumer struct {
	nc      *nats.Conn
	cc      jetstream.ConsumeContext
	shardID string
	orq     *orchestrator.Orchestrator
	nak     time.Duration
	filas   []chan jetstream.Msg
	wg      sync.WaitGroup
	fechar  sync.Once
}

// particao escolhe a fila pelo callId. Hash estável (FNV-1a): a MESMA chamada
// cai sempre na mesma fila, entre restarts inclusive.
func particao(callID string, n int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(callID))
	return int(h.Sum32() % uint32(n))
}

// Connect abre a conexão, garante o stream e começa a consumir.
//
// CONCORRÊNCIA PARTICIONADA POR callId — e por que não é um pool solto.
//
// O handler de `jetstream.Consume` é chamado SEQUENCIALMENTE pela biblioteca: a
// mensagem seguinte só é entregue depois que a anterior retorna, e a reposição
// do pull acontece na mesma volta do laço. Como despachar um comando faz I/O
// (originate por REST no Asterisk + UPDATE no Postgres), UM comando lento segura
// a fila INTEIRA do shard — com o timeout de 15s do cliente ARI, um originate
// travado atrasa em 15s todos os comandos atrás dele, inclusive os `terminate`,
// que são justamente os que não podem esperar. A vazão média não é o problema
// (6 a 11 comandos/s por shard); a cauda é.
//
// Despachar cada mensagem numa goroutine solta resolveria o bloqueio e quebraria
// a ORDEM: um `terminate` publicado logo depois do `startOutbound` da mesma
// chamada poderia executar antes dele, e a chamada ficaria discando sem ninguém
// para desligá-la — o pior desfecho possível, porque o cliente vê o botão de
// desligar sem efeito.
//
// Por isso a concorrência é por PARTIÇÃO: o callId escolhe a fila, e dentro de
// uma fila tudo continua serial. Comandos da MESMA chamada preservam a ordem;
// chamadas diferentes deixam de esperar umas pelas outras.
func Connect(ctx context.Context, url, shardID string, orq *orchestrator.Orchestrator, nak time.Duration, particoes int) (*Consumer, error) {
	if particoes <= 0 {
		particoes = DefaultParticoes
	}
	nc, err := nats.Connect(url, nats.MaxReconnects(-1), nats.ReconnectWait(time.Second))
	if err != nil {
		return nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}
	// Retenção WorkQueue: a mensagem some quando é confirmada. É o que se quer de
	// um COMANDO — diferente de um evento, um startOutbound entregue duas vezes
	// toca o ramal duas vezes.
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:       Stream,
		Subjects:   []string{"ari.cmd.*"},
		Retention:  jetstream.WorkQueuePolicy,
		Duplicates: 2 * time.Minute,
		MaxAge:     time.Hour,
	}); err != nil {
		nc.Close()
		return nil, err
	}
	// Consumidor durável e ack EXPLÍCITO: o ack é nosso, não do transporte. Com
	// ack automático, um crash entre receber e executar perderia o comando em
	// silêncio — e o cliente ficaria olhando uma tela que nunca disca.
	cons, err := js.CreateOrUpdateConsumer(ctx, Stream, jetstream.ConsumerConfig{
		Durable:       Durable(shardID),
		FilterSubject: Subject(shardID),
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxDeliver:    5,
		AckWait:       30 * time.Second,
	})
	if err != nil {
		nc.Close()
		return nil, err
	}

	c := &Consumer{nc: nc, shardID: shardID, orq: orq, nak: nak}
	c.filas = make([]chan jetstream.Msg, particoes)
	for i := range c.filas {
		c.filas[i] = make(chan jetstream.Msg, capFila)
		c.wg.Add(1)
		go func(fila <-chan jetstream.Msg) {
			defer c.wg.Done()
			for m := range fila {
				c.processar(ctx, m)
			}
		}(c.filas[i])
	}

	c.cc, err = cons.Consume(func(m jetstream.Msg) {
		env, ok := Parse(m.Data())
		// Envelope ilegível não tem callId para particionar — e a disposição é
		// Term de qualquer jeito, sem I/O. Resolve aqui mesmo.
		if !ok {
			c.responder(m, Despachar(ctx, env, ok, shardID, orq))
			return
		}
		c.filas[particao(env.CallID, len(c.filas))] <- m
	})
	if err != nil {
		c.fecharFilas()
		c.wg.Wait()
		nc.Close()
		return nil, err
	}
	logx.Info("nats.consumindo", "stream", Stream, "subject", Subject(shardID),
		"durable", Durable(shardID), "particoes", particoes)
	return c, nil
}

func (c *Consumer) processar(ctx context.Context, m jetstream.Msg) {
	env, ok := Parse(m.Data())
	c.responder(m, Despachar(ctx, env, ok, c.shardID, c.orq))
}

func (c *Consumer) responder(m jetstream.Msg, d Disposicao) {
	switch d {
	case Ack:
		_ = m.Ack()
	case Nak:
		_ = m.NakWithDelay(c.nak)
	default:
		_ = m.Term()
	}
}

// Pause para de puxar comando novo sem fechar a conexão — passo 2 da drenagem.
// O que já está nas filas SEGUE sendo processado: são comandos aceitos, e a
// drenagem existe para terminar o que foi aceito, não para descartá-lo.
func (c *Consumer) Pause() {
	if c.cc != nil {
		c.cc.Stop()
	}
}

func (c *Consumer) fecharFilas() {
	c.fechar.Do(func() {
		for _, f := range c.filas {
			close(f)
		}
	})
}

func (c *Consumer) Close() {
	c.Pause()
	c.fecharFilas()
	c.wg.Wait()
	if c.nc != nil {
		_ = c.nc.Drain()
	}
}
