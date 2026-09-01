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

type Consumer struct {
	nc      *nats.Conn
	cc      jetstream.ConsumeContext
	shardID string
	orq     *orchestrator.Orchestrator
	nak     time.Duration
}

func Connect(ctx context.Context, url, shardID string, orq *orchestrator.Orchestrator, nak time.Duration) (*Consumer, error) {
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
	c.cc, err = cons.Consume(func(m jetstream.Msg) {
		env, ok := Parse(m.Data())
		switch Despachar(ctx, env, ok, shardID, orq) {
		case Ack:
			_ = m.Ack()
		case Nak:
			_ = m.NakWithDelay(c.nak)
		default:
			_ = m.Term()
		}
	})
	if err != nil {
		nc.Close()
		return nil, err
	}
	logx.Info("nats.consumindo", "stream", Stream, "subject", Subject(shardID), "durable", Durable(shardID))
	return c, nil
}

// Pause para de puxar comando novo sem fechar a conexão — passo 2 da drenagem.
func (c *Consumer) Pause() {
	if c.cc != nil {
		c.cc.Stop()
	}
}

func (c *Consumer) Close() {
	c.Pause()
	if c.nc != nil {
		_ = c.nc.Drain()
	}
}
