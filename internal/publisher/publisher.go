// Package publisher publica mudanças de estado de chamada num canal Redis
// pub/sub, por organização (`<base>:<organizationId>`).
//
// Port de src/publisher.ts. A API assina o canal da org e repassa aos navegadores
// por SSE — é o que substituiu o polling na tela de chamadas ativas.
//
// Fire-and-forget: uma falha de publish NUNCA pode atrapalhar o controle da
// chamada. O estado de verdade está no banco; isto aqui é só o aviso ao vivo.
package publisher

import (
	"context"
	"encoding/json"
	"time"

	"github.com/lai/worker-ari/internal/logx"
	"github.com/redis/go-redis/v9"
)

type Event struct {
	Type   string `json:"type"` // sempre "call.update"
	CallID string `json:"callId"`
	Status string `json:"status"` // RINGING | IN_PROGRESS | COMPLETED | FAILED | NO_ANSWER
	At     string `json:"at"`     // ISO
}

type Publisher struct {
	rdb  *redis.Client
	base string
}

func New(rdb *redis.Client, base string) *Publisher {
	return &Publisher{rdb: rdb, base: base}
}

func (p *Publisher) Publish(ctx context.Context, organizationID, callID, status string) {
	if p == nil || p.rdb == nil {
		return
	}
	b, err := json.Marshal(Event{
		Type: "call.update", CallID: callID, Status: status,
		At: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return
	}
	if err := p.rdb.Publish(ctx, p.base+":"+organizationID, string(b)).Err(); err != nil {
		logx.Warn("publisher.publish_falhou", "callId", callID, "err", err.Error())
	}
}
