// Package shard é a identidade do shard e o registro dos shards vivos no Redis.
//
// Port de src/shard.ts. A regra antiga era "um dono único global do app Stasis";
// o sharding não a afrouxa, MULTIPLICA — cada nó tem o seu app
// (`connect-<shardId>`) e continua valendo um dono único POR APP.
package shard

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/lai/worker-ari/internal/logx"
	"github.com/redis/go-redis/v9"
)

// Só minúsculas, dígitos e hífen; sem hífen nas pontas; até 32 chars.
//
// A restrição não é estética. O shardId entra em DOIS nomes com gramáticas
// diferentes: o app Stasis (`connect-<id>`), que também é nome de contexto do
// dialplan do Asterisk, e o subject NATS (`ari.cmd.<id>`), onde o PONTO é o
// separador. Um ponto no shardId tornaria `ari.cmd.a.b` ambíguo, e maiúsculas
// quebrariam o casamento com o contexto do Asterisk.
var shardIDRe = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,30}[a-z0-9])?$`)

// Normalize apara e baixa a caixa. Vazio/ausente vira "".
func Normalize(raw string) string {
	return strings.ToLower(strings.TrimSpace(raw))
}

// Resolve valida o shardId do ambiente.
//
// Vazio = modo single-node legado (app `connect`), que mantém o deploy atual de
// pé enquanto o sharding é implantado nó a nó.
//
// Presente e inválido: ERRO, de propósito. Um shardId torto produziria um nome
// de app diferente do que o Asterisk anuncia; o Asterisk mandaria
// `Stasis(connect-a)` e o worker estaria escutando outra coisa. Nenhum evento
// chegaria, o originate por REST continuaria respondendo 200 e a chamada ficaria
// em "Chamando…" para sempre — exatamente o incidente de 18/08/2026, só que
// permanente e por configuração. Melhor não subir.
func Resolve(raw string) (string, error) {
	id := Normalize(raw)
	if id == "" {
		return "", nil
	}
	if !shardIDRe.MatchString(id) {
		return "", fmt.Errorf("SHARD_ID inválido: %q. Use [a-z0-9-], sem hífen nas pontas, até 32 caracteres", raw)
	}
	return id, nil
}

// Name compõe `base<sep><shard>`. Sem shard, devolve `base` intacto.
func Name(base, shardID, sep string) string {
	if shardID == "" {
		return base
	}
	return base + sep + shardID
}

// Snapshot é o que cada worker publica sobre si. `Accepting` é o campo que o
// produtor consulta: false = shard cheio ou drenando, não mande chamada nova.
//
// As tags JSON são o CONTRATO com o produtor (mini-back/internal/shard). Mudar
// um nome aqui sem mudar lá quebra o roteamento em silêncio: o parse falha e o
// shard some da lista de destinos.
type Snapshot struct {
	ShardID     string `json:"shardId"`
	AriApp      string `json:"ariApp"`
	Queue       string `json:"queue"`
	ActiveCalls int    `json:"activeCalls"`
	MaxCalls    int    `json:"maxCalls"`
	Accepting   bool   `json:"accepting"`
	UpdatedAt   int64  `json:"updatedAt"` // ms epoch
}

// Live: heartbeat dentro da janela. Exportada para o PRODUTOR usar a MESMA
// regra — se cada lado tiver a sua noção de "vivo", um manda comando para quem o
// outro já considera morto.
func Live(s Snapshot, now time.Time, ttl time.Duration) bool {
	return now.UnixMilli()-s.UpdatedAt < ttl.Milliseconds()
}

// Stale: velho o bastante para REMOVER do registro.
//
// A janela é muito mais larga que a de Live (ttl*4) porque as duas perguntas são
// diferentes: "posso mandar chamada?" quer ser conservadora e pular um nó lento;
// "posso APAGAR o registro?" quer ser generosa, porque apagar o registro de um nó
// que só estava lento o tira do balanceamento sem necessidade.
func Stale(s Snapshot, now time.Time, ttl time.Duration) bool {
	return now.UnixMilli()-s.UpdatedAt >= ttl.Milliseconds()*4
}

// ParseEntries decodifica o HGETALL, ignorando lixo em vez de quebrar. O registro
// é compartilhado: um JSON corrompido de um nó não pode tirar os outros do ar.
func ParseEntries(raw map[string]string) []Snapshot {
	out := make([]Snapshot, 0, len(raw))
	for field, v := range raw {
		var s Snapshot
		if err := json.Unmarshal([]byte(v), &s); err != nil || s.ShardID == "" || s.UpdatedAt == 0 {
			logx.Warn("shard.entry_invalida", "field", field)
			continue
		}
		out = append(out, s)
	}
	return out
}

type RegistryOptions struct {
	Redis       *redis.Client
	Key         string
	ShardID     string
	AriApp      string
	Queue       string
	MaxCalls    int
	Heartbeat   time.Duration
	TTL         time.Duration
	ActiveCalls func() int
	Accepting   func() bool
	Now         func() time.Time
}

// Registry publica o heartbeat deste shard num hash único (`ari:shards`),
// campo = shardId.
//
// Um hash em vez de uma chave por shard com TTL: o produtor precisa da lista
// INTEIRA a cada decisão de roteamento, e um HGETALL é uma ida ao Redis contra o
// SCAN + MGET que a outra forma exigiria. O preço é que o hash não expira
// sozinho — daí a poda em Beat.
type Registry struct {
	o    RegistryOptions
	now  func() time.Time
	stop chan struct{}
	wg   sync.WaitGroup
	once sync.Once
}

func NewRegistry(o RegistryOptions) *Registry {
	now := o.Now
	if now == nil {
		now = time.Now
	}
	return &Registry{o: o, now: now, stop: make(chan struct{})}
}

func (r *Registry) Snapshot() Snapshot {
	return Snapshot{
		ShardID:     r.o.ShardID,
		AriApp:      r.o.AriApp,
		Queue:       r.o.Queue,
		ActiveCalls: r.o.ActiveCalls(),
		MaxCalls:    r.o.MaxCalls,
		Accepting:   r.o.Accepting(),
		UpdatedAt:   r.now().UnixMilli(),
	}
}

// Beat publica o próprio estado e poda registros abandonados.
func (r *Registry) Beat(ctx context.Context) error {
	b, err := json.Marshal(r.Snapshot())
	if err != nil {
		return err
	}
	if err := r.o.Redis.HSet(ctx, r.o.Key, r.o.ShardID, string(b)).Err(); err != nil {
		return err
	}
	return r.podar(ctx)
}

func (r *Registry) podar(ctx context.Context) error {
	raw, err := r.o.Redis.HGetAll(ctx, r.o.Key).Result()
	if err != nil {
		return err
	}
	agora := r.now()
	var mortos []string
	for _, s := range ParseEntries(raw) {
		// Nunca podar a si mesmo: acabamos de escrever o heartbeat, e se o
		// relógio deste nó estiver adiantado em relação ao dos outros seríamos o
		// primeiro candidato a sumir do próprio registro.
		if s.ShardID == r.o.ShardID {
			continue
		}
		if Stale(s, agora, r.o.TTL) {
			mortos = append(mortos, s.ShardID)
		}
	}
	if len(mortos) == 0 {
		return nil
	}
	if err := r.o.Redis.HDel(ctx, r.o.Key, mortos...).Err(); err != nil {
		return err
	}
	logx.Warn("shard.podados", "shards", strings.Join(mortos, ","))
	return nil
}

func (r *Registry) Start(ctx context.Context) {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		t := time.NewTicker(r.o.Heartbeat)
		defer t.Stop()
		if err := r.Beat(ctx); err != nil {
			logx.Warn("shard.beat_falhou", "err", err.Error())
		}
		for {
			select {
			case <-ctx.Done():
				return
			case <-r.stop:
				return
			case <-t.C:
				if err := r.Beat(ctx); err != nil {
					logx.Warn("shard.beat_falhou", "err", err.Error())
				}
			}
		}
	}()
}

// Deregister sai do registro AGORA. É o primeiro passo do encerramento: o
// produtor para de escolher este shard imediatamente, em vez de esperar o TTL
// vencer e mandar chamada para um nó que já está drenando.
func (r *Registry) Deregister(ctx context.Context) {
	r.once.Do(func() { close(r.stop) })
	r.wg.Wait()
	if err := r.o.Redis.HDel(ctx, r.o.Key, r.o.ShardID).Err(); err != nil {
		logx.Warn("shard.deregister_falhou", "err", err.Error())
	}
}
