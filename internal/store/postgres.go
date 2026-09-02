package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Postgres é a implementação sobre a MESMA tabela `call` do back.
//
// O worker só ATUALIZA no outbound (a API cria a linha) e CRIA no inbound. O
// schema é canônico no back — este arquivo tem que acompanhar as migrações de lá.
type Postgres struct{ pool *pgxpool.Pool }

// NewPostgres abre o pool.
//
// maxConns > 0 sobrescreve o default do pgx, que é max(4, NumCPU): num sidecar
// de 2 vCPUs são QUATRO conexões para as ~4 escritas de cada chamada, e com
// goroutine por evento (ver cmd/worker/main.go) um pico de finalizações vira
// fila no pool antes de virar fila no banco. Um `pool_max_conns` explícito na
// DATABASE_URL continua vencendo — quem escreveu a URL sabe do ambiente.
func NewPostgres(ctx context.Context, url string, maxConns int) (*Postgres, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	if maxConns > 0 && !strings.Contains(url, "pool_max_conns") {
		cfg.MaxConns = int32(maxConns)
		// Duas conexões quentes: abrir conexão no Postgres custa handshake +
		// autenticação, e pagar isso no primeiro UPDATE de um pico é somar
		// latência exatamente onde ela dói.
		cfg.MinConns = 2
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Postgres{pool: pool}, nil
}

func (p *Postgres) Close() { p.pool.Close() }

func (p *Postgres) CreateCall(ctx context.Context, c CreateCall) error {
	_, err := p.pool.Exec(ctx, `
		INSERT INTO "call" (id, organization_id, assigned_to, direction, status, target_phone, caller_did)
		VALUES ($1,$2,$3,$4,$5,$6,$7)
		ON CONFLICT (id) DO NOTHING`,
		c.ID, c.OrganizationID, c.AssignedTo, c.Direction, c.Status, c.TargetPhone, c.CallerDid)
	return err
}

// UpdateCall monta o SET só com os campos presentes. Um patch que só muda o
// status NÃO pode zerar a duração e a causa de hangup já gravadas — daí os
// ponteiros no struct Patch.
func (p *Postgres) UpdateCall(ctx context.Context, callID string, patch Patch) error {
	sets := []string{}
	args := []any{}
	add := func(col string, v any) {
		args = append(args, v)
		sets = append(sets, fmt.Sprintf("%s = $%d", col, len(args)))
	}
	if patch.Status != nil {
		add("status", *patch.Status)
	}
	if patch.FailureReason != nil {
		add("failure_reason", *patch.FailureReason)
	}
	if patch.HangupCause != nil {
		add("hangup_cause", *patch.HangupCause)
	}
	if patch.HangupCauseTx != nil {
		add("hangup_cause_txt", *patch.HangupCauseTx)
	}
	if patch.SipChannelID != nil {
		add("sip_channel_id", *patch.SipChannelID)
	}
	if patch.RecordingName != nil {
		add("recording_name", *patch.RecordingName)
	}
	if patch.RecordingURL != nil {
		add("recording_url", *patch.RecordingURL)
	}
	if patch.StartedAt != nil {
		add("started_at", *patch.StartedAt)
	}
	if patch.EndedAt != nil {
		add("ended_at", *patch.EndedAt)
	}
	if patch.Duration != nil {
		add("duration", *patch.Duration)
	}
	if len(sets) == 0 {
		return nil
	}
	args = append(args, callID)
	q := fmt.Sprintf(`UPDATE "call" SET %s WHERE id = $%d`, strings.Join(sets, ", "), len(args))
	_, err := p.pool.Exec(ctx, q, args...)
	return err
}

// FindCallByChannel resolve a chamada dona de um canal — id e estado na MESMA
// consulta. Ver o comentário de store.ChannelCall.
func (p *Postgres) FindCallByChannel(ctx context.Context, channelID string) (*ChannelCall, error) {
	var c ChannelCall
	err := p.pool.QueryRow(ctx,
		`SELECT id, status, started_at FROM "call" WHERE sip_channel_id = $1 ORDER BY created_at DESC LIMIT 1`,
		channelID).Scan(&c.CallID, &c.Status, &c.StartedAt)
	if errors.Is(err, sql.ErrNoRows) || isNoRows(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &c, nil
}

func (p *Postgres) FindSellerByInboundDid(ctx context.Context, did string) (*InboundSeller, error) {
	// Compara só DÍGITOS: o DID chega do tronco com formatações diferentes
	// (com/sem +55, com/sem 9º dígito de prefixo), e casar a string crua faria a
	// chamada de entrada cair no "sem dono" com o dono cadastrado.
	var s InboundSeller
	err := p.pool.QueryRow(ctx, `
		SELECT organization_id, user_id, sip_username
		FROM sip_agent
		WHERE regexp_replace(COALESCE(inbound_did,''), '\D', '', 'g') = regexp_replace($1, '\D', '', 'g')
		  AND COALESCE(inbound_did,'') <> ''
		LIMIT 1`, did).Scan(&s.OrganizationID, &s.UserID, &s.SipUsername)
	if isNoRows(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &s, nil
}

func (p *Postgres) GetCallTiming(ctx context.Context, callID string) (*CallTiming, error) {
	var t CallTiming
	err := p.pool.QueryRow(ctx,
		`SELECT status, started_at FROM "call" WHERE id = $1`, callID).Scan(&t.Status, &t.StartedAt)
	if isNoRows(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &t, nil
}

// FindPendingRecordings: COMPLETED com gravação criada no Asterisk mas sem key
// no S3 — o alvo do backfill.
func (p *Postgres) FindPendingRecordings(ctx context.Context, limit int) ([]PendingRecording, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT id, organization_id, recording_name
		FROM "call"
		WHERE status = 'COMPLETED' AND recording_name IS NOT NULL AND recording_url IS NULL
		ORDER BY created_at DESC
		LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PendingRecording
	for rows.Next() {
		var r PendingRecording
		if err := rows.Scan(&r.CallID, &r.OrganizationID, &r.RecordingName); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func isNoRows(err error) bool {
	return err != nil && strings.Contains(err.Error(), "no rows")
}

var _ Store = (*Postgres)(nil)
