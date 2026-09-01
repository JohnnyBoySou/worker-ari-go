// Package store é a persistência do worker.
//
// Port de src/store.ts. No OUTBOUND o worker só ATUALIZA linhas de `call` (a API
// cria). No INBOUND ele CRIA a linha (a API não participa) e resolve o vendedor
// dono do DID. GetCallTiming alimenta a reidratação pós-restart.
package store

import (
	"context"
	"time"
)

// Patch é um UPDATE parcial. Ponteiros distinguem "não mexer" de "gravar o zero"
// — o equivalente do campo ausente no objeto do TS. Sem isso, um patch que só
// muda o status zeraria a duração e a causa de hangup já gravadas.
type Patch struct {
	Status        *string
	FailureReason *string
	HangupCause   *int
	HangupCauseTx *string
	SipChannelID  *string
	RecordingName *string
	RecordingURL  *string
	StartedAt     *time.Time
	EndedAt       *time.Time
	Duration      *int
}

type CreateCall struct {
	ID             string
	OrganizationID string
	AssignedTo     *string
	Direction      string // OUTBOUND | INBOUND
	Status         string
	TargetPhone    *string
	CallerDid      *string
}

type InboundSeller struct {
	OrganizationID string
	UserID         string
	SipUsername    string
}

type CallTiming struct {
	Status    string
	StartedAt *time.Time
}

// PendingRecording é uma chamada COMPLETED cuja gravação foi criada no Asterisk
// mas nunca subiu ao S3 — alvo do backfill.
type PendingRecording struct {
	CallID         string
	OrganizationID string
	RecordingName  string
}

type Store interface {
	CreateCall(ctx context.Context, c CreateCall) error
	UpdateCall(ctx context.Context, callID string, p Patch) error
	FindCallIDByChannel(ctx context.Context, channelID string) (string, error)
	FindSellerByInboundDid(ctx context.Context, did string) (*InboundSeller, error)
	GetCallTiming(ctx context.Context, callID string) (*CallTiming, error)
	FindPendingRecordings(ctx context.Context, limit int) ([]PendingRecording, error)
}

// Helpers para montar Patch sem verbosidade no chamador.
func S(v string) *string       { return &v }
func I(v int) *int             { return &v }
func T(v time.Time) *time.Time { return &v }
