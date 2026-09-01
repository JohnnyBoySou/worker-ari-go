package orchestrator

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/lai/worker-ari/internal/ari"
	"github.com/lai/worker-ari/internal/logx"
	"github.com/lai/worker-ari/internal/metrics"
	"github.com/lai/worker-ari/internal/store"
)

func init() { logx.SetOutput(slog.NewTextHandler(io.Discard, nil)) }

type chamada struct {
	op   string
	args []string
}

type fakeAri struct {
	mu       sync.Mutex
	chamadas []chamada
	vars     map[string]string
	// erros por operação: "originate:AudioSocket" falha só o AudioSocket.
	erroOriginatePrefixo string
	erroOriginateTudo    error
	gravacao             []byte
	erroGravacao         error
}

func novoAri() *fakeAri { return &fakeAri{vars: map[string]string{}} }

func (f *fakeAri) reg(op string, args ...string) {
	f.mu.Lock()
	f.chamadas = append(f.chamadas, chamada{op, args})
	f.mu.Unlock()
}

func (f *fakeAri) ops(op string) []chamada {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []chamada
	for _, c := range f.chamadas {
		if c.op == op {
			out = append(out, c)
		}
	}
	return out
}

func (f *fakeAri) Originate(_ context.Context, p ari.OriginateParams) error {
	f.reg("originate", p.Endpoint, p.AppArgs, p.ChannelID, p.Formats)
	if f.erroOriginateTudo != nil {
		return f.erroOriginateTudo
	}
	if f.erroOriginatePrefixo != "" && len(p.Endpoint) >= len(f.erroOriginatePrefixo) &&
		p.Endpoint[:len(f.erroOriginatePrefixo)] == f.erroOriginatePrefixo {
		return &ari.HTTPError{Status: 500, Message: "audiosocket unreachable"}
	}
	return nil
}
func (f *fakeAri) Answer(_ context.Context, id string) error { f.reg("answer", id); return nil }
func (f *fakeAri) Hangup(_ context.Context, id string) error { f.reg("hangup", id); return nil }
func (f *fakeAri) MuteChannel(_ context.Context, id, d string) error {
	f.reg("mute", id, d)
	return nil
}
func (f *fakeAri) UnmuteChannel(_ context.Context, id, d string) error {
	f.reg("unmute", id, d)
	return nil
}
func (f *fakeAri) GetChannelVar(_ context.Context, _, name string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.vars[name]
}
func (f *fakeAri) SetChannelVar(_ context.Context, id, n, v string) error {
	f.reg("setvar", id, n, v)
	return nil
}
func (f *fakeAri) ListChannels(context.Context) ([]ari.Channel, error) { return nil, nil }
func (f *fakeAri) ListBridges(context.Context) ([]ari.Bridge, error)   { return nil, nil }
func (f *fakeAri) CreateBridge(_ context.Context, id, k string) error {
	f.reg("createBridge", id, k)
	return nil
}
func (f *fakeAri) AddChannel(_ context.Context, b, c string) error {
	f.reg("addChannel", b, c)
	return nil
}
func (f *fakeAri) DestroyBridge(_ context.Context, id string) error {
	f.reg("destroyBridge", id)
	return nil
}
func (f *fakeAri) RecordBridge(_ context.Context, b, n, _, _ string) error {
	f.reg("recordBridge", b, n)
	return nil
}
func (f *fakeAri) GetStoredRecording(_ context.Context, n string) ([]byte, error) {
	f.reg("getRecording", n)
	if f.erroGravacao != nil {
		return nil, f.erroGravacao
	}
	return f.gravacao, nil
}
func (f *fakeAri) DeleteStoredRecording(_ context.Context, n string) error {
	f.reg("deleteRecording", n)
	return nil
}

type fakeStore struct {
	mu       sync.Mutex
	patches  []store.Patch
	patchIDs []string
	criadas  []store.CreateCall
	timing   map[string]*store.CallTiming
	seller   *store.InboundSeller
	porCanal map[string]string
	pend     []store.PendingRecording
	erroUpd  error
}

func novoStore() *fakeStore {
	return &fakeStore{timing: map[string]*store.CallTiming{}, porCanal: map[string]string{}}
}

func (s *fakeStore) CreateCall(_ context.Context, c store.CreateCall) error {
	s.mu.Lock()
	s.criadas = append(s.criadas, c)
	s.mu.Unlock()
	return nil
}
func (s *fakeStore) UpdateCall(_ context.Context, id string, p store.Patch) error {
	s.mu.Lock()
	s.patches = append(s.patches, p)
	s.patchIDs = append(s.patchIDs, id)
	s.mu.Unlock()
	return s.erroUpd
}
func (s *fakeStore) FindCallIDByChannel(_ context.Context, ch string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.porCanal[ch], nil
}
func (s *fakeStore) FindSellerByInboundDid(context.Context, string) (*store.InboundSeller, error) {
	return s.seller, nil
}
func (s *fakeStore) GetCallTiming(_ context.Context, id string) (*store.CallTiming, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.timing[id], nil
}
func (s *fakeStore) FindPendingRecordings(context.Context, int) ([]store.PendingRecording, error) {
	return s.pend, nil
}

// statusGravados devolve, em ordem, os status que chegaram no banco.
func (s *fakeStore) statusGravados() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, p := range s.patches {
		if p.Status != nil {
			out = append(out, *p.Status)
		}
	}
	return out
}

func (s *fakeStore) ultimoPatch() (store.Patch, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.patches) == 0 {
		return store.Patch{}, false
	}
	return s.patches[len(s.patches)-1], true
}

type fakeUploader struct {
	mu       sync.Mutex
	enviados []string
	erro     error
}

func (u *fakeUploader) BucketName() string { return "bucket-teste" }
func (u *fakeUploader) Upload(_ context.Context, key string, _ []byte, _ string) error {
	u.mu.Lock()
	u.enviados = append(u.enviados, key)
	u.mu.Unlock()
	return u.erro
}

var tempoBase = time.Unix(1_700_000_000, 0)

// orq monta um orquestrador determinístico: ids sequenciais e relógio fixo.
func orq(a AriClient, s store.Store, o Options) *Orchestrator {
	// O gerador de id do teste precisa de lock: os testes de concorrência chamam
	// os handlers de várias goroutines, e um contador solto aqui viraria uma
	// corrida NO PRÓPRIO ANDAIME — que o -race acusaria como se fosse do
	// orquestrador, escondendo a corrida de verdade se houvesse uma.
	var n int
	var nmu sync.Mutex
	if o.NewID == nil {
		o.NewID = func() string {
			nmu.Lock()
			defer nmu.Unlock()
			n++
			return fmt.Sprintf("id%d", n)
		}
	}
	if o.Now == nil {
		o.Now = func() time.Time { return tempoBase }
	}
	if o.AriApp == "" {
		o.AriApp = "connect-a1"
	}
	if o.TrunkEndpoint == "" {
		o.TrunkEndpoint = "trunk"
	}
	o.Sleep = func(context.Context, time.Duration) {}
	return New(a, s, nil, nil, metrics.New(), o)
}
