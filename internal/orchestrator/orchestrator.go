// Package orchestrator é a orquestração de chamadas via ARI.
//
// Port fiel de src/orchestrator.ts. A diferença estrutural em relação ao
// original está em state.go: o TS é single-threaded e os `Map` não precisavam de
// lock; aqui as goroutines são concorrentes. Fora isso, o fluxo é o mesmo, linha
// a linha — cada guarda deste arquivo existe porque um incidente aconteceu.
package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/lai/worker-ari/internal/ari"
	"github.com/lai/worker-ari/internal/hangupcause"
	"github.com/lai/worker-ari/internal/logx"
	"github.com/lai/worker-ari/internal/metrics"
	"github.com/lai/worker-ari/internal/publisher"
	"github.com/lai/worker-ari/internal/store"
)

// Uploader sobe a gravação para o S3. Interface para o orquestrador não depender
// do SDK da AWS nos testes.
type Uploader interface {
	BucketName() string
	Upload(ctx context.Context, key string, body []byte, contentType string) error
}

type Options struct {
	AriApp        string
	TrunkEndpoint string
	RingTimeout   int
	// MaxCalls <= 0 desliga a admissão (modo single-node legado).
	MaxCalls                  int
	RecordingDownloadAttempts int
	AIAudiosocketAddr         string
	VoiceTalkControlURL       string
	VoiceTalkToken            string
	BackURL                   string
	WorkerAPIKey              string
	HTTPClient                *http.Client
	Now                       func() time.Time
	NewID                     func() string
	Sleep                     func(context.Context, time.Duration)
}

type Orchestrator struct {
	ari  AriClient
	st   store.Store
	pub  *publisher.Publisher
	up   Uploader
	m    *metrics.Registry
	o    Options
	s    *state
	now  func() time.Time
	newI func() string
}

// AriClient é o subconjunto do client que o orquestrador usa. Interface para os
// testes exercitarem o fluxo sem Asterisk.
type AriClient interface {
	Originate(ctx context.Context, p ari.OriginateParams) error
	Answer(ctx context.Context, channelID string) error
	Hangup(ctx context.Context, channelID string) error
	MuteChannel(ctx context.Context, channelID, direction string) error
	UnmuteChannel(ctx context.Context, channelID, direction string) error
	GetChannelVar(ctx context.Context, channelID, variable string) string
	SetChannelVar(ctx context.Context, channelID, variable, value string) error
	ListChannels(ctx context.Context) ([]ari.Channel, error)
	ListBridges(ctx context.Context) ([]ari.Bridge, error)
	CreateBridge(ctx context.Context, bridgeID, kind string) error
	AddChannel(ctx context.Context, bridgeID, channelID string) error
	DestroyBridge(ctx context.Context, bridgeID string) error
	RecordBridge(ctx context.Context, bridgeID, name, format, ifExists string) error
	GetStoredRecording(ctx context.Context, name string) ([]byte, error)
	DeleteStoredRecording(ctx context.Context, name string) error
}

func New(a AriClient, st store.Store, pub *publisher.Publisher, up Uploader, m *metrics.Registry, o Options) *Orchestrator {
	now := o.Now
	if now == nil {
		now = time.Now
	}
	newID := o.NewID
	if newID == nil {
		newID = func() string { return uuid.NewString() }
	}
	if o.HTTPClient == nil {
		o.HTTPClient = &http.Client{Timeout: 10 * time.Second}
	}
	if o.Sleep == nil {
		o.Sleep = func(ctx context.Context, d time.Duration) {
			t := time.NewTimer(d)
			defer t.Stop()
			select {
			case <-ctx.Done():
			case <-t.C:
			}
		}
	}
	return &Orchestrator{ari: a, st: st, pub: pub, up: up, m: m, o: o, s: newState(), now: now, newI: newID}
}

// ActiveCalls é o gauge de chamadas ativas. `callOrg` é povoado no início do
// startOutbound e limpo em TODO desfecho terminal, então seu tamanho é o gauge
// sem bookkeeping extra.
func (c *Orchestrator) ActiveCalls() int { return c.s.activeCalls() }

func newCallID() string { return "call_" + uuid.NewString() }

// pub publica a transição no canal da org (se a org for conhecida).
func (c *Orchestrator) pubStatus(ctx context.Context, callID, status string) {
	if org, ok := c.s.org(callID); ok {
		c.pub.Publish(ctx, org, callID, status)
	}
}

// notifyBackFinalized avisa o back para AVANÇAR A FILA conforme o desfecho.
// Fire-and-forget: falha aqui não pode travar o encerramento da chamada.
func (c *Orchestrator) notifyBackFinalized(ctx context.Context, callID, status, failureReason string) {
	if c.o.BackURL == "" || c.o.WorkerAPIKey == "" {
		return
	}
	body, _ := json.Marshal(map[string]any{
		"callId": callID, "status": status,
		"failureReason": nilIfEmpty(failureReason),
	})
	go func() {
		req, err := http.NewRequest(http.MethodPost, c.o.BackURL+"/v1/internal/call-finalized", bytes.NewReader(body))
		if err != nil {
			return
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("x-api-key", c.o.WorkerAPIKey)
		res, err := c.o.HTTPClient.Do(req)
		if err != nil {
			logx.Warn("notify_back_failed", "callId", callID, "err", err.Error())
			return
		}
		_ = res.Body.Close()
	}()
}

func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// noTeto: este nó já está no teto de chamadas simultâneas?
//
// BACKSTOP, não a defesa principal. Quem deveria evitar isto é o produtor, que
// escolhe um shard com folga no registro. O que sobra aqui é a CORRIDA: entre o
// produtor ler o registro e o comando chegar, outros comandos podem ter enchido
// o nó. Numa janela de heartbeat de 5s e ~6-11 CPS por shard, é real.
//
// Vale recusar porque a falha no teto não é graciosa: passar da capacidade de
// mídia não derruba a chamada nova, degrada o áudio de TODAS as do nó.
func (c *Orchestrator) noTeto() bool {
	return c.o.MaxCalls > 0 && c.ActiveCalls() >= c.o.MaxCalls
}

// recusarPorTeto marca FAILED por saturação.
//
// NÃO devolve erro: o retry cairia no MESMO shard, que continua cheio — só
// gastaria tentativa para falhar igual. O failureReason é distinto
// (`shard_at_capacity`) de propósito: é o sinal de que o dimensionamento ou o
// balanceamento do produtor está errado, e precisa ser separável de falha de
// originate no gráfico.
func (c *Orchestrator) recusarPorTeto(ctx context.Context, callID, org string) {
	logx.Warn("admissao.shard_no_teto", "callId", callID, "ativas", c.ActiveCalls(), "maxCalls", c.o.MaxCalls)
	c.s.setOrg(callID, org) // só para o pub achar a org; limpo abaixo
	_ = c.st.UpdateCall(ctx, callID, store.Patch{
		Status: store.S("FAILED"), FailureReason: store.S("shard_at_capacity"),
	})
	c.m.Finalized("FAILED")
	c.pubStatus(ctx, callID, "FAILED")
	c.s.deleteOrg(callID)
}

// --- Comando: iniciar chamada de saída (perna A = vendedor) ------------------
// A linha `call` (RINGING) já existe (criada pela API). Aqui só originamos a
// perna A no Stasis; o resto flui pelos eventos.

type StartOutbound struct {
	CallID         string `json:"callId"`
	OrganizationID string `json:"organizationId"`
	SellerSipUser  string `json:"sellerSipUsername"`
	CallerDid      string `json:"callerDid"`
	TrunkTarget    string `json:"trunkTarget"`
}

func (c *Orchestrator) StartOutbound(ctx context.Context, cmd StartOutbound, ultimaTentativa bool) error {
	callID := cmd.CallID
	// channelId DETERMINÍSTICO por callId: torna o originate idempotente sob
	// retry. Se um retry reprocessar depois de o originate anterior já ter criado
	// o canal, re-originar com o MESMO id devolve 409 do ARI — tratamos como "já
	// originado" em vez de tocar um segundo ramal do vendedor.
	sellerChannelID := callID + "-A"
	if c.noTeto() {
		c.recusarPorTeto(ctx, callID, cmd.OrganizationID)
		return nil
	}
	c.s.setOrg(callID, cmd.OrganizationID)

	err := c.ari.Originate(ctx, ari.OriginateParams{
		ChannelID: sellerChannelID,
		Endpoint:  "PJSIP/" + cmd.SellerSipUser,
		App:       c.o.AriApp,
		AppArgs:   "outbound," + callID,
		CallerID:  cmd.CallerDid,
		Timeout:   c.o.RingTimeout,
		Variables: map[string]string{
			"CALL_ID":                             callID,
			"ORG":                                 cmd.OrganizationID, // p/ reidratação
			"OUTBOUND_TARGET":                     cmd.TrunkTarget,
			"OUTBOUND_DID":                        cmd.CallerDid,
			"PJSIP_HEADER(add,X-Connect-Call-Id)": callID,
		},
	})
	switch {
	case err == nil:
		c.m.OriginateOK()
	// 409 = canal já existe: o originate de uma tentativa anterior venceu. Segue
	// idempotente (regrava sipChannelId, republica RINGING) em vez de marcar
	// FAILED ou originar de novo.
	case ari.IsStatus(err, http.StatusConflict):
		logx.Warn("startOutbound.originate_409", "callId", callID)
	default:
		c.m.OriginateFail()
		// Só marca FAILED na ÚLTIMA tentativa — senão o retry encontraria a linha
		// já FAILED e o estado divergiria do RINGING real que o próximo originate
		// produz.
		if ultimaTentativa {
			_ = c.st.UpdateCall(ctx, callID, store.Patch{
				Status: store.S("FAILED"), FailureReason: store.S("originate_error"),
			})
			c.m.Finalized("FAILED")
			c.pubStatus(ctx, callID, "FAILED")
			c.s.deleteOrg(callID)
		}
		return err
	}

	_ = c.st.UpdateCall(ctx, callID, store.Patch{SipChannelID: store.S(sellerChannelID)})
	// Perna A tocando de fato: notifica os monitores (a linha RINGING já existe).
	c.pubStatus(ctx, callID, "RINGING")
	return nil
}

// --- Comando: encerrar chamada ----------------------------------------------

func (c *Orchestrator) Terminate(ctx context.Context, callID string) error {
	ref, ok := c.s.bridge(callID)
	if ok {
		for _, ch := range []string{ref.sellerChannelID, ref.leadChannelID, ref.agentChannelID} {
			if ch != "" {
				_ = c.ari.Hangup(ctx, ch)
			}
		}
		c.finalize(ctx, callID, &ref, nil, "")
		return nil
	}
	c.finalize(ctx, callID, nil, nil, "")
	return nil
}

// --- Comando: transferir para humano (assumir) -------------------------------
// Pede ao voice-talk que a IA fale a frase de passagem e encerre a própria perna
// AudioSocket. Quando essa perna cair, o onChannelDestroyed (ramo handingOff)
// desmuta o agente, que assume o lead. O desmute NÃO acontece aqui para não
// abrir o microfone do agente antes de a IA terminar a despedida.
func (c *Orchestrator) Takeover(ctx context.Context, callID string) error {
	ref, ok := c.s.bridge(callID)
	if !ok {
		logx.Warn("takeover.ref_missing", "callId", callID)
		return nil
	}
	if ref.aiMediaUUID == "" {
		logx.Warn("takeover.no_uuid", "callId", callID)
		return nil
	}
	if ref.agentChannelID == "" {
		logx.Warn("takeover.no_agent", "callId", callID)
		return nil
	}
	if c.o.VoiceTalkControlURL == "" {
		logx.Warn("takeover.no_control_url", "callId", callID)
		return fmt.Errorf("voice_control_unset")
	}

	c.s.withRef(callID, func(r *bridgeRef, _ *state) { r.handingOff = true })

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		fmt.Sprintf("%s/calls/%s/handoff", c.o.VoiceTalkControlURL, ref.aiMediaUUID),
		bytes.NewReader([]byte("{}")))
	if err == nil {
		req.Header.Set("Content-Type", "application/json")
		if c.o.VoiceTalkToken != "" {
			req.Header.Set("Authorization", "Bearer "+c.o.VoiceTalkToken)
		}
		var res *http.Response
		res, err = c.o.HTTPClient.Do(req)
		if err == nil {
			_ = res.Body.Close()
			if res.StatusCode >= 400 {
				err = fmt.Errorf("handoff HTTP %d", res.StatusCode)
			}
		}
	}
	if err != nil {
		// Falhou o pedido: reverte o flag (a perna da IA não vai cair sozinha)
		// para não capturar por engano um hangup real como se fosse takeover.
		c.s.withRef(callID, func(r *bridgeRef, _ *state) { r.handingOff = false })
		logx.Error("takeover.handoff_failed", "callId", callID, "err", err.Error())
		return err
	}
	logx.Info("takeover.requested", "callId", callID)
	return nil
}

var _ = hangupcause.Classify
