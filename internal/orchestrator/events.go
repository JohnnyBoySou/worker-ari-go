package orchestrator

import (
	"context"
	"time"

	"github.com/lai/worker-ari/internal/ari"
	"github.com/lai/worker-ari/internal/hangupcause"
	"github.com/lai/worker-ari/internal/logx"
	"github.com/lai/worker-ari/internal/store"
)

// OnStasisStart: uma perna entrou no app Stasis.
func (c *Orchestrator) OnStasisStart(ctx context.Context, ev ari.StasisStart) {
	channel := ev.Channel
	var kind, arg1 string
	if len(ev.Args) > 0 {
		kind = ev.Args[0]
	}
	if len(ev.Args) > 1 {
		arg1 = ev.Args[1]
	}

	// DIAG: registra toda entrada no Stasis. Confirma se a perna que ATENDE
	// chega mesmo quando o celular atende — se NÃO chegar, o answer não voltou do
	// trunk (problema de sinal), e sem este log isso é indistinguível de bug nosso.
	k := kind
	if k == "" {
		k = "(inbound)"
	}
	id := arg1
	if id == "" {
		id = "-"
	}
	logx.Info("stasis", "kind", k, "callId", id, "ch", channel.ID, "state", channel.State)

	switch kind {
	case "outbound":
		c.onOutboundLeg(ctx, arg1, channel)
	case "legB":
		c.onLegB(ctx, arg1, channel)
	case "inbound-seller":
		c.onInboundSeller(ctx, arg1, channel)
	case "ai-lead":
		c.onAiLead(ctx, arg1, channel)
	case "ai-agent":
		c.onAiAgent(ctx, arg1, channel)
	case "ai-media":
		c.onAiMedia(ctx, arg1, channel)
	default:
		// INVITE do trunk, sem kind: roteia por DID -> vendedor dono.
		c.onInbound(ctx, channel)
	}
}

// OUTBOUND: perna A (vendedor) entrou; cria bridge e origina o lead.
func (c *Orchestrator) onOutboundLeg(ctx context.Context, callID string, channel ari.Channel) {
	_ = c.ari.Answer(ctx, channel.ID)

	target := c.ari.GetChannelVar(ctx, channel.ID, "OUTBOUND_TARGET")
	did := c.ari.GetChannelVar(ctx, channel.ID, "OUTBOUND_DID")

	if target == "" {
		_ = c.ari.Hangup(ctx, channel.ID)
		_ = c.st.UpdateCall(ctx, callID, store.Patch{
			Status: store.S("FAILED"), FailureReason: store.S("no_target"),
		})
		c.m.Finalized("FAILED")
		c.pubStatus(ctx, callID, "FAILED")
		c.s.deleteOrg(callID)
		return
	}

	bridgeID := c.newI()
	_ = c.ari.CreateBridge(ctx, bridgeID, "mixing")
	_ = c.ari.AddChannel(ctx, bridgeID, channel.ID)

	org, _ := c.s.org(callID)
	leadChannelID := c.newI()
	vars := map[string]string{"CALL_ID": callID}
	if org != "" {
		vars["ORG"] = org
	}
	// O erro NAO pode ser engolido: sem a perna B a chamada fica so com o
	// vendedor no ar e termina NO_ANSWER, sem nada no log dizendo por que. Foi
	// assim que 91 falhas de um benchmark ficaram sem explicacao.
	if err := c.ari.Originate(ctx, ari.OriginateParams{
		ChannelID: leadChannelID,
		Endpoint:  "PJSIP/" + target + "@" + c.o.TrunkEndpoint,
		App:       c.o.AriApp,
		AppArgs:   "legB," + callID,
		CallerID:  did,
		Timeout:   c.o.RingTimeout,
		Variables: vars,
	}); err != nil {
		c.m.OriginateFail()
		logx.Error("outbound.originate_lead_falhou", "callId", callID,
			"endpoint", "PJSIP/"+target+"@"+c.o.TrunkEndpoint, "err", err.Error())
	} else {
		c.m.OriginateOK()
	}

	recName := "connect-" + callID
	c.s.setBridge(callID, bridgeRef{
		bridgeID:        bridgeID,
		sellerChannelID: channel.ID,
		leadChannelID:   leadChannelID,
		recordingName:   recName,
	})

	c.iniciarGravacao(ctx, callID, bridgeID, recName)
	_ = c.st.UpdateCall(ctx, callID, store.Patch{RecordingName: store.S(recName)})
}

// legB: perna B (lead) entrou; soma ao bridge e marca IN_PROGRESS.
func (c *Orchestrator) onLegB(ctx context.Context, callID string, channel ari.Channel) {
	ref, ok := c.s.bridge(callID)
	if !ok {
		// ref ausente = o mapa não tem essa chamada (worker reiniciou depois de
		// originar?). O answer chega mas NÃO viramos IN_PROGRESS -> o finalize
		// depois marca NO_ANSWER. Este log denuncia exatamente isso.
		logx.Warn("legB.ref_missing", "callId", callID)
		return
	}
	if err := c.ari.Answer(ctx, channel.ID); err != nil {
		logx.Error("legB.answer_falhou", "callId", callID, "err", err.Error())
	}
	if err := c.ari.AddChannel(ctx, ref.bridgeID, channel.ID); err != nil {
		logx.Error("legB.addChannel_falhou", "callId", callID, "err", err.Error())
	}
	agora := c.now()
	c.s.withRef(callID, func(r *bridgeRef, _ *state) {
		r.leadChannelID = channel.ID
		r.startedAt = &agora
	})
	if err := c.st.UpdateCall(ctx, callID, store.Patch{
		Status: store.S("IN_PROGRESS"), StartedAt: store.T(agora),
	}); err != nil {
		logx.Error("legB.in_progress_falhou", "callId", callID, "err", err.Error())
	}
	c.pubStatus(ctx, callID, "IN_PROGRESS")
	logx.Info("legB.in_progress", "callId", callID)
}

// inbound-seller: o ramal do vendedor atendeu; soma ao bridge com o chamador.
func (c *Orchestrator) onInboundSeller(ctx context.Context, callID string, channel ari.Channel) {
	ref, ok := c.s.bridge(callID)
	if !ok {
		return
	}
	_ = c.ari.Answer(ctx, channel.ID)
	_ = c.ari.AddChannel(ctx, ref.bridgeID, channel.ID)
	agora := c.now()
	c.s.withRef(callID, func(r *bridgeRef, _ *state) {
		r.sellerChannelID = channel.ID
		r.startedAt = &agora
	})
	_ = c.st.UpdateCall(ctx, callID, store.Patch{
		Status: store.S("IN_PROGRESS"), StartedAt: store.T(agora),
	})
	c.pubStatus(ctx, callID, "IN_PROGRESS")
}

// INBOUND (INVITE do trunk, sem kind): resolve o vendedor pelo DID discado,
// cria a chamada, atende, faz o bridge e toca o ramal do vendedor. Sem vendedor
// dono -> atende e encerra (comportamento antigo).
func (c *Orchestrator) onInbound(ctx context.Context, channel ari.Channel) {
	did := channel.Dialplan.Exten
	caller := channel.Caller.Number

	seller, err := c.st.FindSellerByInboundDid(ctx, did)
	if err != nil || seller == nil {
		_ = c.ari.Answer(ctx, channel.ID)
		_ = c.ari.Hangup(ctx, channel.ID)
		return
	}

	callID := newCallID()
	org := seller.OrganizationID
	c.s.setOrg(callID, org)

	var targetPhone, callerDid *string
	if caller != "" {
		targetPhone = store.S(caller)
	}
	if did != "" {
		callerDid = store.S(did)
	}
	_ = c.st.CreateCall(ctx, store.CreateCall{
		ID: callID, OrganizationID: org, AssignedTo: store.S(seller.UserID),
		Direction: "INBOUND", Status: "RINGING",
		TargetPhone: targetPhone, CallerDid: callerDid,
	})

	// Marca o canal do chamador p/ a reidratação recuperar callId+org.
	_ = c.ari.SetChannelVar(ctx, channel.ID, "CALL_ID", callID)
	_ = c.ari.SetChannelVar(ctx, channel.ID, "ORG", org)

	_ = c.ari.Answer(ctx, channel.ID)
	bridgeID := c.newI()
	_ = c.ari.CreateBridge(ctx, bridgeID, "mixing")
	_ = c.ari.AddChannel(ctx, bridgeID, channel.ID)

	recName := "connect-" + callID
	sellerChannelID := c.newI()
	c.s.setBridge(callID, bridgeRef{
		bridgeID:        bridgeID,
		sellerChannelID: sellerChannelID, // = id do canal originado abaixo
		leadChannelID:   channel.ID,
		recordingName:   recName,
	})
	c.iniciarGravacao(ctx, callID, bridgeID, recName)
	_ = c.st.UpdateCall(ctx, callID, store.Patch{
		RecordingName: store.S(recName), SipChannelID: store.S(channel.ID),
	})
	c.pubStatus(ctx, callID, "RINGING")

	if err := c.ari.Originate(ctx, ari.OriginateParams{
		ChannelID: sellerChannelID,
		Endpoint:  "PJSIP/" + seller.SipUsername,
		App:       c.o.AriApp,
		AppArgs:   "inbound-seller," + callID,
		CallerID:  caller,
		Timeout:   c.o.RingTimeout,
		Variables: map[string]string{"CALL_ID": callID, "ORG": org},
	}); err != nil {
		logx.Error("inbound.originate_ramal_falhou", "callId", callID, "err", err.Error())
		_ = c.ari.Hangup(ctx, channel.ID)
	}
}

// OnChannelDestroyed: uma perna caiu.
func (c *Orchestrator) OnChannelDestroyed(ctx context.Context, ev ari.ChannelDestroyed) {
	channelID := ev.Channel.ID
	if channelID == "" {
		return
	}

	// Lookup O(1) pelo índice reverso em vez de varrer todos os bridges.
	if callID, ref, ok := c.s.porCanal(channelID); ok {
		// Takeover em curso e quem caiu foi a perna da IA: NÃO derruba a chamada.
		// A IA já falou a despedida e encerrou sua perna; desmutamos o agente (que
		// ouvia) e ele assume o lead.
		if ref.handingOff && channelID == ref.sellerChannelID {
			// Seção crítica 1 (no TS, antes do await do unmute).
			c.s.withRef(callID, func(r *bridgeRef, s *state) {
				r.handingOff = false
				delete(s.channelIndex, channelID) // perna morta da IA sai do índice
			})
			if ref.agentChannelID != "" {
				if err := c.ari.UnmuteChannel(ctx, ref.agentChannelID, "in"); err != nil {
					logx.Error("takeover.unmute_falhou", "callId", callID, "err", err.Error())
				}
				// Seção crítica 2 (no TS, depois do await).
				c.s.withRef(callID, func(r *bridgeRef, _ *state) {
					r.sellerChannelID = r.agentChannelID // agente vira a "outra perna"
					r.agentChannelID = ""
				})
			}
			logx.Info("takeover.agent_assumed", "callId", callID)
			return
		}

		// A perna do agente supervisor é um MONITOR best-effort: entra MUDA só
		// para ouvir a IA↔lead. Se ELA cair sozinha (ex.: softphone WebRTC não
		// fecha ICE/DTLS), NÃO pode derrubar a ligação — apenas destaca o monitor.
		// Sem este guard, o softphone do supervisor caindo levava junto o lead e a
		// perna AudioSocket (BYE cause=16), matando a chamada ~2s após atender.
		if channelID == ref.agentChannelID {
			c.s.withRef(callID, func(r *bridgeRef, s *state) {
				delete(s.channelIndex, channelID)
				r.agentChannelID = ""
			})
			logx.Info("monitor.detached", "callId", callID)
			return
		}

		// Uma perna caiu (ex.: o lead desligou no celular). Derruba TAMBÉM as
		// sobreviventes — senão ficam órfãs e vivas no Stasis: o softphone do
		// vendedor nunca recebe o BYE e a tela trava "em chamada". Com 3 pernas,
		// matar só uma deixaria a terceira órfã.
		for _, ch := range []string{ref.sellerChannelID, ref.leadChannelID, ref.agentChannelID} {
			if ch != "" && ch != channelID {
				_ = c.ari.Hangup(ctx, ch)
			}
		}
		// A causa desta perna explica o desfecho: se foi o lead caindo sem atender
		// (ex.: 503/cause 34 do carrier), o finalize classifica a culpa.
		c.finalize(ctx, callID, &ref, ev.Cause, ev.CauseTxt)
		return
	}

	// Perna A que caiu ANTES do bridge (vendedor não atendeu).
	callID, err := c.st.FindCallIDByChannel(ctx, channelID)
	if err != nil || callID == "" {
		return
	}
	// Só marca NO_ANSWER se a chamada AINDA estava tocando. Se já finalizou ou
	// estava em conversa, este evento é só o eco da perna sobrevivente que NÓS
	// derrubamos no encerramento — não pode sobrescrever o status final.
	timing, _ := c.st.GetCallTiming(ctx, callID)
	outcome := hangupcause.Classify(ev.Cause)
	statusAtual := "?"
	if timing != nil {
		statusAtual = timing.Status
	}
	logx.Info("destroyed.fallback", "ch", channelID, "callId", callID,
		"statusAtual", statusAtual, "cause", ev.Cause, "failureReason", outcome.FailureReason)

	if timing != nil && timing.Status == "RINGING" {
		p := store.Patch{Status: store.S(outcome.Status), EndedAt: store.T(c.now())}
		if outcome.FailureReason != "" {
			p.FailureReason = store.S(outcome.FailureReason)
		}
		if ev.Cause != nil {
			p.HangupCause = ev.Cause
		}
		if ev.CauseTxt != "" {
			p.HangupCauseTx = store.S(ev.CauseTxt)
		}
		_ = c.st.UpdateCall(ctx, callID, p)
		c.m.Finalized(outcome.Status)
		c.pubStatus(ctx, callID, outcome.Status)
		// Perna A caiu tocando: avança a fila.
		c.notifyBackFinalized(ctx, callID, outcome.Status, outcome.FailureReason)
	}
	// IN_PROGRESS aqui é o eco de uma perna que o próprio finalize derrubou.
	// Nesse caso o finalize ainda precisa da org para publicar o status e subir a
	// gravação; apagar agora abre uma corrida e produz rec.skip/org_unknown.
	if timing == nil || timing.Status != "IN_PROGRESS" {
		c.s.deleteOrg(callID)
	}
}

// finalize fecha bridge, calcula duração e grava o status final. Idempotente.
func (c *Orchestrator) finalize(ctx context.Context, callID string, ref *bridgeRef, cause *int, causeTxt string) {
	// Captura ANTES de qualquer I/O: o ChannelDestroyed das outras pernas pode
	// rodar em paralelo enquanto destruímos a bridge. A organização pertence à
	// chamada inteira e deve sobreviver até o upload mesmo que o mapa seja limpo
	// por outro handler durante a finalização.
	org, _ := c.s.org(callID)

	// Sem ref = ou nunca houve bridge (-> NO_ANSWER), ou já finalizamos.
	// Distinguir pelo status atual evita que um retry do terminate sobrescreva um
	// COMPLETED/FAILED já gravado com NO_ANSWER.
	if ref == nil {
		timing, _ := c.st.GetCallTiming(ctx, callID)
		if timing != nil {
			switch timing.Status {
			case "COMPLETED", "FAILED", "NO_ANSWER":
				c.s.deleteOrg(callID)
				return
			}
		}
	} else {
		c.s.removerBridge(callID)
		_ = c.ari.DestroyBridge(ctx, ref.bridgeID)
	}

	endedAt := c.now()
	var duration *int
	// Atendeu (startedAt) => COMPLETED, seja qual for a causa da queda. Não
	// atendeu => classifica a causa Q.850 em status + failureReason (culpa do
	// provedor vs cliente vs número), pro front mostrar o motivo honesto.
	outcome := hangupcause.Classification{Status: "COMPLETED"}
	if ref != nil && ref.startedAt != nil {
		d := int(endedAt.Sub(*ref.startedAt).Round(time.Second).Seconds())
		if d < 0 {
			d = 0
		}
		duration = &d
	} else {
		outcome = hangupcause.Classify(cause)
	}
	c.m.Finalized(outcome.Status)

	// DIAG: mostra por que finalizou assim. startedAt=nil => não atendida (a
	// perna que atende nunca setou o início) — a raiz do "atendi mas marcou não
	// atendeu".
	var startedAtLog any
	if ref != nil && ref.startedAt != nil {
		startedAtLog = ref.startedAt.UnixMilli()
	}
	logx.Info("finalize", "callId", callID, "status", outcome.Status,
		"failureReason", nilIfEmpty(outcome.FailureReason), "cause", cause,
		"startedAt", startedAtLog, "hadRef", ref != nil)

	p := store.Patch{Status: store.S(outcome.Status), EndedAt: store.T(endedAt)}
	if duration != nil {
		p.Duration = duration
	}
	if outcome.FailureReason != "" {
		p.FailureReason = store.S(outcome.FailureReason)
	}
	if cause != nil {
		p.HangupCause = cause
	}
	if causeTxt != "" {
		p.HangupCauseTx = store.S(causeTxt)
	}
	// NÃO engolir em silêncio: se o status final não gravar, a linha fica presa
	// em IN_PROGRESS e o front nunca "desliga".
	if err := c.st.UpdateCall(ctx, callID, p); err != nil {
		logx.Error("finalize.gravar_status_falhou", "callId", callID, "err", err.Error())
	}
	c.pubStatus(ctx, callID, outcome.Status)
	c.notifyBackFinalized(ctx, callID, outcome.Status, outcome.FailureReason)

	// Sobe a gravação (fire-and-forget: não segura o handler). Se havia gravação
	// mas o upload NÃO vai rodar, loga o motivo exato — senão o "sumiço" da
	// gravação fica invisível, que era o cego do incidente.
	if ref != nil && ref.recordingName != "" {
		switch {
		case org != "" && c.up != nil:
			nome, orgUp := ref.recordingName, org
			go func() { _, _ = c.uploadRecording(context.WithoutCancel(ctx), callID, orgUp, nome) }()
		default:
			motivo := "org_unknown"
			if c.up == nil {
				motivo = "s3_disabled"
			}
			logx.Warn("rec.skip", "callId", callID, "recording", ref.recordingName,
				"reason", motivo, "hasUploader", c.up != nil, "hasOrg", org != "")
		}
	}
	// Backstop: se o lead nunca atendeu, o UUID pré-escolhido ainda está no mapa.
	c.s.limparIA(callID)
}

func (c *Orchestrator) iniciarGravacao(ctx context.Context, callID, bridgeID, nome string) {
	if err := c.ari.RecordBridge(ctx, bridgeID, nome, "wav", "overwrite"); err != nil {
		logx.Warn("rec.start_falhou", "callId", callID, "recording", nome, "err", err.Error())
	}
}
