package orchestrator

import (
	"context"
	"fmt"

	"github.com/lai/worker-ari/internal/ari"
	"github.com/lai/worker-ari/internal/logx"
	"github.com/lai/worker-ari/internal/store"
)

// O caminho de IA está ISOLADO neste arquivo, não removido.
//
// A fase atual do plano (PLANO-VOZ-HUMANA.md) é só voz humana, mas apagar isto
// jogaria fora a lógica de supervisão/takeover — que é cara de reconstruir e
// cujos guards existem por incidentes reais (o softphone do supervisor caindo e
// matando a chamada; o restart no ring perdendo a voz pré-registrada). Isolar dá
// o mesmo foco sem o retrabalho.

type StartAiCall struct {
	CallID         string `json:"callId"`
	OrganizationID string `json:"organizationId"`
	TrunkTarget    string `json:"trunkTarget"`
	CallerDid      string `json:"callerDid"`
	// AgentSipUser (opcional): ramal WebRTC do agente que vai OUVIR a IA↔lead.
	AgentSipUser string `json:"agentSipUsername"`
	// AudioSocketID: UUID escolhido pela API, que amarra a voz-por-organização
	// pré-registrada no voice-talk à perna que vai subir.
	AudioSocketID string `json:"audioSocketId"`
}

func (c *Orchestrator) StartAiCall(ctx context.Context, cmd StartAiCall) error {
	callID := cmd.CallID
	if c.noTeto() {
		c.recusarPorTeto(ctx, callID, cmd.OrganizationID)
		return nil
	}
	leadChannelID := c.newI()
	c.s.setOrg(callID, cmd.OrganizationID)
	if cmd.AgentSipUser != "" {
		c.s.setAIAgent(callID, cmd.AgentSipUser)
	}
	if cmd.AudioSocketID != "" {
		c.s.setAIVoiceUUID(callID, cmd.AudioSocketID)
	}

	vars := map[string]string{"CALL_ID": callID, "ORG": cmd.OrganizationID}
	if cmd.AudioSocketID != "" {
		// Grava o audioSocketId na PRÓPRIA variável de canal do lead, não só no
		// mapa em memória. O ring inteiro (~30-45s) roda ANTES de o handler
		// ai-lead consumir esse UUID; se o worker reiniciar nesse meio-tempo, o
		// mapa se perde mas a variável de canal SOBREVIVE no Asterisk. Sem isto,
		// um restart no ring faz o handler gerar um UUID aleatório e a ligação
		// sobe sem a voz/script pré-registrados pela API.
		vars["AI_MEDIA_UUID"] = cmd.AudioSocketID
	}

	if err := c.ari.Originate(ctx, ari.OriginateParams{
		ChannelID: leadChannelID,
		Endpoint:  "PJSIP/" + cmd.TrunkTarget + "@" + c.o.TrunkEndpoint,
		App:       c.o.AriApp,
		AppArgs:   "ai-lead," + callID,
		CallerID:  cmd.CallerDid,
		Timeout:   c.o.RingTimeout,
		Variables: vars,
	}); err != nil {
		_ = c.st.UpdateCall(ctx, callID, store.Patch{
			Status: store.S("FAILED"), FailureReason: store.S("originate_error"),
		})
		c.pubStatus(ctx, callID, "FAILED")
		c.s.limparIA(callID)
		return err
	}

	_ = c.st.UpdateCall(ctx, callID, store.Patch{SipChannelID: store.S(leadChannelID)})
	c.pubStatus(ctx, callID, "RINGING")
	return nil
}

// ai-lead: o lead atendeu. Cria o bridge, adiciona o lead e origina o canal
// AudioSocket que conecta na IA. Sem vendedor humano.
func (c *Orchestrator) onAiLead(ctx context.Context, callID string, channel ari.Channel) {
	_ = c.ari.Answer(ctx, channel.ID)

	bridgeID := c.newI()
	_ = c.ari.CreateBridge(ctx, bridgeID, "mixing")
	_ = c.ari.AddChannel(ctx, bridgeID, channel.ID)

	mediaChannelID := c.newI()
	// Precedência do UUID do AudioSocket: variável de canal (DURÁVEL, sobrevive a
	// um restart do worker durante o ring) -> mapa (fast path) -> aleatório
	// (legado: ligação sem pré-registro na API). A ordem importa: inverter faria
	// um restart no ring descartar a voz-por-org que a API já registrou.
	aiMediaUUID := c.ari.GetChannelVar(ctx, channel.ID, "AI_MEDIA_UUID")
	if v := c.s.consumirAIVoiceUUID(callID); aiMediaUUID == "" {
		aiMediaUUID = v
	}
	if aiMediaUUID == "" {
		aiMediaUUID = c.newI()
	}

	agora := c.now()
	recName := "connect-" + callID
	ref := bridgeRef{
		bridgeID:        bridgeID,
		sellerChannelID: mediaChannelID, // slot "outra perna" = canal AudioSocket
		leadChannelID:   channel.ID,
		aiMediaUUID:     aiMediaUUID,
		startedAt:       &agora,
		recordingName:   recName,
	}
	c.s.setBridge(callID, ref)

	if err := c.ari.Originate(ctx, ari.OriginateParams{
		ChannelID: mediaChannelID,
		Endpoint:  fmt.Sprintf("AudioSocket/%s/%s", c.o.AIAudiosocketAddr, aiMediaUUID),
		App:       c.o.AriApp,
		AppArgs:   "ai-media," + callID,
		// Wideband ASSIMÉTRICO, e não é simetria esquecida:
		//   - Asterisk -> socket segue o formato do canal. slin16 preserva os
		//     16 kHz decodificados do G.722 para o STT (CALL_SR_IN=16000).
		//   - socket -> Asterisk 20.6 é lido pelo res_audiosocket como
		//     ast_format_slin 8 kHz. Por isso o voice-talk TEM de manter
		//     CALL_SR_OUT=8000; enviar 16 kHz deixaria a voz 2x lenta e grave.
		// Sem fixar formats, o Asterisk cria a perna em slin192 e o áudio chega
		// esticado 24x — a voz vira sub-50 Hz e o STT não entende nada.
		Formats: "slin16",
	}); err != nil {
		logx.Error("ai-lead.audiosocket_originate_falhou", "callId", callID, "err", err.Error())
		_ = c.ari.Hangup(ctx, channel.ID)
		_ = c.ari.DestroyBridge(ctx, bridgeID)
		c.s.removerBridge(callID)
		_ = c.st.UpdateCall(ctx, callID, store.Patch{
			Status: store.S("FAILED"), FailureReason: store.S("ai_media_unreachable"),
		})
		c.pubStatus(ctx, callID, "FAILED")
		c.s.deleteOrg(callID)
		c.s.setAIAgent(callID, "") // espelha o aiAgent.delete do original
		return
	}

	// Supervisão: se há agente registrado, origina a perna WebRTC dele para
	// OUVIR (entra como ai-agent, mutada). Best-effort: se falhar, a IA segue sem
	// monitor (não derruba a ligação).
	if ramal := c.s.aiAgentDe(callID); ramal != "" {
		agentChannelID := c.newI()
		c.s.withRef(callID, func(r *bridgeRef, s *state) {
			r.agentChannelID = agentChannelID
			s.channelIndex[agentChannelID] = callID
		})
		org, _ := c.s.org(callID)
		if err := c.ari.Originate(ctx, ari.OriginateParams{
			ChannelID: agentChannelID,
			Endpoint:  "PJSIP/" + ramal,
			App:       c.o.AriApp,
			AppArgs:   "ai-agent," + callID,
			Timeout:   c.o.RingTimeout,
			Variables: map[string]string{"CALL_ID": callID, "ORG": org},
		}); err != nil {
			logx.Error("ai-lead.agente_originate_falhou", "callId", callID, "err", err.Error())
			c.s.withRef(callID, func(r *bridgeRef, s *state) {
				delete(s.channelIndex, agentChannelID)
				r.agentChannelID = ""
			})
		}
	}

	c.iniciarGravacao(ctx, callID, bridgeID, recName)
	_ = c.st.UpdateCall(ctx, callID, store.Patch{
		Status: store.S("IN_PROGRESS"), StartedAt: store.T(agora), RecordingName: store.S(recName),
	})
	c.pubStatus(ctx, callID, "IN_PROGRESS")
	logx.Info("ai-lead.in_progress", "callId", callID)
}

// ai-agent: a perna WebRTC do agente supervisor atendeu. Soma ao bridge MUTADA
// ("in": o agente ouve a IA↔lead, mas o lead não ouve o agente). O takeover
// desmuta essa perna.
func (c *Orchestrator) onAiAgent(ctx context.Context, callID string, channel ari.Channel) {
	ref, ok := c.s.bridge(callID)
	if !ok {
		logx.Warn("ai-agent.ref_missing", "callId", callID)
		return
	}
	if err := c.ari.Answer(ctx, channel.ID); err != nil {
		logx.Error("ai-agent.answer_falhou", "callId", callID, "err", err.Error())
	}
	if err := c.ari.AddChannel(ctx, ref.bridgeID, channel.ID); err != nil {
		logx.Error("ai-agent.addChannel_falhou", "callId", callID, "err", err.Error())
	}
	if err := c.ari.MuteChannel(ctx, channel.ID, "in"); err != nil {
		logx.Error("ai-agent.mute_falhou", "callId", callID, "err", err.Error())
	}
	// O canal real que atendeu pode diferir do id originado; reindexa.
	c.s.withRef(callID, func(r *bridgeRef, s *state) {
		if r.agentChannelID != "" && r.agentChannelID != channel.ID {
			delete(s.channelIndex, r.agentChannelID)
		}
		r.agentChannelID = channel.ID
		s.channelIndex[channel.ID] = callID
	})
	logx.Info("ai-agent.listening", "callId", callID, "ch", channel.ID)
}

// ai-media: o canal AudioSocket entrou no Stasis; soma ao bridge da chamada.
func (c *Orchestrator) onAiMedia(ctx context.Context, callID string, channel ari.Channel) {
	ref, ok := c.s.bridge(callID)
	if !ok {
		logx.Warn("ai-media.ref_missing", "callId", callID)
		return
	}
	if err := c.ari.AddChannel(ctx, ref.bridgeID, channel.ID); err != nil {
		logx.Error("ai-media.addChannel_falhou", "callId", callID, "err", err.Error())
	}
}
