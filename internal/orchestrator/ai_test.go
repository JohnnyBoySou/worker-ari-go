package orchestrator

import (
	"testing"

	"github.com/lai/worker-ari/internal/ari"
)

func cmdAI(id string) StartAiCall {
	return StartAiCall{CallID: id, OrganizationID: "org_1", TrunkTarget: "1842", CallerDid: "5541"}
}

func TestAudioSocketFixaSlin16(t *testing.T) {
	// SEM formats fixo o Asterisk cria a perna em slin192 e o voice-talk recebe
	// o áudio esticado 24x: a voz vira sub-50 Hz e o STT não entende NADA. O
	// sintoma não é erro, é transcrição ruim — caríssimo de diagnosticar.
	a, s := novoAri(), novoStore()
	c := orq(a, s, Options{AIAudiosocketAddr: "10.0.0.9:9092"})
	_ = c.StartAiCall(ctx, cmdAI("call_1"))
	leadID := a.ops("originate")[0].args[2]
	c.OnStasisStart(ctx, ari.StasisStart{Args: []string{"ai-lead", "call_1"}, Channel: ari.Channel{ID: leadID}})

	var achou bool
	for _, op := range a.ops("originate") {
		if len(op.args[0]) > 12 && op.args[0][:12] == "AudioSocket/" {
			achou = true
			if op.args[3] != "slin16" {
				t.Fatalf("formats = %q, tem que ser slin16", op.args[3])
			}
		}
	}
	if !achou {
		t.Fatal("perna AudioSocket não subiu")
	}
}

func TestUUIDDaVozVemDaVariavelDeCanalAntesDoMapa(t *testing.T) {
	// A precedência é o que sobrevive a um restart do worker DURANTE o ring
	// (~30-45s): o mapa em memória se perde, a variável de canal não. Invertida,
	// um restart no ring descartaria a voz-por-org que a API já registrou e a
	// ligação subiria com a voz errada.
	a, s := novoAri(), novoStore()
	a.vars["AI_MEDIA_UUID"] = "uuid-da-api"
	c := orq(a, s, Options{AIAudiosocketAddr: "10.0.0.9:9092"})

	// O mapa tem OUTRO valor: a variável de canal tem que vencer.
	_ = c.StartAiCall(ctx, StartAiCall{CallID: "call_1", OrganizationID: "org_1",
		TrunkTarget: "1842", AudioSocketID: "uuid-do-mapa"})
	leadID := a.ops("originate")[0].args[2]
	c.OnStasisStart(ctx, ari.StasisStart{Args: []string{"ai-lead", "call_1"}, Channel: ari.Channel{ID: leadID}})

	for _, op := range a.ops("originate") {
		if len(op.args[0]) > 12 && op.args[0][:12] == "AudioSocket/" {
			if op.args[0] != "AudioSocket/10.0.0.9:9092/uuid-da-api" {
				t.Fatalf("endpoint = %q; a variável de canal tem que vencer o mapa", op.args[0])
			}
			return
		}
	}
	t.Fatal("perna AudioSocket não subiu")
}

func TestAudioSocketInalcancavelFalhaAChamada(t *testing.T) {
	a, s := novoAri(), novoStore()
	a.erroOriginatePrefixo = "AudioSocket/"
	c := orq(a, s, Options{AIAudiosocketAddr: "10.0.0.9:9092"})
	_ = c.StartAiCall(ctx, cmdAI("call_1"))
	leadID := a.ops("originate")[0].args[2]
	c.OnStasisStart(ctx, ari.StasisStart{Args: []string{"ai-lead", "call_1"}, Channel: ari.Channel{ID: leadID}})

	p, _ := s.ultimoPatch()
	if p.FailureReason == nil || *p.FailureReason != "ai_media_unreachable" {
		t.Fatalf("patch = %+v", p)
	}
	if len(a.ops("destroyBridge")) == 0 {
		t.Fatal("bridge tem que ser destruída")
	}
}

// O GUARD DO MONITOR — um incidente inteiro numa função.
func TestMonitorCaindoNaoDerrubaAChamada(t *testing.T) {
	// A perna do agente supervisor entra MUDA, só para ouvir. Se ela cair sozinha
	// (softphone WebRTC que não fecha ICE/DTLS), NÃO pode derrubar a ligação.
	// Sem este guard, o softphone do supervisor caindo levava junto o lead e a
	// perna AudioSocket (BYE cause=16), matando a chamada ~2s após atender.
	a, s := novoAri(), novoStore()
	c := orq(a, s, Options{AIAudiosocketAddr: "10.0.0.9:9092"})
	_ = c.StartAiCall(ctx, StartAiCall{CallID: "call_1", OrganizationID: "org_1",
		TrunkTarget: "1842", AgentSipUser: "1001"})
	leadID := a.ops("originate")[0].args[2]
	c.OnStasisStart(ctx, ari.StasisStart{Args: []string{"ai-lead", "call_1"}, Channel: ari.Channel{ID: leadID}})

	// O agente atende e entra mutado.
	c.OnStasisStart(ctx, ari.StasisStart{Args: []string{"ai-agent", "call_1"}, Channel: ari.Channel{ID: "ch_agente"}})
	if len(a.ops("mute")) != 1 {
		t.Fatal("o supervisor tem que entrar mutado 'in'")
	}
	hangupsAntes := len(a.ops("hangup"))

	// O softphone do supervisor cai sozinho.
	c.OnChannelDestroyed(ctx, ari.ChannelDestroyed{Channel: ari.Channel{ID: "ch_agente"}})

	if len(a.ops("hangup")) != hangupsAntes {
		t.Fatal("a queda do monitor NÃO pode derrubar nenhuma perna")
	}
	if got := s.statusGravados(); len(got) > 0 && got[len(got)-1] != "IN_PROGRESS" {
		t.Fatalf("a chamada não pode ser finalizada: %v", got)
	}
}

func TestTakeoverDesmutaOAgenteQuandoAPernaDaIACai(t *testing.T) {
	// A ordem é o ponto: o desmute acontece só quando a perna da IA CAI, não no
	// pedido de takeover — senão o microfone do agente abriria antes de a IA
	// terminar a despedida, e o lead ouviria os dois falando junto.
	a, s := novoAri(), novoStore()
	c := orq(a, s, Options{AIAudiosocketAddr: "10.0.0.9:9092"})
	_ = c.StartAiCall(ctx, StartAiCall{CallID: "call_1", OrganizationID: "org_1",
		TrunkTarget: "1842", AgentSipUser: "1001"})
	leadID := a.ops("originate")[0].args[2]
	c.OnStasisStart(ctx, ari.StasisStart{Args: []string{"ai-lead", "call_1"}, Channel: ari.Channel{ID: leadID}})
	c.OnStasisStart(ctx, ari.StasisStart{Args: []string{"ai-agent", "call_1"}, Channel: ari.Channel{ID: "ch_agente"}})

	// Marca o handoff como se o pedido ao voice-talk tivesse dado certo.
	ref, _ := c.s.bridge("call_1")
	c.s.withRef("call_1", func(r *bridgeRef, _ *state) { r.handingOff = true })

	if len(a.ops("unmute")) != 0 {
		t.Fatal("não pode desmutar antes de a perna da IA cair")
	}

	// A perna da IA cai (ela mesma encerrou após a despedida).
	c.OnChannelDestroyed(ctx, ari.ChannelDestroyed{Channel: ari.Channel{ID: ref.sellerChannelID}})

	if len(a.ops("unmute")) != 1 {
		t.Fatal("o agente tem que ser desmutado quando a IA sai")
	}
	// A chamada CONTINUA: o agente assumiu, não é um encerramento.
	if got := s.statusGravados(); len(got) > 0 && got[len(got)-1] == "COMPLETED" {
		t.Fatal("takeover não pode finalizar a chamada")
	}
	// O agente vira a "outra perna"; agentChannelID zera para o guard do monitor
	// não protegê-lo mais (agora o hangup dele É o fim da chamada).
	depois, _ := c.s.bridge("call_1")
	if depois.sellerChannelID != "ch_agente" || depois.agentChannelID != "" {
		t.Fatalf("ref após takeover = %+v", depois)
	}
}

func TestTakeoverSemUrlDeControleFalhaExplicito(t *testing.T) {
	a, s := novoAri(), novoStore()
	c := orq(a, s, Options{AIAudiosocketAddr: "10.0.0.9:9092"})
	_ = c.StartAiCall(ctx, StartAiCall{CallID: "call_1", OrganizationID: "org_1",
		TrunkTarget: "1842", AgentSipUser: "1001"})
	leadID := a.ops("originate")[0].args[2]
	c.OnStasisStart(ctx, ari.StasisStart{Args: []string{"ai-lead", "call_1"}, Channel: ari.Channel{ID: leadID}})
	c.OnStasisStart(ctx, ari.StasisStart{Args: []string{"ai-agent", "call_1"}, Channel: ari.Channel{ID: "ch_agente"}})

	if err := c.Takeover(ctx, "call_1"); err == nil {
		t.Fatal("sem URL de controle o takeover tem que falhar explícito")
	}
	// E o flag NÃO pode ficar ligado: senão um hangup REAL depois seria
	// confundido com takeover e a chamada não seria finalizada.
	ref, _ := c.s.bridge("call_1")
	if ref.handingOff {
		t.Fatal("handingOff tem que ser revertido quando o pedido falha")
	}
}
