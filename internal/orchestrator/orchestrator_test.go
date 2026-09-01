package orchestrator

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lai/worker-ari/internal/ari"
	"github.com/lai/worker-ari/internal/metrics"
	"github.com/lai/worker-ari/internal/store"
)

var ctx = context.Background()

func cmdOut(id string) StartOutbound {
	return StartOutbound{
		CallID: id, OrganizationID: "org_1", SellerSipUser: "1001",
		CallerDid: "554199999999", TrunkTarget: "1842554188887777",
	}
}

// --- Admissão ---------------------------------------------------------------

func TestAdmissaoRecusaNoTetoSemOriginar(t *testing.T) {
	// O ponto do teto é NÃO tocar no Asterisk. Originar e depois desligar já
	// teria consumido canal e porta RTP — que é exatamente o recurso em falta.
	a, s := novoAri(), novoStore()
	c := orq(a, s, Options{MaxCalls: 2})
	for i := 0; i < 2; i++ {
		_ = c.StartOutbound(ctx, cmdOut("call_"+string(rune('a'+i))), false)
	}
	antes := len(a.ops("originate"))

	_ = c.StartOutbound(ctx, cmdOut("call_x"), false)

	if len(a.ops("originate")) != antes {
		t.Fatal("no teto não pode originar")
	}
	if c.ActiveCalls() != 2 {
		t.Fatalf("a recusada não entra no gauge: %d", c.ActiveCalls())
	}
}

func TestAdmissaoMotivoProprio(t *testing.T) {
	// `shard_at_capacity` no gráfico significa "dimensionamento ou balanceamento
	// errado" — ação diferente de "o Asterisk recusou o originate". Somar os dois
	// no mesmo balde esconderia a saturação.
	a, s := novoAri(), novoStore()
	c := orq(a, s, Options{MaxCalls: 1})
	_ = c.StartOutbound(ctx, cmdOut("call_1"), false)
	_ = c.StartOutbound(ctx, cmdOut("call_2"), false)

	p, _ := s.ultimoPatch()
	if p.Status == nil || *p.Status != "FAILED" || p.FailureReason == nil || *p.FailureReason != "shard_at_capacity" {
		t.Fatalf("patch = %+v", p)
	}
}

func TestSemMaxCallsNaoHaTeto(t *testing.T) {
	// O deploy atual roda sem teto. Um default implícito faria um worker em
	// produção começar a recusar chamada no deploy.
	a, s := novoAri(), novoStore()
	c := orq(a, s, Options{})
	for i := 0; i < 30; i++ {
		_ = c.StartOutbound(ctx, cmdOut("call_"+string(rune('a'+i))), false)
	}
	if c.ActiveCalls() != 30 {
		t.Fatalf("ativas = %d", c.ActiveCalls())
	}
}

// --- startOutbound ----------------------------------------------------------

func TestStartOutboundUsaChannelIdDeterministico(t *testing.T) {
	// channelId = <callId>-A torna o originate idempotente sob retry: re-originar
	// com o MESMO id devolve 409 do ARI em vez de tocar um SEGUNDO ramal do
	// vendedor — que é o defeito que o cliente percebe.
	a, s := novoAri(), novoStore()
	c := orq(a, s, Options{})
	_ = c.StartOutbound(ctx, cmdOut("call_1"), false)

	ops := a.ops("originate")
	if len(ops) != 1 || ops[0].args[2] != "call_1-A" {
		t.Fatalf("originate = %+v", ops)
	}
	if ops[0].args[0] != "PJSIP/1001" || ops[0].args[1] != "outbound,call_1" {
		t.Fatalf("endpoint/appArgs errados: %+v", ops[0].args)
	}
}

func TestOriginate409ESeguidoComoJaOriginado(t *testing.T) {
	// 409 = o originate de uma tentativa anterior venceu. Marcar FAILED aqui
	// mataria uma chamada que está tocando de verdade.
	a, s := novoAri(), novoStore()
	a.erroOriginateTudo = &ari.HTTPError{Status: 409, Message: "conflito"}
	c := orq(a, s, Options{})

	if err := c.StartOutbound(ctx, cmdOut("call_1"), true); err != nil {
		t.Fatalf("409 não devia propagar: %v", err)
	}
	for _, st := range s.statusGravados() {
		if st == "FAILED" {
			t.Fatal("409 não pode marcar FAILED")
		}
	}
}

func TestFailedSoNaUltimaTentativa(t *testing.T) {
	// Marcar FAILED numa tentativa intermediária faria o retry encontrar a linha
	// já FAILED, divergindo do RINGING real que o próximo originate produz.
	a, s := novoAri(), novoStore()
	a.erroOriginateTudo = errors.New("asterisk fora do ar")
	c := orq(a, s, Options{})

	_ = c.StartOutbound(ctx, cmdOut("call_1"), false) // não é a última
	if len(s.statusGravados()) != 0 {
		t.Fatalf("não devia gravar status: %v", s.statusGravados())
	}
	if c.ActiveCalls() != 1 {
		t.Fatal("a chamada continua ativa entre tentativas")
	}

	_ = c.StartOutbound(ctx, cmdOut("call_1"), true) // última
	if got := s.statusGravados(); len(got) != 1 || got[0] != "FAILED" {
		t.Fatalf("status = %v", got)
	}
	if c.ActiveCalls() != 0 {
		t.Fatal("esgotado o retry, sai do gauge")
	}
}

// --- Fluxo outbound completo ------------------------------------------------

func TestFluxoOutboundAteInProgress(t *testing.T) {
	a, s := novoAri(), novoStore()
	a.vars["OUTBOUND_TARGET"] = "1842554188887777"
	a.vars["OUTBOUND_DID"] = "554199999999"
	c := orq(a, s, Options{})

	_ = c.StartOutbound(ctx, cmdOut("call_1"), false)
	c.OnStasisStart(ctx, ari.StasisStart{Args: []string{"outbound", "call_1"},
		Channel: ari.Channel{ID: "call_1-A"}})

	// Perna A: answer, bridge, add, originate do lead, gravação.
	if len(a.ops("createBridge")) != 1 || len(a.ops("recordBridge")) != 1 {
		t.Fatal("bridge e gravação têm que subir na perna A")
	}
	ops := a.ops("originate")
	if len(ops) != 2 || !strings.HasPrefix(ops[1].args[0], "PJSIP/1842554188887777@trunk") {
		t.Fatalf("originate do lead errado: %+v", ops)
	}

	// Perna B entra: IN_PROGRESS.
	leadID := ops[1].args[2]
	c.OnStasisStart(ctx, ari.StasisStart{Args: []string{"legB", "call_1"},
		Channel: ari.Channel{ID: leadID}})

	if got := s.statusGravados(); got[len(got)-1] != "IN_PROGRESS" {
		t.Fatalf("status = %v", got)
	}
}

func TestSemTargetFalhaEDesliga(t *testing.T) {
	// Sem OUTBOUND_TARGET não há para quem discar. Deixar a perna A no ar seria
	// um canal órfão vivo no Stasis com o vendedor ouvindo silêncio.
	a, s := novoAri(), novoStore()
	c := orq(a, s, Options{})
	_ = c.StartOutbound(ctx, cmdOut("call_1"), false)
	c.OnStasisStart(ctx, ari.StasisStart{Args: []string{"outbound", "call_1"},
		Channel: ari.Channel{ID: "call_1-A"}})

	if len(a.ops("hangup")) != 1 {
		t.Fatal("tem que desligar a perna A")
	}
	p, _ := s.ultimoPatch()
	if p.FailureReason == nil || *p.FailureReason != "no_target" {
		t.Fatalf("patch = %+v", p)
	}
}

func TestLegBSemRefNaoViraInProgress(t *testing.T) {
	// ref ausente = o worker reiniciou depois de originar. O answer chega mas a
	// chamada NÃO pode virar IN_PROGRESS sem bridge — o finalize depois marca
	// NO_ANSWER, e o log legB.ref_missing é o que denuncia a causa.
	a, s := novoAri(), novoStore()
	c := orq(a, s, Options{})
	c.OnStasisStart(ctx, ari.StasisStart{Args: []string{"legB", "call_orfa"},
		Channel: ari.Channel{ID: "ch1"}})
	if len(s.statusGravados()) != 0 {
		t.Fatalf("não pode gravar status: %v", s.statusGravados())
	}
}

// --- finalize ---------------------------------------------------------------

func TestAtendeuEhCompletedSejaQualForACausa(t *testing.T) {
	// O normal clearing (16) no fim de uma conversa NÃO é falha. Classificar a
	// causa aqui produziria o "atendi e marcou não atendeu".
	a, s := novoAri(), novoStore()
	a.vars["OUTBOUND_TARGET"] = "1842"
	agora := tempoBase
	c := orq(a, s, Options{Now: func() time.Time { return agora }})

	_ = c.StartOutbound(ctx, cmdOut("call_1"), false)
	c.OnStasisStart(ctx, ari.StasisStart{Args: []string{"outbound", "call_1"}, Channel: ari.Channel{ID: "call_1-A"}})
	leadID := a.ops("originate")[1].args[2]
	c.OnStasisStart(ctx, ari.StasisStart{Args: []string{"legB", "call_1"}, Channel: ari.Channel{ID: leadID}})

	agora = tempoBase.Add(42 * time.Second)
	causa := 16
	c.OnChannelDestroyed(ctx, ari.ChannelDestroyed{Cause: &causa, Channel: ari.Channel{ID: leadID}})

	p, _ := s.ultimoPatch()
	if p.Status == nil || *p.Status != "COMPLETED" {
		t.Fatalf("status = %+v", p.Status)
	}
	if p.Duration == nil || *p.Duration != 42 {
		t.Fatalf("duração = %v, queria 42", p.Duration)
	}
}

func TestNaoAtendeuClassificaACausa(t *testing.T) {
	// 34 = congestionamento do carrier. Vira FAILED, não NO_ANSWER: culpa do
	// provedor não pode ficar escondida atrás de "ninguém atendeu".
	a, s := novoAri(), novoStore()
	a.vars["OUTBOUND_TARGET"] = "1842"
	c := orq(a, s, Options{})
	_ = c.StartOutbound(ctx, cmdOut("call_1"), false)
	c.OnStasisStart(ctx, ari.StasisStart{Args: []string{"outbound", "call_1"}, Channel: ari.Channel{ID: "call_1-A"}})
	leadID := a.ops("originate")[1].args[2]

	causa := 34
	c.OnChannelDestroyed(ctx, ari.ChannelDestroyed{Cause: &causa, CauseTxt: "No circuit",
		Channel: ari.Channel{ID: leadID}})

	p, _ := s.ultimoPatch()
	if *p.Status != "FAILED" || *p.FailureReason != "carrier_congestion" {
		t.Fatalf("patch = %+v %+v", p.Status, p.FailureReason)
	}
	if p.HangupCause == nil || *p.HangupCause != 34 || p.HangupCauseTx == nil {
		t.Fatal("a causa crua tem que ser guardada para o suporte")
	}
}

func TestUmaPernaCaiDerrubaTodasAsOutras(t *testing.T) {
	// Senão a sobrevivente fica órfã e VIVA no Stasis: o softphone do vendedor
	// nunca recebe o BYE e a tela trava "em chamada".
	a, s := novoAri(), novoStore()
	a.vars["OUTBOUND_TARGET"] = "1842"
	c := orq(a, s, Options{})
	_ = c.StartOutbound(ctx, cmdOut("call_1"), false)
	c.OnStasisStart(ctx, ari.StasisStart{Args: []string{"outbound", "call_1"}, Channel: ari.Channel{ID: "call_1-A"}})
	leadID := a.ops("originate")[1].args[2]
	c.OnStasisStart(ctx, ari.StasisStart{Args: []string{"legB", "call_1"}, Channel: ari.Channel{ID: leadID}})

	c.OnChannelDestroyed(ctx, ari.ChannelDestroyed{Channel: ari.Channel{ID: leadID}})

	var derrubadas []string
	for _, op := range a.ops("hangup") {
		derrubadas = append(derrubadas, op.args[0])
	}
	if len(derrubadas) != 1 || derrubadas[0] != "call_1-A" {
		t.Fatalf("devia derrubar a perna A sobrevivente: %v", derrubadas)
	}
}

func TestTerminateDuasVezesNaoSobrescreveODesfecho(t *testing.T) {
	// Sem ref, distinguir pelo status atual: senão um retry do terminate
	// sobrescreveria um COMPLETED já gravado com NO_ANSWER.
	a, s := novoAri(), novoStore()
	s.timing["call_1"] = &store.CallTiming{Status: "COMPLETED"}
	c := orq(a, s, Options{})

	_ = c.Terminate(ctx, "call_1")

	if len(s.statusGravados()) != 0 {
		t.Fatalf("não pode regravar status: %v", s.statusGravados())
	}
}

// --- Eco da perna sobrevivente ---------------------------------------------

func TestEcoDePernaEmProgressoNaoViraNoAnswer(t *testing.T) {
	// Este evento é o eco da perna que o PRÓPRIO finalize derrubou. Se ele
	// gravasse NO_ANSWER por cima, toda chamada atendida terminaria com o
	// desfecho errado.
	a, s := novoAri(), novoStore()
	s.porCanal["ch_sobrevivente"] = "call_1"
	s.timing["call_1"] = &store.CallTiming{Status: "IN_PROGRESS"}
	c := orq(a, s, Options{})

	c.OnChannelDestroyed(ctx, ari.ChannelDestroyed{Channel: ari.Channel{ID: "ch_sobrevivente"}})

	if len(s.statusGravados()) != 0 {
		t.Fatalf("IN_PROGRESS não pode virar NO_ANSWER: %v", s.statusGravados())
	}
}

func TestPernaAQueCaiTocandoViraNoAnswer(t *testing.T) {
	a, s := novoAri(), novoStore()
	s.porCanal["call_1-A"] = "call_1"
	s.timing["call_1"] = &store.CallTiming{Status: "RINGING"}
	c := orq(a, s, Options{})

	causa := 19 // no answer
	c.OnChannelDestroyed(ctx, ari.ChannelDestroyed{Cause: &causa, Channel: ari.Channel{ID: "call_1-A"}})

	p, _ := s.ultimoPatch()
	if *p.Status != "NO_ANSWER" || *p.FailureReason != "no_answer" {
		t.Fatalf("patch = %+v", p)
	}
}

// --- INBOUND ----------------------------------------------------------------

func TestInboundSemDonoAtendeEEncerra(t *testing.T) {
	a, s := novoAri(), novoStore()
	c := orq(a, s, Options{})
	c.OnStasisStart(ctx, ari.StasisStart{Channel: func() ari.Channel {
		ch := ari.Channel{ID: "ch_in"}
		ch.Dialplan.Exten = "554133334444"
		return ch
	}()})
	if len(a.ops("answer")) != 1 || len(a.ops("hangup")) != 1 {
		t.Fatal("sem dono: atende e encerra")
	}
	if len(s.criadas) != 0 {
		t.Fatal("não pode criar call sem dono")
	}
}

func TestInboundComDonoCriaCallEToca(t *testing.T) {
	a, s := novoAri(), novoStore()
	s.seller = &store.InboundSeller{OrganizationID: "org_1", UserID: "u1", SipUsername: "1002"}
	c := orq(a, s, Options{})

	ch := ari.Channel{ID: "ch_in"}
	ch.Dialplan.Exten = "554133334444"
	ch.Caller.Number = "5541988887777"
	c.OnStasisStart(ctx, ari.StasisStart{Channel: ch})

	if len(s.criadas) != 1 || s.criadas[0].Direction != "INBOUND" {
		t.Fatalf("criadas = %+v", s.criadas)
	}
	// As vars têm que ser marcadas no canal do CHAMADOR para a reidratação
	// recuperar callId+org depois de um restart.
	var temCallID, temOrg bool
	for _, op := range a.ops("setvar") {
		if op.args[1] == "CALL_ID" {
			temCallID = true
		}
		if op.args[1] == "ORG" {
			temOrg = true
		}
	}
	if !temCallID || !temOrg {
		t.Fatal("CALL_ID e ORG têm que ir para a variável de canal")
	}
	if len(a.ops("originate")) != 1 || a.ops("originate")[0].args[0] != "PJSIP/1002" {
		t.Fatalf("tem que tocar o ramal do dono: %+v", a.ops("originate"))
	}
}

func TestCarimbosDeTempoSaoUTC(t *testing.T) {
	// As colunas de tempo da `call` sao `timestamp` SEM fuso, e o created_at vem
	// do now() do Postgres (UTC). Se o worker gravar no fuso do host, os dois
	// lados da MESMA linha ficam em referenciais diferentes: no laboratorio a
	// latencia de setup (started_at - created_at) deu -3 HORAS.
	//
	// Em producao fica mascarado porque o container roda em UTC — mas este
	// worker e sidecar de no EC2, e o fuso de la nao e garantido.
	c := New(novoAri(), novoStore(), nil, nil, metrics.New(), Options{})
	if _, off := c.now().Zone(); off != 0 {
		t.Fatalf("carimbo com offset %ds; tem que ser UTC", off)
	}
}
