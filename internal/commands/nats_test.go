package commands

import (
	"context"
	"encoding/json"
	"testing"
)

// O que se testa aqui é a DISPOSIÇÃO da mensagem. Errar isso é caro dos dois
// lados: um Nak no lugar de Term faz a mensagem girar até o max_deliver
// ocupando o consumidor e escondendo os comandos bons atrás dela; um Term no
// lugar de Nak descarta uma chamada por um blip do Asterisk.

func env(name, callID, shardID string) Envelope {
	return Envelope{Name: name, CallID: callID, ShardID: shardID, Data: json.RawMessage(`{}`)}
}

func TestSubjectEDurableSaemDoMesmoShard(t *testing.T) {
	// O subject tem que casar com o que o produtor publica (mini-back), e o
	// durable tem que ser estável entre restarts — é ele que faz o JetStream
	// reentregar o que este nó não confirmou antes de cair.
	if Subject("a1") != "ari.cmd.a1" || Durable("a1") != "shard-a1" {
		t.Fatalf("%s / %s", Subject("a1"), Durable("a1"))
	}
}

func TestParseRejeitaLixoSemPanico(t *testing.T) {
	// Um payload torto não pode derrubar o laço de consumo: o nó pararia de
	// aceitar comando por causa de UMA mensagem.
	for _, b := range [][]byte{
		[]byte("{ não é json"),
		[]byte(`{"callId":"call_1"}`),      // sem name
		[]byte(`{"name":"startOutbound"}`), // sem callId
	} {
		if _, ok := Parse(b); ok {
			t.Fatalf("devia rejeitar: %s", b)
		}
	}
	if _, ok := Parse([]byte(`{"name":"terminate","callId":"call_1"}`)); !ok {
		t.Fatal("envelope válido rejeitado")
	}
}

func TestEnvelopeInvalidoEhTerm(t *testing.T) {
	// Reentregar um payload corrompido não muda nada: falha igual nas 5
	// tentativas e só atrasa os comandos que vieram atrás.
	if d := Despachar(context.Background(), Envelope{}, false, "a1", nil); d != Term {
		t.Fatalf("d = %s", d)
	}
}

func TestComandoDeOutroShardEhTermENaoExecuta(t *testing.T) {
	// Endereçamento errado é bug de roteamento do produtor, não falha
	// transitória. Executar seria pior que descartar: este nó montaria uma
	// chamada que o produtor acha que vive em outro shard, e o terminate dela
	// nunca chegaria aqui.
	//
	// orq=nil prova que NÃO executa: se despachasse, entraria em pânico.
	if d := Despachar(context.Background(), env("terminate", "call_1", "a2"), true, "a1", nil); d != Term {
		t.Fatalf("d = %s", d)
	}
}

func TestComandoDesconhecidoEhTerm(t *testing.T) {
	if d := Despachar(context.Background(), env("fazCafe", "call_1", "a1"), true, "a1", nil); d != Term {
		t.Fatalf("d = %s", d)
	}
}
