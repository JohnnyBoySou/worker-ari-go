package shard

import (
	"testing"
	"time"
)

const ttl = 15 * time.Second

var agora = time.Unix(1_700_000_000, 0)

func snap(id string, updatedAt time.Time) Snapshot {
	return Snapshot{ShardID: id, MaxCalls: 1000, Accepting: true, UpdatedAt: updatedAt.UnixMilli()}
}

func TestResolveSemShardIDEModoLegado(t *testing.T) {
	// O deploy em TypeScript roda assim. Inventar um id (hostname, por exemplo)
	// mudaria o nome do app de um worker em produção no primeiro restart.
	id, err := Resolve("")
	if err != nil || id != "" {
		t.Fatalf("id=%q err=%v", id, err)
	}
}

func TestResolveAceitaOFormatoValido(t *testing.T) {
	for _, in := range []string{"a1", "sa-east-1a-07", "7", "A1"} {
		if _, err := Resolve(in); err != nil {
			t.Fatalf("%q devia ser válido: %v", in, err)
		}
	}
	// Valida DEPOIS de normalizar: maiúscula não é erro, é caixa.
	if id, _ := Resolve("A1"); id != "a1" {
		t.Fatalf("id = %q", id)
	}
}

func TestResolveRejeitaIdTorto(t *testing.T) {
	// Ponto é o separador do subject (`ari.cmd.<id>`): "a.b" tornaria
	// `ari.cmd.a.b` ambíguo. Barra e espaço quebram o nome do contexto do
	// dialplan. Subir com qualquer um destes é pior que não subir: o sintoma
	// aparece horas depois, como chamada que nunca monta.
	for _, ruim := range []string{"a.b", "a b", "A1!", "-a1", "a1-", "a/b",
		"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"} {
		if _, err := Resolve(ruim); err == nil {
			t.Fatalf("%q devia ser rejeitado", ruim)
		}
	}
}

func TestNameCompoeOsDoisNomesDoMesmoShard(t *testing.T) {
	// A garantia que importa: app e subject não podem divergir, porque um worker
	// escutando o app de um shard e consumindo os comandos de outro aceitaria
	// comandos que nunca viram evento.
	if Name("connect", "a1", "-") != "connect-a1" {
		t.Fatal("app")
	}
	if Name("ari.cmd", "a1", ".") != "ari.cmd.a1" {
		t.Fatal("subject")
	}
	// Sem shard, a base intacta (compatibilidade com o deploy atual).
	if Name("connect", "", "-") != "connect" {
		t.Fatal("legado")
	}
}

func TestLiveEStaleTemJanelasDiferentesDePROPOSITO(t *testing.T) {
	// As duas perguntas são diferentes. "Posso mandar chamada?" pode ser
	// conservadora e pular um nó lento — no pior caso a chamada vai para outro
	// shard. "Posso APAGAR o registro?" precisa ser generosa: apagar um nó que
	// só estava lento o tira do balanceamento inteiro sem necessidade.
	lento := snap("lento", agora.Add(-2*ttl))
	if Live(lento, agora, ttl) {
		t.Fatal("nó lento não recebe chamada")
	}
	if Stale(lento, agora, ttl) {
		t.Fatal("...mas NÃO pode ser apagado")
	}
	morto := snap("morto", agora.Add(-5*ttl))
	if !Stale(morto, agora, ttl) {
		t.Fatal("nó morto tem que ser podado")
	}
}

func TestParseEntriesIgnoraLixoSemDerrubarOResto(t *testing.T) {
	// O registro é compartilhado. Se um JSON quebrado fizesse o parse falhar,
	// um único nó com problema tiraria TODOS os shards do roteamento.
	out := ParseEntries(map[string]string{
		"bom":         `{"shardId":"bom","updatedAt":1700000000000,"maxCalls":1000,"accepting":true}`,
		"lixo":        `{ isto não é json`,
		"sem_updated": `{"shardId":"x"}`,
	})
	if len(out) != 1 || out[0].ShardID != "bom" {
		t.Fatalf("out = %+v", out)
	}
}
