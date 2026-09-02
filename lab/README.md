# lab — laboratório local do sistema de chamadas

> Vive dentro do `worker-ari-go` porque é o harness que o validou ponta a ponta
> contra um Asterisk real. Também exercita o `mini-back` (`../../mini-back`) e a
> configuração equivalente à do `worker-asterisk`.

Sobe a stack inteira em Docker e roda o **fluxo completo de uma chamada** sem
operadora, sem softphone humano e sem custo.

```
mini-back (Go)  ──NATS ari.cmd.lab──▶  worker-ari-go  ──ARI──▶  Asterisk 20 :5080
     │                                                            │    │    │
     └──▶ Postgres (linha `call`)                        PJSIP/1001  …@trunk  PJSIP/2001
                                                              │        │        │
                                                          SIPp uas  SIPp uas    ▼
                                                          (atende)  (atende)  Kamailio :5062
                                                                                 │ lookup(location)
                                                                                 ▼
                                                                        telefone registrado
                                                                        (RTP pelo rtpengine)
```

Dois caminhos, de propósito. **1001/1002/1003** têm contato estático e existem
para o benchmark: não registram, não passam pelo proxy, e continuam medindo o
mesmo que sempre mediram. **2001** registra no Kamailio de verdade, e é o
caminho que exercita o location service.

## O que ele exercita

Tudo que o `worker-ari-go` faz numa chamada de saída, contra um Asterisk de
verdade: originate da perna A, `StasisStart`, criação da bridge, **gravação**,
originate da perna B pelo `trunk`, entrada no bridge, `IN_PROGRESS`, `terminate`
roteado de volta ao shard de origem, `finalize` com duração e status final.

Resultado de uma execução real:

```
stasis            kind=outbound  ch=call_…-A
stasis            kind=legB      ch=38460d8e-…
legB.in_progress
finalize          status=COMPLETED  startedAt=…  hadRef=True

banco:     COMPLETED | duration=8 | recording_name=connect-call_…
gravação:  connect-call_….wav  (128 KB)
depois:    ativas=0  canais=0  bridges=0   ← sem vazamento
```

## Por que SIPp e não softphone

As pontas não se registram: cada uma tem contato **estático** apontando para um
SIPp em modo `uas`, que atende sozinho. O teste vira reproduzível e sem humano no
meio — e é a mesma ferramenta do plano de medição de capacidade.

Duas armadilhas que custaram tempo e estão resolvidas no compose:

- **`-i 0.0.0.0` no SIPp quebra a chamada.** Ele anuncia `0.0.0.0` no Contact do
  200 OK, o ACK do Asterisk vai para lugar nenhum, o SIPp retransmite o 200 nove
  vezes e aborta. Sem a flag, ele usa o IP real da interface.
- **O SIPp oferece ulaw**; permitir só `alaw` nos endpoints derruba a negociação.

## O tronco

O endpoint se chama `trunk`, o **mesmo nome de produção**, para o worker montar
exatamente o mesmo `PJSIP/<target>@trunk`. Aqui ele entrega num SIPp local.

**Trocar o bloco `[trunk]` do `asterisk/pjsip.conf` pelo do provedor é o único
passo para o tronco virar externo.** Antes disso, ver `TRONCO-EXTERNO.md`: o
provedor identifica por IP de origem e não há `type=registration`, então o
inbound não chega numa máquina nova sem ação do provedor.

## Rodar

```bash
python3 bench/mkpcap.py        # gera os pcaps de RTP; ver "Mídia" abaixo
docker compose up -d
set -a && . ./.env.lab && set +a

(cd ..          && go build -o /tmp/lab-worker ./cmd/worker) && /tmp/lab-worker &
(cd ../../mini-back && go build -o /tmp/lab-back   ./cmd/server) && PORT=8092 /tmp/lab-back &

curl -X POST localhost:8092/v1/calls -H 'content-type: application/json' \
  -d '{"organizationId":"org_lab","targetPhone":"5541988887777","sellerSipUsername":"1001"}'
```

## Mídia

As pontas SIPp não só atendem: elas **tocam RTP em laço** enquanto a chamada
durar. Duas coisas dependem disso.

**Os pcaps são gerados, não versionados.** `python3 bench/mkpcap.py` escreve
`bench/media/alaw-300s.pcap` e `bench/media/g722-300s.pcap` (3,3 MB cada). Gerar
em vez de usar o `g711a.pcap` da imagem do SIPp resolve um problema concreto: o
`play_pcap_audio` toca **uma vez**, e com um arquivo de duração desconhecida o
áudio simplesmente para no meio de um teste que segura a chamada por 15s — sem
erro e sem sintoma, medindo uma carga que não é a que se quis medir.

**Um pcap longo, não um laço.** A alternativa era o cenário rearmar o play num
laço, e ela foi tentada, medida e desfeita: o SIPp replica o pcap byte a byte,
então cada volta reinicia a sequência RTP em 0 com o mesmo SSRC. O Asterisk
passa a reportar perda absurda (`lp=65036`, que é −500 em 16 bits) e a nota de
qualidade `txmes` cai de 88 para 20. O laço mantinha a mídia e destruía a
medição. Com 300s tocando uma vez, os dois funcionam.

O pcap de A-law tem **áudio de verdade** (uma senoide de 440 Hz codificada em
G.711), então a gravação que sobe para o S3 é audível — o teste mais barato de
"gravou som" contra "gravou silêncio".

> **O SIPp carrega o pcap ao parsear o cenário**, não na hora de tocar. Sem os
> arquivos, os três containers `uas-*` sobem quebrados. Gere antes do `up` — e
> depois de regerar, `docker compose restart uas-1001 uas-1002 uas-1003`.

**O modo transcoding.** Com `1001` e `1002` os dois lados falam G.711 e o
Asterisk fica em passthrough: o cenário barato. O endpoint `1003` fala **só
G.722**, e discá-lo põe o Asterisk para decodificar ADPCM de 16 kHz, reamostrar
e recodificar em cada sentido:

```bash
MODO=transcode ./bench/bench.sh 300 40 15
```

Não exige reconfigurar o worker: a perna A é `PJSIP/<sellerSipUsername>`, então
o modo é só um campo diferente no corpo do POST. O áudio do pcap de G.722 é
ruído sintético de propósito (não há codificador de G.722 aqui) — mede CPU de
transcoding, não qualidade.

## Kamailio: registrar e location service

O `worker-asterisk` põe `ps_contacts` no Postgres compartilhado, então **todo
nó lê o contato de um telefone que registrou em outro nó** — e manda INVITE
para um caminho de volta que só aquele nó tem, porque o furo de NAT pertence a
quem recebeu o REGISTER. É o problema que um proxy resolve, e ele precisa
existir **antes do segundo nó**: com ~190 telefones já apontados direto para o
Asterisk, pôr um proxy na frente depois significa reapontar todos.

**Duas portas, e a direção sai da porta.** `5060` é a borda (telefones, único
lugar que aceita REGISTER); `5062` é o lado interno (Asterisk). Decidir a
direção pela porta em vez do IP de origem evita resolver o nome do Asterisk no
parse da config e reiniciar o Kamailio a cada troca de IP. Em produção é a mesma
topologia no mesmo host: Kamailio público em `:5060`, Asterisk atrás em `:5080`,
interno em `127.0.0.1:5062`.

**A localização vive no Postgres, não em memória** (`db_mode=3`, DB_ONLY). É o
ponto do exercício: a tabela `location` é a fonte da verdade, então outro nó lê
a mesma coisa sem replicação e sem cache para ficar velho. Custa uma ida ao
banco por lookup.

**O Path não funciona aqui — medido, não suposto.** O plano era gravar um Path
próprio em cada REGISTER, para o location dizer por qual proxy se chega naquele
contato. Implementado com `add_path_received()` e medido:

```
REGISTER 2001: supported=[path] path=[<null>]
```

`add_path_received()` insere o Path na cópia que vai ser **relayada**. Quando o
próprio Kamailio é o registrar não há relay, e o `save()` lê os headers da
mensagem recebida — onde o Path que acabamos de inserir não está. A função serve
a um proxy de *borda* que encaminha o REGISTER para um registrar separado.

A consequência vale mais que a linha de config: **um Kamailio por nó, cada um
sendo seu próprio registrar sobre a mesma tabela, não resolve o problema** — o
nó B enxerga o contato e não sabe que precisa passar pelo A. A topologia que
funciona é **um Kamailio na borda para N Asterisks**, que é a que este lab monta.
Se um dia cada nó ganhar o seu, o desenho tem de virar borda + registrar central.

**A mídia passa pelo rtpengine**, ancorada no proxy. Isso põe um **segundo teto
de capacidade** no caminho, e ele não tem número: o teto do Asterisk está medido
na issue #1, o do rtpengine não foi medido por ninguém.

### Provar que funciona

```bash
docker compose up -d
docker exec lab-postgres psql -U lab -d lab -c 'select username, contact, received from location'
```

E uma chamada pelo caminho novo — o worker não muda, só o ramal discado:

```bash
curl -X POST localhost:8092/v1/calls -H 'content-type: application/json' \
  -d '{"organizationId":"org_lab","targetPhone":"5541988887777","sellerSipUsername":"2001"}'

docker exec lab-fone-2001 asterisk -rx 'core show channels concise'   # PJSIP/fone ... Up ... Echo
docker logs lab-rtpengine | grep 'Confirmed peer address'             # os dois lados da mídia
```

> O esquema do Kamailio (`db/02-kamailio.sql`) só roda na **primeira**
> inicialização do volume do Postgres. Num banco que já existe:
> `docker exec -i lab-postgres psql -U lab -d lab < db/02-kamailio.sql`

## Detalhes que importam

- **`/var/spool/asterisk/recording` não existe na imagem.** Sem criá-lo,
  `POST /bridges/<id>/record` devolve 500 "No such file or directory" e a
  gravação some sem erro visível no worker. O `command:` do compose cria.
- **`DATABASE_URL` no mini-back não é opcional na prática.** Sem a linha `call`
  no banco, o fallback do `ChannelDestroyed` (perna que cai antes do bridge) não
  resolve o `callId` por canal, a chamada nunca é finalizada e o gauge de ativas
  do shard vaza — foi assim que a primeira corrida travou a drenagem.
- **Matar o worker no meio de uma chamada deixa bridge órfã.** Reproduzido aqui
  com `kill -9`, e é a explicação mais provável das 7 bridges vazias encontradas
  em produção: sem `finalize`, ninguém chama `destroyBridge`.
