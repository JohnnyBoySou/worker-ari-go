# lab — laboratório local do sistema de chamadas

> Vive dentro do `worker-ari-go` porque é o harness que o validou ponta a ponta
> contra um Asterisk real. Também exercita o `mini-back` (`../../mini-back`) e a
> configuração equivalente à do `worker-asterisk`.

Sobe a stack inteira em Docker e roda o **fluxo completo de uma chamada** sem
operadora, sem softphone humano e sem custo.

```
mini-back (Go)  ──NATS ari.cmd.lab──▶  worker-ari-go  ──ARI──▶  Asterisk 20
     │                                                              │  │
     └──▶ Postgres (linha `call`)                          PJSIP/1001  PJSIP/…@trunk
                                                                │         │
                                                            SIPp uas   SIPp uas
                                                            (atende)   (atende)
```

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
