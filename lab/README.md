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
docker compose up -d
set -a && . ./.env.lab && set +a

(cd ..          && go build -o /tmp/lab-worker ./cmd/worker) && /tmp/lab-worker &
(cd ../../mini-back && go build -o /tmp/lab-back   ./cmd/server) && PORT=8092 /tmp/lab-back &

curl -X POST localhost:8092/v1/calls -H 'content-type: application/json' \
  -d '{"organizationId":"org_lab","targetPhone":"5541988887777","sellerSipUsername":"1001"}'
```

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
