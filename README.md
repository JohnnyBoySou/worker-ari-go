# worker-ari-go

Worker ARI em Go. Port de `worker-ari` (Bun/TypeScript), pensado para rodar como
**sidecar 1:1 do Asterisk** — dono único do app Stasis `connect-<shardId>` e de
todo o controle REST daquele nó.

## Por que Go

**Não é vazão.** Depois do sharding, cada shard vê 6–11 CPS (a 100k simultâneas
são 555 CPS divididos por ~50-100 nós). O runtime nunca foi o gargalo. Os motivos
reais são outros:

1. **Coabitação com o Asterisk.** O worker deixa de ser remoto e passa a rodar no
   mesmo host de um processo sensível a jitter de RTP. O GC do Go tem pausas
   sub-milissegundo e footprint previsível; um runtime JS no mesmo host, com
   pausas de dezenas de ms, vira jitter de áudio.
2. **Densidade.** Binário estático (~15 MB, RSS ~30–50 MB) contra ~80–150 MB. Em
   ~100 nós, é RAM que volta para chamadas.
3. **Ecossistema de infra.** O transporte de comandos saiu do BullMQ — que é um
   protocolo JS-first: enfileirar de fora do Node exige reimplementar um script
   Lua com args em msgpack lido de dentro do pacote. O primeiro produtor
   poliglota do sistema esbarra nisso na primeira linha.
4. **Concorrência.** Goroutine por chamada mapeia direto na máquina de estados
   por `callId`.

## A regra do port

O original tem **1.089 linhas de lógica cara de aprender** — cada guarda existe
porque um incidente aconteceu. A disciplina foi **port fiel, sem melhorar de
passagem**: mesma ordem de operações, mesmos erros engolidos, mesmos logs.

Duas traduções são deliberadas e estão documentadas no código:

### 1. O mutex que não existia no original (`internal/orchestrator/state.go`)

O worker em TypeScript é **single-threaded**: o event loop serializa os handlers
e os `Map` só podiam ser observados entre `await`s — por isso o original não tem
um único lock. Em Go, o consumidor de comandos e os eventos do ARI rodam em
goroutines **concorrentes**, e acesso concorrente a map não é um bug sutil: é
panic do runtime.

> **A regra, aplicada mecanicamente:** cada bloco contíguo de operações de mapa
> que no TS **não tem `await` entre elas** vira **uma única seção crítica** aqui.

É por isso que existe `withRef` (mutação atômica sobre o ref vivo) e por que os
getters devolvem **cópia** — entregar o ponteiro convidaria o chamador a mutá-lo
depois de um await, fora do lock.

`go test -race ./...` é o alvo do CI: sem o `-race`, os testes de concorrência
passariam mesmo com o estado desprotegido.

### 2. O selo de geração virou um laço sequencial (`internal/ari/events.go`)

O original carrega um `this.generation` porque callbacks de WebSocket em JS não
são canceláveis: um socket abandonado ainda entrega `close` e `message`
atrasados. Sem o selo isso viraria (a) um segundo laço de reconexão em paralelo e
(b) um `StasisStart` do zumbi originando de novo uma perna que já tem dono.

Em Go o laço de `Run` dá as **mesmas três garantias**, estruturalmente: só existe
uma conexão sendo lida por vez; a prova de vida do app é goroutine filha do
contexto da conexão (morre junto); e a reconexão é o passo seguinte do mesmo
laço, não um callback. **Não há selo porque não há o que selar.**

## O watchdog (incidente de 18/08/2026)

O socket ficou **meio-aberto**: o Asterisk perdeu o registro do app, mas nenhum
`close` chegou ao cliente. O REST seguia respondendo 200, então nada acusava — a
perna do vendedor caía num Stasis sem dono, o lead nunca era discado e a tela
ficava em "Chamando…" por 3 dias.

A prova de vida **não pode ser o estado do socket** — é ele que mente. Por isso
`CheckApp` pergunta ao Asterisk pelo REST, e **só o 404 autoriza reconectar**:
qualquer outro erro é ambíguo (pode ser a checagem, não a conexão), e derrubar um
socket saudável por um blip seria trocar um bug por outro.

## Layout

| Pacote | Papel |
|---|---|
| `internal/ari` | Client ARI: REST + WebSocket de eventos + watchdog do app |
| `internal/orchestrator` | Máquina de estados da chamada (outbound, inbound, finalize, reidratação) |
| `internal/orchestrator/ai.go` | Caminho de IA **isolado** (AudioSocket, supervisão, takeover) |
| `internal/orchestrator/state.go` | Estado sob mutex — ver "A regra do port" |
| `internal/commands` | Consumidor NATS JetStream (`ari.cmd.<shardId>`) |
| `internal/shard` | Identidade do shard + registro no Redis com heartbeat/poda |
| `internal/drain` | Espera as chamadas em curso antes de encerrar (scale-in do ASG) |
| `internal/store` | Postgres (mesma tabela `call` do back) |
| `internal/httpapi` | `/health` e `/metrics` |

## Sharding

Cada nó tem o **seu** app (`connect-<shardId>`) e o seu worker. A regra antiga —
um dono único global — não foi afrouxada, foi **multiplicada**: continua valendo
um dono único **por app**.

- `SHARD_ID` é **obrigatório** aqui (o subject é `ari.cmd.<shardId>`).
- O nome do app e o do subject derivam do **mesmo** `SHARD_ID`: envs
  independentes permitiriam o worker escutar um app e consumir os comandos de
  outro shard — sintoma "comando aceito, chamada nunca montada".
- O nome do app tem que ser **idêntico** ao `ARI_APP` do `worker-asterisk` do nó.
  Divergiu, o watchdog acusa 404 em até 30 s e o `/health` cai para 503.

### Admissão

`ARI_MAX_CALLS` é o teto do nó. No teto o worker **recusa** com
`failureReason=shard_at_capacity`, sem originar. É um **backstop**: quem deveria
evitar isso é o produtor, escolhendo um shard com folga no registro. Vale recusar
porque a falha no teto não é graciosa — passar da capacidade de mídia degrada o
áudio de **todas** as chamadas do nó.

### Drenagem

`SIGTERM` → marca `draining` → **sai do registro** (o produtor para de escolher
este shard na hora, sem esperar o TTL) → pausa o consumo → **espera as chamadas
em curso** → fecha. O `terminating:wait` do lifecycle hook do ASG tem que ser
**≥ `ARI_DRAIN_TIMEOUT_MS`**.

> Drenando, o `/health` continua **200**. Quem tira o nó do roteamento é a saída
> do registro; o `/health` é o healthcheck do **supervisor**, e reprovar durante a
> drenagem o faria matar o processo no meio das chamadas que a drenagem existe
> para preservar.

## Rodar

```bash
go test -race ./...     # o -race é o que dá sentido aos testes de concorrência
go build ./cmd/worker
SHARD_ID=a1 DATABASE_URL=postgres://... ./worker
```

Envs: mesmo contrato do worker em TypeScript, para os dois rodarem com o **mesmo
`.env`** durante a migração. Ver `internal/config/config.go`.
