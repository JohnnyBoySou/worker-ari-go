# Benchmark — capacidade de um nó

Medido em **12 vCPU**, com **mídia RTP real** fluindo nas duas pernas
(g711a.pcap pelo SIPp), gravação ligada e com ulaw/alaw permitidos nos dois
lados.

> Estes números são **desta máquina**, não de um nó de produção. O que vale
> levar é a metodologia, a forma da curva e os tetos que apareceram — não os
> valores absolutos.
>
> E são do **harness antigo**: a mídia tocava uma vez só, o CPS batia no teto do
> próprio script e nada media perda, jitter ou se havia transcoding. O que foi
> consertado desde então está em "O que o harness mede hoje"; a rodada nova
> ainda não foi feita.

## A curva

| Simultâneas | Sucesso | p50 | p95 | p99 | máx | CPU do Asterisk |
|---:|---:|---:|---:|---:|---:|---:|
| 50 | 100% | 8 ms | 11 ms | 16 ms | 16 ms | 19% |
| 150 | 100% | 12 ms | 18 ms | 20 ms | 26 ms | 115% |
| 300 | 100% | 12 ms | 19 ms | 21 ms | 24 ms | 238% |
| 600 | 100% | 12 ms | 16 ms | 19 ms | 22 ms | 403% |
| **1196** | 100% | 14 ms | **109 ms** | **223 ms** | 365 ms | 486% |
| **1833** | quebrou | 26 ms | **31 s** | **48 s** | 53 s | **1033%** |

CPU é percentual de **um** core; a máquina tem 1200% disponíveis.

**Joelho entre 600 e 1200.** Até 600 a latência de setup é plana (p99 ≤ 21 ms) e
o custo é ~0,67% de CPU por chamada. Em ~1200 o p99 sobe 10×. Em ~1800 o
Asterisk satura (1033% de 1200%), o setup vai a dezenas de segundos e o
desligamento não vaza — sobraram 1.9k canais.

**Faixa sustentável nesta máquina: ~600 simultâneas**, com folga de CPU e
latência plana.

## O teto que aparece primeiro não é CPU

Na primeira rodada o sistema travava em **~59 chamadas** com a CPU em 33%. A
causa:

```
WARNING res_rtp_asterisk.c:3596 create_new_socket:
  Unable to allocate RTCP socket: Too many open files
```

O container rodava com `ulimit -n = 1024`. Cada canal com mídia consome vários
descritores (socket RTP + RTCP, por perna). A falha **não aparece como erro de
chamada**: ela simplesmente não monta, e o diagnóstico honesto só veio de olhar
o log do Asterisk.

> **Isto vale para produção.** No VPS o systemd tem `LimitNOFILE=524288`, mas o
> processo roda com **soft limit de 1024** — o mesmo teto. E o `worker-asterisk`
> não configura `nofile` em lugar nenhum.

Com `nofile=65536`, 150 simultâneas passaram de 59/150 para **150/150**.

## O que o harness mede hoje

A primeira rodada tinha quatro limitações conhecidas. Três foram consertadas; a
que sobra está no fim desta seção. **Nenhum dos números da tabela acima foi
medido com o harness corrigido** — eles continuam valendo como forma de curva,
mas a próxima rodada é que dá os valores para comparar.

**CPS agora é medida, e o harness acusa quando o teto é dele.** O disparo era
`curl` sequencial com `sleep 1/CPS`: somava a latência do POST ao intervalo e
travava em ~50/s, sem nada no relatório dizendo que o limite era do script.
Agora cada chamada tem um horário calculado antes do teste e um disparador
dedicado dorme até ele; o atraso de cada disparo em relação ao seu horário é
medido. Contra um back de mentira que responde em 40 ms, o disparo sustentou
**≥500 CPS** nesta máquina — 10× o teto anterior.

E quando o ritmo não é sustentado, o script diz **de quem é a culpa**, que é a
pergunta que a versão anterior não respondia. Disparo fora do horário tem duas
causas opostas: pool pequeno demais (culpa do script) ou disparadores presos
esperando o back responder (achado de verdade, do sistema). A p95 da latência do
POST contra a p50 separa os dois casos, e o veredito sai escrito.

**A mídia dura a chamada inteira — com um pcap longo, não com um laço.** O
`play_pcap_audio` roda uma vez: com o `g711a.pcap` da imagem, de duração
desconhecida, o RTP parava no meio de um teste que segura a chamada por 15s —
sem erro, sem sintoma, medindo uma carga que não era a que se quis medir.

A correção óbvia era um laço no cenário (`ontimeout` rearmando o play). Ela foi
feita, e depois **medida e desfeita**: o SIPp replica o pcap byte a byte, então
cada volta reinicia a sequência RTP em 0 com o mesmo SSRC. Com pcap de 10s, o
mesmo canal reporta:

| momento | `lp` | `txmes` |
|---|---:|---:|
| 5 s (antes da 1ª volta) | 0 | 88,1 |
| 13 s | 0 | 88,1 |
| 24 s (2ª volta) | **65036** | **20,0** |

65036 é −500 em 16 bits, e 500 é exatamente o número de pacotes do pcap. A mídia
continuava fluindo e a estatística de perda virava lixo — o laço mantinha a
carga e destruía a medição que existe para justificá-la. Hoje o pcap tem **300s
e toca uma vez**: a sequência nunca volta atrás, e o relatório imprime os
pacotes recebidos por perna contra o esperado, então mídia que pare antes do fim
aparece em vez de passar batido.

**Transcoding tem um modo.** O endpoint `1003` fala só G.722 contra a perna B em
G.711, então `MODO=transcode ./bench.sh` põe o Asterisk para decodificar ADPCM
de 16 kHz, reamostrar e recodificar em cada sentido. Não muda nada no worker: a
perna A é `PJSIP/<sellerSipUsername>`, então o modo é um campo diferente no
corpo do POST. **O número ainda não foi medido** — o modo existe, a rodada não
foi feita. É a variável de maior alavancagem que continua em aberto.

**A colisão de ids do `mini-back` foi corrigida na raiz.** O gerador derivava 24
dígitos de um LCG semeado com `time.Now().UnixNano()`, e duas requisições no
mesmo nanossegundo produziam o mesmo id — a segunda sobrescrevia a linha da
primeira (1196 linhas para 1200 disparos). Com o disparo agora paralelo isso
deixaria de ser sorte: medido em teste, o gerador antigo dá **954 colisões em
12.800 ids** gerados de 64 goroutines. Agora é `crypto/rand`, com regressão
concorrente no teste. Uma taxa de sucesso medida sobre um contador que perde
linhas não é um resultado.

## O que passou a ser medido, e não era

**Qualidade da mídia.** Latência de setup plana não diz nada sobre o que o
cliente escuta: um nó com áudio picotado passa como sucesso desde que a
sinalização continue rápida. Cada rodada agora amostra pernas vivas e lê
`CHANNEL(rtcp,all)` pelo ARI — perda e jitter medidos pelo **próprio Asterisk**,
não estimados de fora — e reporta a distribuição junto com a amostra crua de uma
perna, para as unidades serem conferíveis.

É `rtcp`, não `rtpqos`: `CHANNEL(rtpqos,...)` é de `chan_sip` e num canal PJSIP
devolve `Unable to read provided function` — com HTTP 200 e a mensagem no corpo,
então o parser via campo vazio e a rodada reportava "nenhuma perna respondeu"
sem dizer por quê. O contador de perda também ganhou guarda de plausibilidade:
`lp` volta como inteiro sem sinal, então sequência que anda para trás vira um
número gigante, e somá-lo produziria um percentual inventado.

**Se há transcoding — e há. Medido, não deduzido.** O relatório afirmava
passthrough por dedução dos codecs configurados. Rodado contra o Asterisk do
lab, com gravação ligada:

```
transcoding: 40 de 40 pernas com formato de leitura fora dos nativos
  (alaw) -> slin               40 pernas
bridges por tecnologia: 21xsoftmix
```

**Nenhuma perna estava em passthrough.** A gravação de bridge acrescenta um
canal à bridge; bridge de três partes usa `softmix`; e no `softmix` tudo passa
por `slin`, independentemente dos codecs negociados. "Sem transcoding" nunca foi
verdade em nenhuma rodada com gravação ligada.

Consequência para o dimensionamento: os 0,672% de CPU por chamada **já incluem**
alaw↔slin em softmix. O `transcoding_factor=1.0` do `call-infra` não descreve
passthrough — descreve *softmix com G.711*, que é barato (tabela de lookup) mas
não é nada. E o modo G.722 vai medir o custo *adicional* de um codec caro sobre
essa base, não a diferença entre passthrough e transcoding.

**File descriptors contra o limite do processo.** O teto que apareceu primeiro
agora está na tabela de cada rodada (`fds` por segundo, e o pico contra o limite
efetivo lido de `/proc/1/limits`). O modo de falha era invisível por construção:
a chamada não monta e não gera erro de chamada.

## O que continua em aberto

- **O caminho de gravação nunca foi exercitado — em nenhuma rodada da série.**
  100% dos uploads voltaram `NoSuchBucket`. O LocalStack do lab sobe com
  `PERSISTENCE=0` e perde o bucket a cada `docker compose up`, enquanto o
  `terraform.tfstate` do `call-infra` continua afirmando que ele existe — então
  nem o `apply` era refeito. O 404 é rápido: não contaminou a latência de setup,
  não derrubou chamada nenhuma e não apareceu em número nenhum da tabela. O que
  ficou sem medida é justamente a carga: em produção são ~450 KB por chamada
  (uma gravação de 20s no lab deu 530 KB) atravessando o funil de download pelo
  ARI + `PutObject`, com no máximo 8 em voo, e todos começando *depois* do
  desligamento. `bench.sh` agora aborta se o bucket não responder (use `REC=0`
  para medir sem gravação de propósito) e mede o funil por três fontes
  independentes: o banco (`recording_url` persistida), o bucket (`Size` e
  `LastModified` de cada objeto — volume, vazão em MB/s e o atraso entre o fim
  da chamada e o `PutObject`) e o `/metrics` do worker
  (`recordings_inflight` contra `recordings_max`, amostrado *durante* o dreno,
  que é a única janela em que dá para ver o semáforo cheio).
- **A rodada com o harness corrigido não foi feita.** Toda a tabela acima vem do
  harness antigo: mídia tocando uma vez, CPS limitado pelo script, sem número de
  perda ou jitter.
- **Transcoding continua sem número.** O modo existe; a medição, não.
- **O tempo de sustentação era menor do que o parâmetro pedia.** O laço de
  amostragem contava iterações, não segundos, e cada volta gasta mais de um
  segundo só no `docker stats` — `./bench.sh 600 80 15` segurava bem mais que
  15s. Corrigido para relógio, mas isso significa que a duração real das rodadas
  da tabela não é a que está escrita nelas.

## Como reproduzir

```bash
cd lab
python3 bench/mkpcap.py                  # antes do up: o SIPp carrega o pcap ao parsear
docker compose up -d
./bench/bench.sh <simultaneas> [cps] [segundos]

./bench/bench.sh 600 80 15               # a faixa sustentável desta máquina
MODO=transcode ./bench/bench.sh 300 40 15  # perna A em G.722
```

A latência sai do banco (`started_at - created_at`), que é o tempo que o cliente
sente entre clicar e o telefone tocar. Perda, jitter, formatos e tecnologia de
bridge saem do próprio Asterisk, com as chamadas ainda de pé.
