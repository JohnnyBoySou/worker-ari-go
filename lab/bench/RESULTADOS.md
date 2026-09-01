# Benchmark — capacidade de um nó

Medido em **12 vCPU**, com **mídia RTP real** fluindo nas duas pernas
(g711a.pcap pelo SIPp), gravação ligada e sem transcoding (ulaw e alaw
permitidos nos dois lados).

> Estes números são **desta máquina**, não de um nó de produção. O que vale
> levar é a metodologia, a forma da curva e os tetos que apareceram — não os
> valores absolutos.

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

## Limitações do próprio harness

- **CPS não foi medida acima de ~50/s.** O `bench.sh` dispara com `curl`
  sequencial; o teto é do harness, não do sistema. Medir CPS de verdade pede
  disparo paralelo.
- **A mídia toca uma vez.** O `play_pcap_audio` roda uma vez por chamada; em
  chamadas longas o RTP para antes do fim. Para carga sustentada de mídia, o
  cenário precisa de laço.
- **Sem transcoding.** Os dois lados falam ulaw/alaw, então é passthrough. O
  §5 do `PLANO-VOZ-HUMANA.md` prevê que transcoding derruba `calls_por_no` em
  ~3× — isso ainda não foi medido.
- O gerador de id do `mini-back` **colide em taxa alta** (1196 linhas para 1200
  disparos). É ferramenta de laboratório; o back real usa UUID.

## Como reproduzir

```bash
cd lab && docker compose up -d
./bench/bench.sh <simultaneas> [cps] [segundos]
./bench/bench.sh 600 80 15
```

A latência sai do banco (`started_at - created_at`), que é o tempo que o cliente
sente entre clicar e o telefone tocar.
