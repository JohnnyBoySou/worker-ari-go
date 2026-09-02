#!/usr/bin/env bash
# Benchmark de capacidade do no.
#
# O QUE ELE MEDE
#   - CPS: pedido, OFERECIDO e ACEITO. Os tres, porque a primeira versao deste
#     script disparava com curl sequencial e travava em ~50/s: o teto medido era
#     do harness, e nada no relatorio dizia isso. Agora o disparo e paralelo com
#     agenda de horarios, e quando o proprio harness nao acompanha o ritmo ele
#     ACUSA em vez de devolver um numero baixo com cara de resultado.
#   - concorrencia sustentada: quantas simultaneas o no segura
#   - latencia de setup (p50/p95/p99): created_at -> started_at, direto do banco.
#     E o numero que o cliente sente: o tempo entre clicar e o telefone tocar.
#   - QUALIDADE DA MIDIA: perda e jitter por perna, do RTP do proprio Asterisk.
#     Sem isso, um no com audio picotado passa como sucesso desde que a
#     sinalizacao continue rapida - e latencia de setup plana nao diz nada sobre
#     o que o cliente ESCUTA.
#   - TRANSCODING: se esta havendo, em quantas pernas, e em que tecnologia de
#     bridge. Deixa de ser suposicao do relatorio e vira medida da rodada.
#   - GRAVACOES: quantas chegaram ao bucket e quanto tempo a fila levou para
#     drenar depois do desligamento. Sem isto o caminho de gravacao falhava
#     inteiro (NoSuchBucket) sem aparecer em numero nenhum do relatorio: o 404 e
#     rapido, nao suja a latencia de setup, e a rodada saia com cara de verde.
#   - file descriptors do Asterisk contra o limite do processo. Foi o primeiro
#     teto a aparecer (~59 chamadas com a CPU em 33%) e nao gera erro de chamada:
#     a chamada simplesmente nao monta.
#
# COMO USAR
#   python3 mkpcap.py                       # uma vez, antes do compose up
#   ./bench.sh <simultaneas> [cps] [segundos_segurando]
#   ./bench.sh 600 80 15
#   MODO=transcode ./bench.sh 300 40 15     # perna A em G.722: forca transcoding
#
# VARIAVEIS
#   MODO=normal|transcode   PAR=<disparadores>   AMOSTRA_MIDIA=<canais>
#   REC=0 pula a checagem e a medicao de gravacao   REC_ESPERA=<segundos>
set -uo pipefail

AQUI="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

ALVO=${1:-50}
CPS=${2:-10}
HOLD=${3:-20}

MODO=${MODO:-normal}
# Tamanho do pool de disparo. Cada disparador dorme ate o SEU horario e so
# entao faz o POST, entao o pool precisa cobrir CPS x latencia_do_POST: com 100ms
# de latencia, um pool do tamanho do CPS cobre folgado. Fora da faixa o default
# satura o harness (embaixo) ou desperdica processo (em cima).
if [ -z "${PAR:-}" ]; then
  PAR=$CPS
  [ "$PAR" -lt 16 ]  && PAR=16
  [ "$PAR" -gt 256 ] && PAR=256
fi
AMOSTRA_MIDIA=${AMOSTRA_MIDIA:-40}
REC=${REC:-1}
# Teto da espera pela fila de upload drenar. O semaforo e de 8 gravacoes
# simultaneas (DefaultRecordingConcurrency) e cada uma ainda espera o Asterisk
# finalizar o WAV com backoff, entao 600 chamadas nao drenam em segundos.
REC_ESPERA=${REC_ESPERA:-90}

BACK=${BACK:-http://127.0.0.1:8092}
WORKER=${WORKER:-http://127.0.0.1:8091}
ARI=${ARI:-http://127.0.0.1:8088}
ARI_AUTH=${ARI_AUTH:-connect-ari:lab-ari-secret}
PG="docker exec lab-postgres psql -U lab -d lab -tAc"
# Mesmos defaults do lab/.env.lab: o que o worker usa para gravar e o que este
# script confere. Divergir aqui seria checar um bucket e medir outro.
export S3=${AWS_ENDPOINT_URL:-http://127.0.0.1:4566}
export BUCKET=${AWS_S3_BUCKET_NAME:-lab-recordings}

case "$MODO" in
  normal)    RAMAL_A=1001 ;;
  transcode) RAMAL_A=1003 ;;
  *) echo "MODO invalido: $MODO (use normal ou transcode)" >&2; exit 2 ;;
esac

# Os pcaps sao carregados pelo SIPp no PARSE do cenario, nao na hora de tocar:
# faltando, os containers uas ja subiram quebrados e o benchmark mediria
# sinalizacao com silencio no lugar da midia.
for f in media/alaw-300s.pcap media/g722-300s.pcap; do
  if [ ! -f "$AQUI/$f" ]; then
    echo "faltando $f. Rode: python3 $AQUI/mkpcap.py && docker compose restart uas-1001 uas-1002 uas-1003" >&2
    exit 2
  fi
done

# O BUCKET PRECISA EXISTIR ANTES DA RODADA, pelo mesmo motivo dos pcaps: sem
# ele a rodada mede um sistema em que a gravacao nunca subiu, e nada denuncia.
#
# O LocalStack do lab sobe com PERSISTENCE=0, entao o bucket morre a cada
# `docker compose up` -- enquanto o call-infra/envs/localstack/terraform.tfstate
# continua dizendo que ele existe. Nesse estado 100% dos uploads voltam
# NoSuchBucket: um 404 rapido, que NAO aparece na latencia de setup, nao derruba
# chamada nenhuma e nao entra em nenhum numero do relatorio.
if [ "$REC" = "1" ]; then
  cod_bucket=$(curl -s -o /dev/null -w '%{http_code}' -m 5 -I "$S3/$BUCKET" 2>/dev/null || echo 000)
  if [ "$cod_bucket" != "200" ]; then
    {
      echo "bucket '$BUCKET' indisponivel em $S3 (HTTP $cod_bucket)."
      echo "Recrie antes de medir:  cd ../../../call-infra && make apply && make smoke"
      echo "(o tfstate pode afirmar que o bucket existe: PERSISTENCE=0 o apaga no up)"
      echo "Para medir de proposito sem gravacao no bucket: REC=0 $0 $*"
    } >&2
    exit 2
  fi
fi

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

agora_ms() { echo $(( $(date +%s%N) / 1000000 )); }
dorme_ms() { [ "$1" -gt 0 ] && sleep "$(( $1 / 1000 )).$(printf '%03d' $(( $1 % 1000 )))"; return 0; }

# Uma variavel de canal pelo ARI. O ARI avalia funcoes do dialplan, entao isto
# alcanca CHANNEL(rtcp,...) - a fonte honesta de perda e jitter, medida pelo
# proprio Asterisk, e nao uma estimativa do lado de fora.
#
# E `rtcp`, nao `rtpqos`: CHANNEL(rtpqos,...) e de chan_sip e devolve
# "Unable to read provided function" num canal PJSIP -- silenciosamente, porque
# o ARI responde 200 com a mensagem de erro no corpo e o parser so via campo
# vazio. Foi assim que a primeira rodada com medicao de midia reportou
# "nenhuma perna respondeu" sem dizer por que.
var_canal() {
  curl -s -m 5 -u "$ARI_AUTH" --get --data-urlencode "variable=$2" \
    "$ARI/ari/channels/$1/variable" 2>/dev/null \
    | sed -n 's/.*"value" *: *"\(.*\)".*/\1/p'
}

asterisk_cli() { docker exec lab-asterisk asterisk -rx "$1" 2>/dev/null; }
asterisk_fds() { docker exec lab-asterisk sh -c 'ls /proc/1/fd 2>/dev/null | wc -l' 2>/dev/null; }

echo "=== benchmark: ${ALVO} simultaneas a ${CPS} CPS, segurando ${HOLD}s ==="
echo "maquina: $(nproc) vCPU, $(free -g | awk '/^Mem/{print $2}')GB"
FD_LIMITE=$(docker exec lab-asterisk sh -c "awk '/open files/{print \$4}' /proc/1/limits" 2>/dev/null)
echo "modo: $MODO (perna A = PJSIP/$RAMAL_A)   disparadores: $PAR   fds do asterisk: limite ${FD_LIMITE:-?}"
echo

# Estado limpo: numeros so significam algo partindo do zero.
$PG 'TRUNCATE "call"' >/dev/null 2>&1
# O bucket NAO e limpo junto: `destroy`/`apply` e ciclo do call-infra, nao deste
# script. Quem separa esta rodada das anteriores e o join por callId la embaixo.

# --- disparo, com ritmo e em paralelo ---------------------------------------
#
# A versao sequencial (curl; sleep 1/CPS) somava a latencia do POST ao intervalo
# e nunca passava de ~50/s. Aqui cada chamada tem um HORARIO fixo, calculado
# antes de tudo comecar, e um disparador dedicado dorme ate ele. O atraso de
# cada disparo em relacao ao seu horario e o que mede se o HARNESS aguentou.
export TMP BACK
export PAYLOAD="{\"organizationId\":\"org_lab\",\"targetPhone\":\"5541988887777\",\"sellerSipUsername\":\"$RAMAL_A\",\"callerDid\":\"5541999999999\"}"

cat > "$TMP/dispara.sh" <<'EOS'
#!/usr/bin/env bash
# $1 = indice (so para leitura de log), $2 = horario alvo em ms epoch
alvo=$2
falta=$(( alvo - $(date +%s%N) / 1000000 ))
if [ "$falta" -gt 0 ]; then
  sleep "$(( falta / 1000 )).$(printf '%03d' $(( falta % 1000 )))"
else
  # Saiu depois da hora: o pool nao acompanhou. Guardar o atraso e o que
  # distingue "o sistema nao aguentou" de "o script nao conseguiu perguntar".
  echo "$(( -falta ))" >> "$TMP/atrasos"
fi
# O instante do disparo entra na linha: a janela real do teste e do PRIMEIRO
# ao ULTIMO POST, e nao o relogio de parede do bloco (que inclui a folga
# inicial e a latencia do ultimo POST, e derrubaria o CPS oferecido de graca).
saiu=$(( $(date +%s%N) / 1000000 ))
resposta=$(curl -s -o /dev/null -m 10 -w '%{http_code} %{time_total}' \
  -X POST "$BACK/v1/calls" -H 'content-type: application/json' -d "$PAYLOAD")
echo "$resposta $saiu" >> "$TMP/disparo"
EOS
chmod +x "$TMP/dispara.sh"

T0=$(( $(agora_ms) + 500 ))   # meio segundo de folga para o pool encher
awk -v n="$ALVO" -v t0="$T0" -v c="$CPS" \
  'BEGIN{ for (i = 0; i < n; i++) printf "%d %d\n", i+1, t0 + (i * 1000.0 / c) }' \
  > "$TMP/agenda"

: > "$TMP/disparo"
DISPARO_INICIO=$(agora_ms)
xargs -P "$PAR" -n 2 "$TMP/dispara.sh" < "$TMP/agenda"
DISPARO_FIM=$(agora_ms)

JANELA=$(awk '{ if (!m || $3 < m) m = $3; if ($3 > M) M = $3 }
                END{ d = (M - m) / 1000; printf "%.3f", (d > 0 ? d : 0.001) }' "$TMP/disparo")
PAREDE=$(awk -v a="$DISPARO_INICIO" -v b="$DISPARO_FIM" 'BEGIN{printf "%.2f", (b-a)/1000}')
aceitas=$(awk '$1 == 201' "$TMP/disparo" | wc -l)
recusadas=$(awk '$1 != 201' "$TMP/disparo" | wc -l)
atrasadas=$( [ -f "$TMP/atrasos" ] && wc -l < "$TMP/atrasos" || echo 0 )
read -r LAT_P50 LAT_P95 LAT_MAX < <(sort -n -k2 "$TMP/disparo" | awk \
  '{v[NR]=$2} END{ if (!NR) { print "0 0 0"; exit }
     printf "%.0f %.0f %.0f\n", v[int(NR*0.5)+1]*1000, v[int(NR*0.95)+1]*1000, v[NR]*1000 }')

echo "disparo: ${JANELA}s do primeiro ao ultimo POST (${PAREDE}s de relogio)"
# n eventos entre o primeiro e o ultimo disparo cobrem n-1 intervalos: usar n/j
# infla a taxa em n/(n-1), invisivel em 1200 chamadas e grosseiro em 10.
awk -v n="$ALVO" -v ok="$aceitas" -v j="$JANELA" -v c="$CPS" \
  'BEGIN{ taxa = (n > 1 ? (n-1)/j : 0)
          printf "  CPS pedido=%.0f  oferecido=%.1f  aceito=%.1f\n", c, taxa, taxa*ok/n }'
echo "  aceitas(201): $aceitas   recusadas: $recusadas"
echo "  latencia do POST: p50=${LAT_P50}ms p95=${LAT_P95}ms max=${LAT_MAX}ms"
if [ "$recusadas" -gt 0 ]; then
  echo "  codigos:$(awk '$1 != 201 {print $1}' "$TMP/disparo" | sort | uniq -c | awk '{printf " %sx%s", $1, $2}')"
fi

# De quem e o teto? A pergunta que a versao anterior deste script nao respondia.
#
# Disparo fora do horario tem duas causas OPOSTAS, e confundi-las e o erro que
# faz um benchmark mentir: ou o pool e pequeno demais (culpa do script) ou os
# disparadores ficaram PRESOS esperando o back responder (culpa do sistema, e
# um achado de verdade). A p95 do POST contra a p50 separa os dois casos.
if [ "$atrasadas" -gt 0 ]; then
  p95a=$(sort -n "$TMP/atrasos" | awk '{v[NR]=$1} END{print v[int(NR*0.95)+1]}')
  pct=$(awk -v a="$atrasadas" -v n="$ALVO" 'BEGIN{printf "%.0f", 100*a/n}')
  echo "  disparos fora do horario: $atrasadas (${pct}%), p95 do atraso = ${p95a}ms"
  if [ "$pct" -ge 5 ]; then
    if [ "$LAT_P50" -gt 0 ] && [ "$LAT_P95" -gt $(( LAT_P50 * 3 )) ]; then
      echo "  >> Os disparadores ficaram presos no BACK (p95 do POST ${LAT_P95}ms contra p50 ${LAT_P50}ms)."
      echo "     Subir PAR nao ajuda: este teto e DO SISTEMA, e o numero acima de CPS aceito vale."
    else
      echo "  >> O HARNESS nao sustentou o ritmo, e nao foi espera do back."
      echo "     Este CPS e teto DO SCRIPT. Suba PAR (atual $PAR) ou dispare de mais de uma maquina."
    fi
  fi
fi

# --- amostragem enquanto segura ---------------------------------------------
#
# O laco e por RELOGIO, nao por iteracao. A versao anterior fazia `for s in
# $(seq 1 $HOLD); do ...; sleep 1; done`, e como cada volta ja gasta mais de um
# segundo so no `docker stats --no-stream`, `./bench.sh 600 80 15` segurava bem
# mais que 15s. O tempo de sustentacao e parametro do teste: ele nao pode
# depender de quanto o docker demorou para responder.
echo
printf "%-6s %-10s %-10s %-12s %-10s %-10s\n" "s" "ativas" "canais" "asterisk_cpu" "worker_rss" "fds"
pico_ativas=0; pico_cpu=0; pico_fds=0
HOLD_INICIO=$(agora_ms)
HOLD_FIM=$(( HOLD_INICIO + HOLD * 1000 ))
while [ "$(agora_ms)" -lt "$HOLD_FIM" ]; do
  volta=$(agora_ms)
  ativas=$(curl -s -m 5 "$WORKER/health" | python3 -c 'import sys,json;print(json.load(sys.stdin)["shard"]["activeCalls"])' 2>/dev/null || echo 0)
  canais=$(curl -s -m 5 -u "$ARI_AUTH" "$ARI/ari/channels" | python3 -c 'import sys,json;print(len(json.load(sys.stdin)))' 2>/dev/null || echo 0)
  stats=$(docker stats --no-stream --format '{{.CPUPerc}}' lab-asterisk 2>/dev/null | tr -d '%')
  rss=$(ps -o rss= -C lab-worker 2>/dev/null | awk '{s+=$1} END{printf "%.0fMB", s/1024}')
  fds=$(asterisk_fds)
  [ "${ativas:-0}" -gt "$pico_ativas" ] && pico_ativas=$ativas
  [ "${fds:-0}" -gt "$pico_fds" ] && pico_fds=$fds
  pico_cpu=$(python3 -c "print(max($pico_cpu, ${stats:-0}))")
  printf "%-6s %-10s %-10s %-12s %-10s %-10s\n" \
    "$(( ($(agora_ms) - HOLD_INICIO) / 1000 ))" "$ativas" "$canais" "${stats:-?}%" "${rss:-?}" "${fds:-?}"
  # Uma linha por segundo. O `docker stats --no-stream` sozinho ja gasta perto de
  # um segundo, entao no lab isto quase nunca dorme; sem o piso, uma amostragem
  # que falha rapido (lab fora do ar) viraria centenas de linhas de nada.
  dorme_ms $(( 1000 - ($(agora_ms) - volta) ))
done

# --- midia e transcoding, com as chamadas AINDA de pe ------------------------
#
# Tem de ser aqui: depois do terminate os canais somem e com eles as estatisticas
# de RTP. Amostra, nao censo - a pergunta e a forma da distribuicao, e uma
# amostra de algumas dezenas de pernas ja a responde sem somar centenas de
# requisicoes ao ARI no pior momento do teste.
echo
echo "=== midia (amostra de ate $AMOSTRA_MIDIA pernas, com as chamadas de pe) ==="
ids=$(curl -s -m 15 -u "$ARI_AUTH" "$ARI/ari/channels" \
      | python3 -c 'import sys,json;[print(c["id"]) for c in json.load(sys.stdin)]' 2>/dev/null \
      | shuf | head -n "$AMOSTRA_MIDIA")
: > "$TMP/midia"
for id in $ids; do
  nativo=$(var_canal "$id" 'CHANNEL(audionativeformat)')
  leitura=$(var_canal "$id" 'CHANNEL(audioreadformat)')
  qos=$(var_canal "$id" 'CHANNEL(rtcp,all)')
  # Separador TAB, nao `|`: o proprio nome do formato nativo pode trazer `|`
  # quando o canal tem varios ("(alaw|ulaw)"), e ai o parser comeria os campos.
  [ -n "$qos" ] && printf '%s\t%s\t%s\n' "${nativo:-?}" "${leitura:-?}" "$qos" >> "$TMP/midia"
done

if [ ! -s "$TMP/midia" ]; then
  echo "  nenhuma perna respondeu (chamadas ja caidas?)"
else
  echo "  amostra crua de uma perna: $(head -1 "$TMP/midia")"
  # O formato nativo vem entre parenteses e pode listar varios separados por
  # `|` ("(alaw|ulaw)"); o de leitura vem cru ("alaw"). Comparar as duas strings
  # direto acusaria transcoding em 100% das pernas, sempre. A pergunta certa e
  # se o formato de LEITURA esta ENTRE os nativos: se nao esta, houve traducao.
  awk -F'\t' -v hold="$HOLD" '
    function tem_traducao(nativos, leitura,   crus, k, n) {
      gsub(/[()]/, "", nativos)
      n = split(nativos, crus, /[|,\/]+/)
      for (k = 1; k <= n; k++) if (crus[k] == leitura) return 0
      return 1
    }
    { split($3, kv, ";"); for (i in kv) { split(kv[i], p, "="); v[p[1]] = p[2] }
      n++
      j[n] = v["rxjitter"] + 0
      c[n] = v["rxcount"] + 0
      # `lp` e a perda que o Asterisk deduz de buracos na sequencia RTP, e volta
      # como inteiro sem sinal: uma sequencia que ANDA PARA TRAS vira um numero
      # gigante (65036 = -500 em 16 bits). Somar isso na perda produziria um
      # percentual inventado, entao a perna implausivel e contada a parte em vez
      # de poluir o total. Foi assim que o laco do pcap se denunciou.
      if (v["lp"] + 0 > v["rxcount"] + 0) suspeitas++
      else { lp += v["lp"]; rx += v["rxcount"]; rlp += v["rlp"]; medidas++ }
      if ($1 != "?" && $2 != "?") {
        conhecidas++
        par[$1 " -> " $2]++
        if (tem_traducao($1, $2)) trans++
      }
      delete v }
    END {
      if (!n) exit
      for (a = 1; a < n; a++) for (b = a+1; b <= n; b++) if (j[b] < j[a]) { t = j[a]; j[a] = j[b]; j[b] = t }
      for (a = 1; a < n; a++) for (b = a+1; b <= n; b++) if (c[b] < c[a]) { t = c[a]; c[a] = c[b]; c[b] = t }
      printf "  pernas na amostra: %d\n", n
      printf "  pacotes RX por perna: p50=%d (esperado ~%d para %ds de midia a 50 pps)\n", \
             c[int(n*0.5)+1], hold*50, hold
      printf "  perda RX: %d perdidos em %d recebidos (%.3f%%) em %d pernas   perda relatada pelo par: %d\n", \
             lp, rx, (rx ? 100*lp/(lp+rx) : 0), medidas, rlp
      if (suspeitas) printf "  >> %d pernas com contador de perda implausivel (sequencia RTP andou para tras); fora do total acima\n", suspeitas
      printf "  jitter RX (s, como o Asterisk reporta): p50=%.6f p95=%.6f max=%.6f\n", \
             j[int(n*0.5)+1], j[int(n*0.95)+1], j[n]
      printf "  transcoding: %d de %d pernas com formato de leitura fora dos nativos\n", trans, conhecidas
      for (k in par) printf "    %-28s %d pernas\n", k, par[k]
    }' "$TMP/midia"
fi
echo "  bridges por tecnologia:$(asterisk_cli 'bridge show all' | grep -oE 'softmix|simple_bridge|native_rtp|holding_bridge' | sort | uniq -c | awk '{printf " %sx%s", $1, $2}')"
echo "  fds do asterisk no pico: $pico_fds de ${FD_LIMITE:-?}"

# --- desligamento ------------------------------------------------------------
echo
echo "desligando..."
for id in $($PG "select id from \"call\" where status in ('RINGING','IN_PROGRESS')" 2>/dev/null); do
  curl -s -o /dev/null -m 5 -X POST "$BACK/v1/calls/$id/terminate" &
done
wait
REC_T0=$(agora_ms)
# Sem REC, a espera fixa continua: o bloco de resultado le o banco e precisa do
# finalize assentado. Com REC, quem espera e o laco de gravacoes -- e ele espera
# MEDINDO, que e o unico jeito de ver o semaforo enquanto ele ainda esta cheio.
[ "$REC" = "1" ] || sleep 6

# --- gravacoes ---------------------------------------------------------------
#
# A gravacao e a UNICA parte do sistema que comeca depois que a chamada acaba, e
# por isso ficava fora de todo numero da rodada: o Asterisk so fecha o WAV quando
# a bridge cai, o worker baixa pelo ARI com backoff e so entao faz o PutObject,
# com no maximo `recordings_max` em voo. Sao ~450 KB por chamada nesse funil.
#
# As tres fontes sao independentes e nenhuma delas e estimativa:
#   banco     recording_url preenchida = a key PERSISTIDA (o que o player acha)
#   bucket    Size e LastModified de cada objeto = volume e instante do PutObject
#   /metrics  recordings_inflight contra recordings_max = saturacao do semaforo
if [ "$REC" = "1" ]; then
  echo
  echo "=== gravacoes ==="
  atendidas=$($PG 'select count(*) from "call" where started_at is not null' 2>/dev/null | tr -d ' ')
  atendidas=${atendidas:-0}
  REC_MAX=$(curl -s -m 3 "$WORKER/metrics" 2>/dev/null | awk '/^ari_worker_recordings_max /{print $2}')
  REC_MAX=${REC_MAX:-0}

  # O criterio de parada e o dreno ESTABILIZAR, nao um sleep fixo: sleep curto
  # reportaria "poucas gravacoes" quando a fila so estava andando, e e assim que
  # se inventa um gargalo que nao existe.
  salvas=0; anterior=-1; parado=0; voltas=0
  : > "$TMP/inflight"
  REC_FIM=$(( REC_T0 + REC_ESPERA * 1000 ))
  while [ "$(agora_ms)" -lt "$REC_FIM" ]; do
    volta=$(agora_ms)
    salvas=$($PG 'select count(*) from "call" where recording_url is not null' 2>/dev/null | tr -d ' ')
    salvas=${salvas:-0}
    # Amostra do semaforo NO MEIO do dreno: depois que ele esvazia, o gauge e
    # zero e nao resta vestigio de ter saturado.
    voo=$(curl -s -m 3 "$WORKER/metrics" 2>/dev/null \
          | awk '/^ari_worker_recordings_inflight /{print $2}')
    voo=${voo:-0}
    echo "$voo" >> "$TMP/inflight"
    voltas=$(( voltas + 1 ))
    # Completou, mas so sai depois de 3 amostras: uma leitura isolada do
    # semaforo nao distingue "nunca encheu" de "esvaziou antes de eu olhar".
    [ "$salvas" -ge "$atendidas" ] && [ "$atendidas" -gt 0 ] && [ "$voltas" -ge 3 ] && break
    # DESISTIR EXIGE AS TRES CONDICOES, e nao so "o numero parou de subir".
    #
    # A versao anterior parava depois de 5 voltas sem gravacao nova e reportava
    # 0 de N: nada tinha subido AINDA porque o worker estava no backoff
    # esperando o Asterisk fechar o WAV -- e os uploads apareciam no log do
    # worker depois que o script ja havia impresso o relatorio. Um dreno lento
    # saia como dreno falho.
    #
    #   inflight == 0   ninguem esta baixando nem subindo agora
    #   > 20s           passou a janela em que o primeiro backoff ainda corre
    #   parado 5x       o contador do banco confirma que estagnou
    if [ "$salvas" = "$anterior" ] && [ "$voo" -eq 0 ] \
       && [ $(( $(agora_ms) - REC_T0 )) -gt 20000 ]; then
      parado=$(( parado + 1 ))
      [ "$parado" -ge 5 ] && break
    else
      parado=0
    fi
    anterior=$salvas
    dorme_ms $(( 1000 - ($(agora_ms) - volta) ))
  done
  REC_DT=$(awk -v a="$REC_T0" -v b="$(agora_ms)" 'BEGIN{printf "%.1f", (b-a)/1000}')

  printf "  persistidas no banco (recording_url): %s de %s atendidas" "$salvas" "$atendidas"
  awk -v s="$salvas" -v a="$atendidas" 'BEGIN{printf " (%.1f%%)\n", (a ? 100*s/a : 0)}'
  echo "  dreno: ${REC_DT}s desde o terminate"

  # Ocupacao do semaforo. Media e pico contra o teto, mais a fracao do tempo em
  # que ficou CHEIO -- que e o unico dos tres que responde "o teto mordeu?".
  awk -v teto="$REC_MAX" '
    { n++; soma += $1; if ($1 > pico) pico = $1; if (teto > 0 && $1 >= teto) cheio++ }
    END {
      if (!n) { print "  semaforo: sem amostra (worker fora do ar?)"; exit }
      printf "  semaforo de upload: media=%.1f pico=%d de %s em voo", soma/n, pico, (teto ? teto : "?")
      printf "   saturado em %.0f%% das %d amostras\n", 100*cheio/n, n
      if (teto > 0 && cheio > n/5)
        printf "  >> a fila passou %.0f%% do dreno no teto: o gargalo e o semaforo, nao a rede\n", 100*cheio/n
    }' "$TMP/inflight"

  # Volume e tempo, do proprio bucket. As keys sao recordings/<org>/<callId>.wav
  # e o TRUNCATE ja zerou o banco, entao o join por callId descarta sozinho os
  # objetos de rodadas anteriores e o do smoke -- sem precisar limpar o bucket.
  $PG "select id || '|' || to_char(ended_at, 'YYYY-MM-DD\"T\"HH24:MI:SS')
       from \"call\" where ended_at is not null" 2>/dev/null \
    | python3 -c '
import os, re, sys, urllib.parse, urllib.request
from datetime import datetime

s3, bucket = os.environ["S3"], os.environ["BUCKET"]

fim = {}
for linha in sys.stdin:
    linha = linha.strip()
    if "|" in linha:
        cid, quando = linha.split("|", 1)
        try:
            fim[cid] = datetime.strptime(quando, "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            pass

# ListObjectsV2 pagina em 1000 keys. Sem seguir o continuation-token, uma rodada
# de 600 num bucket ja usado leria so o primeiro pedaco e reportaria volume a
# menos -- silenciosamente, que e o pior jeito de errar.
objs, token = [], None
while True:
    q = {"list-type": "2", "prefix": "recordings/"}
    if token:
        q["continuation-token"] = token
    with urllib.request.urlopen(f"{s3}/{bucket}?" + urllib.parse.urlencode(q), timeout=20) as r:
        xml = r.read().decode()
    objs += re.findall(
        r"<Key>(.*?)</Key>.*?<LastModified>(.*?)</LastModified>.*?<Size>(\d+)</Size>", xml, re.S)
    m = re.search(r"<NextContinuationToken>(.*?)</NextContinuationToken>", xml)
    if not m:
        break
    token = m.group(1)

linhas = []
for key, quando, tam in objs:
    cid = key.rsplit("/", 1)[-1].removesuffix(".wav")
    if cid not in fim:
        continue
    t = datetime.strptime(quando.split(".")[0].rstrip("Z"), "%Y-%m-%dT%H:%M:%S")
    linhas.append((int(tam), t, (t - fim[cid]).total_seconds()))

if not linhas:
    print("  bucket: nenhum objeto desta rodada (as gravacoes nao subiram)")
    sys.exit()

def pct(v, p):
    return sorted(v)[min(int(len(v) * p), len(v) - 1)]

tams = [t for t, _, _ in linhas]
lats = [l for _, _, l in linhas]
inicio, ultimo = min(t for _, t, _ in linhas), max(t for _, t, _ in linhas)
janela = (ultimo - inicio).total_seconds()
mb = sum(tams) / 1048576

print(f"  no bucket: {len(linhas)} objetos, {mb:.1f} MB no total")
print(f"  tamanho por gravacao: p50={pct(tams,.5)/1024:.0f} KB "
      f"p95={pct(tams,.95)/1024:.0f} KB max={max(tams)/1024:.0f} KB")
if janela > 0:
    print(f"  vazao de upload: {mb/janela:.1f} MB/s, {len(linhas)/janela:.1f} gravacoes/s "
          f"(janela de {janela:.0f}s, do primeiro ao ultimo PutObject)")
else:
    print(f"  vazao de upload: {len(linhas)} objetos dentro do mesmo segundo")
# LastModified do S3 tem resolucao de 1s: a latencia abaixo e grosseira por
# construcao, e serve para a FORMA da distribuicao, nao para o valor exato.
print(f"  fim da chamada -> objeto no bucket (+-1s): p50={pct(lats,.5):.0f}s "
      f"p95={pct(lats,.95):.0f}s max={max(lats):.0f}s")
' || echo "  bucket: falha ao ler a listagem"

  if [ "$atendidas" -gt 0 ] && [ "$salvas" -lt "$atendidas" ]; then
    faltam=$(( atendidas - salvas ))
    echo "  >> $faltam chamadas atendidas sem gravacao persistida."
    echo "     O motivo esta no log do worker: rec.unavailable (o ARI nao entregou o WAV),"
    echo "     rec.upload_failed (o PutObject falhou) ou rec.db_save_failed (subiu e nao gravou"
    echo "     a key -- o arquivo existe e o player nunca acha)."
  fi
fi

# --- resultado ---------------------------------------------------------------
echo
echo "=== resultado ==="
$PG "
select
  count(*)                                                        as total,
  count(*) filter (where started_at is not null)                  as atenderam,
  count(*) filter (where status = 'COMPLETED')                    as completed,
  count(*) filter (where status in ('FAILED','NO_ANSWER'))        as falharam,
  count(*) filter (where failure_reason = 'shard_at_capacity')    as no_teto
from \"call\"" | awk -F'|' '{printf "  total=%s  atenderam=%s  completed=%s  falharam=%s  no_teto=%s\n",$1,$2,$3,$4,$5}'

echo
echo "  latencia de setup (created_at -> started_at, ms):"
$PG "
select
  round(percentile_cont(0.50) within group (order by ms))::text || ' / ' ||
  round(percentile_cont(0.95) within group (order by ms))::text || ' / ' ||
  round(percentile_cont(0.99) within group (order by ms))::text || ' / ' ||
  round(max(ms))::text
from (select extract(epoch from (started_at - created_at))*1000 as ms
      from \"call\" where started_at is not null) t" \
  | awk '{print "    p50 / p95 / p99 / max = "$0}'

echo
echo "  pico de ativas: $pico_ativas   pico de CPU do Asterisk: ${pico_cpu}%"
echo "  sobrou: canais=$(curl -s -m 5 -u "$ARI_AUTH" "$ARI/ari/channels" | python3 -c 'import sys,json;print(len(json.load(sys.stdin)))' 2>/dev/null) bridges=$(curl -s -m 5 -u "$ARI_AUTH" "$ARI/ari/bridges" | python3 -c 'import sys,json;print(len(json.load(sys.stdin)))' 2>/dev/null) ativas=$(curl -s -m 5 "$WORKER/health" | python3 -c 'import sys,json;print(json.load(sys.stdin)["shard"]["activeCalls"])' 2>/dev/null)"
