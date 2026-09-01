#!/usr/bin/env bash
# Benchmark de capacidade do no.
#
# O QUE ELE MEDE
#   - CPS aceito: quantas chamadas por segundo o no monta sem degradar
#   - concorrencia sustentada: quantas simultaneas ele segura
#   - latencia de setup (p50/p95/p99): created_at -> started_at, direto do banco.
#     E o numero que o cliente sente: o tempo entre clicar e o telefone tocar.
#   - taxa de sucesso e CPU/RAM do Asterisk e do worker no pico
#
# COMO USAR
#   ./bench.sh <simultaneas> [cps] [segundos_segurando]
#   ./bench.sh 50 10 20
set -uo pipefail

ALVO=${1:-50}
CPS=${2:-10}
HOLD=${3:-20}

BACK=${BACK:-http://127.0.0.1:8092}
WORKER=${WORKER:-http://127.0.0.1:8091}
ARI=${ARI:-http://127.0.0.1:8088}
ARI_AUTH=${ARI_AUTH:-connect-ari:lab-ari-secret}
PG="docker exec lab-postgres psql -U lab -d lab -tAc"

echo "=== benchmark: ${ALVO} simultaneas a ${CPS} CPS, segurando ${HOLD}s ==="
echo "maquina: $(nproc) vCPU, $(free -g | awk '/Mem:/{print $2}')GB"
echo

# Estado limpo: numeros so significam algo partindo do zero.
$PG 'TRUNCATE "call"' >/dev/null 2>&1
RUN_INICIO=$(date +%s.%N)

# --- disparo, com ritmo -----------------------------------------------------
INTERVALO=$(python3 -c "print(1.0/$CPS)")
enviadas=0; recusadas=0
for i in $(seq 1 "$ALVO"); do
  code=$(curl -s -o /dev/null -w '%{http_code}' -m 10 -X POST "$BACK/v1/calls" \
    -H 'content-type: application/json' \
    -d '{"organizationId":"org_lab","targetPhone":"5541988887777","sellerSipUsername":"1001","callerDid":"5541999999999"}')
  if [ "$code" = "201" ]; then enviadas=$((enviadas+1)); else recusadas=$((recusadas+1)); fi
  sleep "$INTERVALO"
done
DISPARO_FIM=$(date +%s.%N)
CPS_REAL=$(python3 -c "print(f'{$enviadas/($DISPARO_FIM-$RUN_INICIO):.1f}')")
echo "disparadas: $enviadas   recusadas: $recusadas   CPS real: $CPS_REAL"

# --- amostragem enquanto segura ---------------------------------------------
echo
printf "%-6s %-10s %-10s %-12s %-10s\n" "s" "ativas" "canais" "asterisk_cpu" "worker_rss"
pico_ativas=0; pico_cpu=0
for s in $(seq 1 "$HOLD"); do
  ativas=$(curl -s -m 5 "$WORKER/health" | python3 -c 'import sys,json;print(json.load(sys.stdin)["shard"]["activeCalls"])' 2>/dev/null || echo 0)
  canais=$(curl -s -m 5 -u "$ARI_AUTH" "$ARI/ari/channels" | python3 -c 'import sys,json;print(len(json.load(sys.stdin)))' 2>/dev/null || echo 0)
  stats=$(docker stats --no-stream --format '{{.CPUPerc}}' lab-asterisk 2>/dev/null | tr -d '%')
  rss=$(ps -o rss= -C lab-worker 2>/dev/null | awk '{s+=$1} END{printf "%.0fMB", s/1024}')
  [ "${ativas:-0}" -gt "$pico_ativas" ] && pico_ativas=$ativas
  pico_cpu=$(python3 -c "print(max($pico_cpu, ${stats:-0}))")
  printf "%-6s %-10s %-10s %-12s %-10s\n" "$s" "$ativas" "$canais" "${stats:-?}%" "${rss:-?}"
  sleep 1
done

# --- desligamento ------------------------------------------------------------
echo
echo "desligando..."
for id in $($PG "select id from \"call\" where status in ('RINGING','IN_PROGRESS')" 2>/dev/null); do
  curl -s -o /dev/null -m 5 -X POST "$BACK/v1/calls/$id/terminate" &
done
wait
sleep 6

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
