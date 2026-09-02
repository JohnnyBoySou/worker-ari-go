# Kamailio do laboratorio.
#
# Vem do repositorio OFICIAL do projeto (deb.kamailio.org), e nao de uma imagem
# pronta do Docker Hub, por dois motivos. O primeiro e idade: as imagens
# publicadas (kamailio/kamailio-ci, ghcr.io/kamailio/kamailio) pararam no 5.4/5.5
# de 2021. O segundo importa mais: e o MESMO caminho de instalacao que o VPS de
# producao vai usar, entao o que funciona aqui e o que vai funcionar la. Uma
# imagem de terceiro validaria uma instalacao que ninguem faz.
FROM debian:bookworm-slim

ARG KAMAILIO_REPO=kamailio58

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl gnupg \
 && curl -fsSL http://deb.kamailio.org/kamailiodebkey.gpg \
      | gpg --dearmor -o /usr/share/keyrings/kamailio.gpg \
 && echo "deb [signed-by=/usr/share/keyrings/kamailio.gpg] http://deb.kamailio.org/${KAMAILIO_REPO} bookworm main" \
      > /etc/apt/sources.list.d/kamailio.list \
 && apt-get update \
 && apt-get install -y --no-install-recommends \
      kamailio kamailio-postgres-modules kamailio-utils-modules kamailio-extra-modules \
 && apt-get purge -y curl gnupg && apt-get autoremove -y \
 && rm -rf /var/lib/apt/lists/*

# -DD desliga o fork em daemon (o container e o processo); -E manda log para
# stderr, que e onde o `docker logs` olha.
CMD ["kamailio", "-DD", "-E", "-f", "/etc/kamailio/kamailio.cfg"]
