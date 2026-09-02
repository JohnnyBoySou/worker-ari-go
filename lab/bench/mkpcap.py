#!/usr/bin/env python3
"""Gera os pcaps de RTP que o SIPp toca no benchmark.

POR QUE GERAR EM VEZ DE USAR O g711a.pcap DA IMAGEM

  O `play_pcap_audio` toca UMA VEZ. Com o `g711a.pcap` da imagem, de duracao
  desconhecida, o RTP parava no meio de um teste que segura a chamada por 15s:
  a carga de midia deixava de existir sem ninguem perceber.

  A primeira tentativa foi um LACO no cenario (`ontimeout` rearmando o play).
  Ela mantem a midia e DESTROI a medicao: o SIPp replica o pcap como esta no
  arquivo, entao cada volta reinicia a sequencia RTP em 0 com o mesmo SSRC.
  Medido no lab com pcap de 10s: aos 5s e aos 13s o canal reporta `lp=0`; na
  segunda volta vira `lp=65036` (= -500 em 16 bits, e 500 e exatamente o numero
  de pacotes do pcap) e o `txmes` do Asterisk cai de 88 para 20. A midia continua
  fluindo e a estatistica de perda vira lixo -- as duas coisas nao convivem.

  Por isso o pcap e LONGO em vez de repetido: 300s por default cobrem qualquer
  rodada, a sequencia RTP nunca volta atras e as estatisticas seguem validas. Se
  a rodada passar da duracao do arquivo o RTP para, e agora isso e visivel: o
  `rxcount` do canal denuncia.

  O segundo motivo e transcoding: nao existe pcap de G.722 na imagem, e sem uma
  perna que fale outro codec o Asterisk fica em passthrough e a variavel de
  maior peso do plano de capacidade continua sem numero.

O QUE TEM DENTRO

  --pt 8   (alaw)  audio DE VERDADE: uma senoide codificada em A-law pelo
                   algoritmo da G.711. A gravacao do benchmark fica audivel, o
                   que e o unico teste barato de "gravou som ou gravou silencio".

  --pt 9   (g722)  bytes SINTETICOS, nao audio. Nao ha codificador de G.722 aqui,
                   e nao faz falta: o custo do transcode e por amostra e nao
                   depende do conteudo — decodificar ADPCM de lixo custa o mesmo
                   que decodificar ADPCM de voz. O que sai do outro lado e ruido.
                   Serve para MEDIR CPU de transcoding; nao serve para julgar
                   qualidade de audio.

USO
  python3 mkpcap.py                 # gera os dois, em bench/media/
  python3 mkpcap.py --pt 8 --seconds 60 --out media/alaw-60s.pcap
"""
import argparse
import math
import os
import struct

# ---------------------------------------------------------------------------
# G.711 A-law. Transcricao do linear_to_alaw da spandsp: a mesma tabela de
# segmentos da ITU G.711, com top_bit() virando bit_length().
# ---------------------------------------------------------------------------
ALAW_AMI_MASK = 0x55


def lin2alaw(amostra: int) -> int:
    positivo = amostra >= 0
    if positivo:
        mask = ALAW_AMI_MASK | 0x80
    else:
        mask = ALAW_AMI_MASK
        amostra = -amostra - 8
        if amostra < 0:
            amostra = 0
    seg = (amostra | 0xFF).bit_length() - 1 - 7
    if seg >= 8:
        return (0x7F ^ mask) if positivo else (0x80 ^ mask)
    desloc = seg + 3 if seg else 4
    return ((seg << 4) | ((amostra >> desloc) & 0x0F)) ^ mask


# Um segundo de audio, depois LADRILHADO ate a duracao pedida. Nao e so
# velocidade (300s sao 2,4 milhoes de amostras em Python puro): a 8 kHz, 440 Hz
# fecham 440 ciclos INTEIROS em exatamente um segundo, entao a emenda nao tem
# descontinuidade de fase e o tom sai continuo do comeco ao fim.
def carga_alaw(quadros: int, amostras_por_quadro: int, hz: float) -> list:
    """Senoide de `hz` a 8 kHz, meia escala, codificada em A-law."""
    por_segundo = 8000 // amostras_por_quadro
    base = []
    n = 0
    for _ in range(por_segundo):
        buf = bytearray(amostras_por_quadro)
        for i in range(amostras_por_quadro):
            pcm = int(16000 * math.sin(2 * math.pi * hz * n / 8000.0))
            buf[i] = lin2alaw(pcm) & 0xFF
            n += 1
        base.append(bytes(buf))
    return [base[i % len(base)] for i in range(quadros)]


def carga_sintetica(quadros: int, tamanho: int) -> list:
    """Bytes deterministicos (LCG). Ver o cabecalho: e ruido de proposito."""
    por_segundo = 8000 // tamanho
    base = []
    estado = 0x2545F491
    for _ in range(por_segundo):
        buf = bytearray(tamanho)
        for i in range(tamanho):
            estado = (estado * 1103515245 + 12345) & 0xFFFFFFFF
            buf[i] = (estado >> 16) & 0xFF
        base.append(bytes(buf))
    return [base[i % len(base)] for i in range(quadros)]


# ---------------------------------------------------------------------------
# Empacotamento. O SIPp exige Ethernet + IPv4 + UDP: o prepare_pkts dele le o
# ether_type, anda o IP header e copia a partir do UDP usando uh_ulen como
# tamanho — se o comprimento do UDP mentir, ele copia lixo ou corta o payload.
# As portas e os IPs sao reescritos pelo SIPp no envio; o que ele preserva sao o
# PAYLOAD e o ESPACAMENTO dos timestamps do pcap, que e o que dita o ritmo.
# ---------------------------------------------------------------------------
def checksum_ip(cab: bytes) -> int:
    total = 0
    for i in range(0, len(cab), 2):
        total += (cab[i] << 8) | cab[i + 1]
    while total >> 16:
        total = (total & 0xFFFF) + (total >> 16)
    return (~total) & 0xFFFF


def quadro_ethernet(payload_udp: bytes, sport: int, dport: int) -> bytes:
    udp = struct.pack("!HHHH", sport, dport, 8 + len(payload_udp), 0) + payload_udp
    total_ip = 20 + len(udp)
    ip_sem_ck = struct.pack(
        "!BBHHHBBH4s4s", 0x45, 0, total_ip, 0, 0, 64, 17, 0,
        bytes((10, 0, 0, 1)), bytes((10, 0, 0, 2)),
    )
    ck = checksum_ip(ip_sem_ck)
    ip = ip_sem_ck[:10] + struct.pack("!H", ck) + ip_sem_ck[12:]
    eth = b"\x00\x00\x00\x00\x00\x02" + b"\x00\x00\x00\x00\x00\x01" + struct.pack("!H", 0x0800)
    return eth + ip + udp


def escreve_pcap(caminho: str, quadros: list, pt: int, ms: int, ts_passo: int) -> None:
    os.makedirs(os.path.dirname(caminho) or ".", exist_ok=True)
    with open(caminho, "wb") as f:
        # magic, versao 2.4, thiszone, sigfigs, snaplen, DLT_EN10MB
        f.write(struct.pack("<IHHiIII", 0xA1B2C3D4, 2, 4, 0, 0, 65535, 1))
        ssrc = 0x0BADCAFE
        ts = 0
        us = 0
        for seq, carga in enumerate(quadros):
            # Marker so no primeiro pacote: e o inicio do talkspurt.
            b1 = (0x80 | pt) if seq == 0 else pt
            rtp = struct.pack("!BBHII", 0x80, b1, seq & 0xFFFF, ts & 0xFFFFFFFF, ssrc) + carga
            pkt = quadro_ethernet(rtp, 6000, 6000)
            f.write(struct.pack("<IIII", us // 1000000, us % 1000000, len(pkt), len(pkt)))
            f.write(pkt)
            ts += ts_passo
            us += ms * 1000


def gerar(pt: int, segundos: float, ms: int, saida: str, hz: float) -> str:
    amostras = (8000 * ms) // 1000          # 160 em 20ms, para alaw e para g722
    quadros_n = int(round((segundos * 1000) / ms))
    if pt == 8:
        quadros = carga_alaw(quadros_n, amostras, hz)
    else:
        # G.722 a 64 kbit/s da os mesmos 160 bytes por 20ms, e o relogio de RTP
        # dele e 8000 (a excecao da RFC 3551) — mesmo passo de timestamp.
        quadros = carga_sintetica(quadros_n, amostras)
    escreve_pcap(saida, quadros, pt, ms, amostras)
    return saida


def main() -> None:
    p = argparse.ArgumentParser(description="Gera pcaps de RTP para o SIPp.")
    p.add_argument("--pt", type=int, help="payload type RTP: 8=alaw, 9=g722")
    p.add_argument("--seconds", type=float, default=300.0,
                   help="duracao; o default cobre qualquer rodada do bench")
    p.add_argument("--ms", type=int, default=20, help="ms por pacote")
    p.add_argument("--hz", type=float, default=440.0, help="tom da senoide (alaw)")
    p.add_argument("--out")
    a = p.parse_args()

    raiz = os.path.dirname(os.path.abspath(__file__))
    if a.pt is not None:
        destino = a.out or os.path.join(raiz, "media", f"pt{a.pt}-{int(a.seconds)}s.pcap")
        print(gerar(a.pt, a.seconds, a.ms, destino, a.hz))
        return

    for pt, nome in ((8, "alaw-300s.pcap"), (9, "g722-300s.pcap")):
        destino = os.path.join(raiz, "media", nome)
        gerar(pt, 300.0, 20, destino, 440.0)
        print(f"{destino}  ({os.path.getsize(destino)//1024} KB, 300s, pt={pt})")


if __name__ == "__main__":
    main()
