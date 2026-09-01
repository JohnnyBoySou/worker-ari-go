# Ligar o tronco real ao laboratório

O laboratório roda com um `trunk` local (SIPp). Para apontá-lo ao provedor,
troque o bloco `[trunk]` de `asterisk/pjsip.conf` pelo de produção — mas há dois
bloqueios que **não são de configuração** e precisam ser resolvidos antes.

## 1. Inbound não vai funcionar

O `render_config.py` do `worker-asterisk` **não gera nenhum `type=registration`**.
O tronco usa:

- `type=identify` com `match=<IPs do SBC>` — a entrada é identificada pelo IP de
  origem do provedor;
- `type=acl` — só aqueles IPs podem falar com a gente;
- `outbound_auth` — a saída autentica com usuário e senha.

Sem REGISTER, **o provedor roteia o DID para um IP fixo** (hoje
`179.197.77.153`). Uma máquina local não recebe chamada de entrada: o provedor
não sabe que ela existe. Isso não se resolve mexendo no Asterisk local.

## 2. Outbound depende de o provedor aceitar o IP novo

A saída manda INVITE de um IP de origem diferente. SBCs brasileiros costumam
fixar o cliente por IP, e o INVITE é recusado na ACL **antes** de chegar à
autenticação. Precisa pedir ao provedor a liberação do IP de origem do
laboratório.

## 3. O risco que não pode ser ignorado

Se em algum momento passar a existir um `type=registration` nessa conta, **duas
máquinas registrando o mesmo usuário disputam o registro**: o provedor guarda o
último, e a entrada de produção passaria a ser entregue no laboratório. Antes de
apontar o tronco real, confirmar que não há REGISTER na conta.

## Credenciais

Estão em `/etc/asterisk/pjsip.conf` no VPS (`ssh asterisk`), legível só por
`asterisk:asterisk` — exige sudo com senha:

```bash
sudo sed -n '/\[gsvoip-auth\]/,/^$/p;/\[trunk\]/,/^$/p;/type=identify/,/^$/p' /etc/asterisk/pjsip.conf
```

O que trazer para cá: `username`, `password`, o host/domínio do SBC, a lista de
`match=` do identify, `from_user`, `from_domain` e o prefixo técnico de discagem.
