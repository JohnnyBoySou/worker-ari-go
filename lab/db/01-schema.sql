-- Schema minimo que o worker ARI usa. A fonte canonica e o back
-- (drizzle); aqui esta so o subset necessario para o laboratorio local.
CREATE TABLE IF NOT EXISTS "call" (
  id                text PRIMARY KEY,
  organization_id   text NOT NULL,
  assigned_to       text,
  direction         text NOT NULL,          -- OUTBOUND | INBOUND
  status            text NOT NULL,          -- RINGING | IN_PROGRESS | COMPLETED | FAILED | NO_ANSWER
  target_phone      text,
  caller_did        text,
  sip_channel_id    text,
  -- Passo 3 do contrato do produtor: ONDE a chamada nasceu. Sem persistir isto,
  -- o terminate nao tem como achar o no certo e chegaria num shard que nunca
  -- ouviu falar dela. Coluna nova, ainda nao existe no back.
  shard_id          text,
  started_at        timestamp,
  ended_at          timestamp,
  duration          integer,
  recording_name    text,
  recording_url     text,
  failure_reason    text,
  hangup_cause      integer,                -- causa Q.850 crua
  hangup_cause_txt  text,
  notes             text,
  disposition_id    text,
  dispositioned_at  timestamp,
  dispositioned_by  text,
  form_answers      jsonb,
  form_version      integer,
  form_submitted_at timestamp,
  form_submitted_by text,
  created_at        timestamp NOT NULL DEFAULT now(),
  updated_at        timestamp NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS call_org_status_idx   ON "call" (organization_id, status);
CREATE INDEX IF NOT EXISTS call_channel_idx      ON "call" (sip_channel_id);

CREATE TABLE IF NOT EXISTS sip_agent (
  organization_id text NOT NULL,
  user_id         text NOT NULL,
  sip_username    text NOT NULL,
  secret_enc      text,
  enabled         boolean NOT NULL DEFAULT true,
  inbound_did     text,                     -- DID que toca ESTE vendedor
  created_at      timestamp NOT NULL DEFAULT now(),
  updated_at      timestamp NOT NULL DEFAULT now(),
  PRIMARY KEY (organization_id, user_id)
);

-- Dois "vendedores" locais: e com eles que o fluxo completo roda sem tronco.
-- 1001 recebe a chamada (perna A) e 1002 faz o papel do lead (perna B).
INSERT INTO sip_agent (organization_id, user_id, sip_username, inbound_did)
VALUES ('org_lab', 'user_1001', '1001', '551133334444'),
       ('org_lab', 'user_1002', '1002', NULL)
ON CONFLICT DO NOTHING;
