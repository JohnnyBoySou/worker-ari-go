-- Esquema do Kamailio: registrar e location service.
--
-- COPIADO DO PROPRIO KAMAILIO, nao escrito a mao. Sai de
-- /usr/share/kamailio/postgres/{standard,auth_db,usrloc}-create.sql da imagem
-- 5.8.8. As linhas do `version` importam: o Kamailio confere a versao de cada
-- tabela no boot e ABORTA se divergir do que o modulo espera. Uma tabela escrita
-- a mao com uma coluna a menos nao da erro de SQL -- da erro de boot, ou pior,
-- silencio na hora do REGISTER.
--
-- Este arquivo so roda na PRIMEIRA inicializacao do volume do Postgres
-- (docker-entrypoint-initdb.d). Num banco que ja existe, aplique a mao:
--   docker exec -i lab-postgres psql -U lab -d lab < db/02-kamailio.sql

CREATE TABLE version (
    id SERIAL PRIMARY KEY NOT NULL,
    table_name VARCHAR(32) NOT NULL,
    table_version INTEGER DEFAULT 0 NOT NULL,
    CONSTRAINT version_table_name_idx UNIQUE (table_name)
);

INSERT INTO version (table_name, table_version) values ('version','1');

CREATE TABLE subscriber (
    id SERIAL PRIMARY KEY NOT NULL,
    username VARCHAR(64) DEFAULT '' NOT NULL,
    domain VARCHAR(64) DEFAULT '' NOT NULL,
    password VARCHAR(64) DEFAULT '' NOT NULL,
    ha1 VARCHAR(128) DEFAULT '' NOT NULL,
    ha1b VARCHAR(128) DEFAULT '' NOT NULL,
    CONSTRAINT subscriber_account_idx UNIQUE (username, domain)
);

CREATE INDEX subscriber_username_idx ON subscriber (username);

INSERT INTO version (table_name, table_version) values ('subscriber','7');

CREATE TABLE location (
    id SERIAL PRIMARY KEY NOT NULL,
    ruid VARCHAR(64) DEFAULT '' NOT NULL,
    username VARCHAR(64) DEFAULT '' NOT NULL,
    domain VARCHAR(64) DEFAULT NULL,
    contact VARCHAR(512) DEFAULT '' NOT NULL,
    received VARCHAR(128) DEFAULT NULL,
    path VARCHAR(512) DEFAULT NULL,
    expires TIMESTAMP WITHOUT TIME ZONE DEFAULT '2030-05-28 21:32:15' NOT NULL,
    q REAL DEFAULT 1.0 NOT NULL,
    callid VARCHAR(255) DEFAULT 'Default-Call-ID' NOT NULL,
    cseq INTEGER DEFAULT 1 NOT NULL,
    last_modified TIMESTAMP WITHOUT TIME ZONE DEFAULT '2000-01-01 00:00:01' NOT NULL,
    flags INTEGER DEFAULT 0 NOT NULL,
    cflags INTEGER DEFAULT 0 NOT NULL,
    user_agent VARCHAR(255) DEFAULT '' NOT NULL,
    socket VARCHAR(64) DEFAULT NULL,
    methods INTEGER DEFAULT NULL,
    instance VARCHAR(255) DEFAULT NULL,
    reg_id INTEGER DEFAULT 0 NOT NULL,
    server_id INTEGER DEFAULT 0 NOT NULL,
    connection_id INTEGER DEFAULT 0 NOT NULL,
    keepalive INTEGER DEFAULT 0 NOT NULL,
    partition INTEGER DEFAULT 0 NOT NULL,
    CONSTRAINT location_ruid_idx UNIQUE (ruid)
);

CREATE INDEX location_account_contact_idx ON location (username, domain, contact);
CREATE INDEX location_expires_idx ON location (expires);
CREATE INDEX location_tcpcon_idx ON location (connection_id);
CREATE INDEX location_connection_idx ON location (server_id, connection_id);

INSERT INTO version (table_name, table_version) values ('location','9');

CREATE TABLE location_attrs (
    id SERIAL PRIMARY KEY NOT NULL,
    ruid VARCHAR(64) DEFAULT '' NOT NULL,
    username VARCHAR(64) DEFAULT '' NOT NULL,
    domain VARCHAR(64) DEFAULT NULL,
    aname VARCHAR(64) DEFAULT '' NOT NULL,
    atype INTEGER DEFAULT 0 NOT NULL,
    avalue VARCHAR(512) DEFAULT '' NOT NULL,
    last_modified TIMESTAMP WITHOUT TIME ZONE DEFAULT '2000-01-01 00:00:01' NOT NULL
);

CREATE INDEX location_attrs_account_record_idx ON location_attrs (username, domain, ruid);
CREATE INDEX location_attrs_last_modified_idx ON location_attrs (last_modified);

INSERT INTO version (table_name, table_version) values ('location_attrs','1');


-- ---------------------------------------------------------------------------
-- Ramais do laboratorio que REGISTRAM de verdade.
--
-- Os ramais 1001/1002/1003 do pjsip.conf tem contato ESTATICO e existem para o
-- benchmark: nao registram, nao passam pelo Kamailio, e continuam medindo o
-- caminho de sempre. Os 2001+ sao o caminho novo -- registram no Kamailio, e a
-- localizacao deles vive na tabela `location`, compartilhada entre nos.
--
-- Senha em claro porque e laboratorio. Em producao o ha1 e o que deve viajar.
-- ---------------------------------------------------------------------------
INSERT INTO subscriber (username, domain, password) VALUES
  ('2001', 'lab.local', 'lab2001'),
  ('2002', 'lab.local', 'lab2002')
ON CONFLICT (username, domain) DO NOTHING;
