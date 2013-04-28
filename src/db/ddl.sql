DROP TABLE IF EXISTS state;
CREATE TABLE state (
  a     BIGINT NOT NULL,
  count BIGINT NOT NULL,
  bsum  BIGINT NOT NULL,
  csum  BIGINT NOT NULL
);
ALTER TABLE state ADD CONSTRAINT uk_state UNIQUE KEY(a);

DROP TABLE IF EXISTS state_transactional;
CREATE TABLE state_transactional (
  a     BIGINT NOT NULL,
  count BIGINT NOT NULL,
  bsum  BIGINT NOT NULL,
  csum  BIGINT NOT NULL,
  txid  BIGINT NULL
);
ALTER TABLE state_transactional ADD CONSTRAINT uk_state_transactional UNIQUE KEY(a);

DROP TABLE IF EXISTS state_opaque;
CREATE TABLE state_opaque (
  a     BIGINT NOT NULL,
  count BIGINT NOT NULL,
  bsum  BIGINT NOT NULL,
  csum  BIGINT NOT NULL,
  txid  BIGINT NULL,
  prev_count BIGINT NULL,
  prev_bsum  BIGINT NULL,
  prev_csum  BIGINT NULL
);
ALTER TABLE state_opaque ADD CONSTRAINT uk_state_opaque UNIQUE KEY(a);