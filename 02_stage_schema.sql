-- ============================================================
--  PUNIM SEMESTRAL: Projektimi dhe Implementimi i Data Warehouse
-- ============================================================
--
--  SKEDARI : 02_stage_schema.sql
--  SKEMA    : stage
--  QËLLIMI  : Shtresa ndërmjetëse (Staging Area).
--             Izolulon skemat burim nga DW. Mundëson:
--             (a) Validimin e të dhënave para ngarkimit;
--             (b) Full Refresh pa ndikuar mbi DW;
--             (c) Auditimin e çdo cikli ngarkimi (_batch_id,
--                 _loaded_at, _row_hash).
--
--  VARËSI   : 01_source_schema.sql
--
--  EKZEKUTIMI:
--      psql -U postgres -d datawarehouse -f 02_stage_schema.sql
-- ============================================================

SET client_encoding = 'UTF8';

CREATE SCHEMA IF NOT EXISTS stage;
COMMENT ON SCHEMA stage IS
  'Zona ndërmjetëse e ngarkimit (Staging Area). Çdo tabelë këtu '
  'është pasqyrë e tabelës përkatëse në source, e pasuruar me '
  'kolona auditimi (_batch_id, _loaded_at, _row_hash).';

-- ── Sekuencë batch_id për identifikim ciklesh ngarkimi ──────
CREATE SEQUENCE IF NOT EXISTS stage.seq_batch_id START 1;
COMMENT ON SEQUENCE stage.seq_batch_id IS
  'Identifikues unik i çdo cikli ngarkimi (batch). '
  'Çdo CALL stage.master_stage_load() merr vlerën tjetër.';

-- ── Tabela log-u e ngarkimeve ────────────────────────────────
CREATE TABLE IF NOT EXISTS stage.load_log (
    log_id          SERIAL      PRIMARY KEY,
    batch_id        BIGINT      NOT NULL,
    table_name      VARCHAR(60) NOT NULL,
    rows_loaded     INT         NOT NULL DEFAULT 0,
    started_at      TIMESTAMP   NOT NULL,
    finished_at     TIMESTAMP,
    duration_sec    DECIMAL(10,2)
                    GENERATED ALWAYS AS
                    (EXTRACT(EPOCH FROM (finished_at - started_at))) STORED,
    status          VARCHAR(10) NOT NULL DEFAULT 'RUNNING'
                    CHECK (status IN ('RUNNING','SUCCESS','FAILED')),
    error_message   TEXT
);
COMMENT ON TABLE stage.load_log IS
  'Regjistron çdo operacion ngarkimi: batch_id, tabelë, rreshta, kohëzgjatje. '
  'Përdoret për monitorim dhe debugim të procesit ETL.';

-- ── Makro e kolonave auditimi ─────────────────────────────────
-- Çdo tabelë stage ka 3 kolona auditimi:
--   _batch_id  : identifikon ciklin e ngarkimit
--   _loaded_at : timestamp i ngarkimit
--   _row_hash  : hash MD5 i rreshtit, për detektimin e ndryshimeve

CREATE TABLE IF NOT EXISTS stage.categories (
    -- ── Çelësat ──
    category_id     INT         NOT NULL,
    -- ── Atributet e biznesit ──
    category_name   VARCHAR(100) NOT NULL,
    description     TEXT,
    is_active       BOOLEAN,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    -- ── Auditim ──
    _batch_id       BIGINT      NOT NULL,
    _loaded_at      TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _row_hash       CHAR(32),
    CONSTRAINT pk_stg_categories PRIMARY KEY (category_id)
);
COMMENT ON TABLE stage.categories IS 'Pasqyrë e source.categories. Full Refresh per cikël.';

CREATE TABLE IF NOT EXISTS stage.subcategories (
    subcategory_id   INT         NOT NULL,
    category_id      INT         NOT NULL,
    subcategory_name VARCHAR(100) NOT NULL,
    description      TEXT,
    is_active        BOOLEAN,
    created_at       TIMESTAMP,
    updated_at       TIMESTAMP,
    _batch_id        BIGINT      NOT NULL,
    _loaded_at       TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _row_hash        CHAR(32),
    CONSTRAINT pk_stg_subcategories PRIMARY KEY (subcategory_id)
);
COMMENT ON TABLE stage.subcategories IS 'Pasqyrë e source.subcategories.';

CREATE TABLE IF NOT EXISTS stage.products (
    product_id       INT         NOT NULL,
    subcategory_id   INT         NOT NULL,
    product_code     VARCHAR(20) NOT NULL,
    product_name     VARCHAR(200) NOT NULL,
    unit_price       DECIMAL(10,2) NOT NULL,
    cost_price       DECIMAL(10,2),
    unit_of_measure  VARCHAR(20),
    is_active        BOOLEAN,
    created_at       TIMESTAMP,
    updated_at       TIMESTAMP,
    _batch_id        BIGINT      NOT NULL,
    _loaded_at       TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _row_hash        CHAR(32),
    CONSTRAINT pk_stg_products PRIMARY KEY (product_id)
);
COMMENT ON TABLE stage.products IS
  'Pasqyrë e source.products. _row_hash lejon detektimin e '
  'ndryshimeve të çmimit (SCD Type 2 trigger në ETL).';

CREATE TABLE IF NOT EXISTS stage.regions (
    region_id       INT          NOT NULL,
    country         VARCHAR(100) NOT NULL,
    county          VARCHAR(100) NOT NULL,
    city            VARCHAR(100) NOT NULL,
    postal_code     VARCHAR(10),
    created_at      TIMESTAMP,
    _batch_id       BIGINT       NOT NULL,
    _loaded_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _row_hash       CHAR(32),
    CONSTRAINT pk_stg_regions PRIMARY KEY (region_id)
);

CREATE TABLE IF NOT EXISTS stage.customers (
    customer_id     INT          NOT NULL,
    customer_code   VARCHAR(20),
    first_name      VARCHAR(100) NOT NULL,
    last_name       VARCHAR(100) NOT NULL,
    full_name       VARCHAR(200),
    email           VARCHAR(200),
    phone           VARCHAR(20),
    gender          CHAR(1),
    birth_date      DATE,
    region_id       INT,
    customer_since  DATE,
    is_active       BOOLEAN,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    _batch_id       BIGINT       NOT NULL,
    _loaded_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _row_hash       CHAR(32),
    CONSTRAINT pk_stg_customers PRIMARY KEY (customer_id)
);
COMMENT ON TABLE stage.customers IS
  'Pasqyrë e source.customers. _row_hash detekton ndryshime '
  'lokacioni ose emri (trigger për SCD Type 2 në DIM_CUSTOMER).';

CREATE TABLE IF NOT EXISTS stage.sales (
    sale_id         INT          NOT NULL,
    sale_code       VARCHAR(20),
    customer_id     INT          NOT NULL,
    region_id       INT,
    sale_date       DATE         NOT NULL,
    sale_channel    VARCHAR(20),
    payment_method  VARCHAR(20),
    subtotal        DECIMAL(12,2),
    discount_total  DECIMAL(12,2),
    tax_total       DECIMAL(12,2),
    grand_total     DECIMAL(12,2),
    created_at      TIMESTAMP,
    _batch_id       BIGINT       NOT NULL,
    _loaded_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _row_hash       CHAR(32),
    CONSTRAINT pk_stg_sales PRIMARY KEY (sale_id)
);

CREATE TABLE IF NOT EXISTS stage.sale_items (
    item_id         INT           NOT NULL,
    sale_id         INT           NOT NULL,
    product_id      INT           NOT NULL,
    line_number     SMALLINT,
    quantity        INT           NOT NULL,
    unit_price      DECIMAL(10,2) NOT NULL,
    discount_pct    DECIMAL(5,2),
    tax_rate        DECIMAL(5,2),
    line_subtotal   DECIMAL(12,2),
    line_discount   DECIMAL(12,2),
    line_net        DECIMAL(12,2),
    line_tax        DECIMAL(12,2),
    line_total      DECIMAL(12,2),
    created_at      TIMESTAMP,
    _batch_id       BIGINT        NOT NULL,
    _loaded_at      TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _row_hash       CHAR(32),
    CONSTRAINT pk_stg_sale_items PRIMARY KEY (item_id)
);
COMMENT ON TABLE stage.sale_items IS
  'Linjat e faturave. line_total = vlera e transferuar në FACT_SALES '
  'si masë revenue (neto + TVSH).';
