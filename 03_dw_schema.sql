-- ============================================================
--  PUNIM SEMESTRAL: Projektimi dhe Implementimi i Data Warehouse
-- ============================================================
--
--  SKEDARI : 03_dw_schema.sql
--  SKEMA    : dw
--  QËLLIMI  : Shtresa analitike (OLAP). Implementon modelin
--             dimensional Star Schema me:
--               · 3 Dimensione: DIM_TIME, DIM_PRODUCT, DIM_CUSTOMER
--               · 1 Tabelë Fakt: FACT_SALES
--
--  PRINCIPET E MODELIMIT:
--     - Çdo dimension ka kolonën total_label (niveli All/Total)
--       i nevojshëm për agregime hierarkike ROLLUP.
--     - DIM_TIME dhe DIM_PRODUCT / DIM_CUSTOMER zbatojnë
--       SCD Type 2 (Slowly Changing Dimensions).
--     - Çelësat surrogate (product_key, customer_key) janë
--       të ndara nga çelësat natyrorë (product_id, customer_id)
--       për të mbështetur historikun e ndryshimeve.
--
--  VARËSI   : 02_stage_schema.sql
-- ============================================================

SET client_encoding = 'UTF8';

CREATE SCHEMA IF NOT EXISTS dw;
COMMENT ON SCHEMA dw IS
  'Shtresa analitike (OLAP) – Data Warehouse. '
  'Star Schema: 3 Dimensione + 1 Tabelë Fakt. '
  'Optimizuar për query analitike, raportim dhe BI.';

-- ============================================================
--  DIMENSION 1: dw.dim_time
--
--  HIERARKIA:  Data (L5) → Muaj (L4) → Tremujor (L3)
--                        → Vit  (L2) → Total    (L1)
--
--  ARSYEJA:    Dimension kohe nuk ka SCD — datat janë
--              të pandryshueshme. Popullohet nga një procedurë
--              specialeqë gjeneron çdo datë kalendarike.
-- ============================================================
CREATE TABLE IF NOT EXISTS dw.dim_time (
    -- ── Çelësi surrogate ──────────────────────────────────
    time_key            SERIAL      PRIMARY KEY,

    -- ── Çelësi natyror ───────────────────────────────────
    full_date           DATE        NOT NULL,

    -- ── L1: Nivel Total (aggragimi i plotë) ───────────────
    total_label         VARCHAR(10) NOT NULL DEFAULT 'Total',

    -- ── L2: Vit ──────────────────────────────────────────
    year_number         SMALLINT    NOT NULL,
    year_label          VARCHAR(6)  NOT NULL,           -- '2024'

    -- ── L3: Tremujor ─────────────────────────────────────
    quarter_number      SMALLINT    NOT NULL            -- 1..4
                        CHECK (quarter_number BETWEEN 1 AND 4),
    quarter_label       VARCHAR(12) NOT NULL,           -- 'Q1-2024'

    -- ── L4: Muaj ─────────────────────────────────────────
    month_number        SMALLINT    NOT NULL
                        CHECK (month_number BETWEEN 1 AND 12),
    month_name          VARCHAR(15) NOT NULL,           -- 'Janar'
    month_short         VARCHAR(5)  NOT NULL,           -- 'Jan'
    month_label         VARCHAR(25) NOT NULL,           -- 'Janar 2024'
    month_year_key      INT         NOT NULL,           -- 202401 (për join të shpejtë)

    -- ── L5: Datë ─────────────────────────────────────────
    day_of_month        SMALLINT    NOT NULL
                        CHECK (day_of_month BETWEEN 1 AND 31),
    day_of_year         SMALLINT    NOT NULL
                        CHECK (day_of_year BETWEEN 1 AND 366),
    day_of_week         SMALLINT    NOT NULL            -- 0=E Diel .. 6=E Shtunë
                        CHECK (day_of_week BETWEEN 0 AND 6),
    day_name            VARCHAR(15) NOT NULL,           -- 'E Hënë'
    day_name_short      VARCHAR(5)  NOT NULL,           -- 'Hën'
    iso_week_number     SMALLINT    NOT NULL,
    is_weekend          BOOLEAN     NOT NULL,
    is_workday          BOOLEAN     NOT NULL,
    is_holiday          BOOLEAN     NOT NULL DEFAULT FALSE,
    holiday_name        VARCHAR(80),

    CONSTRAINT uq_dim_time_date UNIQUE (full_date)
);

COMMENT ON TABLE  dw.dim_time                 IS 'Dimension Kohe. 5 nivele hierarkie. Gjenerohet nga etl_dim_time_initial().';
COMMENT ON COLUMN dw.dim_time.month_year_key  IS 'Çelës numerik i muajit-vitit: YYYYMM. P.sh. 202403 = Mars 2024.';
COMMENT ON COLUMN dw.dim_time.is_holiday      IS 'TRUE = ditë feste zyrtare. Popullohet manualisht ose nga kalendarë.';

-- ── Indekse për filtrim të shpejtë ──────────────────────────
CREATE INDEX IF NOT EXISTS idx_dtime_full_date   ON dw.dim_time(full_date);
CREATE INDEX IF NOT EXISTS idx_dtime_year        ON dw.dim_time(year_number);
CREATE INDEX IF NOT EXISTS idx_dtime_month_year  ON dw.dim_time(month_year_key);

-- ============================================================
--  DIMENSION 2: dw.dim_product
--
--  HIERARKIA:  Produkt (L4) → NënKategori (L3)
--                           → Kategori   (L2)
--                           → Total      (L1)
--
--  SCD TYPE 2: Kur ndryshon unit_price ose emri i produktit,
--              rekordi aktual mbyllet dhe krijohet rekord i ri.
--              Historiku ruhet plotësisht.
-- ============================================================
CREATE TABLE IF NOT EXISTS dw.dim_product (
    -- ── Çelësi surrogate ──────────────────────────────────
    product_key         SERIAL      PRIMARY KEY,

    -- ── Çelësi natyror (NK) ───────────────────────────────
    product_id          INT         NOT NULL,

    -- ── L1: Total ────────────────────────────────────────
    total_label         VARCHAR(10) NOT NULL DEFAULT 'Total',

    -- ── L2: Kategori ─────────────────────────────────────
    category_id         INT         NOT NULL,
    category_name       VARCHAR(100) NOT NULL,

    -- ── L3: NënKategori ──────────────────────────────────
    subcategory_id      INT         NOT NULL,
    subcategory_name    VARCHAR(100) NOT NULL,

    -- ── L4: Produkt ──────────────────────────────────────
    product_code        VARCHAR(20) NOT NULL,
    product_name        VARCHAR(200) NOT NULL,
    unit_price          DECIMAL(10,2) NOT NULL,
    cost_price          DECIMAL(10,2),
    unit_of_measure     VARCHAR(20),

    -- ── Atribute analitike të derivuara ───────────────────
    price_band          VARCHAR(20)   -- 'Ekonomik' / 'Mesatar' / 'Premium'
        GENERATED ALWAYS AS (
            CASE
                WHEN unit_price <  50  THEN 'Ekonomik'
                WHEN unit_price < 300  THEN 'Mesatar'
                ELSE 'Premium'
            END
        ) STORED,
    gross_margin_pct    DECIMAL(5,2)  -- (price - cost) / price * 100
        GENERATED ALWAYS AS (
            CASE WHEN cost_price IS NOT NULL AND unit_price > 0
                 THEN ROUND((unit_price - cost_price) / unit_price * 100, 2)
                 ELSE NULL
            END
        ) STORED,

    -- ── SCD Type 2: Atribute historiku ───────────────────
    effective_from      DATE        NOT NULL DEFAULT CURRENT_DATE,
    effective_to        DATE,                -- NULL = rekord aktual
    is_current          BOOLEAN     NOT NULL DEFAULT TRUE,
    dw_version          SMALLINT    NOT NULL DEFAULT 1,  -- nr. i versionit

    CONSTRAINT chk_dim_product_dates
        CHECK (effective_to IS NULL OR effective_to >= effective_from)
);

COMMENT ON TABLE  dw.dim_product                   IS 'Dimension Produkti me SCD Type 2. 4 nivele hierarkie.';
COMMENT ON COLUMN dw.dim_product.product_key        IS 'Çelës surrogate: unik edhe ndërmjet versioneve SCD.';
COMMENT ON COLUMN dw.dim_product.product_id         IS 'Çelës natyror nga source.products.';
COMMENT ON COLUMN dw.dim_product.price_band         IS 'Banda çmimi e gjeneruar automatikisht. Ekonomik < 50 | Mesatar 50-300 | Premium > 300 ALL.';
COMMENT ON COLUMN dw.dim_product.gross_margin_pct   IS 'Marzhi bruto % = (çmim - kosto) / çmim × 100.';
COMMENT ON COLUMN dw.dim_product.dw_version         IS 'Numëron versionet SCD2: 1 = versioni i parë, 2 = pas ndryshimit të parë, etj.';

CREATE INDEX IF NOT EXISTS idx_dprod_product_id  ON dw.dim_product(product_id);
CREATE INDEX IF NOT EXISTS idx_dprod_current     ON dw.dim_product(product_id) WHERE is_current = TRUE;
CREATE INDEX IF NOT EXISTS idx_dprod_category    ON dw.dim_product(category_name);
CREATE INDEX IF NOT EXISTS idx_dprod_price_band  ON dw.dim_product(price_band);

-- ============================================================
--  DIMENSION 3: dw.dim_customer
--
--  HIERARKIA:  Klient (L4) → Qytet  (L3)
--                          → Qark   (L2)
--                          → Total  (L1)
--
--  SCD TYPE 2: Ndryshimet e lokacionit dhe emrit ruhen
--              historikisht sipas të njëjtit parim si dim_product.
-- ============================================================
CREATE TABLE IF NOT EXISTS dw.dim_customer (
    -- ── Çelësi surrogate ──────────────────────────────────
    customer_key        SERIAL      PRIMARY KEY,

    -- ── Çelësi natyror ────────────────────────────────────
    customer_id         INT         NOT NULL,

    -- ── L1: Total ────────────────────────────────────────
    total_label         VARCHAR(10) NOT NULL DEFAULT 'Total',

    -- ── L2: Shtet/Qark ───────────────────────────────────
    country             VARCHAR(100) NOT NULL,
    county              VARCHAR(100) NOT NULL,

    -- ── L3: Qytet ────────────────────────────────────────
    city                VARCHAR(100) NOT NULL,
    postal_code         VARCHAR(10),

    -- ── L4: Klient ───────────────────────────────────────
    customer_code       VARCHAR(20),
    customer_name       VARCHAR(200) NOT NULL,
    email               VARCHAR(200),
    gender              CHAR(1),
    birth_date          DATE,
    customer_since      DATE,

    -- ── Atribute analitike të derivuara ───────────────────
    age_group           VARCHAR(20)  -- 'Rinor' / 'I Rritur' / 'Senior'
        GENERATED ALWAYS AS (
            CASE
                WHEN birth_date IS NULL THEN 'I Panjohur'
                WHEN EXTRACT(YEAR FROM AGE(birth_date)) < 25 THEN 'Rinor'
                WHEN EXTRACT(YEAR FROM AGE(birth_date)) < 60 THEN 'I Rritur'
                ELSE 'Senior'
            END
        ) STORED,
    tenure_years        DECIMAL(4,1)
        GENERATED ALWAYS AS (
            CASE WHEN customer_since IS NOT NULL
                 THEN ROUND(EXTRACT(EPOCH FROM AGE(customer_since)) / 31557600.0, 1)
                 ELSE NULL END
        ) STORED,

    -- ── SCD Type 2 ───────────────────────────────────────
    effective_from      DATE        NOT NULL DEFAULT CURRENT_DATE,
    effective_to        DATE,
    is_current          BOOLEAN     NOT NULL DEFAULT TRUE,
    dw_version          SMALLINT    NOT NULL DEFAULT 1,

    CONSTRAINT chk_dim_customer_dates
        CHECK (effective_to IS NULL OR effective_to >= effective_from)
);

COMMENT ON TABLE  dw.dim_customer               IS 'Dimension Klienti me SCD Type 2. 4 nivele hierarkie.';
COMMENT ON COLUMN dw.dim_customer.age_group      IS 'Grupi mosha i gjeneruar: Rinor (<25) | I Rritur (25-60) | Senior (>60).';
COMMENT ON COLUMN dw.dim_customer.tenure_years   IS 'Vitet si klient = (sot - customer_since) / 365.25.';

CREATE INDEX IF NOT EXISTS idx_dcust_customer_id ON dw.dim_customer(customer_id);
CREATE INDEX IF NOT EXISTS idx_dcust_current     ON dw.dim_customer(customer_id) WHERE is_current = TRUE;
CREATE INDEX IF NOT EXISTS idx_dcust_city        ON dw.dim_customer(city);
CREATE INDEX IF NOT EXISTS idx_dcust_age_group   ON dw.dim_customer(age_group);

-- ============================================================
--  TABELA FAKT: dw.fact_sales
--
--  GRANULARITETI: 1 rresht = 1 linjë produkti e 1 fature.
--
--  MASAT (MEASURES):
--    1. quantity     – sasia e shitur
--    2. revenue_net  – të ardhura neto (pas zbritjes, pa TVSH)
--    3. revenue_gross– të ardhura bruto (me TVSH)
--    4. discount_amt – vlera monetare e zbritjes
--    5. cost_total   – kosto totale (sasi × kosto njësi)
--    6. gross_profit – fitimi bruto (revenue_net - cost_total)
--
--  DEGENERATIVE DIMENSIONS (atribute faturash pa dim. të veçantë):
--    · sale_code, line_number, sale_channel, payment_method
-- ============================================================
CREATE TABLE IF NOT EXISTS dw.fact_sales (
    -- ── Çelësi surrogate i faktit ─────────────────────────
    sale_item_key       BIGSERIAL   PRIMARY KEY,

    -- ── Çelësi natyror (traceability nga source) ──────────
    item_id             INT         NOT NULL,

    -- ── Çelësat e dimensioneve (FK) ───────────────────────
    time_key            INT         NOT NULL
                        REFERENCES dw.dim_time(time_key),
    product_key         INT         NOT NULL
                        REFERENCES dw.dim_product(product_key),
    customer_key        INT         NOT NULL
                        REFERENCES dw.dim_customer(customer_key),

    -- ── Dimensionet e degjeneruara ─────────────────────────
    sale_code           VARCHAR(20),            -- nr. i faturës
    line_number         SMALLINT,               -- linja brenda faturës
    sale_channel        VARCHAR(20),            -- dyqan / online / telefon
    payment_method      VARCHAR(20),            -- cash / kartë / transfer

    -- ── Masat ─────────────────────────────────────────────
    quantity            INT             NOT NULL CHECK (quantity > 0),
    unit_price          DECIMAL(10,2)   NOT NULL CHECK (unit_price >= 0),
    discount_pct        DECIMAL(5,2)    NOT NULL DEFAULT 0,
    tax_rate            DECIMAL(5,2)    NOT NULL DEFAULT 20,

    -- Masa 1: Sasia
    -- quantity  (shih sipër)

    -- Masa 2: Të ardhura neto (pas zbritjes, pa TVSH)
    revenue_net         DECIMAL(12,2)   NOT NULL,

    -- Masa 3: Të ardhura bruto (me TVSH 20%)
    revenue_gross       DECIMAL(12,2)   NOT NULL,

    -- Masa 4: Vlera monetare e zbritjes
    discount_amt        DECIMAL(12,2)   NOT NULL DEFAULT 0,

    -- Masa 5: Kosto totale (quantity × cost_price nga dimensioni)
    cost_total          DECIMAL(12,2),

    -- Masa 6: Fitim bruto (revenue_net - cost_total)
    gross_profit        DECIMAL(12,2),

    -- ── Metadata ngarkimi ─────────────────────────────────
    batch_id            BIGINT,
    loaded_at           TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE  dw.fact_sales                 IS
  'Tabela Fakt – kubt kryesor. Granulariteti: 1 linjë fature. '
  '6 masa: quantity, revenue_net, revenue_gross, discount_amt, cost_total, gross_profit.';
COMMENT ON COLUMN dw.fact_sales.item_id          IS 'Çelës natyror nga source.sale_items. Përdoret për incremental load.';
COMMENT ON COLUMN dw.fact_sales.revenue_net      IS 'qty × unit_price × (1 − discount_pct/100). Pa TVSH.';
COMMENT ON COLUMN dw.fact_sales.revenue_gross    IS 'revenue_net × (1 + tax_rate/100). Me TVSH.';
COMMENT ON COLUMN dw.fact_sales.gross_profit     IS 'revenue_net − cost_total. NULL nëse kosto nuk disponohet.';

-- ── Indekset e performancës ──────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_fact_time_key     ON dw.fact_sales(time_key);
CREATE INDEX IF NOT EXISTS idx_fact_product_key  ON dw.fact_sales(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_customer_key ON dw.fact_sales(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_item_id      ON dw.fact_sales(item_id);
CREATE INDEX IF NOT EXISTS idx_fact_channel      ON dw.fact_sales(sale_channel);
CREATE INDEX IF NOT EXISTS idx_fact_loaded_at    ON dw.fact_sales(loaded_at);
