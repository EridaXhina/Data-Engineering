-- ============================================================
--  PUNIM SEMESTRAL: Projektimi dhe Implementimi i Data Warehouse
-- ============================================================
--
--  SKEDARI : 07_raporte_analitike.sql
--  SKEMA    : dw
--  QËLLIMI  : 7 raporte dinamike (VIEW) mbi DataMart.
--
--  ┌─────────────────────────────────────────────────────────┐
--  │  #  │ Emërtimi                     │ Teknika SQL        │
--  ├─────┼──────────────────────────────┼────────────────────┤
--  │ R01 │ Revenue sipas Vitit/Kategorisë│ ROLLUP             │
--  │ R02 │ Top 10 Produkte              │ ORDER BY, RANK()   │
--  │ R03 │ Trend Mujor + Kumulative     │ Window SUM()       │
--  │ R04 │ Performanca Gjeografike      │ GROUP BY multilevel│
--  │ R05 │ Krahasim Tremujor YoY        │ LAG() OVER()       │
--  │ R06 │ Matricë Produkt × Muaj       │ CASE WHEN (PIVOT)  │
--  │ R07 │ Segmentim Pareto 80/20       │ CTE + RANK() + %   │
--  └─────┴──────────────────────────────┴────────────────────┘
--
--  TË GJITHA VIEW-t janë dinamike:
--  Çdo SELECT prodhon rezultat bazuar mbi gjendjen aktuale
--  të FACT_SALES dhe dimensioneve.
-- ============================================================

-- ════════════════════════════════════════════════════════════
--  R01 — Revenue sipas Vitit dhe Kategorisë (ROLLUP)
--
--  QËLLIMI:  Ofron shikim hierarkik të të ardhurave neto:
--            · Total i përgjithshëm (vit=NULL, kat=NULL)
--            · Total per vit       (kat=NULL)
--            · Detajuar per vit×kat
--
--  TEKNIKA:  GROUP BY ROLLUP — krijon automatikisht subtotalet
--            dhe grand total pa nevojën e UNION-eve manuale.
-- ════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW dw.v_r01_revenue_by_year_category AS
SELECT
    COALESCE(dt.year_label, '— TOTAL —')      AS viti,
    COALESCE(dp.category_name, '— Të gjitha —') AS kategoria,
    COUNT(DISTINCT fs.sale_item_key)           AS nr_transaksioneve,
    SUM(fs.quantity)                           AS sasia_totale,
    ROUND(SUM(fs.revenue_net)::NUMERIC,    2)  AS te_ardhura_neto_all,
    ROUND(SUM(fs.revenue_gross)::NUMERIC,  2)  AS te_ardhura_bruto_all,
    ROUND(SUM(fs.discount_amt)::NUMERIC,   2)  AS zbritjet_totale_all,
    ROUND(SUM(fs.gross_profit)::NUMERIC,   2)  AS fitimi_bruto_all,
    ROUND(AVG(fs.revenue_net)::NUMERIC,    2)  AS mesatare_per_transaksion
FROM dw.fact_sales    fs
JOIN dw.dim_time      dt ON fs.time_key    = dt.time_key
JOIN dw.dim_product   dp ON fs.product_key = dp.product_key
GROUP BY ROLLUP (dt.year_label, dp.category_name)
ORDER BY dt.year_label NULLS LAST, dp.category_name NULLS LAST;

COMMENT ON VIEW dw.v_r01_revenue_by_year_category IS
  'R01 — Agregim hierarkik me ROLLUP. '
  'Rreshtat me NULL = subtotale ose grand total.';

-- ════════════════════════════════════════════════════════════
--  R02 — Top 10 Produkte sipas Fitimit Bruto
--
--  QËLLIMI:  Identifikon produktet me kontributin më të lartë
--            ndaj fitimit bruto të organizatës.
--
--  TEKNIKA:  RANK() OVER ORDER BY me LIMIT 10.
-- ════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW dw.v_r02_top_products AS
WITH product_metrics AS (
    SELECT
        dp.product_code,
        dp.product_name,
        dp.category_name,
        dp.subcategory_name,
        dp.price_band,
        dp.unit_price                                        AS cmimi_current,
        SUM(fs.quantity)                                     AS sasia_shitur,
        ROUND(SUM(fs.revenue_net)::NUMERIC,   2)            AS te_ardhura_neto,
        ROUND(SUM(fs.gross_profit)::NUMERIC,  2)            AS fitimi_bruto,
        ROUND(SUM(fs.discount_amt)::NUMERIC,  2)            AS zbritjet,
        COUNT(DISTINCT fs.customer_key)                      AS klientet_unik,
        ROUND(AVG(fs.discount_pct)::NUMERIC,  2)            AS zbritja_mesatare_pct,
        ROUND(SUM(fs.gross_profit)::NUMERIC * 100.0
              / NULLIF(SUM(fs.revenue_net)::NUMERIC, 0), 2) AS marzhi_bruto_pct
    FROM dw.fact_sales    fs
    JOIN dw.dim_product   dp ON fs.product_key = dp.product_key
    GROUP BY dp.product_code, dp.product_name, dp.category_name,
             dp.subcategory_name, dp.price_band, dp.unit_price
)
SELECT
    RANK() OVER (ORDER BY fitimi_bruto DESC NULLS LAST) AS renditja,
    product_code, product_name, category_name, subcategory_name,
    price_band, cmimi_current, sasia_shitur,
    te_ardhura_neto, fitimi_bruto, marzhi_bruto_pct,
    zbritjet, zbritja_mesatare_pct, klientet_unik
FROM product_metrics
ORDER BY fitimi_bruto DESC NULLS LAST
LIMIT 10;

COMMENT ON VIEW dw.v_r02_top_products IS
  'R02 — Top 10 produkte sipas fitimit bruto. '
  'Përfshin marzhët, zbritjet dhe klientët unikë.';

-- ════════════════════════════════════════════════════════════
--  R03 — Trend Mujor me Vlerë Kumulative
--
--  QËLLIMI:  Tregon ecurinë mujore të shitjeve dhe
--            akumulimin e vlerës gjatë vitit.
--
--  TEKNIKA:  SUM() OVER (PARTITION BY year ORDER BY month)
--            Window frame: ROWS UNBOUNDED PRECEDING.
-- ════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW dw.v_r03_monthly_trend AS
WITH monthly AS (
    SELECT
        dt.year_label,
        dt.year_number,
        dt.month_number,
        dt.month_name,
        dt.month_short,
        COUNT(DISTINCT fs.sale_item_key)                    AS transaksionet,
        COUNT(DISTINCT fs.customer_key)                     AS klientet_aktiv,
        SUM(fs.quantity)                                    AS sasia,
        ROUND(SUM(fs.revenue_net)::NUMERIC,   2)           AS te_ardhura_neto,
        ROUND(SUM(fs.revenue_gross)::NUMERIC, 2)           AS te_ardhura_bruto,
        ROUND(SUM(fs.discount_amt)::NUMERIC,  2)           AS zbritjet,
        ROUND(AVG(fs.revenue_net)::NUMERIC,   2)           AS aov  -- Avg Order Value
    FROM dw.fact_sales fs
    JOIN dw.dim_time   dt ON fs.time_key = dt.time_key
    GROUP BY dt.year_label, dt.year_number, dt.month_number, dt.month_name, dt.month_short
)
SELECT
    year_label, month_number, month_name, month_short,
    transaksionet, klientet_aktiv, sasia,
    te_ardhura_neto, te_ardhura_bruto, zbritjet, aov,
    -- Kumulative brenda vitit
    ROUND(SUM(te_ardhura_neto) OVER (
        PARTITION BY year_label
        ORDER BY month_number
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )::NUMERIC, 2)                                         AS kumulativ_neto,
    -- Ndryshimi % ndaj muajit paraardhës
    ROUND((te_ardhura_neto - LAG(te_ardhura_neto) OVER (
        PARTITION BY year_label ORDER BY month_number
    )) * 100.0 / NULLIF(LAG(te_ardhura_neto) OVER (
        PARTITION BY year_label ORDER BY month_number
    ), 0), 2)                                              AS ndryshimi_pct_mom
FROM monthly
ORDER BY year_label, month_number;

COMMENT ON VIEW dw.v_r03_monthly_trend IS
  'R03 — Trend mujor me kumulativ vjetor (Window SUM) '
  'dhe ndryshim % muaj-mbi-muaj (Window LAG).';

-- ════════════════════════════════════════════════════════════
--  R04 — Performanca Gjeografike (Qytet / Qark)
--
--  QËLLIMI:  Identifikon tregjet me potencial dhe ato nën
--            mesataren. Bazë për vendime strategjike gjeografike.
-- ════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW dw.v_r04_geographic_performance AS
WITH geo AS (
    SELECT
        dc.country, dc.county, dc.city,
        COUNT(DISTINCT dc.customer_id)                      AS klientet,
        COUNT(DISTINCT fs.sale_item_key)                    AS blerjet,
        SUM(fs.quantity)                                    AS sasia,
        ROUND(SUM(fs.revenue_net)::NUMERIC,   2)           AS te_ardhura_neto,
        ROUND(SUM(fs.gross_profit)::NUMERIC,  2)           AS fitimi_bruto,
        ROUND(AVG(fs.revenue_net)::NUMERIC,   2)           AS aov
    FROM dw.fact_sales    fs
    JOIN dw.dim_customer  dc ON fs.customer_key = dc.customer_key
    GROUP BY dc.country, dc.county, dc.city
)
SELECT
    country, county, city,
    klientet, blerjet, sasia, te_ardhura_neto, fitimi_bruto, aov,
    ROUND(te_ardhura_neto / NULLIF(klientet, 0), 2)        AS ltv_klient,    -- Lifetime Value
    ROUND(blerjet::NUMERIC / NULLIF(klientet, 0), 2)       AS blerje_per_klient,
    -- Renditja sipas të ardhurave
    RANK() OVER (ORDER BY te_ardhura_neto DESC)            AS rank_te_ardhura,
    -- % kontribut ndaj totalit
    ROUND(te_ardhura_neto * 100.0
          / SUM(te_ardhura_neto) OVER (), 2)               AS pct_e_totalit
FROM geo
ORDER BY te_ardhura_neto DESC;

COMMENT ON VIEW dw.v_r04_geographic_performance IS
  'R04 — Performanca sipas qytetit me LTV (Lifetime Value) dhe '
  'kontribut % ndaj totalit të të ardhurave.';

-- ════════════════════════════════════════════════════════════
--  R05 — Krahasim Tremujor Vit-mbi-Vit (YoY)
--
--  QËLLIMI:  Krahason performancën e çdo tremujori me të njëjtin
--            tremujor të vitit të mëparshëm.
--
--  TEKNIKA:  LAG(value, 1) OVER (PARTITION BY quarter ORDER BY year)
-- ════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW dw.v_r05_quarterly_yoy AS
WITH quarterly AS (
    SELECT
        dt.year_label, dt.year_number,
        dt.quarter_number, dt.quarter_label,
        COUNT(DISTINCT fs.sale_item_key)               AS transaksionet,
        SUM(fs.quantity)                               AS sasia,
        ROUND(SUM(fs.revenue_net)::NUMERIC,  2)       AS te_ardhura_neto,
        ROUND(SUM(fs.gross_profit)::NUMERIC, 2)       AS fitimi_bruto,
        COUNT(DISTINCT fs.customer_key)                AS klientet_aktiv
    FROM dw.fact_sales fs
    JOIN dw.dim_time   dt ON fs.time_key = dt.time_key
    GROUP BY dt.year_label, dt.year_number, dt.quarter_number, dt.quarter_label
)
SELECT
    quarter_label, year_label, quarter_number,
    transaksionet, sasia, te_ardhura_neto, fitimi_bruto, klientet_aktiv,
    -- Vlerat e vitit të mëparshëm (LAG)
    LAG(te_ardhura_neto)   OVER (PARTITION BY quarter_number ORDER BY year_number)
                                                       AS te_ardhura_vitin_pare,
    -- Ndryshimi absolut
    te_ardhura_neto
    - LAG(te_ardhura_neto) OVER (PARTITION BY quarter_number ORDER BY year_number)
                                                       AS ndryshimi_absolut,
    -- Ndryshimi %  (YoY Growth Rate)
    ROUND((te_ardhura_neto
    - LAG(te_ardhura_neto) OVER (PARTITION BY quarter_number ORDER BY year_number))
    * 100.0
    / NULLIF(
        LAG(te_ardhura_neto) OVER (PARTITION BY quarter_number ORDER BY year_number),
      0), 2)                                           AS yoy_growth_pct
FROM quarterly
ORDER BY year_number, quarter_number;

COMMENT ON VIEW dw.v_r05_quarterly_yoy IS
  'R05 — Krahasim YoY (Year-over-Year) per tremujor. '
  'Njeh rritjen ose rënien % ndaj vitit të mëparshëm.';

-- ════════════════════════════════════════════════════════════
--  R06 — Matricë Produkt × Muaj (PIVOT manual)
--
--  QËLLIMI:  Tregon shitjet e çdo produkti të shpërndara
--            sipas muajve. Identifikon sezonalitetin.
--
--  TEKNIKA:  CASE WHEN month_number = N THEN revenue END
--            Agregimi nul-i prodhon 0 për muaj pa shitje.
-- ════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW dw.v_r06_product_month_pivot AS
SELECT
    dt.year_label                                            AS viti,
    dp.category_name                                         AS kategoria,
    dp.product_name                                          AS produkti,
    -- 12 kolona mujore
    ROUND(SUM(CASE WHEN dt.month_number =  1 THEN fs.revenue_net ELSE 0 END)::NUMERIC,2) AS jan,
    ROUND(SUM(CASE WHEN dt.month_number =  2 THEN fs.revenue_net ELSE 0 END)::NUMERIC,2) AS shk,
    ROUND(SUM(CASE WHEN dt.month_number =  3 THEN fs.revenue_net ELSE 0 END)::NUMERIC,2) AS mar,
    ROUND(SUM(CASE WHEN dt.month_number =  4 THEN fs.revenue_net ELSE 0 END)::NUMERIC,2) AS pri,
    ROUND(SUM(CASE WHEN dt.month_number =  5 THEN fs.revenue_net ELSE 0 END)::NUMERIC,2) AS maj,
    ROUND(SUM(CASE WHEN dt.month_number =  6 THEN fs.revenue_net ELSE 0 END)::NUMERIC,2) AS qer,
    ROUND(SUM(CASE WHEN dt.month_number =  7 THEN fs.revenue_net ELSE 0 END)::NUMERIC,2) AS kor,
    ROUND(SUM(CASE WHEN dt.month_number =  8 THEN fs.revenue_net ELSE 0 END)::NUMERIC,2) AS gus,
    ROUND(SUM(CASE WHEN dt.month_number =  9 THEN fs.revenue_net ELSE 0 END)::NUMERIC,2) AS sht,
    ROUND(SUM(CASE WHEN dt.month_number = 10 THEN fs.revenue_net ELSE 0 END)::NUMERIC,2) AS tet,
    ROUND(SUM(CASE WHEN dt.month_number = 11 THEN fs.revenue_net ELSE 0 END)::NUMERIC,2) AS nen,
    ROUND(SUM(CASE WHEN dt.month_number = 12 THEN fs.revenue_net ELSE 0 END)::NUMERIC,2) AS dhj,
    -- Totali dhe maksimumi mujor
    ROUND(SUM(fs.revenue_net)::NUMERIC, 2)                  AS total_vjetor,
    ROUND(MAX(CASE WHEN dt.month_number =  1 THEN fs.revenue_net ELSE 0 END +
              CASE WHEN dt.month_number =  2 THEN fs.revenue_net ELSE 0 END +
              CASE WHEN dt.month_number =  3 THEN fs.revenue_net ELSE 0 END)::NUMERIC, 2)
                                                             AS total_q1,
    ROUND(MAX(CASE WHEN dt.month_number =  4 THEN fs.revenue_net ELSE 0 END +
              CASE WHEN dt.month_number =  5 THEN fs.revenue_net ELSE 0 END +
              CASE WHEN dt.month_number =  6 THEN fs.revenue_net ELSE 0 END)::NUMERIC, 2)
                                                             AS total_q2,
    ROUND(MAX(CASE WHEN dt.month_number =  7 THEN fs.revenue_net ELSE 0 END +
              CASE WHEN dt.month_number =  8 THEN fs.revenue_net ELSE 0 END +
              CASE WHEN dt.month_number =  9 THEN fs.revenue_net ELSE 0 END)::NUMERIC, 2)
                                                             AS total_q3,
    ROUND(MAX(CASE WHEN dt.month_number = 10 THEN fs.revenue_net ELSE 0 END +
              CASE WHEN dt.month_number = 11 THEN fs.revenue_net ELSE 0 END +
              CASE WHEN dt.month_number = 12 THEN fs.revenue_net ELSE 0 END)::NUMERIC, 2)
                                                             AS total_q4
FROM dw.fact_sales    fs
JOIN dw.dim_time      dt ON fs.time_key    = dt.time_key
JOIN dw.dim_product   dp ON fs.product_key = dp.product_key
GROUP BY dt.year_label, dp.category_name, dp.product_name
ORDER BY dt.year_label, total_vjetor DESC;

COMMENT ON VIEW dw.v_r06_product_month_pivot IS
  'R06 — Matricë PIVOT 12×produkt. Evidenton sezonalitetin. '
  'Kolonat janë muajt e vitit + totalet tremujore.';

-- ════════════════════════════════════════════════════════════
--  R07 — Segmentim Klientësh: Analiza Pareto 80/20
--
--  QËLLIMI:  Identifikon klientët VIP (20% e klientëve
--            që gjenerojnë 80% të të ardhurave).
--            Bazë për programe fidelizimi.
--
--  TEKNIKA:  CTE → RANK() → SUM kumulativ → % ndaj totalit
-- ════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW dw.v_r07_pareto_customers AS
WITH customer_agg AS (
    -- Agreg i plotë per klient
    SELECT
        dc.customer_id,
        dc.customer_name,
        dc.city,
        dc.county,
        dc.age_group,
        dc.tenure_years,
        COUNT(DISTINCT fs.sale_item_key)               AS nr_blerjeve,
        SUM(fs.quantity)                               AS sasia_totale,
        ROUND(SUM(fs.revenue_net)::NUMERIC,   2)      AS te_ardhura,
        ROUND(SUM(fs.gross_profit)::NUMERIC,  2)      AS fitimi,
        ROUND(AVG(fs.revenue_net)::NUMERIC,   2)      AS aov,
        COUNT(DISTINCT dt.month_year_key)              AS muaj_aktiv,
        MIN(dt.full_date)                              AS blerja_e_pare,
        MAX(dt.full_date)                              AS blerja_e_fundit
    FROM dw.fact_sales    fs
    JOIN dw.dim_customer  dc ON fs.customer_key = dc.customer_key
    JOIN dw.dim_time      dt ON fs.time_key     = dt.time_key
    GROUP BY dc.customer_id, dc.customer_name, dc.city, dc.county,
             dc.age_group, dc.tenure_years
),
ranked AS (
    -- Shto renditjen dhe % kontribut
    SELECT *,
        RANK() OVER (ORDER BY te_ardhura DESC)        AS renditja,
        ROUND(te_ardhura * 100.0
              / SUM(te_ardhura) OVER (), 2)           AS pct_e_totalit,
        ROUND(SUM(te_ardhura) OVER (
            ORDER BY te_ardhura DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) * 100.0 / SUM(te_ardhura) OVER (), 2)       AS pct_kumulativ
    FROM customer_agg
)
SELECT
    renditja, customer_id, customer_name, city, county,
    age_group, tenure_years,
    nr_blerjeve, sasia_totale, te_ardhura, fitimi, aov,
    muaj_aktiv, blerja_e_pare, blerja_e_fundit,
    pct_e_totalit, pct_kumulativ,
    -- Segmenti Pareto
    CASE
        WHEN pct_kumulativ <= 80 THEN 'VIP (Pareto 80%)'
        WHEN pct_kumulativ <= 95 THEN 'Aktiv'
        ELSE 'Sporadik'
    END                                                AS segmenti,
    -- Frekuenca mesatare (ditë ndërmjet blerjeve)
    CASE WHEN nr_blerjeve > 1
         THEN ROUND((blerja_e_fundit - blerja_e_pare)::NUMERIC / (nr_blerjeve - 1), 0)
         ELSE NULL
    END                                                AS dite_ndermjet_blerjeve
FROM ranked
ORDER BY renditja;

COMMENT ON VIEW dw.v_r07_pareto_customers IS
  'R07 — Segmentim Pareto 80/20. '
  'VIP = klientët e parë që gjenerojnë 80% të të ardhurave. '
  'Përfshin frekuencën e blerjeve dhe AOV (Avg Order Value).';

-- ════════════════════════════════════════════════════════════
--  PAMJE NDIHMËSE: Auditimi i ngarkimeve stage
-- ════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW dw.v_audit_stage_log AS
SELECT
    l.batch_id,
    l.table_name,
    l.rows_loaded,
    l.started_at,
    l.finished_at,
    l.duration_sec,
    l.status,
    l.error_message
FROM stage.load_log l
ORDER BY l.batch_id DESC, l.log_id ASC;

COMMENT ON VIEW dw.v_audit_stage_log IS
  'Pamje auditimi: shfaq historikun e të gjitha ngarkimeve stage, '
  'me kohëzgjatje dhe status per çdo batch.';
