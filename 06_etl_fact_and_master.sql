-- ============================================================
--  PUNIM SEMESTRAL: Projektimi dhe Implementimi i Data Warehouse
-- ============================================================
--
--  SKEDARI : 06_etl_fact_and_master.sql
--  QËLLIMI  : ETL për FACT_SALES + Master Script i plotë.
--
--  GRANULARITETI FACT_SALES:
--     1 rresht = 1 linjë produkti brenda 1 fature.
--     Çelësi natyror: item_id (nga source.sale_items).
--
--  STRATEGJIA NGARKIMIT:
--     · Initial:     TRUNCATE + INSERT të gjitha të dhënat
--     · Incremental: INSERT vetëm item_id që s'ekzistojnë ende
--
--  LLOGARITJA E MASAVE:
--     revenue_net   = line_net   (nga stage — neto pa TVSH)
--     revenue_gross = line_total (nga stage — me TVSH)
--     discount_amt  = line_discount
--     cost_total    = quantity × cost_price nga dim_product
--     gross_profit  = revenue_net - cost_total
-- ============================================================

-- ════════════════════════════════════════════════════════════
--  FACT_SALES — Celje Fillestare
-- ════════════════════════════════════════════════════════════
CREATE OR REPLACE PROCEDURE dw.etl_fact_sales_initial()
LANGUAGE plpgsql AS $$
DECLARE
    v_rows INT;
    v_missed_time     INT;
    v_missed_product  INT;
    v_missed_customer INT;
BEGIN
    -- ── Validim paraprak: kontrollo referencat e dimensioneve ──
    SELECT COUNT(DISTINCT si.item_id) INTO v_missed_time
    FROM stage.sale_items si
    JOIN stage.sales      sa ON si.sale_id = sa.sale_id
    WHERE NOT EXISTS (
        SELECT 1 FROM dw.dim_time dt WHERE dt.full_date = sa.sale_date
    );
    IF v_missed_time > 0 THEN
        RAISE WARNING '[ETL] FACT_SALES: % linja pa çelës DIM_TIME — ekzekuto etl_dim_time_incremental().',
            v_missed_time;
    END IF;

    SELECT COUNT(DISTINCT si.product_id) INTO v_missed_product
    FROM stage.sale_items si
    WHERE NOT EXISTS (
        SELECT 1 FROM dw.dim_product dp
        WHERE dp.product_id = si.product_id AND dp.is_current = TRUE
    );
    IF v_missed_product > 0 THEN
        RAISE WARNING '[ETL] FACT_SALES: % produkte pa çelës DIM_PRODUCT.', v_missed_product;
    END IF;

    SELECT COUNT(DISTINCT sa.customer_id) INTO v_missed_customer
    FROM stage.sales sa
    WHERE NOT EXISTS (
        SELECT 1 FROM dw.dim_customer dc
        WHERE dc.customer_id = sa.customer_id AND dc.is_current = TRUE
    );
    IF v_missed_customer > 0 THEN
        RAISE WARNING '[ETL] FACT_SALES: % klientë pa çelës DIM_CUSTOMER.', v_missed_customer;
    END IF;

    -- ── Ngarko ──────────────────────────────────────────────
    TRUNCATE TABLE dw.fact_sales;

    INSERT INTO dw.fact_sales (
        item_id,
        time_key, product_key, customer_key,
        sale_code, line_number, sale_channel, payment_method,
        quantity, unit_price, discount_pct, tax_rate,
        revenue_net, revenue_gross, discount_amt,
        cost_total, gross_profit,
        loaded_at
    )
    SELECT
        si.item_id,
        dt.time_key,
        dp.product_key,
        dc.customer_key,
        sa.sale_code,
        si.line_number,
        sa.sale_channel,
        sa.payment_method,
        si.quantity,
        si.unit_price,
        si.discount_pct,
        si.tax_rate,
        si.line_net,                                              -- revenue_net
        si.line_total,                                            -- revenue_gross
        si.line_discount,                                         -- discount_amt
        CASE WHEN dp.cost_price IS NOT NULL
             THEN ROUND(si.quantity * dp.cost_price, 2)
             ELSE NULL END,                                       -- cost_total
        CASE WHEN dp.cost_price IS NOT NULL
             THEN ROUND(si.line_net - si.quantity * dp.cost_price, 2)
             ELSE NULL END,                                       -- gross_profit
        CURRENT_TIMESTAMP
    FROM stage.sale_items   si
    JOIN stage.sales        sa  ON si.sale_id      = sa.sale_id
    JOIN dw.dim_time        dt  ON sa.sale_date    = dt.full_date
    JOIN dw.dim_product     dp  ON si.product_id   = dp.product_id AND dp.is_current = TRUE
    JOIN dw.dim_customer    dc  ON sa.customer_id  = dc.customer_id AND dc.is_current = TRUE;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RAISE NOTICE '[ETL] fact_sales — INITIAL: % rreshta u ngarkuan.', v_rows;
END;
$$;
COMMENT ON PROCEDURE dw.etl_fact_sales_initial IS
  'Ngarkon FACT_SALES nga e para. Validon referencat ndaj 3 dimensioneve '
  'dhe lëshon paralajmërime nëse ka të dhëna pa çelës.';

-- ════════════════════════════════════════════════════════════
--  FACT_SALES — Rritje Periodike
-- ════════════════════════════════════════════════════════════
CREATE OR REPLACE PROCEDURE dw.etl_fact_sales_incremental()
LANGUAGE plpgsql AS $$
DECLARE
    v_rows INT;
BEGIN
    INSERT INTO dw.fact_sales (
        item_id,
        time_key, product_key, customer_key,
        sale_code, line_number, sale_channel, payment_method,
        quantity, unit_price, discount_pct, tax_rate,
        revenue_net, revenue_gross, discount_amt,
        cost_total, gross_profit,
        loaded_at
    )
    SELECT
        si.item_id,
        dt.time_key,   dp.product_key,   dc.customer_key,
        sa.sale_code,  si.line_number,
        sa.sale_channel, sa.payment_method,
        si.quantity,   si.unit_price,    si.discount_pct,  si.tax_rate,
        si.line_net,   si.line_total,    si.line_discount,
        CASE WHEN dp.cost_price IS NOT NULL
             THEN ROUND(si.quantity * dp.cost_price, 2) ELSE NULL END,
        CASE WHEN dp.cost_price IS NOT NULL
             THEN ROUND(si.line_net - si.quantity * dp.cost_price, 2) ELSE NULL END,
        CURRENT_TIMESTAMP
    FROM stage.sale_items   si
    JOIN stage.sales        sa  ON si.sale_id      = sa.sale_id
    JOIN dw.dim_time        dt  ON sa.sale_date    = dt.full_date
    JOIN dw.dim_product     dp  ON si.product_id   = dp.product_id AND dp.is_current = TRUE
    JOIN dw.dim_customer    dc  ON sa.customer_id  = dc.customer_id AND dc.is_current = TRUE
    WHERE NOT EXISTS (                                -- vetëm item_id të rinj
        SELECT 1 FROM dw.fact_sales fs WHERE fs.item_id = si.item_id
    );

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RAISE NOTICE '[ETL] fact_sales — INCREMENTAL: % rreshta të rinj u shtuan.', v_rows;
END;
$$;

-- ════════════════════════════════════════════════════════════
--  MASTER ETL — Pikë hyrëse e vetme
-- ════════════════════════════════════════════════════════════

-- ── Master: Ngarkimi Fillestar (ekzekutohet vetëm 1 herë) ───
CREATE OR REPLACE PROCEDURE dw.master_etl_initial()
LANGUAGE plpgsql AS $$
DECLARE v_start TIMESTAMP := CURRENT_TIMESTAMP;
BEGIN
    RAISE NOTICE '##################################################';
    RAISE NOTICE '#  MASTER ETL — NGARKIMI FILLESTAR               #';
    RAISE NOTICE '#  Filloi: %          #', v_start;
    RAISE NOTICE '##################################################';

    -- HAPI 1: Kopjo source → stage (me batch_id)
    RAISE NOTICE '--- HAPI 1: STAGE LOAD ---';
    CALL stage.master_stage_load();

    -- HAPI 2: Dimensioni Kohës (duhet para faktit)
    RAISE NOTICE '--- HAPI 2: ETL DIM_TIME ---';
    CALL dw.etl_dim_time_initial('2023-01-01', '2025-12-31');

    -- HAPI 3: Dimensioni Produktit
    RAISE NOTICE '--- HAPI 3: ETL DIM_PRODUCT ---';
    CALL dw.etl_dim_product_initial();

    -- HAPI 4: Dimensioni Klientit
    RAISE NOTICE '--- HAPI 4: ETL DIM_CUSTOMER ---';
    CALL dw.etl_dim_customer_initial();

    -- HAPI 5: Tabela Fakt (vetëm pasi janë gati të gjitha dimensionet)
    RAISE NOTICE '--- HAPI 5: ETL FACT_SALES ---';
    CALL dw.etl_fact_sales_initial();

    RAISE NOTICE '##################################################';
    RAISE NOTICE '#  MASTER ETL FILLESTAR PËRFUNDOI                #';
    RAISE NOTICE '#  Kohëzgjatja: % sek.    #',
        ROUND(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start))::NUMERIC, 2);
    RAISE NOTICE '##################################################';
END;
$$;
COMMENT ON PROCEDURE dw.master_etl_initial IS
  'Ekzekuton ngarkimin fillestar të plotë: Stage → 3 Dim → Fact. '
  'Thirret vetëm herën e parë. Për ngarkime të mëvonshme: master_etl_incremental().';

-- ── Master: Rritja Periodike (çdo periudhë) ─────────────────
CREATE OR REPLACE PROCEDURE dw.master_etl_incremental()
LANGUAGE plpgsql AS $$
DECLARE v_start TIMESTAMP := CURRENT_TIMESTAMP;
BEGIN
    RAISE NOTICE '##################################################';
    RAISE NOTICE '#  MASTER ETL — RRITJA PERIODIKE                 #';
    RAISE NOTICE '#  Filloi: %          #', v_start;
    RAISE NOTICE '##################################################';

    CALL stage.master_stage_load();

    RAISE NOTICE '--- RRITJA: DIM_TIME ---';
    CALL dw.etl_dim_time_incremental(CURRENT_DATE + 730);

    RAISE NOTICE '--- RRITJA: DIM_PRODUCT (SCD Type 2) ---';
    CALL dw.etl_dim_product_incremental();

    RAISE NOTICE '--- RRITJA: DIM_CUSTOMER (SCD Type 2) ---';
    CALL dw.etl_dim_customer_incremental();

    RAISE NOTICE '--- RRITJA: FACT_SALES ---';
    CALL dw.etl_fact_sales_incremental();

    RAISE NOTICE '##################################################';
    RAISE NOTICE '#  MASTER ETL RRITJE PËRFUNDOI                   #';
    RAISE NOTICE '#  Kohëzgjatja: % sek.    #',
        ROUND(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start))::NUMERIC, 2);
    RAISE NOTICE '##################################################';
END;
$$;
COMMENT ON PROCEDURE dw.master_etl_incremental IS
  'Rritja periodike: rifresko Stage + përditëso 3 Dim me SCD2 + shto '
  'transaksionet e reja në Fact. Thirret pas çdo periudhe raportimi.';
