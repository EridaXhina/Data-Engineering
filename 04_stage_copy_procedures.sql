-- ============================================================
--  PUNIM SEMESTRAL: Projektimi dhe Implementimi i Data Warehouse
-- ============================================================
--
--  SKEDARI : 04_stage_copy_procedures.sql
--  SKEMA    : stage
--  QËLLIMI  : Procedurat e kopjimit source → stage.
--             Zbaton strategjinë FULL REFRESH:
--             · TRUNCATE tabelën stage
--             · INSERT të gjitha të dhënat nga source
--             · Llogarit _row_hash (MD5) per detektim ndryshimesh
--             · Regjistron ngarkimin në stage.load_log
--
--  STRATEGJIA FULL REFRESH vs INCREMENTAL:
--     Full Refresh zgjidhet këtu sepse:
--     (a) skemat source janë të vogla (< 100K rreshta);
--     (b) simplifikon validimin — gjithmonë kemi pasqyrë
--         të saktë të gjendjes aktuale;
--     (c) izolulon DW nga ndryshimet e papritura në source.
-- ============================================================

-- ── Utilitet: fillon një batch të ri ────────────────────────
CREATE OR REPLACE FUNCTION stage.begin_batch()
RETURNS BIGINT
LANGUAGE sql AS $$
    SELECT nextval('stage.seq_batch_id');
$$;
COMMENT ON FUNCTION stage.begin_batch() IS
  'Gjeneron dhe kthen batch_id të ri. Thirret një herë në fillim '
  'të çdo cikli master_stage_load().';

-- ─────────────────────────────────────────────────────────────
--  PROCEDURA: stage.copy_categories(batch_id)
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE PROCEDURE stage.copy_categories(p_batch_id BIGINT)
LANGUAGE plpgsql AS $$
DECLARE
    v_rows INT;
    v_start TIMESTAMP := CURRENT_TIMESTAMP;
    v_log_id INT;
BEGIN
    -- Regjistro fillimin
    INSERT INTO stage.load_log (batch_id, table_name, started_at, status)
    VALUES (p_batch_id, 'stage.categories', v_start, 'RUNNING')
    RETURNING log_id INTO v_log_id;

    TRUNCATE TABLE stage.categories;

    INSERT INTO stage.categories
        (category_id, category_name, description, is_active,
         created_at, updated_at, _batch_id, _loaded_at, _row_hash)
    SELECT
        category_id, category_name, description, is_active,
        created_at, updated_at,
        p_batch_id,
        CURRENT_TIMESTAMP,
        MD5(category_id::TEXT || COALESCE(category_name,'') || COALESCE(is_active::TEXT,''))
    FROM source.categories;

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    UPDATE stage.load_log
    SET rows_loaded = v_rows, finished_at = CURRENT_TIMESTAMP, status = 'SUCCESS'
    WHERE log_id = v_log_id;

    RAISE NOTICE '[STAGE] categories: % rreshta u ngarkuan (batch #%).', v_rows, p_batch_id;

EXCEPTION WHEN OTHERS THEN
    UPDATE stage.load_log
    SET finished_at = CURRENT_TIMESTAMP, status = 'FAILED', error_message = SQLERRM
    WHERE log_id = v_log_id;
    RAISE;
END;
$$;

-- ─────────────────────────────────────────────────────────────
--  PROCEDURA: stage.copy_subcategories(batch_id)
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE PROCEDURE stage.copy_subcategories(p_batch_id BIGINT)
LANGUAGE plpgsql AS $$
DECLARE
    v_rows INT; v_start TIMESTAMP := CURRENT_TIMESTAMP; v_log_id INT;
BEGIN
    INSERT INTO stage.load_log (batch_id, table_name, started_at, status)
    VALUES (p_batch_id, 'stage.subcategories', v_start, 'RUNNING')
    RETURNING log_id INTO v_log_id;

    TRUNCATE TABLE stage.subcategories;

    INSERT INTO stage.subcategories
        (subcategory_id, category_id, subcategory_name, description, is_active,
         created_at, updated_at, _batch_id, _loaded_at, _row_hash)
    SELECT
        subcategory_id, category_id, subcategory_name, description, is_active,
        created_at, updated_at, p_batch_id, CURRENT_TIMESTAMP,
        MD5(subcategory_id::TEXT || COALESCE(subcategory_name,'') || category_id::TEXT)
    FROM source.subcategories;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    UPDATE stage.load_log SET rows_loaded=v_rows, finished_at=CURRENT_TIMESTAMP, status='SUCCESS' WHERE log_id=v_log_id;
    RAISE NOTICE '[STAGE] subcategories: % rreshta u ngarkuan.', v_rows;
EXCEPTION WHEN OTHERS THEN
    UPDATE stage.load_log SET finished_at=CURRENT_TIMESTAMP, status='FAILED', error_message=SQLERRM WHERE log_id=v_log_id;
    RAISE;
END;
$$;

-- ─────────────────────────────────────────────────────────────
--  PROCEDURA: stage.copy_products(batch_id)
--  SHËNIM: _row_hash përfshin unit_price — ndryshimi i çmimit
--          do të gjeneroj hash të ndryshëm, duke aktivizuar
--          SCD Type 2 në ETL të dimensionit.
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE PROCEDURE stage.copy_products(p_batch_id BIGINT)
LANGUAGE plpgsql AS $$
DECLARE
    v_rows INT; v_start TIMESTAMP := CURRENT_TIMESTAMP; v_log_id INT;
BEGIN
    INSERT INTO stage.load_log (batch_id, table_name, started_at, status)
    VALUES (p_batch_id, 'stage.products', v_start, 'RUNNING')
    RETURNING log_id INTO v_log_id;

    TRUNCATE TABLE stage.products;

    INSERT INTO stage.products
        (product_id, subcategory_id, product_code, product_name,
         unit_price, cost_price, unit_of_measure, is_active,
         created_at, updated_at, _batch_id, _loaded_at, _row_hash)
    SELECT
        product_id, subcategory_id, product_code, product_name,
        unit_price, cost_price, unit_of_measure, is_active,
        created_at, updated_at, p_batch_id, CURRENT_TIMESTAMP,
        -- Hash përfshin unit_price dhe product_name
        MD5(product_id::TEXT || COALESCE(product_name,'')
            || unit_price::TEXT || COALESCE(cost_price::TEXT,''))
    FROM source.products;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    UPDATE stage.load_log SET rows_loaded=v_rows, finished_at=CURRENT_TIMESTAMP, status='SUCCESS' WHERE log_id=v_log_id;
    RAISE NOTICE '[STAGE] products: % rreshta u ngarkuan.', v_rows;
EXCEPTION WHEN OTHERS THEN
    UPDATE stage.load_log SET finished_at=CURRENT_TIMESTAMP, status='FAILED', error_message=SQLERRM WHERE log_id=v_log_id;
    RAISE;
END;
$$;

-- ─────────────────────────────────────────────────────────────
--  PROCEDURA: stage.copy_regions
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE PROCEDURE stage.copy_regions(p_batch_id BIGINT)
LANGUAGE plpgsql AS $$
DECLARE
    v_rows INT; v_start TIMESTAMP := CURRENT_TIMESTAMP; v_log_id INT;
BEGIN
    INSERT INTO stage.load_log (batch_id, table_name, started_at, status)
    VALUES (p_batch_id, 'stage.regions', v_start, 'RUNNING')
    RETURNING log_id INTO v_log_id;

    TRUNCATE TABLE stage.regions;

    INSERT INTO stage.regions
        (region_id, country, county, city, postal_code,
         created_at, _batch_id, _loaded_at, _row_hash)
    SELECT
        region_id, country, county, city, postal_code,
        created_at, p_batch_id, CURRENT_TIMESTAMP,
        MD5(region_id::TEXT || country || county || city)
    FROM source.regions;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    UPDATE stage.load_log SET rows_loaded=v_rows, finished_at=CURRENT_TIMESTAMP, status='SUCCESS' WHERE log_id=v_log_id;
    RAISE NOTICE '[STAGE] regions: % rreshta u ngarkuan.', v_rows;
EXCEPTION WHEN OTHERS THEN
    UPDATE stage.load_log SET finished_at=CURRENT_TIMESTAMP, status='FAILED', error_message=SQLERRM WHERE log_id=v_log_id;
    RAISE;
END;
$$;

-- ─────────────────────────────────────────────────────────────
--  PROCEDURA: stage.copy_customers
--  SHËNIM: _row_hash përfshin region_id dhe emrin —
--          ndryshimet aktivizojnë SCD2 në dim_customer.
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE PROCEDURE stage.copy_customers(p_batch_id BIGINT)
LANGUAGE plpgsql AS $$
DECLARE
    v_rows INT; v_start TIMESTAMP := CURRENT_TIMESTAMP; v_log_id INT;
BEGIN
    INSERT INTO stage.load_log (batch_id, table_name, started_at, status)
    VALUES (p_batch_id, 'stage.customers', v_start, 'RUNNING')
    RETURNING log_id INTO v_log_id;

    TRUNCATE TABLE stage.customers;

    INSERT INTO stage.customers
        (customer_id, customer_code, first_name, last_name, full_name,
         email, phone, gender, birth_date, region_id,
         customer_since, is_active, created_at, updated_at,
         _batch_id, _loaded_at, _row_hash)
    SELECT
        customer_id, customer_code, first_name, last_name, full_name,
        email, phone, gender, birth_date, region_id,
        customer_since, is_active, created_at, updated_at,
        p_batch_id, CURRENT_TIMESTAMP,
        MD5(customer_id::TEXT || first_name || last_name
            || COALESCE(region_id::TEXT,'') || COALESCE(email,''))
    FROM source.customers;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    UPDATE stage.load_log SET rows_loaded=v_rows, finished_at=CURRENT_TIMESTAMP, status='SUCCESS' WHERE log_id=v_log_id;
    RAISE NOTICE '[STAGE] customers: % rreshta u ngarkuan.', v_rows;
EXCEPTION WHEN OTHERS THEN
    UPDATE stage.load_log SET finished_at=CURRENT_TIMESTAMP, status='FAILED', error_message=SQLERRM WHERE log_id=v_log_id;
    RAISE;
END;
$$;

-- ─────────────────────────────────────────────────────────────
--  PROCEDURA: stage.copy_sales
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE PROCEDURE stage.copy_sales(p_batch_id BIGINT)
LANGUAGE plpgsql AS $$
DECLARE
    v_rows INT; v_start TIMESTAMP := CURRENT_TIMESTAMP; v_log_id INT;
BEGIN
    INSERT INTO stage.load_log (batch_id, table_name, started_at, status)
    VALUES (p_batch_id, 'stage.sales', v_start, 'RUNNING')
    RETURNING log_id INTO v_log_id;

    TRUNCATE TABLE stage.sales;

    INSERT INTO stage.sales
        (sale_id, sale_code, customer_id, region_id, sale_date,
         sale_channel, payment_method, subtotal, discount_total,
         tax_total, grand_total, created_at, _batch_id, _loaded_at, _row_hash)
    SELECT
        sale_id, sale_code, customer_id, region_id, sale_date,
        sale_channel, payment_method, subtotal, discount_total,
        tax_total, grand_total, created_at,
        p_batch_id, CURRENT_TIMESTAMP,
        MD5(sale_id::TEXT || sale_date::TEXT || customer_id::TEXT)
    FROM source.sales;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    UPDATE stage.load_log SET rows_loaded=v_rows, finished_at=CURRENT_TIMESTAMP, status='SUCCESS' WHERE log_id=v_log_id;
    RAISE NOTICE '[STAGE] sales: % rreshta u ngarkuan.', v_rows;
EXCEPTION WHEN OTHERS THEN
    UPDATE stage.load_log SET finished_at=CURRENT_TIMESTAMP, status='FAILED', error_message=SQLERRM WHERE log_id=v_log_id;
    RAISE;
END;
$$;

-- ─────────────────────────────────────────────────────────────
--  PROCEDURA: stage.copy_sale_items
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE PROCEDURE stage.copy_sale_items(p_batch_id BIGINT)
LANGUAGE plpgsql AS $$
DECLARE
    v_rows INT; v_start TIMESTAMP := CURRENT_TIMESTAMP; v_log_id INT;
BEGIN
    INSERT INTO stage.load_log (batch_id, table_name, started_at, status)
    VALUES (p_batch_id, 'stage.sale_items', v_start, 'RUNNING')
    RETURNING log_id INTO v_log_id;

    TRUNCATE TABLE stage.sale_items;

    INSERT INTO stage.sale_items
        (item_id, sale_id, product_id, line_number, quantity,
         unit_price, discount_pct, tax_rate,
         line_subtotal, line_discount, line_net, line_tax, line_total,
         created_at, _batch_id, _loaded_at, _row_hash)
    SELECT
        item_id, sale_id, product_id, line_number, quantity,
        unit_price, discount_pct, tax_rate,
        line_subtotal, line_discount, line_net, line_tax, line_total,
        created_at, p_batch_id, CURRENT_TIMESTAMP,
        MD5(item_id::TEXT || sale_id::TEXT || product_id::TEXT || quantity::TEXT)
    FROM source.sale_items;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    UPDATE stage.load_log SET rows_loaded=v_rows, finished_at=CURRENT_TIMESTAMP, status='SUCCESS' WHERE log_id=v_log_id;
    RAISE NOTICE '[STAGE] sale_items: % rreshta u ngarkuan.', v_rows;
EXCEPTION WHEN OTHERS THEN
    UPDATE stage.load_log SET finished_at=CURRENT_TIMESTAMP, status='FAILED', error_message=SQLERRM WHERE log_id=v_log_id;
    RAISE;
END;
$$;

-- ─────────────────────────────────────────────────────────────
--  MASTER: stage.master_stage_load()
--  Thërret të gjitha procedurat në renditjen e duhur
--  dhe raporton kohëzgjatjen totale.
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE PROCEDURE stage.master_stage_load()
LANGUAGE plpgsql AS $$
DECLARE
    v_batch_id BIGINT := stage.begin_batch();
    v_start    TIMESTAMP := CURRENT_TIMESTAMP;
BEGIN
    RAISE NOTICE '============================================';
    RAISE NOTICE 'STAGE LOAD FILLOI  | Batch #% | %', v_batch_id, v_start;
    RAISE NOTICE '============================================';

    -- Renditja respekton varësinë logjike
    CALL stage.copy_categories(v_batch_id);
    CALL stage.copy_subcategories(v_batch_id);
    CALL stage.copy_products(v_batch_id);
    CALL stage.copy_regions(v_batch_id);
    CALL stage.copy_customers(v_batch_id);
    CALL stage.copy_sales(v_batch_id);
    CALL stage.copy_sale_items(v_batch_id);

    RAISE NOTICE '============================================';
    RAISE NOTICE 'STAGE LOAD PËRFUNDOI | Batch #% | Kohëzgjatja: % sek.',
        v_batch_id,
        ROUND(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start))::NUMERIC, 2);
    RAISE NOTICE '============================================';
END;
$$;
COMMENT ON PROCEDURE stage.master_stage_load() IS
  'Pikë hyrëse e ngarkimit stage. Thërret të gjitha procedurat '
  'copy_* në radhë. Gjeneron batch_id të ri e regjistron çdo hap '
  'në stage.load_log.';
