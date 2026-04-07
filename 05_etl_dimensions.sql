-- ============================================================
--  PUNIM SEMESTRAL: Projektimi dhe Implementimi i Data Warehouse
-- ============================================================
--
--  SKEDARI : 05_etl_dimensions.sql
--  SKEMA    : dw
--  QËLLIMI  : Procedurat ETL për 3 Dimensionet.
--             Çdo dimension ka dy procedura:
--               · etl_dim_X_initial()   – celje fillestare
--               · etl_dim_X_incremental() – rritje periodike
--
--  KONCEPTI SCD TYPE 2 (Slowly Changing Dimensions):
--     Kur vlera e një atributi të rëndësishëm ndryshon
--     (p.sh. çmimi i produktit, qyteti i klientit):
--       1. Rekordi i vjetër mbyllet:
--          effective_to = CURRENT_DATE - 1
--          is_current   = FALSE
--       2. Krijohet rekord i ri:
--          effective_from = CURRENT_DATE
--          effective_to   = NULL
--          is_current     = TRUE
--          dw_version     = version_i_vjetër + 1
--     Rezultati: çdo transaksion historik mbetet i lidhur
--     me versionin e saktë të atributit për atë periudhë.
-- ============================================================

-- ════════════════════════════════════════════════════════════
--  DIM_TIME  –  ETL
-- ════════════════════════════════════════════════════════════

-- ── Celje Fillestare ────────────────────────────────────────
CREATE OR REPLACE PROCEDURE dw.etl_dim_time_initial(
    p_start_date DATE DEFAULT '2023-01-01',
    p_end_date   DATE DEFAULT '2025-12-31'
)
LANGUAGE plpgsql AS $$
DECLARE
    v_date DATE := p_start_date;
    v_cnt  INT  := 0;

    -- Emrat e muajve dhe ditëve sipas kalendarit shqiptar
    v_month_names  TEXT[] := ARRAY['Janar','Shkurt','Mars','Prill','Maj','Qershor',
                                   'Korrik','Gusht','Shtator','Tetor','Nëntor','Dhjetor'];
    v_month_short  TEXT[] := ARRAY['Jan','Shk','Mar','Pri','Maj','Qer',
                                   'Kor','Gus','Sht','Tet','Nën','Dhj'];
    v_day_names    TEXT[] := ARRAY['E Diel','E Hënë','E Martë','E Mërkurë',
                                   'E Enjte','E Premte','E Shtunë'];
    v_day_short    TEXT[] := ARRAY['Die','Hën','Mar','Mër','Enj','Pre','Sht'];

    v_dow      INT;
    v_mon      INT;
    v_yr       INT;
    v_is_hol   BOOLEAN;
    v_hol_name VARCHAR(80);

    -- Festat zyrtare të Shqipërisë (muaj-ditë, pa vit)
    -- Shënohen duke kontrolluar (MONTH, DAY)
    TYPE holiday_rec IS RECORD (hmonth INT, hday INT, hname TEXT);
    v_holidays holiday_rec[];
BEGIN
    TRUNCATE TABLE dw.dim_time CASCADE;

    -- Festat zyrtare fikse (pa Pashkën që është e lëvizshme)
    v_holidays := ARRAY[
        ROW(1,1,'Viti i Ri')::holiday_rec,
        ROW(1,2,'Viti i Ri')::holiday_rec,
        ROW(3,14,'Dita e Verës')::holiday_rec,
        ROW(3,22,'Dita e Nevruzit')::holiday_rec,
        ROW(4,21,'Dita e Punës')::holiday_rec,
        ROW(5,1,'Dita Ndërkombëtare e Punës')::holiday_rec,
        ROW(10,19,'Dita e Nënë Terezës')::holiday_rec,
        ROW(11,28,'Dita e Pavarësisë')::holiday_rec,
        ROW(11,29,'Dita e Çlirimit')::holiday_rec,
        ROW(12,8,'Dita Kombëtare e Rinisë')::holiday_rec,
        ROW(12,25,'Krishtlindjet')::holiday_rec
    ];

    WHILE v_date <= p_end_date LOOP
        v_dow  := EXTRACT(DOW  FROM v_date)::INT;
        v_mon  := EXTRACT(MONTH FROM v_date)::INT;
        v_yr   := EXTRACT(YEAR  FROM v_date)::INT;

        -- Kontrollo festat
        v_is_hol   := FALSE;
        v_hol_name := NULL;
        FOR i IN 1..array_length(v_holidays, 1) LOOP
            IF v_holidays[i].hmonth = v_mon
               AND v_holidays[i].hday = EXTRACT(DAY FROM v_date)::INT
            THEN
                v_is_hol   := TRUE;
                v_hol_name := v_holidays[i].hname;
                EXIT;
            END IF;
        END LOOP;

        INSERT INTO dw.dim_time (
            full_date, total_label,
            year_number, year_label,
            quarter_number, quarter_label,
            month_number, month_name, month_short, month_label, month_year_key,
            day_of_month, day_of_year, day_of_week,
            day_name, day_name_short, iso_week_number,
            is_weekend, is_workday, is_holiday, holiday_name
        )
        VALUES (
            v_date,
            'Total',
            v_yr,
            v_yr::TEXT,
            EXTRACT(QUARTER FROM v_date)::INT,
            'Q' || EXTRACT(QUARTER FROM v_date)::INT || '-' || v_yr,
            v_mon,
            v_month_names[v_mon],
            v_month_short[v_mon],
            v_month_names[v_mon] || ' ' || v_yr,
            (v_yr * 100 + v_mon),
            EXTRACT(DAY  FROM v_date)::INT,
            EXTRACT(DOY  FROM v_date)::INT,
            v_dow,
            v_day_names[v_dow + 1],
            v_day_short[v_dow + 1],
            EXTRACT(WEEK FROM v_date)::INT,
            v_dow IN (0, 6),
            v_dow NOT IN (0, 6) AND NOT v_is_hol,
            v_is_hol,
            v_hol_name
        );

        v_cnt  := v_cnt + 1;
        v_date := v_date + 1;
    END LOOP;

    RAISE NOTICE '[ETL] dim_time — INITIAL: % rreshta gjeneruar (% → %).',
        v_cnt, p_start_date, p_end_date;
END;
$$;
COMMENT ON PROCEDURE dw.etl_dim_time_initial IS
  'Gjeneron çdo datë kalendarike midis p_start_date dhe p_end_date. '
  'Shënon festat zyrtare të Shqipërisë. Thirret vetëm një herë.';

-- ── Rritje Periodike ────────────────────────────────────────
CREATE OR REPLACE PROCEDURE dw.etl_dim_time_incremental(
    p_end_date DATE DEFAULT CURRENT_DATE + 730
)
LANGUAGE plpgsql AS $$
DECLARE
    v_last_date DATE;
    v_cnt INT := 0;
BEGIN
    SELECT MAX(full_date) INTO v_last_date FROM dw.dim_time;

    IF v_last_date IS NULL THEN
        RAISE EXCEPTION 'dim_time është bosh. Ekzekuto etl_dim_time_initial() fillimisht.';
    END IF;

    IF v_last_date >= p_end_date THEN
        RAISE NOTICE '[ETL] dim_time — INCREMENTAL: Asnjë datë e re (max ekzistues: %).', v_last_date;
        RETURN;
    END IF;

    -- Ripërdor logjikën fillestare vetëm për datat e reja
    CALL dw.etl_dim_time_initial(v_last_date + 1, p_end_date);

    RAISE NOTICE '[ETL] dim_time — INCREMENTAL: U shtuan datat % → %.', v_last_date + 1, p_end_date;
END;
$$;


-- ════════════════════════════════════════════════════════════
--  DIM_PRODUCT  –  ETL
-- ════════════════════════════════════════════════════════════

-- ── Celje Fillestare ────────────────────────────────────────
CREATE OR REPLACE PROCEDURE dw.etl_dim_product_initial()
LANGUAGE plpgsql AS $$
DECLARE
    v_rows INT;
BEGIN
    TRUNCATE TABLE dw.dim_product CASCADE;

    INSERT INTO dw.dim_product (
        product_id, total_label,
        category_id,     category_name,
        subcategory_id,  subcategory_name,
        product_code,    product_name,
        unit_price,      cost_price, unit_of_measure,
        effective_from,  effective_to, is_current, dw_version
    )
    SELECT
        p.product_id,    'Total',
        c.category_id,   c.category_name,
        s.subcategory_id, s.subcategory_name,
        p.product_code,  p.product_name,
        p.unit_price,    p.cost_price, p.unit_of_measure,
        CURRENT_DATE,    NULL, TRUE, 1
    FROM stage.products      p
    JOIN stage.subcategories s ON p.subcategory_id = s.subcategory_id
    JOIN stage.categories    c ON s.category_id    = c.category_id
    WHERE p.is_active = TRUE;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RAISE NOTICE '[ETL] dim_product — INITIAL: % produkte u ngarkuan.', v_rows;
END;
$$;

-- ── Rritje Periodike me SCD Type 2 ──────────────────────────
CREATE OR REPLACE PROCEDURE dw.etl_dim_product_incremental()
LANGUAGE plpgsql AS $$
DECLARE
    v_new     INT := 0;
    v_updated INT := 0;
    r RECORD;
BEGIN
    -- ── 1. Produkte KREJTËSISHT TË REJA (nuk ekzistojnë fare në DW) ──
    FOR r IN
        SELECT p.product_id, c.category_id, c.category_name,
               s.subcategory_id, s.subcategory_name,
               p.product_code, p.product_name, p.unit_price,
               p.cost_price, p.unit_of_measure
        FROM stage.products p
        JOIN stage.subcategories s ON p.subcategory_id = s.subcategory_id
        JOIN stage.categories    c ON s.category_id    = c.category_id
        WHERE p.is_active = TRUE
          AND NOT EXISTS (
              SELECT 1 FROM dw.dim_product dp WHERE dp.product_id = p.product_id
          )
    LOOP
        INSERT INTO dw.dim_product (
            product_id, total_label,
            category_id, category_name, subcategory_id, subcategory_name,
            product_code, product_name, unit_price, cost_price, unit_of_measure,
            effective_from, effective_to, is_current, dw_version
        ) VALUES (
            r.product_id, 'Total',
            r.category_id, r.category_name, r.subcategory_id, r.subcategory_name,
            r.product_code, r.product_name, r.unit_price, r.cost_price, r.unit_of_measure,
            CURRENT_DATE, NULL, TRUE, 1
        );
        v_new := v_new + 1;
    END LOOP;

    -- ── 2. Produkte me NDRYSHIM ATRIBUTI (SCD Type 2) ──────────────
    --    Kushti: unit_price ose product_name ka ndryshuar
    FOR r IN
        SELECT p.product_id, c.category_id, c.category_name,
               s.subcategory_id, s.subcategory_name,
               p.product_code, p.product_name, p.unit_price,
               p.cost_price, p.unit_of_measure,
               dp.dw_version AS old_version
        FROM stage.products p
        JOIN stage.subcategories s  ON p.subcategory_id = s.subcategory_id
        JOIN stage.categories    c  ON s.category_id    = c.category_id
        JOIN dw.dim_product      dp ON dp.product_id = p.product_id
                                    AND dp.is_current = TRUE
        WHERE p.is_active = TRUE
          AND (dp.unit_price   <> p.unit_price
            OR dp.product_name <> p.product_name
            OR dp.cost_price   IS DISTINCT FROM p.cost_price)
    LOOP
        -- Mbyll versionin aktual
        UPDATE dw.dim_product
        SET effective_to = CURRENT_DATE - 1,
            is_current   = FALSE
        WHERE product_id = r.product_id
          AND is_current = TRUE;

        -- Hap versionin e ri
        INSERT INTO dw.dim_product (
            product_id, total_label,
            category_id, category_name, subcategory_id, subcategory_name,
            product_code, product_name, unit_price, cost_price, unit_of_measure,
            effective_from, effective_to, is_current, dw_version
        ) VALUES (
            r.product_id, 'Total',
            r.category_id, r.category_name, r.subcategory_id, r.subcategory_name,
            r.product_code, r.product_name, r.unit_price, r.cost_price, r.unit_of_measure,
            CURRENT_DATE, NULL, TRUE, r.old_version + 1
        );
        v_updated := v_updated + 1;
    END LOOP;

    RAISE NOTICE '[ETL] dim_product — INCREMENTAL: % produkte të rinj, % SCD2 ndryshime.',
        v_new, v_updated;
END;
$$;
COMMENT ON PROCEDURE dw.etl_dim_product_incremental IS
  'SCD Type 2: produktet e reja shtohen direkt; produktet me çmim ose '
  'emër të ndryshuar mbyllin versionin e vjetër dhe hapin version të ri.';


-- ════════════════════════════════════════════════════════════
--  DIM_CUSTOMER  –  ETL
-- ════════════════════════════════════════════════════════════

-- ── Celje Fillestare ────────────────────────────────────────
CREATE OR REPLACE PROCEDURE dw.etl_dim_customer_initial()
LANGUAGE plpgsql AS $$
DECLARE v_rows INT;
BEGIN
    TRUNCATE TABLE dw.dim_customer CASCADE;

    INSERT INTO dw.dim_customer (
        customer_id, total_label,
        country, county, city, postal_code,
        customer_code, customer_name, email,
        gender, birth_date, customer_since,
        effective_from, effective_to, is_current, dw_version
    )
    SELECT
        c.customer_id, 'Total',
        r.country, r.county, r.city, r.postal_code,
        c.customer_code, c.full_name, c.email,
        c.gender, c.birth_date, c.customer_since,
        CURRENT_DATE, NULL, TRUE, 1
    FROM stage.customers c
    JOIN stage.regions   r ON c.region_id = r.region_id
    WHERE c.is_active = TRUE;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RAISE NOTICE '[ETL] dim_customer — INITIAL: % klientë u ngarkuan.', v_rows;
END;
$$;

-- ── Rritje Periodike me SCD Type 2 ──────────────────────────
CREATE OR REPLACE PROCEDURE dw.etl_dim_customer_incremental()
LANGUAGE plpgsql AS $$
DECLARE
    v_new     INT := 0;
    v_updated INT := 0;
    r RECORD;
BEGIN
    -- 1. Klientë krejtësisht të rinj
    FOR r IN
        SELECT c.customer_id, r.country, r.county, r.city, r.postal_code,
               c.customer_code, c.full_name, c.email,
               c.gender, c.birth_date, c.customer_since
        FROM stage.customers c
        JOIN stage.regions   r ON c.region_id = r.region_id
        WHERE c.is_active = TRUE
          AND NOT EXISTS (
              SELECT 1 FROM dw.dim_customer dc WHERE dc.customer_id = c.customer_id
          )
    LOOP
        INSERT INTO dw.dim_customer (
            customer_id, total_label,
            country, county, city, postal_code,
            customer_code, customer_name, email,
            gender, birth_date, customer_since,
            effective_from, effective_to, is_current, dw_version
        ) VALUES (
            r.customer_id, 'Total',
            r.country, r.county, r.city, r.postal_code,
            r.customer_code, r.full_name, r.email,
            r.gender, r.birth_date, r.customer_since,
            CURRENT_DATE, NULL, TRUE, 1
        );
        v_new := v_new + 1;
    END LOOP;

    -- 2. Klientë me ndryshim lokacioni ose emri (SCD Type 2)
    FOR r IN
        SELECT c.customer_id, r.country, r.county, r.city, r.postal_code,
               c.customer_code, c.full_name, c.email,
               c.gender, c.birth_date, c.customer_since,
               dc.dw_version AS old_version
        FROM stage.customers  c
        JOIN stage.regions    r  ON c.region_id    = r.region_id
        JOIN dw.dim_customer  dc ON dc.customer_id = c.customer_id
                                AND dc.is_current  = TRUE
        WHERE c.is_active = TRUE
          AND (dc.city    <> r.city
            OR dc.county  <> r.county
            OR dc.customer_name <> c.full_name)
    LOOP
        -- Mbyll versionin aktual
        UPDATE dw.dim_customer
        SET effective_to = CURRENT_DATE - 1,
            is_current   = FALSE
        WHERE customer_id = r.customer_id
          AND is_current  = TRUE;

        -- Hap versionin e ri
        INSERT INTO dw.dim_customer (
            customer_id, total_label,
            country, county, city, postal_code,
            customer_code, customer_name, email,
            gender, birth_date, customer_since,
            effective_from, effective_to, is_current, dw_version
        ) VALUES (
            r.customer_id, 'Total',
            r.country, r.county, r.city, r.postal_code,
            r.customer_code, r.full_name, r.email,
            r.gender, r.birth_date, r.customer_since,
            CURRENT_DATE, NULL, TRUE, r.old_version + 1
        );
        v_updated := v_updated + 1;
    END LOOP;

    RAISE NOTICE '[ETL] dim_customer — INCREMENTAL: % klientë të rinj, % SCD2 ndryshime.',
        v_new, v_updated;
END;
$$;
COMMENT ON PROCEDURE dw.etl_dim_customer_incremental IS
  'SCD Type 2 për klientë. Ndryshimet e qytetit, qarkut ose emrit '
  'gjenerojnë version të ri; versioni i vjetër mbyllet me effective_to.';
