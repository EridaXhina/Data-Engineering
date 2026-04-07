-- ============================================================
--  PUNIM SEMESTRAL: Projektimi dhe Implementimi i Data Warehouse
--  Universiteti Politeknik i Tiranës · Departamenti i Informatikës
--  Viti Akademik 2023 – 2024
-- ============================================================
--
--  SKEDARI : 01_source_schema.sql
--  SKEMA    : source
--  QËLLIMI  : Modelon shtresën transaksionale (OLTP) të një
--             sistemi shitjesh me pakicë (Retail Sales).
--             Të gjitha tabelat burim janë ndërtuar këtu.
--
--  VARËSI   : Asnjë (skedari i parë që ekzekutohet)
--
--  EKZEKUTIMI:
--      psql -U postgres -d datawarehouse -f 01_source_schema.sql
--
--  HISTORIKU I NDRYSHIMEVE:
--      v1.0  2024-01-10  Krijimi fillestar
--      v1.1  2024-02-15  Shtimi i indekseve dhe të dhënave shtesë
-- ============================================================

-- ── Parakushtet ─────────────────────────────────────────────
SET client_encoding = 'UTF8';
SET search_path     = public;

-- ── Krijimi i skemës ────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS source;
COMMENT ON SCHEMA source IS
  'Shtresa transaksionale (OLTP). Përmban tabelat operacionale '
  'të sistemit të shitjeve me pakicë. Pasqyron gjendjen aktuale '
  'pa historik ndryshimesh.';

-- ============================================================
--  TABELA: source.categories
--  Kategoritë kryesore të produkteve (niveli 1 i hierarkisë)
-- ============================================================
CREATE TABLE IF NOT EXISTS source.categories (
    category_id     SERIAL          PRIMARY KEY,
    category_name   VARCHAR(100)    NOT NULL,
    description     TEXT,
    is_active       BOOLEAN         NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_category_name UNIQUE (category_name)
);
COMMENT ON TABLE  source.categories              IS 'Kategoritë kryesore të produkteve';
COMMENT ON COLUMN source.categories.category_id  IS 'Çelësi primar surrogate (auto-increment)';
COMMENT ON COLUMN source.categories.is_active    IS 'FALSE = kategoria është çaktivizuar';

-- ============================================================
--  TABELA: source.subcategories
--  Nënkategoritë e produkteve (niveli 2 i hierarkisë)
-- ============================================================
CREATE TABLE IF NOT EXISTS source.subcategories (
    subcategory_id   SERIAL       PRIMARY KEY,
    category_id      INT          NOT NULL
                                  REFERENCES source.categories(category_id)
                                  ON UPDATE CASCADE ON DELETE RESTRICT,
    subcategory_name VARCHAR(100) NOT NULL,
    description      TEXT,
    is_active        BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at       TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_subcategory UNIQUE (category_id, subcategory_name)
);
COMMENT ON TABLE source.subcategories IS
  'Nënkategoritë e produkteve. Secila nënkategori i përket saktësisht '
  'një kategorie (marrëdhënie 1-shumë).';

-- ============================================================
--  TABELA: source.products
--  Katalogu i produkteve me çmimin aktual
-- ============================================================
CREATE TABLE IF NOT EXISTS source.products (
    product_id       SERIAL          PRIMARY KEY,
    subcategory_id   INT             NOT NULL
                                     REFERENCES source.subcategories(subcategory_id)
                                     ON UPDATE CASCADE ON DELETE RESTRICT,
    product_code     VARCHAR(20)     NOT NULL,
    product_name     VARCHAR(200)    NOT NULL,
    unit_price       DECIMAL(10,2)   NOT NULL CHECK (unit_price > 0),
    cost_price       DECIMAL(10,2)              CHECK (cost_price > 0),
    unit_of_measure  VARCHAR(20)     NOT NULL DEFAULT 'copë',
    is_active        BOOLEAN         NOT NULL DEFAULT TRUE,
    created_at       TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_product_code UNIQUE (product_code)
);
COMMENT ON TABLE  source.products             IS 'Katalogu i produkteve. Çmimi këtu pasqyron çmimin aktual (OLTP).';
COMMENT ON COLUMN source.products.product_code IS 'Kodi unik i produktit (p.sh. EL-TEL-001)';
COMMENT ON COLUMN source.products.cost_price   IS 'Çmimi i blerjes (kosto). Përdoret për llogaritjen e marzhit.';

-- ============================================================
--  TABELA: source.regions
--  Rajonet gjeografike (shtet / qark / qytet — 3 nivele)
-- ============================================================
CREATE TABLE IF NOT EXISTS source.regions (
    region_id       SERIAL       PRIMARY KEY,
    country         VARCHAR(100) NOT NULL DEFAULT 'Shqipëri',
    county          VARCHAR(100) NOT NULL,   -- qarku
    city            VARCHAR(100) NOT NULL,   -- qyteti
    postal_code     VARCHAR(10),
    created_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_region UNIQUE (country, county, city)
);
COMMENT ON TABLE source.regions IS
  'Hierarkia gjeografike 3-nivelëshe: Qytet → Qark → Shtet. '
  'Përdoret nga klientët dhe pikat e shitjes.';

-- ============================================================
--  TABELA: source.customers
--  Klientët e sistemit
-- ============================================================
CREATE TABLE IF NOT EXISTS source.customers (
    customer_id     SERIAL          PRIMARY KEY,
    customer_code   VARCHAR(20)     NOT NULL,
    first_name      VARCHAR(100)    NOT NULL,
    last_name       VARCHAR(100)    NOT NULL,
    full_name       VARCHAR(200)    GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED,
    email           VARCHAR(200),
    phone           VARCHAR(20),
    gender          CHAR(1)         CHECK (gender IN ('M','F','T')),
    birth_date      DATE,
    region_id       INT             REFERENCES source.regions(region_id)
                                    ON UPDATE CASCADE ON DELETE SET NULL,
    customer_since  DATE            NOT NULL DEFAULT CURRENT_DATE,
    is_active       BOOLEAN         NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_customer_code  UNIQUE (customer_code),
    CONSTRAINT uq_customer_email UNIQUE (email)
);
COMMENT ON TABLE  source.customers              IS 'Klientët e regjistruar në sistem.';
COMMENT ON COLUMN source.customers.full_name    IS 'Kolona e gjeneruar automatikisht (first + last name).';
COMMENT ON COLUMN source.customers.customer_since IS 'Data e regjistrimit të parë të klientit.';

-- ============================================================
--  TABELA: source.sales
--  Kokat e faturave të shitjes (header)
-- ============================================================
CREATE TABLE IF NOT EXISTS source.sales (
    sale_id         SERIAL          PRIMARY KEY,
    sale_code       VARCHAR(20)     NOT NULL,
    customer_id     INT             NOT NULL
                                    REFERENCES source.customers(customer_id)
                                    ON UPDATE CASCADE ON DELETE RESTRICT,
    region_id       INT             REFERENCES source.regions(region_id)
                                    ON UPDATE CASCADE ON DELETE SET NULL,
    sale_date       DATE            NOT NULL,
    sale_channel    VARCHAR(20)     NOT NULL DEFAULT 'dyqan'
                                    CHECK (sale_channel IN ('dyqan','online','telefon','partner')),
    payment_method  VARCHAR(20)     NOT NULL DEFAULT 'cash'
                                    CHECK (payment_method IN ('cash','kartë','transfer','kredi')),
    subtotal        DECIMAL(12,2)   NOT NULL DEFAULT 0 CHECK (subtotal >= 0),
    discount_total  DECIMAL(12,2)   NOT NULL DEFAULT 0 CHECK (discount_total >= 0),
    tax_total       DECIMAL(12,2)   NOT NULL DEFAULT 0 CHECK (tax_total >= 0),
    grand_total     DECIMAL(12,2)   NOT NULL DEFAULT 0 CHECK (grand_total >= 0),
    notes           TEXT,
    created_at      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_sale_code UNIQUE (sale_code)
);
COMMENT ON TABLE  source.sales                IS 'Kokat e faturave — çdo rresht = 1 faturë.';
COMMENT ON COLUMN source.sales.sale_channel   IS 'Kanali i shitjes: dyqan / online / telefon / partner.';
COMMENT ON COLUMN source.sales.payment_method IS 'Metoda e pagesës.';
COMMENT ON COLUMN source.sales.grand_total    IS 'subtotal - discount_total + tax_total.';

-- ============================================================
--  TABELA: source.sale_items
--  Linjat e faturave (detail / line items)
-- ============================================================
CREATE TABLE IF NOT EXISTS source.sale_items (
    item_id         SERIAL          PRIMARY KEY,
    sale_id         INT             NOT NULL
                                    REFERENCES source.sales(sale_id)
                                    ON UPDATE CASCADE ON DELETE CASCADE,
    product_id      INT             NOT NULL
                                    REFERENCES source.products(product_id)
                                    ON UPDATE CASCADE ON DELETE RESTRICT,
    line_number     SMALLINT        NOT NULL CHECK (line_number > 0),
    quantity        INT             NOT NULL CHECK (quantity > 0),
    unit_price      DECIMAL(10,2)   NOT NULL CHECK (unit_price >= 0),
    discount_pct    DECIMAL(5,2)    NOT NULL DEFAULT 0
                                    CHECK (discount_pct BETWEEN 0 AND 100),
    tax_rate        DECIMAL(5,2)    NOT NULL DEFAULT 20
                                    CHECK (tax_rate BETWEEN 0 AND 100),
    line_subtotal   DECIMAL(12,2)   GENERATED ALWAYS AS
                                    (ROUND(quantity * unit_price, 2)) STORED,
    line_discount   DECIMAL(12,2)   GENERATED ALWAYS AS
                                    (ROUND(quantity * unit_price * discount_pct / 100, 2)) STORED,
    line_net        DECIMAL(12,2)   GENERATED ALWAYS AS
                                    (ROUND(quantity * unit_price * (1 - discount_pct / 100), 2)) STORED,
    line_tax        DECIMAL(12,2)   GENERATED ALWAYS AS
                                    (ROUND(quantity * unit_price * (1 - discount_pct / 100) * tax_rate / 100, 2)) STORED,
    line_total      DECIMAL(12,2)   GENERATED ALWAYS AS
                                    (ROUND(quantity * unit_price * (1 - discount_pct / 100) * (1 + tax_rate / 100), 2)) STORED,
    created_at      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_sale_line UNIQUE (sale_id, line_number)
);
COMMENT ON TABLE  source.sale_items              IS 'Linjat detajuese të çdo fature.';
COMMENT ON COLUMN source.sale_items.line_subtotal IS 'qty × unit_price (pa zbritje, pa TVSH)';
COMMENT ON COLUMN source.sale_items.line_net      IS 'Pas zbritjes, para TVSH-së';
COMMENT ON COLUMN source.sale_items.line_total    IS 'Shuma finale duke përfshirë TVSH-në';

-- ============================================================
--  INDEKSET E PERFORMANCËS
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_products_subcat    ON source.products(subcategory_id);
CREATE INDEX IF NOT EXISTS idx_customers_region   ON source.customers(region_id);
CREATE INDEX IF NOT EXISTS idx_sales_customer     ON source.sales(customer_id);
CREATE INDEX IF NOT EXISTS idx_sales_date         ON source.sales(sale_date);
CREATE INDEX IF NOT EXISTS idx_sales_channel      ON source.sales(sale_channel);
CREATE INDEX IF NOT EXISTS idx_sale_items_sale    ON source.sale_items(sale_id);
CREATE INDEX IF NOT EXISTS idx_sale_items_product ON source.sale_items(product_id);

-- ============================================================
--  TË DHËNAT FILLESTARE (SEED DATA)
--  Të dhëna realiste për domenin "Retail Albania"
-- ============================================================

-- ── Kategoritë ──────────────────────────────────────────────
INSERT INTO source.categories (category_name, description) VALUES
  ('Elektronikë',     'Pajisje elektronike dhe aksesorë teknologjikë'),
  ('Veshmbathje',     'Artikuj veshjeje për të gjitha moshat dhe sezone'),
  ('Ushqim & Pije',   'Produkte ushqimore dhe pije'),
  ('Kozmetikë',       'Produkte kujdesi personal dhe kozmetikë'),
  ('Shtëpi & Kopësht','Mobilje, dekorime dhe pajisje shtëpiake')
ON CONFLICT (category_name) DO NOTHING;

-- ── Nënkategoritë ───────────────────────────────────────────
INSERT INTO source.subcategories (category_id, subcategory_name, description)
SELECT c.category_id, v.sname, v.sdesc
FROM source.categories c
JOIN (VALUES
  ('Elektronikë',     'Telefona Celularë',  'Smartphone dhe telefona bazë'),
  ('Elektronikë',     'Laptop & Tablet',    'Kompjuterë portativë dhe tabletë'),
  ('Elektronikë',     'Aksesorë Teknologjikë', 'Karikues, kufje, mbrojtëse'),
  ('Elektronikë',     'Televizorë & Audio', 'Televizorë, altoparlantë, sisteme audio'),
  ('Veshmbathje',     'Meshkuj Të Rritur',  'Veshje për meshkuj mbi 18 vjeç'),
  ('Veshmbathje',     'Femra Të Rritura',   'Veshje për femra mbi 18 vjeç'),
  ('Veshmbathje',     'Fëmijë 0–14',        'Veshje për fëmijë deri 14 vjeç'),
  ('Veshmbathje',     'Këpucë & Çorape',    'Këpucë dhe aksesorë këmbësh'),
  ('Ushqim & Pije',   'Fruta & Perime',     'Produkte të freskëta bimore'),
  ('Ushqim & Pije',   'Bulmet & Vezë',      'Qumësht, djathë, kos, vezë'),
  ('Ushqim & Pije',   'Pije & Ujë',         'Pije freskuese, ujë mineral, lëngje'),
  ('Ushqim & Pije',   'Kafe & Çaj',         'Kafeja, çajrat dhe produkte infuzioni'),
  ('Kozmetikë',       'Kujdes Lëkure',      'Krem, serum, produkte pastrimi'),
  ('Kozmetikë',       'Parfume & Deodorantë','Parfume dhe produkte higjiene'),
  ('Shtëpi & Kopësht','Pajisje Kuzhine',    'Mikser, tostiere, kazan elektrik'),
  ('Shtëpi & Kopësht','Dekorime Shtëpie',   'Korniza, qilima, jastëkë, kandela')
) AS v(cname, sname, sdesc) ON c.category_name = v.cname
ON CONFLICT (category_id, subcategory_name) DO NOTHING;

-- ── Produktet ──────────────────────────────────────────────
INSERT INTO source.products (subcategory_id, product_code, product_name, unit_price, cost_price, unit_of_measure)
SELECT s.subcategory_id, v.code, v.pname, v.price, v.cost, v.uom
FROM source.subcategories s
JOIN source.categories c ON s.category_id = c.category_id
JOIN (VALUES
  -- Telefona Celularë
  ('Elektronikë','Telefona Celularë','EL-TEL-001','iPhone 15 Pro 256GB',    1299.00, 950.00,'copë'),
  ('Elektronikë','Telefona Celularë','EL-TEL-002','Samsung Galaxy S24 128GB', 999.00, 730.00,'copë'),
  ('Elektronikë','Telefona Celularë','EL-TEL-003','Xiaomi 13T Pro 256GB',     749.00, 540.00,'copë'),
  ('Elektronikë','Telefona Celularë','EL-TEL-004','Realme 11 Pro 128GB',      399.00, 280.00,'copë'),
  -- Laptop & Tablet
  ('Elektronikë','Laptop & Tablet','EL-LAP-001','MacBook Air M3 16GB',       1599.00,1200.00,'copë'),
  ('Elektronikë','Laptop & Tablet','EL-LAP-002','Dell XPS 15 Intel i7',      1399.00,1000.00,'copë'),
  ('Elektronikë','Laptop & Tablet','EL-LAP-003','Lenovo IdeaPad 5 Ryzen 7',  899.00, 640.00,'copë'),
  ('Elektronikë','Laptop & Tablet','EL-LAP-004','iPad Air 11" M2 WiFi',      799.00, 580.00,'copë'),
  -- Aksesorë
  ('Elektronikë','Aksesorë Teknologjikë','EL-AKS-001','Kufje Sony WH-1000XM5',    299.00, 180.00,'copë'),
  ('Elektronikë','Aksesorë Teknologjikë','EL-AKS-002','Karikues GaN 65W USB-C',    49.00,  22.00,'copë'),
  ('Elektronikë','Aksesorë Teknologjikë','EL-AKS-003','Mbrojtëse Ekrani Tempered',  9.99,   2.50,'copë'),
  -- Meshkuj
  ('Veshmbathje','Meshkuj Të Rritur','VE-MES-001','Xhaketë Dimri Woolrich',    149.00,  75.00,'copë'),
  ('Veshmbathje','Meshkuj Të Rritur','VE-MES-002','Pantallona Chino Slim Fit',  69.00,  32.00,'copë'),
  ('Veshmbathje','Meshkuj Të Rritur','VE-MES-003','Bluzë Polo Pambuk',          34.99,  14.00,'copë'),
  -- Femra
  ('Veshmbathje','Femra Të Rritura','VE-FEM-001','Fustane Festive Mëndafshi',  189.00,  90.00,'copë'),
  ('Veshmbathje','Femra Të Rritura','VE-FEM-002','Xhaketë Leshi Merino',       129.00,  60.00,'copë'),
  ('Veshmbathje','Femra Të Rritura','VE-FEM-003','Pantallona Yoga High-Waist',  59.00,  25.00,'copë'),
  -- Fëmijë
  ('Veshmbathje','Fëmijë 0–14','VE-FEK-001','Kostum Dimri Fëmijësh 3–8v',  79.00,  38.00,'copë'),
  ('Veshmbathje','Fëmijë 0–14','VE-FEK-002','T-Shirt Pambuk Fëmijësh',      19.99,   7.00,'copë'),
  -- Ushqim
  ('Ushqim & Pije','Fruta & Perime','US-FRU-001','Mollë Starking kg',            2.50,   1.20,'kg'),
  ('Ushqim & Pije','Fruta & Perime','US-FRU-002','Domate Organike kg',           3.20,   1.50,'kg'),
  ('Ushqim & Pije','Bulmet & Vezë', 'US-BUL-001','Djathë i bardhë Gjirokastër 400g', 5.50, 3.20,'copë'),
  ('Ushqim & Pije','Bulmet & Vezë', 'US-BUL-002','Kos Natyral Trofta 500g',     2.99,   1.60,'copë'),
  ('Ushqim & Pije','Pije & Ujë',    'US-PIJ-001','Ujë Mineral Tepelena 6×1.5L', 4.50,   2.00,'paketë'),
  ('Ushqim & Pije','Kafe & Çaj',    'US-KAF-001','Kafe Illy Espresso 250g',     8.99,   5.00,'copë'),
  -- Kozmetikë
  ('Kozmetikë','Kujdes Lëkure','KO-LEK-001','Serum Vitamin C 30ml',        39.99,  18.00,'copë'),
  ('Kozmetikë','Parfume & Deodorantë','KO-PAR-001','Parfum Dior Sauvage 100ml',  129.00,  65.00,'copë'),
  -- Shtëpi
  ('Shtëpi & Kopësht','Pajisje Kuzhine','SH-KUZ-001','Mikser Bamix 200W',      89.00,  45.00,'copë'),
  ('Shtëpi & Kopësht','Dekorime Shtëpie','SH-DEK-001','Qilim Leshi Kilim 160×240',299.00, 140.00,'copë')
) AS v(cname, sname, code, pname, price, cost, uom)
ON c.category_name = v.cname AND s.subcategory_name = v.sname
ON CONFLICT (product_code) DO NOTHING;

-- ── Rajonet ────────────────────────────────────────────────
INSERT INTO source.regions (country, county, city, postal_code) VALUES
  ('Shqipëri','Tiranë',        'Tiranë',       '1001'),
  ('Shqipëri','Tiranë',        'Kamëz',        '1030'),
  ('Shqipëri','Durrës',        'Durrës',       '2001'),
  ('Shqipëri','Durrës',        'Shijak',       '2010'),
  ('Shqipëri','Vlorë',         'Vlorë',        '9401'),
  ('Shqipëri','Vlorë',         'Sarandë',      '9701'),
  ('Shqipëri','Shkodër',       'Shkodër',      '4001'),
  ('Shqipëri','Elbasan',       'Elbasan',      '3001'),
  ('Shqipëri','Korçë',         'Korçë',        '7001'),
  ('Shqipëri','Fier',          'Fier',         '5001'),
  ('Shqipëri','Berat',         'Berat',        '5001'),
  ('Shqipëri','Gjirokastër',   'Gjirokastër',  '6001')
ON CONFLICT (country, county, city) DO NOTHING;

-- ── Klientët (50 klientë me të dhëna realiste) ────────────
DO $$
DECLARE
  first_names_m TEXT[] := ARRAY['Arben','Genci','Ilir','Klevis','Drini','Nesti','Urim','Shpetim','Qemal','Albert','Pjerin','Ergys','Ledio','Kristi','Hekuran'];
  first_names_f TEXT[] := ARRAY['Blerta','Elisa','Fatjona','Hana','Joana','Loreta','Mirela','Ora','Rozeta','Teuta','Valbona','Xheni','Yllka','Zana','Besa'];
  last_names    TEXT[] := ARRAY['Hoxha','Koci','Marku','Gjoka','Domi','Basha','Çela','Prendi','Shehu','Ndoja','Bekteshi','Cara','Leka','Xhafaj','Deda','Rustemi','Hysaj','Xhaka','Malaj','Cela'];
  channels      TEXT[] := ARRAY['dyqan','online','dyqan','dyqan','online','telefon','partner'];
  r_id INT; fn TEXT; ln TEXT; g CHAR(1); birth DATE; since DATE;
BEGIN
  FOR i IN 1..50 LOOP
    IF i % 2 = 0 THEN
      fn := first_names_m[(i / 2 % 15) + 1]; g := 'M';
    ELSE
      fn := first_names_f[((i+1) / 2 % 15) + 1]; g := 'F';
    END IF;
    ln    := last_names[(i % 20) + 1];
    r_id  := (i % 12) + 1;
    birth := DATE '1975-01-01' + ((RANDOM() * 17520)::INT);  -- 1975-2022
    since := DATE '2019-01-01' + ((RANDOM() * 1825)::INT);   -- 2019-2024

    INSERT INTO source.customers
      (customer_code, first_name, last_name, email, gender, birth_date, region_id, customer_since)
    VALUES (
      'CUST-' || LPAD(i::TEXT, 4, '0'),
      fn, ln,
      LOWER(REPLACE(fn,' ','.')) || '.' || LOWER(REPLACE(ln,' ','.')) || i || '@email.al',
      g, birth, r_id, since
    ) ON CONFLICT DO NOTHING;
  END LOOP;
END $$;

-- ── Shitjet (600 fatura me linja detajuese realiste) ────────
DO $$
DECLARE
  v_sale_id    INT;
  v_cust_id    INT;
  v_region_id  INT;
  v_date       DATE;
  v_channel    TEXT;
  v_payment    TEXT;
  v_prod_id    INT;
  v_qty        INT;
  v_price      DECIMAL;
  v_disc       DECIMAL;
  v_lines      INT;
  channels     TEXT[] := ARRAY['dyqan','dyqan','online','dyqan','online','telefon'];
  payments     TEXT[] := ARRAY['cash','kartë','cash','transfer','kartë','cash'];
  disc_opts    DECIMAL[] := ARRAY[0,0,0,5,5,10,15,20];
BEGIN
  FOR n IN 1..600 LOOP
    v_cust_id   := (RANDOM() * 49)::INT + 1;
    v_date      := DATE '2023-01-01' + (RANDOM() * 730)::INT;
    v_channel   := channels[(RANDOM()*5)::INT + 1];
    v_payment   := payments[(RANDOM()*5)::INT + 1];

    SELECT region_id INTO v_region_id
    FROM source.customers WHERE customer_id = v_cust_id;

    INSERT INTO source.sales
      (sale_code, customer_id, region_id, sale_date, sale_channel, payment_method,
       subtotal, discount_total, tax_total, grand_total)
    VALUES (
      'INV-2023-' || LPAD(n::TEXT, 5, '0'),
      v_cust_id, v_region_id, v_date, v_channel, v_payment,
      0, 0, 0, 0
    )
    RETURNING sale_id INTO v_sale_id;

    v_lines := (RANDOM() * 4 + 1)::INT;  -- 1 deri 5 linja

    FOR ln IN 1..v_lines LOOP
      v_prod_id := (RANDOM() * 28)::INT + 1;
      v_qty     := (RANDOM() * 3 + 1)::INT;
      v_disc    := disc_opts[(RANDOM() * 7)::INT + 1];

      SELECT unit_price INTO v_price
      FROM source.products WHERE product_id = v_prod_id;

      INSERT INTO source.sale_items
        (sale_id, product_id, line_number, quantity, unit_price, discount_pct, tax_rate)
      VALUES
        (v_sale_id, v_prod_id, ln, v_qty, v_price, v_disc, 20);
    END LOOP;

    -- Përditëso totalet e faturës
    UPDATE source.sales sa
    SET
      subtotal       = COALESCE((SELECT SUM(line_subtotal) FROM source.sale_items WHERE sale_id = v_sale_id), 0),
      discount_total = COALESCE((SELECT SUM(line_discount) FROM source.sale_items WHERE sale_id = v_sale_id), 0),
      tax_total      = COALESCE((SELECT SUM(line_tax)      FROM source.sale_items WHERE sale_id = v_sale_id), 0),
      grand_total    = COALESCE((SELECT SUM(line_total)    FROM source.sale_items WHERE sale_id = v_sale_id), 0)
    WHERE sa.sale_id = v_sale_id;
  END LOOP;
END $$;

-- ── Verifikimi ──────────────────────────────────────────────
DO $$
BEGIN
  RAISE NOTICE '=== VERIFIKIMI I SKEMËS SOURCE ===';
  RAISE NOTICE 'categories    : % rreshta', (SELECT COUNT(*) FROM source.categories);
  RAISE NOTICE 'subcategories : % rreshta', (SELECT COUNT(*) FROM source.subcategories);
  RAISE NOTICE 'products      : % rreshta', (SELECT COUNT(*) FROM source.products);
  RAISE NOTICE 'regions       : % rreshta', (SELECT COUNT(*) FROM source.regions);
  RAISE NOTICE 'customers     : % rreshta', (SELECT COUNT(*) FROM source.customers);
  RAISE NOTICE 'sales         : % rreshta', (SELECT COUNT(*) FROM source.sales);
  RAISE NOTICE 'sale_items    : % rreshta', (SELECT COUNT(*) FROM source.sale_items);
  RAISE NOTICE '==================================';
END $$;
