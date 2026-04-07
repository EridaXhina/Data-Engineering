# Projektimi dhe Implementimi i një Data Warehouse
## Punim Semestral — Universiteti Politeknik i Tiranës
### Viti Akademik 2023–2024

---

## 1. Përshkrimi i Projektit

Ky projekt implementon një **Data Warehouse** mbi sistemin e menaxhimit
të bazave të të dhënave **PostgreSQL 14+**, duke ndjekur metodologjinë e
modelimit dimensional **Star Schema**.

### Domeni i Biznesit
Sistem shitjesh me pakicë (Retail Albania) me:
- **29 produkte** të organizuara në 5 kategori dhe 16 nënkategori
- **50 klientë** nga 12 qytete shqiptare
- **~600 fatura** me ~2.000 linja produkti (2023–2024)

---

## 2. Arkitektura: 3 Skemat

```
┌─────────────────────────────────────────────────────────────┐
│                    SHTRESA SOURCE (OLTP)                     │
│  source.categories  source.subcategories  source.products   │
│  source.regions     source.customers      source.sales      │
│                     source.sale_items                       │
└───────────────────────────┬─────────────────────────────────┘
                    Full Refresh │ (CALL stage.master_stage_load())
                    + MD5 Hash   │
┌───────────────────────────▼─────────────────────────────────┐
│                  SHTRESA STAGE (Ndërmjetëse)                 │
│  stage.categories  stage.subcategories   stage.products     │
│  stage.regions     stage.customers       stage.sales        │
│                    stage.sale_items                         │
│  ── Auditim: _batch_id · _loaded_at · _row_hash ──          │
│  ── Log: stage.load_log ────────────────────────────────    │
└───────────────────────────┬─────────────────────────────────┘
               ETL (SCD Type 2) │ (CALL dw.master_etl_initial/incremental)
┌───────────────────────────▼─────────────────────────────────┐
│               SHTRESA DW — STAR SCHEMA (OLAP)                │
│                                                              │
│   DIM_TIME         DIM_PRODUCT        DIM_CUSTOMER           │
│   (5 nivele)       (4 nivele, SCD2)   (4 nivele, SCD2)       │
│        │                │                   │                │
│        └────────────────┤   FACT_SALES  ────┘                │
│                         │  6 masa       │                    │
│                         │ quantity      │                    │
│                         │ revenue_net   │                    │
│                         │ revenue_gross │                    │
│                         │ discount_amt  │                    │
│                         │ cost_total    │                    │
│                         │ gross_profit  │                    │
│                                                              │
│   ── 7 VIEW Analitike (v_r01 … v_r07) ──                    │
└─────────────────────────────────────────────────────────────┘
```

---

## 3. Struktura e Skedarëve

```
sql/
├── 01_source/
│   └── 01_source_schema.sql          ← 7 tabela OLTP + 600 fatura sample
│
├── 02_stage/
│   └── 02_stage_schema.sql           ← 7 tabela stage + load_log + seq_batch_id
│
├── 03_dw/
│   └── 03_dw_schema.sql              ← DIM_TIME · DIM_PRODUCT · DIM_CUSTOMER
│                                        FACT_SALES (6 masa) + indekse
│
├── 04_stage_copy/
│   └── 04_stage_copy_procedures.sql  ← 7 procedura copy_* + master_stage_load()
│
├── 05_etl_dims/
│   └── 05_etl_dimensions.sql         ← ETL initial + incremental (SCD Type 2)
│                                        për 3 dimensionet
│
├── 06_etl_fact/
│   └── 06_etl_fact_and_master.sql    ← ETL initial + incremental FACT_SALES
│                                        + master_etl_initial/incremental()
│
└── 07_reports/
    └── 07_raporte_analitike.sql      ← 7 VIEW analitike + v_audit_stage_log
```

---

## 4. Renditja e Ekzekutimit (Herën e Parë)

```sql
-- ── Krijohen skemat dhe tabelat ──────────────────────────────
\i sql/01_source/01_source_schema.sql
\i sql/02_stage/02_stage_schema.sql
\i sql/03_dw/03_dw_schema.sql

-- ── Krijohen procedurat ──────────────────────────────────────
\i sql/04_stage_copy/04_stage_copy_procedures.sql
\i sql/05_etl_dims/05_etl_dimensions.sql
\i sql/06_etl_fact/06_etl_fact_and_master.sql

-- ── Krijohen raportet ────────────────────────────────────────
\i sql/07_reports/07_raporte_analitike.sql

-- ── Ekzekuto ngarkimin fillestar ─────────────────────────────
CALL dw.master_etl_initial();
```

## 5. Ngarkimi Periodik

```sql
-- Ekzekutohet çdo ditë / javë / muaj sipas nevojës
CALL dw.master_etl_incremental();
```

---

## 6. Hierarkitë e Dimensioneve

| Dimension     | L1      | L2       | L3          | L4      | L5   |
|---------------|---------|----------|-------------|---------|------|
| DIM_TIME      | Total   | Vit      | Tremujor    | Muaj    | Datë |
| DIM_PRODUCT   | Total   | Kategori | NënKategori | Produkt | —    |
| DIM_CUSTOMER  | Total   | Shtet    | Qytet       | Klient  | —    |

---

## 7. Masat e FACT_SALES

| Masa           | Llogaritja                                       |
|----------------|--------------------------------------------------|
| `quantity`     | Sasia e produktit të shitur                      |
| `revenue_net`  | qty × price × (1 − disc%)                       |
| `revenue_gross`| revenue_net × (1 + tax_rate%)                   |
| `discount_amt` | qty × price × disc%                             |
| `cost_total`   | qty × cost_price (nga dim_product)              |
| `gross_profit` | revenue_net − cost_total                         |

---

## 8. Raportet Analitike

| ID   | VIEW                           | Teknika SQL         |
|------|--------------------------------|---------------------|
| R01  | v_r01_revenue_by_year_category | GROUP BY ROLLUP     |
| R02  | v_r02_top_products             | RANK() OVER         |
| R03  | v_r03_monthly_trend            | SUM() + LAG() OVER  |
| R04  | v_r04_geographic_performance   | RANK() + %          |
| R05  | v_r05_quarterly_yoy            | LAG() PARTITION BY  |
| R06  | v_r06_product_month_pivot      | CASE WHEN (PIVOT)   |
| R07  | v_r07_pareto_customers         | CTE + Cumulative %  |
| AUD  | v_audit_stage_log              | Auditimi ETL        |

---

## 9. Tiparet Teknike

- **SCD Type 2**: `effective_from`, `effective_to`, `is_current`, `dw_version`
- **Auditim batch**: `stage.seq_batch_id`, `stage.load_log`, `_row_hash` MD5
- **Kolona të gjeneruara**: `full_name`, `line_net`, `price_band`, `gross_margin_pct`, `age_group`, `tenure_years`
- **Festat zyrtare**: 11 festa kombëtare shqiptare shënohen në `dim_time.is_holiday`
- **Validim ETL**: paralajmërime nëse ka çelësa dimensionesh të munguar para ngarkimit të faktit
- **Indekse**: 12 indekse për performancë optimale të query-ve analitike
