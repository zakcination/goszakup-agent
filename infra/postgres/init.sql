-- ══════════════════════════════════════════════════════════════════════════════
-- goszakup-agent · PostgreSQL Schema · Spiral 1
-- Run order matters (FK dependencies): extensions → refs → core → analytics
-- ══════════════════════════════════════════════════════════════════════════════

-- Extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";   -- fuzzy text search on lot names

-- ─────────────────────────────────────────────────────────────────────────────
-- REFERENCE TABLES (no FK dependencies — load first)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS units_ref (
    code        INTEGER PRIMARY KEY,
    name_ru     TEXT NOT NULL,
    name_kz     TEXT,
    name_norm   TEXT NOT NULL,  -- normalised: 'шт', 'кг', 'м²', 'услуга', etc.
    aliases     TEXT[],         -- ['штука','единица','шт.']  → used in ETL clean
    created_at  TIMESTAMPTZ DEFAULT NOW()
);
COMMENT ON TABLE units_ref IS 'МКЕЙ — units of measurement, normalised for analytics';

CREATE TABLE IF NOT EXISTS kato_ref (
    code        VARCHAR(20) PRIMARY KEY,
    name_ru     TEXT NOT NULL,
    name_kz     TEXT,
    level       SMALLINT NOT NULL CHECK (level BETWEEN 1 AND 5),
    parent_code VARCHAR(20) REFERENCES kato_ref(code),
    region_id   VARCHAR(20),   -- oblast-level code for K_region grouping
    created_at  TIMESTAMPTZ DEFAULT NOW()
);
COMMENT ON TABLE kato_ref IS 'КАТО — Kazakhstan administrative territories';

CREATE TABLE IF NOT EXISTS trd_buy_kato_metadata (
    id            SERIAL PRIMARY KEY,
    trd_buy_id    BIGINT NOT NULL,
    lot_id        BIGINT REFERENCES lots(id),
    kato_code     VARCHAR(20),
    parent_kato   VARCHAR(20),
    city_ru       TEXT,
    city_kz       TEXT,
    source        TEXT DEFAULT 'trd-buy',
    raw_json      JSONB,
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    updated_at    TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (trd_buy_id, lot_id, kato_code)
);
COMMENT ON TABLE trd_buy_kato_metadata IS 'Raw kato metadata extracted from trd-buy payloads';

CREATE TABLE IF NOT EXISTS enstru_ref (
    code        VARCHAR(60) PRIMARY KEY,
    name_ru     TEXT NOT NULL,
    name_kz     TEXT,
    section     VARCHAR(10),   -- top-level section code
    division    VARCHAR(10),
    group_code  VARCHAR(10),
    is_active   BOOLEAN DEFAULT TRUE,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);
COMMENT ON TABLE enstru_ref IS 'КТРУ / ЕНС ТРУ — goods/services classifier';
CREATE INDEX idx_enstru_section ON enstru_ref(section);

CREATE TABLE IF NOT EXISTS trade_methods_ref (
    id          INTEGER PRIMARY KEY,
    name_ru     TEXT NOT NULL,
    name_kz     TEXT,
    code        VARCHAR(30)
);
COMMENT ON TABLE trade_methods_ref IS 'Purchase methods: tender, auction, single source, etc.';

CREATE TABLE IF NOT EXISTS lot_statuses_ref (
    id          INTEGER PRIMARY KEY,
    name_ru     TEXT NOT NULL,
    name_kz     TEXT
);

CREATE TABLE IF NOT EXISTS contract_statuses_ref (
    id          INTEGER PRIMARY KEY,
    name_ru     TEXT NOT NULL,
    name_kz     TEXT
);

CREATE TABLE IF NOT EXISTS macro_indices (
    year             SMALLINT PRIMARY KEY,
    inflation_pct    NUMERIC(6,3) NOT NULL,
    gdp_growth_pct   NUMERIC(6,3) NOT NULL,
    basket_price_kzt NUMERIC(18,2),
    source           TEXT DEFAULT 'manual',
    created_at       TIMESTAMPTZ DEFAULT NOW(),
    updated_at       TIMESTAMPTZ DEFAULT NOW()
);
COMMENT ON TABLE macro_indices IS 'National macro indices by year (inflation, GDP growth, basket price)';

-- ─────────────────────────────────────────────────────────────────────────────
-- CORE TABLES
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS subjects (
    bin             VARCHAR(12) PRIMARY KEY,
    name_ru         TEXT NOT NULL,
    name_kz         TEXT,
    full_name_ru    TEXT,
    is_customer     BOOLEAN DEFAULT FALSE,
    is_supplier     BOOLEAN DEFAULT FALSE,
    is_organizer    BOOLEAN DEFAULT FALSE,
    kato_code       VARCHAR(20) REFERENCES kato_ref(code),
    ref_kopf_code   VARCHAR(20),    -- legal form: ГУ, ТОО, АО, etc.
    is_rnu          BOOLEAN DEFAULT FALSE,   -- in bad suppliers registry
    rnu_checked_at  TIMESTAMPTZ,
    email           TEXT,
    phone           TEXT,
    website         TEXT,
    is_qvazi        BOOLEAN DEFAULT FALSE,   -- quasi-state sector
    pid             INTEGER,                 -- OWS internal subject ID
    last_synced_at  TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);
COMMENT ON TABLE subjects IS 'All organisations: customers (TARGET_BINS) + suppliers';
CREATE INDEX idx_subjects_is_customer ON subjects(is_customer) WHERE is_customer = TRUE;
CREATE INDEX idx_subjects_is_rnu ON subjects(is_rnu) WHERE is_rnu = TRUE;

CREATE TABLE IF NOT EXISTS plan_points (
    id                      BIGINT PRIMARY KEY,  -- OWS plan point ID
    customer_bin            VARCHAR(12) NOT NULL REFERENCES subjects(bin),
    enstru_code             VARCHAR(60) REFERENCES enstru_ref(code),
    fin_year                SMALLINT NOT NULL,
    plan_act_number         VARCHAR(30),
    name_ru                 TEXT NOT NULL,
    name_kz                 TEXT,
    unit_code               INTEGER REFERENCES units_ref(code),
    quantity                NUMERIC(18,4),
    unit_price              NUMERIC(18,4),
    total_amount            NUMERIC(18,4),
    kato_delivery           VARCHAR(20) REFERENCES kato_ref(code),
    delivery_address_ru     TEXT,
    trade_method_id         INTEGER REFERENCES trade_methods_ref(id),
    status_id               INTEGER,
    ref_finsource_id        INTEGER,
    desc_ru                 TEXT,
    is_qvazi                BOOLEAN DEFAULT FALSE,
    rootrecord_id           BIGINT,
    date_approved           TIMESTAMPTZ,
    created_at              TIMESTAMPTZ DEFAULT NOW(),
    synced_at               TIMESTAMPTZ DEFAULT NOW()
);
COMMENT ON TABLE plan_points IS 'Annual procurement plans — year range × TARGET_BINS';
CREATE INDEX idx_plans_customer_year  ON plan_points(customer_bin, fin_year);
CREATE INDEX idx_plans_enstru         ON plan_points(enstru_code);
CREATE INDEX idx_plans_amount         ON plan_points(total_amount);
CREATE INDEX idx_plans_kato           ON plan_points(kato_delivery);

CREATE TABLE IF NOT EXISTS announcements (
    id              BIGINT PRIMARY KEY,
    number_anno     VARCHAR(40) UNIQUE NOT NULL,
    customer_bin    VARCHAR(12) REFERENCES subjects(bin),
    organizer_bin   VARCHAR(12) REFERENCES subjects(bin),
    name_ru         TEXT,
    total_sum       NUMERIC(18,4),
    publish_date    DATE,
    end_date        DATE,
    status_id       INTEGER REFERENCES lot_statuses_ref(id),
    trade_method_id INTEGER REFERENCES trade_methods_ref(id),
    system_id       SMALLINT,
    last_updated    TIMESTAMPTZ,
    synced_at       TIMESTAMPTZ DEFAULT NOW()
);
COMMENT ON TABLE announcements IS 'Procurement announcements (trd_buy)';
CREATE INDEX idx_ann_customer    ON announcements(customer_bin);
CREATE INDEX idx_ann_publish     ON announcements(publish_date);
CREATE INDEX idx_ann_status      ON announcements(status_id);

CREATE TABLE IF NOT EXISTS lots (
    id              BIGINT PRIMARY KEY,
    announcement_id BIGINT REFERENCES announcements(id),
    source_trd_buy_id BIGINT,
    customer_bin    VARCHAR(12) REFERENCES subjects(bin),
    enstru_code     VARCHAR(60) REFERENCES enstru_ref(code),
    name_ru         TEXT,
    name_kz         TEXT,
    name_clean      TEXT,               -- ETL-cleaned: lowercase, stripped noise
    description_ru  TEXT,
    unit_code       INTEGER REFERENCES units_ref(code),
    quantity        NUMERIC(18,4),
    unit_price      NUMERIC(18,4),
    lot_amount      NUMERIC(18,4),
    kato_delivery   VARCHAR(20) REFERENCES kato_ref(code),
    status_id       INTEGER,
    trade_method_id INTEGER REFERENCES trade_methods_ref(id),
    is_price_valid  BOOLEAN DEFAULT TRUE,  -- FALSE if price=0 or obviously wrong
    system_id       SMALLINT,
    synced_at       TIMESTAMPTZ DEFAULT NOW()
);
COMMENT ON TABLE lots IS 'Individual lots within announcements';
CREATE INDEX idx_lots_customer  ON lots(customer_bin);
CREATE INDEX idx_lots_enstru    ON lots(enstru_code);
CREATE INDEX idx_lots_kato      ON lots(kato_delivery);
CREATE INDEX idx_lots_amount    ON lots(lot_amount);
CREATE INDEX idx_lots_name_trgm ON lots USING GIN (name_clean gin_trgm_ops);

CREATE TABLE IF NOT EXISTS lot_plan_points (
    lot_id          BIGINT NOT NULL REFERENCES lots(id),
    plan_point_id   BIGINT NOT NULL REFERENCES plan_points(id),
    PRIMARY KEY (lot_id, plan_point_id)
);
COMMENT ON TABLE lot_plan_points IS 'Join table: lots ↔ plan_points (point_list)';
CREATE INDEX idx_lpp_plan_point ON lot_plan_points(plan_point_id);

CREATE TABLE IF NOT EXISTS contracts (
    id                  BIGINT PRIMARY KEY,
    contract_number     VARCHAR(60) UNIQUE,
    announcement_id     BIGINT REFERENCES announcements(id),
    source_trd_buy_id   BIGINT,
    lot_id              BIGINT REFERENCES lots(id),
    customer_bin        VARCHAR(12) REFERENCES subjects(bin),
    supplier_bin        VARCHAR(12) REFERENCES subjects(bin),
    contract_sum        NUMERIC(18,4),
    sign_date           DATE,
    start_date          DATE,
    end_date            DATE,
    status_id           INTEGER REFERENCES contract_statuses_ref(id),
    fin_year            SMALLINT,
    trade_method_id     INTEGER REFERENCES trade_methods_ref(id),
    ref_contract_type_id INTEGER,
    system_id           SMALLINT,
    last_updated        TIMESTAMPTZ,
    synced_at           TIMESTAMPTZ DEFAULT NOW()
);
COMMENT ON TABLE contracts IS 'Signed contracts';
CREATE INDEX idx_contracts_customer   ON contracts(customer_bin);
CREATE INDEX idx_contracts_supplier   ON contracts(supplier_bin);
CREATE INDEX idx_contracts_year       ON contracts(fin_year);
CREATE INDEX idx_contracts_sign_date  ON contracts(sign_date);
CREATE INDEX idx_contracts_sum        ON contracts(contract_sum);

CREATE TABLE IF NOT EXISTS contract_items (
    id              BIGINT PRIMARY KEY,
    contract_id     BIGINT NOT NULL REFERENCES contracts(id),
    pln_point_id    BIGINT REFERENCES plan_points(id),
    enstru_code     VARCHAR(60) REFERENCES enstru_ref(code),
    name_ru         TEXT,
    name_clean      TEXT,
    unit_code       INTEGER REFERENCES units_ref(code),
    quantity        NUMERIC(18,4),
    unit_price      NUMERIC(18,4),
    total_price     NUMERIC(18,4),
    is_price_valid  BOOLEAN DEFAULT TRUE,
    synced_at       TIMESTAMPTZ DEFAULT NOW()
);
COMMENT ON TABLE contract_items IS 'Line items within contracts — used for Fair Price calc';
CREATE INDEX idx_citems_contract ON contract_items(contract_id);
CREATE INDEX idx_citems_plan_point ON contract_items(pln_point_id);
CREATE INDEX idx_citems_enstru   ON contract_items(enstru_code);
CREATE INDEX idx_citems_price    ON contract_items(unit_price) WHERE is_price_valid = TRUE;

CREATE TABLE IF NOT EXISTS contract_acts (
    id              BIGINT PRIMARY KEY,
    contract_id     BIGINT NOT NULL REFERENCES contracts(id),
    approve_date    DATE,
    revoke_date     DATE,
    total_sum       NUMERIC(18,4),
    is_revoked      BOOLEAN DEFAULT FALSE,
    parent_id       BIGINT,
    synced_at       TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_acts_contract ON contract_acts(contract_id);

-- ─────────────────────────────────────────────────────────────────────────────
-- RAW LANDING TABLES (incremental high-volume endpoints)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS raw_plans_kato (
    id                          BIGINT PRIMARY KEY,
    pln_points_id               BIGINT NOT NULL,
    ref_kato_code               VARCHAR(20),
    full_delivery_place_name_ru TEXT,
    index_date                  TIMESTAMPTZ,
    payload                     JSONB NOT NULL,
    synced_at                   TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_raw_plans_kato_point ON raw_plans_kato(pln_points_id);
CREATE INDEX idx_raw_plans_kato_kato ON raw_plans_kato(ref_kato_code);
CREATE INDEX idx_raw_plans_kato_index_date ON raw_plans_kato(index_date);

CREATE TABLE IF NOT EXISTS raw_plans_spec (
    id                  BIGINT PRIMARY KEY,
    pln_points_id       BIGINT NOT NULL,
    ekrb_code           VARCHAR(30),
    fkrb_program_code   VARCHAR(30),
    amount              NUMERIC(18,4),
    index_date          TIMESTAMPTZ,
    payload             JSONB NOT NULL,
    synced_at           TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_raw_plans_spec_point ON raw_plans_spec(pln_points_id);
CREATE INDEX idx_raw_plans_spec_ekrb ON raw_plans_spec(ekrb_code);
CREATE INDEX idx_raw_plans_spec_index_date ON raw_plans_spec(index_date);

CREATE TABLE IF NOT EXISTS raw_acts (
    id              BIGINT PRIMARY KEY,
    contract_id     BIGINT,
    status_id       INTEGER,
    approve_date    DATE,
    revoke_date     DATE,
    index_date      TIMESTAMPTZ,
    payload         JSONB NOT NULL,
    synced_at       TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_raw_acts_contract ON raw_acts(contract_id);
CREATE INDEX idx_raw_acts_status ON raw_acts(status_id);
CREATE INDEX idx_raw_acts_index_date ON raw_acts(index_date);

CREATE TABLE IF NOT EXISTS raw_treasury_pay (
    id               BIGINT PRIMARY KEY,
    contract_id      BIGINT,
    kato             VARCHAR(20),
    item_description TEXT,
    pay_amount       NUMERIC(18,4),
    pay_date         DATE,
    index_date       TIMESTAMPTZ,
    payload          JSONB NOT NULL,
    synced_at        TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_raw_treasury_contract ON raw_treasury_pay(contract_id);
CREATE INDEX idx_raw_treasury_kato ON raw_treasury_pay(kato);
CREATE INDEX idx_raw_treasury_pay_date ON raw_treasury_pay(pay_date);
CREATE INDEX idx_raw_treasury_index_date ON raw_treasury_pay(index_date);

CREATE TABLE IF NOT EXISTS raw_trd_buy_events (
    trd_buy_id      BIGINT NOT NULL,
    event_type      VARCHAR(20) NOT NULL, -- cancel|pause
    has_event       BOOLEAN NOT NULL DEFAULT FALSE,
    payload         JSONB NOT NULL,
    checked_at      TIMESTAMPTZ DEFAULT NOW(),
    synced_at       TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (trd_buy_id, event_type)
);
CREATE INDEX idx_raw_trd_buy_events_type ON raw_trd_buy_events(event_type, checked_at DESC);

-- ─────────────────────────────────────────────────────────────────────────────
-- OPERATIONAL TABLES (ETL tracking + anomalies)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS etl_runs (
    id              SERIAL PRIMARY KEY,
    run_ts          TIMESTAMPTZ DEFAULT NOW(),
    entity          VARCHAR(40) NOT NULL,   -- 'subjects', 'plans', 'lots', etc.
    customer_bin    VARCHAR(12),            -- NULL = full run
    fin_year        SMALLINT,
    records_fetched INTEGER DEFAULT 0,
    records_inserted INTEGER DEFAULT 0,
    records_updated  INTEGER DEFAULT 0,
    records_skipped  INTEGER DEFAULT 0,
    errors          INTEGER DEFAULT 0,
    duration_sec    NUMERIC(10,2),
    status          VARCHAR(20) DEFAULT 'running',  -- running|ok|partial|failed
    error_detail    TEXT,
    completed_at    TIMESTAMPTZ
);
COMMENT ON TABLE etl_runs IS 'ETL audit log — every run recorded here';
CREATE INDEX idx_etl_entity  ON etl_runs(entity, run_ts DESC);
CREATE INDEX idx_etl_status  ON etl_runs(status) WHERE status != 'ok';

CREATE TABLE IF NOT EXISTS etl_state (
    entity       VARCHAR(40) NOT NULL,
    customer_bin VARCHAR(12),
    last_id      BIGINT,
    resume_path  TEXT,
    updated_at   TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (entity, customer_bin)
);
COMMENT ON TABLE etl_state IS 'Checkpoint state for resume-by-BIN ingestion';

CREATE TABLE IF NOT EXISTS journal_entries (
    id          BIGSERIAL PRIMARY KEY,
    entity      VARCHAR(40),
    object_id   BIGINT,
    customer_bin VARCHAR(12),
    payload     JSONB,
    occurred_at TIMESTAMPTZ,
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_journal_entity_time ON journal_entries(entity, occurred_at);
CREATE INDEX idx_journal_customer ON journal_entries(customer_bin);

CREATE TABLE IF NOT EXISTS analytics_export_state (
    table_name      VARCHAR(60) PRIMARY KEY,
    last_exported_at TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);
COMMENT ON TABLE analytics_export_state IS 'Tracks incremental Parquet exports';

CREATE TABLE IF NOT EXISTS anomaly_flags (
    id              SERIAL PRIMARY KEY,
    detected_at     TIMESTAMPTZ DEFAULT NOW(),
    entity_type     VARCHAR(20) NOT NULL,   -- 'lot', 'contract', 'plan'
    entity_id       BIGINT NOT NULL,
    anomaly_type    VARCHAR(40) NOT NULL,   -- 'price_high', 'price_low', 'volume_spike', etc.
    enstru_code     VARCHAR(60),
    customer_bin    VARCHAR(12),
    actual_value    NUMERIC(18,4),
    reference_value NUMERIC(18,4),          -- median / fair_price
    deviation_pct   NUMERIC(8,2),           -- % deviation from reference
    method          VARCHAR(30),            -- 'IQR', 'MAD', 'isolation_forest', 'rule', 'yoy'
    sample_n        INTEGER,                -- N used in statistical calc
    confidence      VARCHAR(10),            -- 'HIGH', 'MEDIUM', 'LOW'
    is_reviewed     BOOLEAN DEFAULT FALSE,
    notes           TEXT
);
COMMENT ON TABLE anomaly_flags IS 'Detected anomalies — populated by analytics layer';
CREATE INDEX idx_anomaly_entity   ON anomaly_flags(entity_type, entity_id);
CREATE INDEX idx_anomaly_customer ON anomaly_flags(customer_bin);
CREATE INDEX idx_anomaly_type     ON anomaly_flags(anomaly_type);
CREATE INDEX idx_anomaly_reviewed ON anomaly_flags(is_reviewed) WHERE NOT is_reviewed;

CREATE TABLE IF NOT EXISTS quality_snapshots (
    id            BIGSERIAL PRIMARY KEY,
    run_id        VARCHAR(80) NOT NULL,
    metric_name   VARCHAR(120) NOT NULL,
    metric_value  NUMERIC(20,6) NOT NULL,
    captured_at   TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_quality_run ON quality_snapshots(run_id);
CREATE INDEX idx_quality_metric ON quality_snapshots(metric_name, captured_at DESC);

-- ─────────────────────────────────────────────────────────────────────────────
-- HELPER VIEWS
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW v_etl_summary AS
SELECT
    entity,
    COUNT(*) FILTER (WHERE status = 'ok')      AS runs_ok,
    COUNT(*) FILTER (WHERE status = 'failed')  AS runs_failed,
    SUM(records_inserted)                       AS total_inserted,
    SUM(records_updated)                        AS total_updated,
    MAX(run_ts)                                 AS last_run,
    MAX(completed_at) FILTER (WHERE status='ok') AS last_success
FROM etl_runs
GROUP BY entity;

CREATE OR REPLACE VIEW v_coverage AS
SELECT
    s.bin,
    s.name_ru,
    COUNT(DISTINCT pp.id)  AS plan_points,
    COUNT(DISTINCT a.id)   AS announcements,
    COUNT(DISTINCT l.id)   AS lots,
    COUNT(DISTINCT c.id)   AS contracts,
    SUM(c.contract_sum)    AS total_contracted
FROM subjects s
LEFT JOIN plan_points  pp ON pp.customer_bin = s.bin
LEFT JOIN announcements a ON a.customer_bin  = s.bin
LEFT JOIN lots         l  ON l.customer_bin  = s.bin
LEFT JOIN contracts    c  ON c.customer_bin  = s.bin
WHERE s.is_customer = TRUE
GROUP BY s.bin, s.name_ru
ORDER BY total_contracted DESC NULLS LAST;

-- NOTE:
-- Target BINs are configured via TARGET_BINS in .env and populated by ETL:
--   python etl/load_subjects.py
SELECT 'Schema created.' AS status;
