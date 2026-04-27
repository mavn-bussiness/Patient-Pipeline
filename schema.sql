-- =============================================================
-- schema.sql
-- Global Patent Intelligence Pipeline – Database Schema
-- SQLite-compatible DDL
-- =============================================================

-- Drop order respects FK dependencies
DROP TABLE IF EXISTS relationships;
DROP TABLE IF EXISTS patents;
DROP TABLE IF EXISTS inventors;
DROP TABLE IF EXISTS companies;

-- ─────────────────────────────────────────────────────────────
-- Core tables
-- ─────────────────────────────────────────────────────────────

CREATE TABLE patents (
    patent_id    TEXT    PRIMARY KEY,
    title        TEXT,
    abstract     TEXT,
    filing_date  TEXT,                      -- stored as YYYY-MM-DD
    year         INTEGER
);

CREATE TABLE inventors (
    inventor_id  TEXT    PRIMARY KEY,
    name         TEXT    NOT NULL DEFAULT 'Unknown',
    country      TEXT                        -- 2-letter ISO code
);

CREATE TABLE companies (
    company_id   TEXT    PRIMARY KEY,
    name         TEXT    NOT NULL DEFAULT 'Unknown',
    country      TEXT                        -- 2-letter ISO code
);

-- ─────────────────────────────────────────────────────────────
-- Relationship / bridge table
-- ─────────────────────────────────────────────────────────────

CREATE TABLE relationships (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    patent_id    TEXT,
    inventor_id  TEXT,
    company_id   TEXT,
    FOREIGN KEY (patent_id)   REFERENCES patents(patent_id),
    FOREIGN KEY (inventor_id) REFERENCES inventors(inventor_id),
    FOREIGN KEY (company_id)  REFERENCES companies(company_id)
);

-- ─────────────────────────────────────────────────────────────
-- Indexes  (critical for JOIN / GROUP BY performance)
-- ─────────────────────────────────────────────────────────────

CREATE INDEX idx_patents_year      ON patents(year);
CREATE INDEX idx_inventors_country ON inventors(country);
CREATE INDEX idx_companies_country ON companies(country);
CREATE INDEX idx_rel_patent_id     ON relationships(patent_id);
CREATE INDEX idx_rel_inventor_id   ON relationships(inventor_id);
CREATE INDEX idx_rel_company_id    ON relationships(company_id);