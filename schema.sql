-- realtor-pro-system :: fsbo_listings schema
-- Run via migrate.py, or paste into the Supabase SQL editor directly.
-- Idempotent: safe to run multiple times.

CREATE TABLE IF NOT EXISTS fsbo_listings (
    id                  BIGSERIAL PRIMARY KEY,

    -- Identity / dedup key. Address is the natural key the pipeline upserts
    -- on (see StoreActor._upsert -> on_conflict="address"). If your source
    -- gives stable listing IDs/URLs, consider switching the unique
    -- constraint to listing_url instead - addresses can have formatting
    -- drift (e.g. "St" vs "Street") that URLs don't.
    address             TEXT NOT NULL,
    zip_code            VARCHAR(10) NOT NULL,

    -- Listing details (matches Listing dataclass fields 1:1)
    price               NUMERIC(12, 2),
    beds                NUMERIC(4, 1),
    baths               NUMERIC(4, 1),
    sqft                NUMERIC(9, 2),
    listing_url         TEXT,
    source              VARCHAR(50) DEFAULT 'fsbo_scraper',

    -- Skip-traced owner contact info
    owner_name          VARCHAR(255),
    owner_phone         VARCHAR(50),
    owner_email         VARCHAR(255),
    skip_trace_status   VARCHAR(30) DEFAULT 'pending',
        -- pending | mocked | cached | success | no_match | budget_exceeded | failed

    -- Outreach + overall lead lifecycle
    outreach_status     VARCHAR(30) DEFAULT 'not_started',
        -- not_started | drafted_only | sent | send_failed
    lead_status         VARCHAR(30) DEFAULT 'new',
        -- new -> traced -> contacted -> responded (or skipped_duplicate)

    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT fsbo_listings_address_unique UNIQUE (address)
);

CREATE INDEX IF NOT EXISTS idx_fsbo_listings_zip_code ON fsbo_listings (zip_code);
CREATE INDEX IF NOT EXISTS idx_fsbo_listings_lead_status ON fsbo_listings (lead_status);
CREATE INDEX IF NOT EXISTS idx_fsbo_listings_skip_trace_status ON fsbo_listings (skip_trace_status);

-- Keep updated_at current on every UPDATE (including the pipeline's upserts)
CREATE OR REPLACE FUNCTION set_updated_at() RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_fsbo_listings_updated_at ON fsbo_listings;
CREATE TRIGGER trg_fsbo_listings_updated_at
    BEFORE UPDATE ON fsbo_listings
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at();
