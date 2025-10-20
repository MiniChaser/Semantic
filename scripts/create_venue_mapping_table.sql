-- Create venue_mapping table for fast conference name lookups
-- This table maps raw venue strings to normalized conference names

CREATE TABLE IF NOT EXISTS venue_mapping (
    venue_raw TEXT PRIMARY KEY,              -- Original venue string from papers
    conference_name TEXT NOT NULL,           -- Normalized conference name (e.g., 'AAAI', 'ACL')
    match_method TEXT DEFAULT 'auto',        -- How this mapping was created: 'auto', 'python', 'manual'
    match_confidence REAL DEFAULT 1.0,       -- Confidence score (0.0 to 1.0)
    created_at TIMESTAMP DEFAULT NOW(),      -- When this mapping was created
    updated_at TIMESTAMP DEFAULT NOW()       -- Last update time
);

-- Index on conference_name for reverse lookups
CREATE INDEX IF NOT EXISTS idx_venue_mapping_conference
ON venue_mapping(conference_name);

-- Index on match_method for filtering
CREATE INDEX IF NOT EXISTS idx_venue_mapping_method
ON venue_mapping(match_method);

-- Comments for documentation
COMMENT ON TABLE venue_mapping IS 'Fast lookup table mapping raw venue strings to normalized conference names';
COMMENT ON COLUMN venue_mapping.venue_raw IS 'Original venue string as it appears in papers';
COMMENT ON COLUMN venue_mapping.conference_name IS 'Standardized conference name';
COMMENT ON COLUMN venue_mapping.match_method IS 'Method used: auto (regex), python (matcher), manual (human)';
COMMENT ON COLUMN venue_mapping.match_confidence IS 'Matching confidence score (1.0 = certain, 0.5 = uncertain)';
