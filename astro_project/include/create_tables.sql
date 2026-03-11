
CREATE TABLE IF NOT EXISTS chicago_crime (
    id                   BIGINT          PRIMARY KEY,
    case_number          VARCHAR(20)     NOT NULL,
    date                 TIMESTAMP,
    block                VARCHAR(100),
    iucr                 VARCHAR(10),
    primary_type         VARCHAR(100),
    description          VARCHAR(200),
    location_description VARCHAR(100),
    arrest               BOOLEAN,
    domestic             BOOLEAN,
    beat                 VARCHAR(10),
    district             SMALLINT,
    ward                 SMALLINT,
    community_area       SMALLINT,
    fbi_code             VARCHAR(10),
    x_coordinate         DOUBLE PRECISION,
    y_coordinate         DOUBLE PRECISION,
    year                 SMALLINT,
    updated_on           TIMESTAMP,
    latitude             DOUBLE PRECISION,
    longitude            DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_crime_date          ON chicago_crime (date);
CREATE INDEX IF NOT EXISTS idx_crime_primary_type  ON chicago_crime (primary_type);
CREATE INDEX IF NOT EXISTS idx_crime_district      ON chicago_crime (district);
CREATE INDEX IF NOT EXISTS idx_crime_year          ON chicago_crime (year);
CREATE INDEX IF NOT EXISTS idx_crime_arrest        ON chicago_crime (arrest);
