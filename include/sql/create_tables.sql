CREATE TABLE IF NOT EXISTS raw_crimes (
    id TEXT PRIMARY KEY,
    date TEXT,
    block TEXT,
    primary_type TEXT,
    description TEXT,
    location_description TEXT,
    arrest BOOLEAN,
    domestic BOOLEAN,
    district TEXT,
    ward TEXT,
    community_area TEXT,
    latitude FLOAT,
    longitude FLOAT,
    year TEXT
);

CREATE TABLE IF NOT EXISTS transformed_crimes (
    primary_type TEXT,
    year TEXT,
    district TEXT,
    total_crimes INT,
    arrest_count INT,
    arrest_rate FLOAT
);