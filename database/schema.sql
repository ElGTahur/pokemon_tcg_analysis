-- Tabla de expansiones
CREATE TABLE IF NOT EXISTS expansions (
    expansion_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    generation TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabla de cartas
CREATE TABLE IF NOT EXISTS cards (
    card_id INTEGER PRIMARY KEY AUTOINCREMENT,
    expansion_id INTEGER NOT NULL,
    pokemon_name TEXT NOT NULL,
    card_type TEXT NOT NULL,
    card_number TEXT,
    price REAL NOT NULL CHECK (price >= 0),
    is_rare BOOLEAN DEFAULT FALSE,
    rarity_level TEXT,
    rarity_score INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (expansion_id) REFERENCES expansions(expansion_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

-- Tabla de metadatos del ETL
CREATE TABLE IF NOT EXISTS etl_metadata (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    cards_loaded INTEGER,
    expansions_loaded INTEGER,
    duration_seconds REAL,
    status TEXT CHECK (status IN ('success', 'error')),
    error_message TEXT
);

-- Índices
CREATE INDEX IF NOT EXISTS idx_cards_pokemon ON cards(pokemon_name);
CREATE INDEX IF NOT EXISTS idx_cards_price ON cards(price DESC);
CREATE INDEX IF NOT EXISTS idx_cards_rarity ON cards(rarity_level);
CREATE INDEX IF NOT EXISTS idx_cards_type ON cards(card_type);
CREATE INDEX IF NOT EXISTS idx_cards_expansion ON cards(expansion_id);
CREATE INDEX IF NOT EXISTS idx_expansions_generation ON expansions(generation);