-- Esquema de base de datos para Pokémon TCG
-- Base de datos: pokemon_cards.db

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

-- Índices para mejorar el rendimiento de consultas
CREATE INDEX IF NOT EXISTS idx_cards_pokemon ON cards(pokemon_name);
CREATE INDEX IF NOT EXISTS idx_cards_price ON cards(price DESC);
CREATE INDEX IF NOT EXISTS idx_cards_rarity ON cards(rarity_level);
CREATE INDEX IF NOT EXISTS idx_cards_type ON cards(card_type);
CREATE INDEX IF NOT EXISTS idx_cards_expansion ON cards(expansion_id);
CREATE INDEX IF NOT EXISTS idx_expansions_generation ON expansions(generation);

-- Vista para consultas comunes
CREATE VIEW IF NOT EXISTS vw_card_details AS
SELECT 
    c.card_id,
    c.pokemon_name,
    c.card_type,
    c.price,
    c.rarity_level,
    c.is_rare,
    e.name as expansion_name,
    e.generation
FROM cards c
JOIN expansions e ON c.expansion_id = e.expansion_id;

-- Vista para estadísticas
CREATE VIEW IF NOT EXISTS vw_statistics AS
SELECT 
    COUNT(*) as total_cards,
    COUNT(DISTINCT pokemon_name) as unique_pokemon,
    COUNT(DISTINCT expansion_id) as unique_expansions,
    AVG(price) as avg_price,
    MAX(price) as max_price,
    SUM(CASE WHEN is_rare THEN 1 ELSE 0 END) as rare_cards_count
FROM cards;

-- Vista para precios por generación
CREATE VIEW IF NOT EXISTS vw_prices_by_generation AS
SELECT 
    e.generation,
    COUNT(*) as card_count,
    AVG(c.price) as avg_price,
    MIN(c.price) as min_price,
    MAX(c.price) as max_price
FROM cards c
JOIN expansions e ON c.expansion_id = e.expansion_id
GROUP BY e.generation
ORDER BY avg_price DESC;

-- Vista para rareza
CREATE VIEW IF NOT EXISTS vw_rarity_distribution AS
SELECT 
    rarity_level,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM cards), 2) as percentage
FROM cards
GROUP BY rarity_level
ORDER BY count DESC;

-- Función para limpieza (ejemplo de trigger)
CREATE TRIGGER IF NOT EXISTS trg_clean_pokemon_name
BEFORE INSERT ON cards
FOR EACH ROW
BEGIN
    -- Limpiar nombre del Pokémon
    SET NEW.pokemon_name = TRIM(NEW.pokemon_name);
    
    -- Asegurar que el precio no sea negativo
    IF NEW.price < 0 THEN
        SET NEW.price = 0;
    END IF;
    
    -- Auto-calcular is_rare si no se proporciona
    IF NEW.is_rare IS NULL THEN
        SET NEW.is_rare = CASE WHEN NEW.price > 10 THEN 1 ELSE 0 END;
    END IF;
END;

-- Inserción de metadatos inicial
INSERT OR IGNORE INTO etl_metadata (timestamp, status) 
VALUES (CURRENT_TIMESTAMP, 'initial_setup');