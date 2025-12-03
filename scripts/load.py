"""
Módulo de carga de datos para Pokémon TCG
Carga los datos transformados a la base de datos SQLite
"""

import pandas as pd
import sqlite3
import logging
from pathlib import Path
from typing import Optional
from sqlalchemy import create_engine, text
from datetime import datetime

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_database_schema(db_path: str = "pokemon_cards.db") -> None:
    """
    Crea el esquema de la base de datos
    
    Args:
        db_path: Ruta a la base de datos SQLite
    """
    try:
        # Crear conexión
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Leer schema.sql
        schema_path = Path("../database/schema.sql")
        if schema_path.exists():
            with open(schema_path, 'r', encoding='utf-8') as f:
                schema_sql = f.read()
            
            # Ejecutar sentencias SQL
            cursor.executescript(schema_sql)
            conn.commit()
            logger.info(f"Esquema de base de datos creado en: {db_path}")
        else:
            logger.error(f"Archivo schema.sql no encontrado en: {schema_path}")
            raise FileNotFoundError(f"schema.sql no encontrado")
            
        conn.close()
        
    except Exception as e:
        logger.error(f"Error al crear esquema de base de datos: {str(e)}")
        raise

def load_expansions(df: pd.DataFrame, db_path: str = "pokemon_cards.db") -> dict:
    """
    Carga las expansiones a la base de datos y retorna un mapeo de IDs
    
    Args:
        df: DataFrame con datos transformados
        db_path: Ruta a la base de datos
    
    Returns:
        Diccionario con mapeo nombre_expansión -> id
    """
    try:
        conn = sqlite3.connect(db_path)
        
        # Obtener expansiones únicas
        expansions = df[['expansion_name', 'generation']].drop_duplicates()
        
        # Cargar expansiones
        expansion_map = {}
        for _, row in expansions.iterrows():
            cursor = conn.cursor()
            cursor.execute(
                "INSERT OR IGNORE INTO expansions (name, generation) VALUES (?, ?)",
                (row['expansion_name'], row['generation'])
            )
            conn.commit()
            
            # Obtener el ID insertado
            cursor.execute("SELECT expansion_id FROM expansions WHERE name = ?", (row['expansion_name'],))
            result = cursor.fetchone()
            if result:
                expansion_map[row['expansion_name']] = result[0]
        
        logger.info(f"Cargadas {len(expansion_map)} expansiones únicas")
        conn.close()
        return expansion_map
        
    except Exception as e:
        logger.error(f"Error al cargar expansiones: {str(e)}")
        raise

def load_cards(df: pd.DataFrame, expansion_map: dict, db_path: str = "pokemon_cards.db") -> int:
    """
    Carga las cartas a la base de datos
    
    Args:
        df: DataFrame con datos transformados
        expansion_map: Diccionario de mapeo expansión -> id
        db_path: Ruta a la base de datos
    
    Returns:
        Número de cartas cargadas
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Preparar datos para inserción
        cards_data = []
        for _, row in df.iterrows():
            expansion_id = expansion_map.get(row['expansion_name'])
            if expansion_id:
                cards_data.append((
                    expansion_id,
                    row['pokemon_name'],
                    row['card_type'],
                    row['card_number'],
                    row['price'],
                    int(row['is_rare']),
                    row['rarity_level']
                ))
        
        # Insertar en lote
        cursor.executemany(
            """INSERT INTO cards 
               (expansion_id, pokemon_name, card_type, card_number, price, is_rare, rarity_level)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            cards_data
        )
        
        conn.commit()
        loaded_count = cursor.rowcount
        conn.close()
        
        logger.info(f"Cargadas {loaded_count} cartas a la base de datos")
        return loaded_count
        
    except Exception as e:
        logger.error(f"Error al cargar cartas: {str(e)}")
        raise

def verify_data_loaded(db_path: str = "pokemon_cards.db") -> dict:
    """
    Verifica que los datos se cargaron correctamente
    
    Args:
        db_path: Ruta a la base de datos
    
    Returns:
        Diccionario con estadísticas de verificación
    """
    try:
        conn = sqlite3.connect(db_path)
        
        # Consultas de verificación
        queries = {
            "total_cards": "SELECT COUNT(*) FROM cards",
            "total_expansions": "SELECT COUNT(*) FROM expansions",
            "avg_price": "SELECT AVG(price) FROM cards",
            "max_price": "SELECT MAX(price) FROM cards",
            "rare_cards": "SELECT COUNT(*) FROM cards WHERE is_rare = 1"
        }
        
        results = {}
        for name, query in queries.items():
            cursor = conn.cursor()
            cursor.execute(query)
            results[name] = cursor.fetchone()[0]
        
        conn.close()
        
        logger.info("=== VERIFICACIÓN DE DATOS ===")
        logger.info(f"Total cartas: {results['total_cards']}")
        logger.info(f"Total expansiones: {results['total_expansions']}")
        logger.info(f"Precio promedio: {results['avg_price']:.2f}")
        logger.info(f"Precio máximo: {results['max_price']:.2f}")
        logger.info(f"Cartas raras: {results['rare_cards']}")
        
        return results
        
    except Exception as e:
        logger.error(f"Error en verificación: {str(e)}")
        raise

def load_data_to_db(df: pd.DataFrame, db_path: str = "pokemon_cards.db") -> dict:
    """
    Función principal para cargar datos a la base de datos
    
    Args:
        df: DataFrame con datos transformados
        db_path: Ruta a la base de datos
    
    Returns:
        Diccionario con resultados de la carga
    """
    try:
        logger.info(f"Iniciando carga de datos a base de datos: {db_path}")
        
        # 1. Crear esquema de base de datos
        create_database_schema(db_path)
        
        # 2. Cargar expansiones y obtener mapeo
        expansion_map = load_expansions(df, db_path)
        
        # 3. Cargar cartas
        cards_loaded = load_cards(df, expansion_map, db_path)
        
        # 4. Verificar carga
        verification_results = verify_data_loaded(db_path)
        
        # 5. Agregar metadatos
        metadata = {
            "timestamp": datetime.now().isoformat(),
            "cards_loaded": cards_loaded,
            "expansions_loaded": len(expansion_map),
            "verification": verification_results
        }
        
        logger.info("Carga de datos completada exitosamente")
        return metadata
        
    except Exception as e:
        logger.error(f"Error en carga de datos: {str(e)}")
        raise

if __name__ == "__main__":
    # Para pruebas
    from transformation import transform_data
    from extraction import extract_data
    
    # Ejecutar pipeline completo
    df_raw = extract_data()
    df_transformed = transform_data(df_raw)
    results = load_data_to_db(df_transformed)
    print("Resultados de la carga:", results)