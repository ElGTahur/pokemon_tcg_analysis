"""
Módulo de extracción de datos para Pokémon TCG
Lee el archivo CSV original y extrae los datos
"""

import pandas as pd
import logging
from pathlib import Path

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def extract_data(file_path: str = "C:\\Users\\Gael Pérez Cruz\\Desktop\\pokemon_tcg_analysis\\data\\raw\\pokemon_cards.csv") -> pd.DataFrame:
    """
    Extrae los datos del archivo CSV
    
    Args:
        file_path: Ruta al archivo CSV
    
    Returns:
        DataFrame de pandas con los datos extraídos
    """
    try:
        # Verificar que el archivo existe
        path = Path(file_path)
        if not path.exists():
            logger.error(f"Archivo no encontrado: {file_path}")
            raise FileNotFoundError(f"Archivo no encontrado: {file_path}")
        
        logger.info(f"Extrayendo datos de: {file_path}")
        
        # Leer el archivo CSV
        # Nota: El archivo usa 'Ł' como símbolo de libra, lo manejaremos en la transformación
        df = pd.read_csv(file_path, encoding='utf-8')
        
        logger.info(f"Datos extraídos exitosamente. Forma: {df.shape}")
        logger.info(f"Columnas: {list(df.columns)}")
        
        # Mostrar información básica
        logger.info(f"Número de registros: {len(df)}")
        logger.info(f"Número de columnas: {len(df.columns)}")
        logger.info(f"Primeras 5 filas:\n{df.head()}")
        
        return df
        
    except Exception as e:
        logger.error(f"Error en la extracción de datos: {str(e)}")
        raise

def save_raw_data(df: pd.DataFrame, output_path: str = "../data/raw/raw_data_backup.csv"):
    """
    Guarda una copia de los datos extraídos
    
    Args:
        df: DataFrame con los datos
        output_path: Ruta donde guardar el backup
    """
    try:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False, encoding='utf-8')
        logger.info(f"Datos crudos guardados en: {output_path}")
    except Exception as e:
        logger.error(f"Error al guardar datos crudos: {str(e)}")

if __name__ == "__main__":
    # Ejecutar extracción si se llama directamente
    df = extract_data()
    save_raw_data(df)