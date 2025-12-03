"""
Módulo de transformación de datos para Pokémon TCG
Limpia y transforma los datos extraídos
"""

import pandas as pd
import numpy as np
import logging
from typing import Tuple
import re
from pathlib import Path

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def clean_price_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia la columna de precio, manejando el símbolo 'Ł' y valores inválidos
    
    Args:
        df: DataFrame con los datos
    
    Returns:
        DataFrame con la columna de precio limpia
    """
    df_clean = df.copy()
    
    # Renombrar columna si es necesario
    if 'Price Ł' in df_clean.columns:
        df_clean = df_clean.rename(columns={'Price Ł': 'Price'})
    
    # Verificar que existe la columna Price
    if 'Price' not in df_clean.columns:
        logger.error("No se encontró la columna 'Price' o 'Price Ł'")
        return df_clean
    
    # Convertir a string y limpiar
    df_clean['Price'] = df_clean['Price'].astype(str)
    
    # Remover el símbolo 'Ł' si existe
    df_clean['Price'] = df_clean['Price'].str.replace('Ł', '', regex=False)
    
    # Remover espacios en blanco
    df_clean['Price'] = df_clean['Price'].str.strip()
    
    # Convertir a numérico, manejando errores
    df_clean['Price'] = pd.to_numeric(df_clean['Price'], errors='coerce')
    
    # Eliminar filas con precios inválidos o negativos
    initial_count = len(df_clean)
    df_clean = df_clean[df_clean['Price'].notna() & (df_clean['Price'] >= 0)]
    removed_count = initial_count - len(df_clean)
    
    if removed_count > 0:
        logger.warning(f"Eliminadas {removed_count} filas con precios inválidos")
    
    logger.info(f"Precio mínimo: {df_clean['Price'].min():.2f}")
    logger.info(f"Precio máximo: {df_clean['Price'].max():.2f}")
    logger.info(f"Precio promedio: {df_clean['Price'].mean():.2f}")
    
    return df_clean

def extract_expansion_info(expansion_text: str) -> Tuple[str, str]:
    """
    Extrae información de la expansión y generación
    
    Args:
        expansion_text: Texto de la columna Generation
    
    Returns:
        Tupla con (generación, nombre_expansión)
    """
    if pd.isna(expansion_text):
        return "Unknown", "Unknown"
    
    expansion_text = str(expansion_text).strip()
    
    # Identificar generación basada en patrones comunes
    if "B&W" in expansion_text or "BLACK & WHITE" in expansion_text.upper():
        generation = "Generation 5 (Black & White)"
    elif "ARCEUS" in expansion_text.upper():
        generation = "Generation 4 (Diamond & Pearl)"
    elif "AQUAPOLIS" in expansion_text.upper():
        generation = "Generation 2 (Gold & Silver)"
    elif "LEGENDARY TREASURES" in expansion_text.upper():
        generation = "Generation 5 (Black & White)"
    elif "PLASMA" in expansion_text.upper():
        generation = "Generation 5 (Black & White)"
    elif "NEXT DESTINIES" in expansion_text.upper():
        generation = "Generation 5 (Black & White)"
    elif "NOBLE VICTORIES" in expansion_text.upper():
        generation = "Generation 5 (Black & White)"
    elif "EMERGING POWERS" in expansion_text.upper():
        generation = "Generation 5 (Black & White)"
    elif "DARK EXPLORERS" in expansion_text.upper():
        generation = "Generation 5 (Black & White)"
    elif "DRAGONS EXALTED" in expansion_text.upper():
        generation = "Generation 5 (Black & White)"
    elif "BOUNDARIES CROSSED" in expansion_text.upper():
        generation = "Generation 5 (Black & White)"
    else:
        generation = "Other"
    
    return generation, expansion_text

def extract_card_number_info(card_number_text: str) -> Tuple[str, str, int]:
    """
    Extrae información del número de carta
    
    Args:
        card_number_text: Texto de la columna Card Number
    
    Returns:
        Tupla con (número_carta, número_total, rareza)
    """
    if pd.isna(card_number_text):
        return "000", "000", 0
    
    card_text = str(card_number_text).strip()
    
    # Extraer número de carta y total
    match = re.search(r'(\d+|[A-Z]\d+)\s*OF\s*(\d+)', card_text.upper())
    if match:
        card_num = match.group(1)
        total_num = match.group(2)
    else:
        card_num = "000"
        total_num = "147"  # Valor por defecto
    
    # Determinar rareza basada en el formato del número
    try:
        total = int(total_num)
        # Cartas con números especiales (H, SH, AR) son más raras
        if 'H' in card_num.upper() or 'SH' in card_num.upper() or 'AR' in card_num.upper():
            rarity_score = 3  # Alta rareza
        elif total <= 100:  # Sets más pequeños pueden indicar mayor rareza
            rarity_score = 2  # Media rareza
        else:
            rarity_score = 1  # Baja rareza
    except:
        rarity_score = 0
    
    return card_num, total_num, rarity_score

def calculate_rarity_level(price: float, rarity_score: int) -> str:
    """
    Calcula el nivel de rareza basado en precio y score
    
    Args:
        price: Precio de la carta
        rarity_score: Score de rareza extraído del número
    
    Returns:
        Nivel de rareza como string
    """
    if price >= 50:
        return "Ultra Rare"
    elif price >= 20:
        return "Secret Rare"
    elif price >= 10:
        if rarity_score >= 2:
            return "Holofoil Rare"
        else:
            return "Rare"
    elif price >= 5:
        if rarity_score >= 2:
            return "Uncommon Holo"
        else:
            return "Uncommon"
    else:
        if rarity_score >= 3:
            return "Common Holo"
        else:
            return "Common"

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma y limpia los datos
    
    Args:
        df: DataFrame con datos extraídos
    
    Returns:
        DataFrame transformado
    """
    logger.info("Iniciando transformación de datos...")
    
    # Hacer una copia para no modificar el original
    df_transformed = df.copy()
    
    # 1. Limpiar precios
    df_transformed = clean_price_column(df_transformed)
    
    # 2. Renombrar columnas para consistencia
    df_transformed = df_transformed.rename(columns={
        'Pokemon': 'pokemon_name',
        'Card Type': 'card_type',
        'Generation': 'expansion_raw',
        'Card Number': 'card_number_raw'
    })
    
    # 3. Limpiar nombres de Pokémon
    df_transformed['pokemon_name'] = df_transformed['pokemon_name'].str.strip().str.title()
    
    # 4. Limpiar tipos de carta
    df_transformed['card_type'] = df_transformed['card_type'].str.strip().str.upper()
    
    # 5. Extraer información de expansión
    logger.info("Extrayendo información de expansión...")
    expansion_info = df_transformed['expansion_raw'].apply(extract_expansion_info)
    df_transformed['generation'] = expansion_info.apply(lambda x: x[0])
    df_transformed['expansion_name'] = expansion_info.apply(lambda x: x[1])
    
    # 6. Extraer información del número de carta
    logger.info("Extrayendo información del número de carta...")
    card_number_info = df_transformed['card_number_raw'].apply(extract_card_number_info)
    df_transformed['card_number'] = card_number_info.apply(lambda x: x[0])
    df_transformed['set_total'] = card_number_info.apply(lambda x: x[1])
    df_transformed['rarity_score'] = card_number_info.apply(lambda x: x[2])
    
    # 7. Calcular nivel de rareza
    logger.info("Calculando niveles de rareza...")
    df_transformed['rarity_level'] = df_transformed.apply(
        lambda row: calculate_rarity_level(row['Price'], row['rarity_score']), axis=1
    )
    
    # 8. Marcar cartas raras (precio > 10)
    df_transformed['is_rare'] = df_transformed['Price'] > 10
    
    # 9. Crear categorías de precio
    def categorize_price(price):
        if price >= 50:
            return "Very High (>50)"
        elif price >= 20:
            return "High (20-50)"
        elif price >= 10:
            return "Medium (10-20)"
        elif price >= 5:
            return "Low (5-10)"
        elif price >= 1:
            return "Very Low (1-5)"
        else:
            return "Basic (<1)"
    
    df_transformed['price_category'] = df_transformed['Price'].apply(categorize_price)
    
    # 10. Seleccionar y ordenar columnas finales
    final_columns = [
        'pokemon_name', 'card_type', 'generation', 'expansion_name',
        'card_number', 'set_total', 'Price', 'rarity_level', 'rarity_score',
        'is_rare', 'price_category'
    ]
    
    df_transformed = df_transformed[final_columns].rename(columns={'Price': 'price'})
    
    # 11. Eliminar duplicados
    initial_count = len(df_transformed)
    df_transformed = df_transformed.drop_duplicates()
    final_count = len(df_transformed)
    
    if initial_count != final_count:
        logger.info(f"Eliminados {initial_count - final_count} registros duplicados")
    
    # 12. Log de estadísticas finales
    logger.info("=== ESTADÍSTICAS FINALES ===")
    logger.info(f"Total registros: {len(df_transformed)}")
    logger.info(f"Pokémon únicos: {df_transformed['pokemon_name'].nunique()}")
    logger.info(f"Expansiones únicas: {df_transformed['expansion_name'].nunique()}")
    logger.info(f"Tipos de carta únicos: {df_transformed['card_type'].nunique()}")
    logger.info(f"Distribución de rareza:\n{df_transformed['rarity_level'].value_counts()}")
    
    return df_transformed

def save_transformed_data(df: pd.DataFrame, output_path: str = "../data/processed/pokemon_cards_clean.csv"):
    """
    Guarda los datos transformados
    
    Args:
        df: DataFrame transformado
        output_path: Ruta donde guardar los datos limpios
    """
    try:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False, encoding='utf-8')
        logger.info(f"Datos transformados guardados en: {output_path}")
    except Exception as e:
        logger.error(f"Error al guardar datos transformados: {str(e)}")

if __name__ == "__main__":
    # Para pruebas
    from extraction import extract_data
    
    df_raw = extract_data()
    df_transformed = transform_data(df_raw)
    save_transformed_data(df_transformed)
    print(df_transformed.head())