"""
Dashboard interactivo para análisis de cartas Pokémon TCG
Usa Streamlit para visualización de datos
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sqlite3
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime

# Configuración de la página
st.set_page_config(
    page_title="Pokémon TCG Analytics",
    page_icon="Dragon",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Título principal
st.title("Dragon Pokémon TCG Analytics Dashboard")
st.markdown("---")

# Conexión a la base de datos
@st.cache_resource
def get_db_connection():
    """Establece conexión a la base de datos SQLite"""
    db_path = "pokemon_cards.db"
    if Path(db_path).exists():
        return sqlite3.connect(db_path)
    else:
        st.error(f"Base de datos no encontrada: {db_path}")
        st.info("Por favor ejecute el pipeline ETL primero: `python scripts/main_etl.py`")
        return None

# Cargar datos desde la base de datos
@st.cache_data(ttl=3600)  # Cache por 1 hora
def load_data():
    """Carga todos los datos de la base de datos"""
    conn = get_db_connection()
    if conn is None:
        return None
    
    # Cargar datos de la vista de detalles
    query = """
    SELECT 
        card_id,
        pokemon_name,
        card_type,
        price,
        rarity_level,
        is_rare,
        expansion_name,
        generation
    FROM vw_card_details
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    return df

@st.cache_data(ttl=3600)
def load_statistics():
    """Carga estadísticas desde la base de datos"""
    conn = get_db_connection()
    if conn is None:
        return None
    
    # Cargar estadísticas
    stats_query = "SELECT * FROM vw_statistics"
    generation_query = "SELECT * FROM vw_prices_by_generation"
    rarity_query = "SELECT * FROM vw_rarity_distribution"
    
    stats_df = pd.read_sql_query(stats_query, conn)
    generation_df = pd.read_sql_query(generation_query, conn)
    rarity_df = pd.read_sql_query(rarity_query, conn)
    
    conn.close()
    
    return {
        'stats': stats_df.iloc[0].to_dict() if not stats_df.empty else {},
        'generation_prices': generation_df,
        'rarity_distribution': rarity_df
    }

# Sidebar para filtros
st.sidebar.header("🎛️ Filtros y Controles")

# Cargar datos
df = load_data()
if df is None:
    st.stop()

stats_data = load_statistics()

# Filtro 1: Rango de precios
price_range = st.sidebar.slider(
    " Rango de Precios (£)",
    min_value=float(df['price'].min()),
    max_value=float(df['price'].max()),
    value=(0.0, float(df['price'].max())),
    step=0.5
)

# Filtro 2: Tipo de carta
card_types = sorted(df['card_type'].unique())
selected_types = st.sidebar.multiselect(
    "Tipo de Carta",
    options=card_types,
    default=card_types
)

# Filtro 3: Generación
generations = sorted(df['generation'].unique())
selected_generation = st.sidebar.selectbox(
    "Generación",
    options=["Todas"] + list(generations),
    index=0
)

# Filtro 4: Nivel de rareza
rarity_levels = sorted(df['rarity_level'].unique())
selected_rarity = st.sidebar.multiselect(
    " Nivel de Rareza",
    options=rarity_levels,
    default=rarity_levels
)

# Filtro 5: Expansión
if st.sidebar.checkbox("Filtrar por expansión específica"):
    expansions = sorted(df['expansion_name'].unique())
    selected_expansion = st.sidebar.selectbox(
        " Expansión",
        options=["Todas"] + list(expansions)
    )
else:
    selected_expansion = "Todas"

# Filtro 6: Pokémon específico
pokemon_search = st.sidebar.text_input("🔍 Buscar Pokémon específico", "")

# Aplicar filtros
filtered_df = df.copy()

# Filtrar por rango de precios
filtered_df = filtered_df[
    (filtered_df['price'] >= price_range[0]) & 
    (filtered_df['price'] <= price_range[1])
]

# Filtrar por tipo de carta
if selected_types:
    filtered_df = filtered_df[filtered_df['card_type'].isin(selected_types)]

# Filtrar por generación
if selected_generation != "Todas":
    filtered_df = filtered_df[filtered_df['generation'] == selected_generation]

# Filtrar por rareza
if selected_rarity:
    filtered_df = filtered_df[filtered_df['rarity_level'].isin(selected_rarity)]

# Filtrar por expansión
if selected_expansion != "Todas":
    filtered_df = filtered_df[filtered_df['expansion_name'] == selected_expansion]

# Filtrar por búsqueda de Pokémon
if pokemon_search:
    filtered_df = filtered_df[
        filtered_df['pokemon_name'].str.contains(pokemon_search, case=False, na=False)
    ]

# Mostrar resumen de filtros
st.sidebar.markdown("---")
st.sidebar.markdown(f"** Resultados:** {len(filtered_df):,} cartas")
st.sidebar.markdown(f"** Rango:** £{price_range[0]:.2f} - £{price_range[1]:.2f}")
st.sidebar.markdown(f"** Tipos:** {len(selected_types)} seleccionados")

# Botón para resetear filtros
if st.sidebar.button(" Resetear Filtros"):
    st.rerun()

# Contenido principal
# Sección 1: Métricas clave
st.subheader(" Métricas Clave")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        "Total de Cartas",
        f"{len(filtered_df):,}",
        delta=f"{len(filtered_df) - len(df):+,}" if len(filtered_df) != len(df) else None
    )

with col2:
    avg_price = filtered_df['price'].mean()
    overall_avg = df['price'].mean()
    st.metric(
        "Precio Promedio",
        f"£{avg_price:.2f}",
        delta=f"£{avg_price - overall_avg:.2f}"
    )

with col3:
    max_price = filtered_df['price'].max()
    st.metric(
        "Precio Más Alto",
        f"£{max_price:.2f}"
    )

with col4:
    rare_cards = filtered_df['is_rare'].sum()
    rare_percentage = (rare_cards / len(filtered_df) * 100) if len(filtered_df) > 0 else 0
    st.metric(
        "Cartas Raras",
        f"{rare_cards:,}",
        f"{rare_percentage:.1f}%"
    )

st.markdown("---")

# Sección 2: Visualizaciones
st.subheader(" Visualizaciones")

# Organizar gráficos en pestañas
tab1, tab2, tab3, tab4 = st.tabs([
    "Distribución de Precios",
    " Top Pokémon Más Caros",
    " Precios por Tipo",
    " Análisis por Generación"
])

with tab1:
    # Gráfico 1: Distribución de precios
    col1, col2 = st.columns([2, 1])
    
    with col1:
        fig1 = px.histogram(
            filtered_df,
            x='price',
            nbins=50,
            title='Distribución de Precios',
            labels={'price': 'Precio (£)', 'count': 'Número de Cartas'},
            color_discrete_sequence=['#FF6B6B']
        )
        fig1.update_layout(
            xaxis_title="Precio (£)",
            yaxis_title="Número de Cartas",
            hovermode='x unified'
        )
        st.plotly_chart(fig1, use_container_width=True)
    
    with col2:
        # Estadísticas de precio
        st.markdown("** Estadísticas de Precio**")
        
        price_stats = filtered_df['price'].describe()
        
        st.metric("Mínimo", f"£{price_stats['min']:.2f}")
        st.metric("25% Percentil", f"£{price_stats['25%']:.2f}")
        st.metric("Mediana", f"£{price_stats['50%']:.2f}")
        st.metric("75% Percentil", f"£{price_stats['75%']:.2f}")
        st.metric("Máximo", f"£{price_stats['max']:.2f}")
        
        # Box plot simple
        fig_box = go.Figure(data=[go.Box(
            y=filtered_df['price'],
            name='Precios',
            boxpoints='outliers',
            marker_color='#4ECDC4'
        )])
        fig_box.update_layout(
            title='Box Plot de Precios',
            yaxis_title='Precio (£)',
            height=300
        )
        st.plotly_chart(fig_box, use_container_width=True)

with tab2:
    # Gráfico 2: Top 10 Pokémon más caros
    top_10 = filtered_df.nlargest(10, 'price')[['pokemon_name', 'price', 'expansion_name', 'rarity_level']]
    
    fig2 = px.bar(
        top_10,
        x='price',
        y='pokemon_name',
        orientation='h',
        title='Top 10 Pokémon Más Caros',
        labels={'price': 'Precio (£)', 'pokemon_name': 'Pokémon'},
        color='price',
        color_continuous_scale='Viridis',
        hover_data=['expansion_name', 'rarity_level']
    )
    fig2.update_layout(
        yaxis={'categoryorder': 'total ascending'},
        xaxis_title="Precio (£)",
        yaxis_title="Pokémon",
        height=500
    )
    st.plotly_chart(fig2, use_container_width=True)
    
    # Mostrar tabla con detalles
    with st.expander("Ver detalles del Top 10"):
        st.dataframe(
            top_10.style.format({'price': '£{:.2f}'}),
            use_container_width=True
        )

with tab3:
    # Gráfico 3: Precios por tipo de carta
    col1, col2 = st.columns(2)
    
    with col1:
        # Box plot por tipo
        fig3 = px.box(
            filtered_df,
            x='card_type',
            y='price',
            title='Distribución de Precios por Tipo de Carta',
            labels={'card_type': 'Tipo de Carta', 'price': 'Precio (£)'},
            color='card_type',
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        fig3.update_layout(
            xaxis_title="Tipo de Carta",
            yaxis_title="Precio (£)",
            showlegend=False,
            height=400
        )
        st.plotly_chart(fig3, use_container_width=True)
    
    with col2:
        # Precio promedio por tipo
        avg_by_type = filtered_df.groupby('card_type')['price'].agg(['mean', 'count']).reset_index()
        avg_by_type = avg_by_type.sort_values('mean', ascending=False)
        
        fig4 = px.bar(
            avg_by_type,
            x='card_type',
            y='mean',
            title='Precio Promedio por Tipo de Carta',
            labels={'card_type': 'Tipo de Carta', 'mean': 'Precio Promedio (£)'},
            color='mean',
            color_continuous_scale='Blues',
            hover_data=['count']
        )
        fig4.update_layout(
            xaxis_title="Tipo de Carta",
            yaxis_title="Precio Promedio (£)",
            height=400
        )
        st.plotly_chart(fig4, use_container_width=True)

with tab4:
    # Gráfico 4: Análisis por generación
    if stats_data and 'generation_prices' in stats_data:
        generation_df = stats_data['generation_prices']
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Precio promedio por generación
            fig5 = px.bar(
                generation_df,
                x='generation',
                y='avg_price',
                title='Precio Promedio por Generación',
                labels={'generation': 'Generación', 'avg_price': 'Precio Promedio (£)'},
                color='avg_price',
                color_continuous_scale='thermal',
                hover_data=['card_count', 'min_price', 'max_price']
            )
            fig5.update_layout(
                xaxis_title="Generación",
                yaxis_title="Precio Promedio (£)",
                height=400
            )
            st.plotly_chart(fig5, use_container_width=True)
        
        with col2:
            # Distribución de cartas por generación
            fig6 = px.pie(
                generation_df,
                values='card_count',
                names='generation',
                title='Distribución de Cartas por Generación',
                hole=0.4,
                color_discrete_sequence=px.colors.qualitative.Pastel
            )
            fig6.update_traces(textposition='inside', textinfo='percent+label')
            fig6.update_layout(height=400)
            st.plotly_chart(fig6, use_container_width=True)
    
    # Scatter plot: Precio vs Rareza por generación
    if 'rarity_score' in filtered_df.columns:
        fig7 = px.scatter(
            filtered_df,
            x='rarity_score',
            y='price',
            color='generation',
            size='price',
            hover_data=['pokemon_name', 'card_type', 'expansion_name'],
            title='Precio vs Score de Rareza por Generación',
            labels={'rarity_score': 'Score de Rareza', 'price': 'Precio (£)'},
            opacity=0.7
        )
        fig7.update_layout(height=500)
        st.plotly_chart(fig7, use_container_width=True)

# Sección 3: Tabla de datos
st.markdown("---")
st.subheader(" Datos Detallados")

# Mostrar tabla con opciones
show_data = st.checkbox("Mostrar tabla de datos completos", value=False)

if show_data:
    # Seleccionar columnas para mostrar
    columns_to_show = st.multiselect(
        "Seleccionar columnas para mostrar:",
        options=filtered_df.columns.tolist(),
        default=['pokemon_name', 'card_type', 'price', 'rarity_level', 'expansion_name', 'generation']
    )
    
    if columns_to_show:
        # Mostrar datos con paginación
        items_per_page = st.slider("Items por página", 10, 100, 20)
        
        total_pages = max(1, len(filtered_df) // items_per_page)
        page = st.number_input("Página", min_value=1, max_value=total_pages, value=1)
        
        start_idx = (page - 1) * items_per_page
        end_idx = start_idx + items_per_page
        
        st.dataframe(
            filtered_df[columns_to_show].iloc[start_idx:end_idx].style.format({'price': '£{:.2f}'}),
            use_container_width=True
        )
        
        st.caption(f"Mostrando {start_idx+1}-{min(end_idx, len(filtered_df))} de {len(filtered_df)} registros")
    
    # Opción para descargar datos
    st.download_button(
        label=" Descargar datos filtrados (CSV)",
        data=filtered_df.to_csv(index=False).encode('utf-8'),
        file_name=f"pokemon_cards_filtered_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )

# Sección 4: Análisis avanzado
st.markdown("---")
st.subheader(" Análisis Avanzado")

col1, col2 = st.columns(2)

with col1:
    if st.button(" Correlación Precio-Rareza"):
        # Calcular correlación
        if 'rarity_score' in filtered_df.columns:
            correlation = filtered_df['price'].corr(filtered_df['rarity_score'])
            
            fig_corr = px.scatter(
                filtered_df,
                x='rarity_score',
                y='price',
                trendline="ols",
                title=f'Correlación: Precio vs Rareza (r = {correlation:.3f})',
                labels={'rarity_score': 'Score de Rareza', 'price': 'Precio (£)'}
            )
            st.plotly_chart(fig_corr, use_container_width=True)
            
            st.info(f"**Coeficiente de correlación:** {correlation:.3f}")
            if correlation > 0.7:
                st.success("Fuerte correlación positiva: Mayor rareza = Mayor precio")
            elif correlation > 0.3:
                st.warning("Correlación moderada positiva")
            elif correlation > -0.3:
                st.info("Correlación débil")
            else:
                st.error("Correlación negativa")

with col2:
    if st.button("Distribución de Rareza"):
        if stats_data and 'rarity_distribution' in stats_data:
            rarity_df = stats_data['rarity_distribution']
            
            fig_rarity = px.bar(
                rarity_df,
                x='rarity_level',
                y='percentage',
                title='Distribución de Niveles de Rareza (%)',
                labels={'rarity_level': 'Nivel de Rareza', 'percentage': 'Porcentaje (%)'},
                color='percentage',
                color_continuous_scale='sunset'
            )
            fig_rarity.update_layout(height=400)
            st.plotly_chart(fig_rarity, use_container_width=True)

# Footer
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center'>
        <p>Dashboard Pokémon TCG Analytics | Creado con  usando Streamlit</p>
        <p>Datos actualizados automáticamente desde la base de datos SQL</p>
    </div>
    """,
    unsafe_allow_html=True
)