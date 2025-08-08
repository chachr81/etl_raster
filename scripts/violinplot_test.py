# -*- coding: utf-8 -*-
import os
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from dotenv import dotenv_values
from sqlalchemy import create_engine
from tqdm import tqdm

# ======================
# [1/4] CONFIGURACIÓN
# ======================

print("[1/4] Cargando configuración...")

N_PUNTOS_POR_CLASE_ANIO = 500  # ← PARAMETRIZABLE

env = dotenv_values(os.path.expanduser("/home/dps_chanar/.env"))
pg_url = f"postgresql://{env['DB_USER_P']}:{env['DB_PASSWORD_P']}@{env['DB_HOST_P']}/{env['DB_NAME_P']}"
engine = create_engine(pg_url)

# ======================
# [2/4] CONSULTA SQL POR AÑO
# ======================

print("[2/4] Consultando años disponibles...")
with engine.connect() as conn:
    years = pd.read_sql("""
        SELECT DISTINCT year
        FROM ecos_acuatico_continental.muestreo_humedales_giz
        WHERE valor BETWEEN 1 AND 13
        ORDER BY year
    """, conn)["year"].tolist()

print(f"[2/4] Años detectados: {years}")
print("[2/4] Ejecutando muestreo estratificado por clase y año...")

df_total = pd.DataFrame()

for year in tqdm(years, desc="Procesando años"):
    query_year = f"""
        WITH datos_ordenados AS (
            SELECT
                year,
                clase_referencia,
                valor,
                ROW_NUMBER() OVER (
                    PARTITION BY clase_referencia
                    ORDER BY RANDOM()
                ) AS rn
            FROM ecos_acuatico_continental.muestreo_humedales_giz
            WHERE valor BETWEEN 1 AND 13 AND year = {year}
        )
        SELECT year, clase_referencia, valor
        FROM datos_ordenados
        WHERE rn <= {N_PUNTOS_POR_CLASE_ANIO}
    """
    with engine.connect() as conn:
        df_chunk = pd.read_sql(query_year, conn)
        df_total = pd.concat([df_total, df_chunk], ignore_index=True)

print(f"[2/4] Total registros cargados: {len(df_total):,}")

# ======================
# [3/4] GRAFICAR VIOLINPLOTS
# ======================

print("[3/4] Generando gráfico de violines...")

df_total["year"] = df_total["year"].astype(str)
ordered_years = sorted(df_total["year"].unique())

# Mapeo de colores por clase
pixel_class_map = {
    1: "Superficie agrícola", 2: "Superficie arbórea", 3: "Superficie herbácea",
    4: "Superficie arbustiva y estepas leñosas", 5: "Superficies artificiales",
    6: "Vegetación dispersa", 7: "Suelo desnudo", 8: "Hielo y nieve",
    9: "Mares y océanos", 10: "Turberas Sphagnosas",
    11: "Turberas Sphagnosas y/o Pulvinadas", 12: "Vegas y mallines",
    13: "Cuerpos de agua continental"
}
clases_ordenadas = [pixel_class_map[i] for i in range(1, 14)]

palette = sns.color_palette("tab20", n_colors=13)
color_dict = {clase: palette[i] for i, clase in enumerate(clases_ordenadas)}

fig, axes = plt.subplots(nrows=1, ncols=len(ordered_years), figsize=(5 * len(ordered_years), 6), sharey=True)

# Asegurar que axes sea iterable
if len(ordered_years) == 1:
    axes = [axes]

for i, year in enumerate(tqdm(ordered_years, desc="Graficando subplots")):
    ax = axes[i]
    data_year = df_total[df_total["year"] == year]

    if data_year.empty:
        ax.set_title(f"Año {year} (sin datos)")
        ax.axis("off")
        continue

    sns.violinplot(
        data=data_year,
        x="clase_referencia",
        y="valor",
        ax=ax,
        scale="width",
        inner="box",
        linewidth=1,
        hue="clase_referencia",
        palette=color_dict,
        order=clases_ordenadas
    )

    ax.set_title(f"Año {year}", fontsize=12)
    ax.set_xlabel("")
    ax.set_xticks([])  # Quitar etiquetas del eje x

    if i == 0:
        ax.set_ylabel("Valor de clase observada")
    else:
        ax.set_ylabel("")

# Crear leyenda global
handles = [mpatches.Patch(color=color_dict[clase], label=clase) for clase in clases_ordenadas]
fig.legend(handles=handles, title="Clase de cobertura", loc="lower center", ncol=5, bbox_to_anchor=(0.5, -0.05))

# Título general y ajuste de layout
plt.tight_layout(rect=(0, 0.05, 1, 0.93))
fig.suptitle(
    f"Distribución por clase y año Muestra: {N_PUNTOS_POR_CLASE_ANIO} puntos por clase/año",
    fontsize=14,
    bbox=dict(facecolor="white", edgecolor="black", boxstyle="round,pad=0.3")
)

# ======================
# [4/4] GUARDAR FIGURA
# ======================

output_path = f"/home/dps_chanar/etl_raster/figures/violinplot_muestreo_sql_{N_PUNTOS_POR_CLASE_ANIO}.png"
os.makedirs(os.path.dirname(output_path), exist_ok=True)
plt.savefig(output_path, dpi=600)
print(f"[4/4] Gráfico guardado en: {output_path}")
