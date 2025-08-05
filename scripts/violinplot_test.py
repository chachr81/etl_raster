# -*- coding: utf-8 -*-
import os
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from dotenv import dotenv_values
from sqlalchemy import create_engine
from tqdm import tqdm

# ======================
# [1/4] CONFIGURACIÓN
# ======================

print("[1/4] Cargando configuración...")

N_PUNTOS_POR_CLASE_ANIO = 2000  # ← PARAMETRIZABLE

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

fig, axes = plt.subplots(nrows=1, ncols=10, figsize=(40, 6), sharey=True)

for i, year in enumerate(tqdm(ordered_years, desc="Graficando subplots")):
    ax = axes[i]
    data_year = df_total[df_total["year"] == year]
    sns.violinplot(data=data_year, x="clase_referencia", y="valor",
                   ax=ax, scale="width", inner="box", linewidth=1)
    ax.set_title(f"Año {year}", fontsize=12)
    ax.set_xlabel("")
    if i == 0:
        ax.set_ylabel("Valor de clase observada")
    else:
        ax.set_ylabel("")
    ax.tick_params(axis='x', rotation=90)

# Ajustar el layout para dejar espacio al título
plt.tight_layout(rect=(0, 0, 1, 0.93))

# Título global
fig.suptitle(
    f"Distribución por clase y año\nMuestra: {N_PUNTOS_POR_CLASE_ANIO} puntos por clase/año",
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
