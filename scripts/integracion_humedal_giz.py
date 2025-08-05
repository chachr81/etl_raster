# -*- coding: utf-8 -*-
"""
Muestreo estratificado optimizado por bloques (1024×1024) desde stack multibanda.

Christian Chacón · agosto 2025
"""

import os
import uuid
import numpy as np
import geoalchemy2
import rasterio
from rasterio.windows import Window
from shapely.geometry import Point
import geopandas as gpd
from sqlalchemy import create_engine, text
from dotenv import dotenv_values
from tqdm import tqdm
from collections import defaultdict
import logging

# ==========================
# [1] CONFIGURACIÓN GENERAL
# ==========================

os.makedirs(os.path.expanduser("/home/dps_chanar/etl_raster/logs/"), exist_ok=True)
logging.basicConfig(
    filename=os.path.expanduser("/home/dps_chanar/etl_raster/logs/muestreo_humedales.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("[1/7] Cargando configuración...")

input_tif = os.path.expanduser("/home/dps_chanar/raster_data/humedales_giz/stack_humedales.tif")
env = dotenv_values(os.path.expanduser("/home/dps_chanar/.env"))
pg_url = f"postgresql://{env['DB_USER_P']}:{env['DB_PASSWORD_P']}@{env['DB_HOST_P']}/{env['DB_NAME_P']}"
engine = create_engine(pg_url)
output_table = "ecos_acuatico_continental.muestreo_humedales_giz"

anios = list(range(2015, 2025))
bandas_idx = list(range(1, 11))  # rasterio is 1-based
chunk_size = 1024
porcentaje = 0.10
valores_validos = set(range(1, 14))
bloque_insercion = 500_000

pixel_class_map = {
    1: "Superficie agrícola", 2: "Superficie arbórea", 3: "Superficie herbácea",
    4: "Superficie arbustiva y estepas leñosas", 5: "Superficies artificiales",
    6: "Vegetación dispersa", 7: "Suelo desnudo", 8: "Hielo y nieve",
    9: "Mares y océanos", 10: "Turberas Sphagnosas",
    11: "Turberas Sphagnosas y/o Pulvinadas", 12: "Vegas y mallines",
    13: "Cuerpos de agua continental"
}

# ==========================
# [2] LEER METADATOS
# ==========================

logging.info("[2/7] Leyendo metadatos del stack...")

with rasterio.open(input_tif) as src:
    nodata = src.nodata
    width, height = src.width, src.height
    crs = src.crs
    epsg = crs.to_epsg()

logging.info(f"[2/7] Stack multibanda detectado con tamaño {height}x{width}, nodata={nodata}, EPSG={epsg}")

# ==========================
# [3] PROCESAMIENTO POR BLOQUES
# ==========================

logging.info("[3/7] Buscando píxeles válidos por bloque...")

candidatos_por_clase = defaultdict(list)

with rasterio.open(input_tif) as src:
    for row_off in tqdm(range(0, height, chunk_size), desc="Filas"):
        for col_off in range(0, width, chunk_size):
            win = Window(col_off, row_off,
                         min(chunk_size, width - col_off),
                         min(chunk_size, height - row_off))

            bloques = [src.read(bidx, window=win) for bidx in bandas_idx]
            stack = np.stack(bloques, axis=0)
            validez = ((stack != nodata) & np.isfinite(stack) & (stack >= 1) & (stack <= 13)).all(axis=0)
            base = bloques[0]
            rows, cols = np.where(validez)

            for r, c in zip(rows, cols):
                clase_base = int(base[r, c])
                if clase_base in valores_validos:
                    abs_row = row_off + r
                    abs_col = col_off + c
                    candidatos_por_clase[clase_base].append((abs_row, abs_col))

logging.info("[3/7] Índices válidos por clase recopilados.")

# ==========================
# [4] MUESTREO ESTRATIFICADO
# ==========================

logging.info("[4/7] Muestreo aleatorio estratificado por clase (10%)...")

muestras_idx = []
for clase, lista_indices in candidatos_por_clase.items():
    total = len(lista_indices)
    if total == 0:
        continue
    n_sample = max(1, int(round(total * porcentaje)))
    seleccion = np.random.choice(range(total), size=n_sample, replace=False)
    for idx in seleccion:
        row, col = lista_indices[idx]
        muestras_idx.append((clase, row, col))

logging.info(f"[4/7] Total puntos muestreados: {len(muestras_idx):,}")

# ==========================
# [4.5] UUIDs YA INSERTADOS
# ==========================

def obtener_uuids_existentes(engine, tabla_completa):
    esquema, tabla = tabla_completa.split(".")
    with engine.connect() as conn:
        try:
            existe = conn.execute(text(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = :esquema AND table_name = :tabla
                )
            """), {"esquema": esquema, "tabla": tabla}).scalar()
            
            if not existe:
                logging.warning(f"[!] La tabla '{tabla_completa}' no existe aún. Continuando sin filtrar UUIDs.")
                return set()
            
            result = conn.execute(text(f"SELECT DISTINCT uuid_muestra FROM {tabla_completa}"))
            return set(row[0] for row in result)
        
        except Exception as e:
            logging.warning(f"[!] Error inesperado al consultar UUIDs existentes: {e}")
            return set()

def uuid_determinista(row, col):
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{row}_{col}"))

logging.info("[4.5/7] Consultando UUIDs ya insertados...")
uuids_existentes = obtener_uuids_existentes(engine, output_table)
logging.info(f"[4.5/7] UUIDs encontrados en BD: {len(uuids_existentes):,}")

logging.info("[4.6/7] Filtrando muestras ya insertadas...")

muestras_filtradas = []
for clase, row, col in muestras_idx:
    uid = uuid_determinista(row, col)
    if uid not in uuids_existentes:
        muestras_filtradas.append((clase, row, col, uid))

logging.info(f"[4.6/7] Puntos nuevos a procesar: {len(muestras_filtradas):,}")

# ==========================
# [5/7] EXTRACCIÓN E INSERCIÓN POR BLOQUES
# ==========================

logging.info("[5/7] Extrayendo valores multitemporales e insertando por bloques...")

registros = []
contador_insertados = 0

with rasterio.open(input_tif) as src:
    for idx, (clase_id, row, col, uuid_m) in enumerate(tqdm(muestras_filtradas, desc="Procesando puntos")):
        try:
            x, y = src.xy(row, col, offset="center")
            clase_nombre = pixel_class_map.get(clase_id, f"Clase {clase_id}")

            for i, anio in enumerate(anios):
                val = src.read(i + 1, window=Window(col, row, 1, 1))[0, 0]
                registros.append({
                    "uuid_muestra": uuid_m,
                    "year": anio,
                    "clase_referencia": clase_nombre,
                    "valor": int(val),
                    "x": float(x),
                    "y": float(y),
                    "geometria": Point(float(x), float(y))
                })

        except Exception as e:
            logging.warning(f"[!] Error extrayendo punto ({row}, {col}) o banda {i + 1}: {e}")
            continue

        if len(registros) >= bloque_insercion:
            gdf_bloque = gpd.GeoDataFrame(registros, geometry="geometria", crs=crs)
            gdf_bloque.to_postgis("muestreo_humedales_giz", engine,
                                  schema="ecos_acuatico_continental",
                                  if_exists="append", index=False)
            contador_insertados += len(registros)
            logging.info(f"[5/7] Insertados acumulados: {contador_insertados:,}")
            registros.clear()

# ==========================
# [6/7] INSERTAR RESTANTES
# ==========================

logging.info("[6/7] Insertando registros restantes...")

if registros:
    gdf_final = gpd.GeoDataFrame(registros, geometry="geometria", crs=crs)
    gdf_final.to_postgis("muestreo_humedales_giz", engine,
                         schema="ecos_acuatico_continental",
                         if_exists="append", index=False)
    contador_insertados += len(registros)
    logging.info(f"[6/7] Insertados acumulados (final): {contador_insertados:,}")
    registros.clear()

# ==========================
# [7/7] CIERRE
# ==========================

logging.info(f"[7/7] Muestreo completo. Total registros insertados: {contador_insertados:,}")
logging.info("[7/7] Proceso finalizado exitosamente.")