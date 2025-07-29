# -*- coding: utf-8 -*-
"""
Script para:
1. Crear stack multibanda a partir de TIFFs anuales.
2. Convertirlo a xarray.DataArray con coordenadas temporales.
3. Guardar en formato NetCDF (.nc) con compresión.
4. Extraer clases válidas por año (paralelo, bajo RAM).
5. Generar violinplot con la evolución de clases.

Autor: Christian Chacón (versión final optimizada)
"""

import os
import numpy as np
import pandas as pd
import rasterio
import rioxarray
import xarray as xr
from collections import defaultdict
from collections import Counter
from dask.diagnostics.progress import ProgressBar
import matplotlib.pyplot as plt
import seaborn as sns
from tqdm import tqdm

# ==========================================
# [1/5] CONFIGURACIÓN INICIAL
# ==========================================

print("[1/5] Configurando rutas y mapeo de clases...")

pixel_class_map = {
    1: "Superficie agrícola", 2: "Superficie arbórea", 3: "Superficie herbácea",
    4: "Superficie arbustiva y estepas leñosas", 5: "Superficies artificiales",
    6: "Vegetación dispersa", 7: "Suelo desnudo", 8: "Hielo y nieve",
    9: "Mares y océanos", 10: "Turberas Sphagnosas", 11: "Turberas Sphagnosas y/o Pulvinadas",
    12: "Vegas y mallines", 13: "Cuerpos de agua continental"
}

input_dir = os.path.expanduser("~/raster_data/humedales_giz")
stack_path = os.path.join(input_dir, "stack_humedales.tif")
nc_path = os.path.join(input_dir, "stack_humedales.nc")

raster_files = {
    2015: "bb97bc4d-3490-4d50-9de4-2409a160c48e.tif", 2016: "3b342275-80e1-41dc-b10b-bbfec3d959d3.tif",
    2017: "2770a8a2-612c-4e00-b3f2-b8b48bd7d4f6.tif", 2018: "cfb82bc7-a213-4958-af4d-4a7bbf49230c.tif",
    2019: "abf4e94b-abb4-4937-a1a6-cf5be31d7619.tif", 2020: "efbde6dd-0e3e-49a2-99a9-9e84ea09b06e.tif",
    2021: "e75459aa-5d85-4a82-b566-12819c8f3412.tif", 2022: "af2683cb-f449-487c-bda7-9cae9ff67086.tif",
    2023: "a781d13a-f64e-426a-894b-b30724f88bc0.tif", 2024: "3f25ffed-3c4b-4901-9b63-5a58d45300c9.tif"
}
anios = sorted(raster_files.keys())

# ==========================================
# [2/5] CREACIÓN DE STACK MULTIBANDA
# ==========================================

if not os.path.exists(stack_path):
    print("[2/5] Generando stack multibanda...")
    primer_raster = os.path.join(input_dir, raster_files[anios[0]])

    with rasterio.open(primer_raster) as src0:
        meta = src0.meta.copy()
        meta.update(count=len(anios), dtype=src0.dtypes[0], nodata=src0.nodata)

        with rasterio.open(stack_path, "w", **meta) as dst:
            for i, anio in tqdm(enumerate(anios), total=len(anios), desc="Apilando bandas"):
                path_raster = os.path.join(input_dir, raster_files[anio])
                with rasterio.open(path_raster) as src:
                    dst.write(src.read(1), i + 1)
    print(f"[✓] Stack guardado en: {stack_path}")
else:
    print(f"[✓] Stack ya existe en: {stack_path}, se reutiliza.")

# ==========================================
# [3/5] CARGA EN XARRAY Y GUARDADO NETCDF
# ==========================================

print("[3/5] Cargando stack en xarray...")

try:
    if os.path.exists(nc_path):
        print(f"[✓] NetCDF ya existe en: {nc_path}, cargando directamente con chunks...")

        ds = xr.open_dataset(nc_path, chunks={"anio": 1})
        da = ds["clase"]

        # Aplicar máscara para ignorar nodata
        da = da.where(da != -127)

        # Vista rápida del contenido para validación
        print("\n Preview del NetCDF:")
        print(f"  Dimensiones: {da.dims}")
        print(f"  Coordenadas: {list(da.coords)}")
        print(f"  Tipo de datos: {da.dtype}")
        print(f"  Chunking (Dask): {da.chunks}")
        print(f"  Valor nodata aplicado: -127\n")

    else:
        print("[i] NetCDF no encontrado. Cargando stack TIFF con chunks...")

        da_raw = rioxarray.open_rasterio(stack_path, chunks={"band": 1, "x": 512, "y": 512})

        if isinstance(da_raw, list):
            raise ValueError("La lectura del stack devolvió una lista. Verifica el archivo TIFF.")

        da = da_raw.squeeze(dim="spatial_ref", drop=True) if "spatial_ref" in da_raw.dims else da_raw

        if "band" not in da.dims:
            raise ValueError("El stack no tiene dimensión 'band'.")

        if len(da.coords["band"]) != len(anios):
            raise ValueError("Número de bandas no coincide con años.")

        # Renombrar banda y aplicar coordenadas
        da = da.rename(band="anio").assign_coords(anio=anios)

        # Aplicar máscara para nodata
        da = da.where(da != -127)

        print(f"[✓] Guardando stack como NetCDF en: {nc_path}")
        da.to_dataset(name="clase").to_netcdf(
            nc_path,
            encoding={
                "clase": {
                    "zlib": True,
                    "complevel": 4,
                    "_FillValue": -127
                }
            },
            format="NETCDF4"
        )

except Exception as e:
    print(f"[✗] Error al cargar stack o guardar NetCDF: {e}")
    exit(1)

# ==========================================
# [4/5] EXTRACCIÓN DE CLASES POR AÑO (EFICIENTE)
# ==========================================

print("[4/5] Extrayendo clases por año (procesamiento eficiente sin saturar RAM)...")

valores = []
height, width = da.sizes["y"], da.sizes["x"]
block_size = 512

for i, anio in tqdm(enumerate(anios), total=len(anios), desc="Extrayendo clases"):
    capa = da.isel(anio=i)
    conteo_clases = defaultdict(int)

    for row_off in range(0, height, block_size):
        for col_off in range(0, width, block_size):
            window = dict(
                y=slice(row_off, min(row_off + block_size, height)),
                x=slice(col_off, min(col_off + block_size, width))
            )

            # Extraer bloque como numpy array
            bloque = capa.isel(**window).data.compute()

            # Filtrado robusto: elimina NaN, infinitos, nodata (-127) y clases fuera de rango
            bloque = bloque[np.isfinite(bloque)]
            bloque = bloque[bloque != -127]
            bloque = bloque[(bloque >= 1) & (bloque <= 13)]

            if bloque.size > 0:
                unicos, cuentas = np.unique(bloque.astype(int), return_counts=True)
                for clase, count in zip(unicos, cuentas):
                    conteo_clases[clase] += count

    for clase, count in conteo_clases.items():
        nombre_clase = pixel_class_map.get(clase, f"Clase {clase}")
        valores.extend([(anio, nombre_clase)] * count)

df_violin = pd.DataFrame(valores, columns=["Año", "Clase"])
print(df_violin.head(10))

# # ==========================================
# # [5/5] VIOLINPLOT – Distribución por clase y año
# # ==========================================

# # Asegurar tipos correctos
# df_violin["Clase"] = df_violin["Clase"].astype("Int64")
# df_violin["Año"] = df_violin["Año"].astype("Int64")

# # Verificar contenido antes de graficar
# print("[✓] DataFrame para violinplot contiene:")
# print(df_violin.head())

# if not df_violin.empty:
#     plt.figure(figsize=(10, 6))
#     sns.violinplot(
#         data=df_violin,
#         x="Año",
#         y="Clase",
#         palette="Set3",
#         inner="box",
#         linewidth=1
#     )
#     plt.title("Distribución de clases por año (violín)")
#     plt.xlabel("Año")
#     plt.ylabel("Clase")
#     plt.tight_layout()
#     plt.savefig(input_dir, "violinplot_clases.png", dpi=300)
#     plt.show()
# else:
#     print("[✗] El DataFrame `df_violin` está vacío. No se puede graficar.")
