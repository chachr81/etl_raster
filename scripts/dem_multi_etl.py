import os
import requests
from osgeo import gdal
import subprocess
from urllib.parse import urlparse
import psycopg2
from dotenv import dotenv_values

# Cargar configuración del entorno
config = dotenv_values("/home/dps_chanar/.env")

def download_file(url, destination):
    print(f"Descargando archivo desde {url}...")
    response = requests.get(url, stream=True)
    response.raise_for_status()
    with open(destination, 'wb') as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)
    print(f"Archivo descargado y guardado en {destination}")

def ensure_rar_extension(rar_path):
    if not os.path.exists(rar_path):
        return rar_path
    if not rar_path.endswith('.rar'):
        rar_path_with_extension = rar_path + '.rar'
        os.rename(rar_path, rar_path_with_extension)
        print(f"Renombrado a {rar_path_with_extension}")
        return rar_path_with_extension
    return rar_path

def extract_all_rars(rar_path, extract_to):
    print(f"Extrayendo archivos desde {rar_path} en {extract_to} con 7z...")
    jp2_paths = []
    
    # Listar contenido del archivo RAR
    list_cmd = ['7z', 'l', rar_path]
    result = subprocess.run(list_cmd, capture_output=True, text=True, check=True)
    
    # Identificar y manejar RARs internos y archivos JP2
    for line in result.stdout.splitlines():
        if line.strip().endswith('.rar'):
            inner_rar = line.split()[-1]
            inner_rar_path = os.path.join(extract_to, inner_rar)
            extract_cmd = ['7z', 'x', rar_path, f'-o{extract_to}', inner_rar]
            subprocess.run(extract_cmd, check=True)
            jp2_paths.extend(extract_all_rars(inner_rar_path, extract_to))  # Llamada recursiva
        elif line.strip().endswith('.jp2'):
            jp2_filename = line.split()[-1]
            extract_cmd = ['7z', 'x', rar_path, f'-o{extract_to}', jp2_filename]
            subprocess.run(extract_cmd, check=True)
            jp2_paths.append(os.path.join(extract_to, jp2_filename))
    
    return jp2_paths

def merge_rasters(raster_paths, output_path):
    print(f"Fusionando rásteres en {output_path}...")

    # Abrir los rásteres y crear una lista de ellos
    rasters = [gdal.Open(raster_path) for raster_path in raster_paths]

    # Configurar opciones para la fusión
    vrt_options = gdal.BuildVRTOptions(resampleAlg='nearest', addAlpha=False)
    vrt = gdal.BuildVRT('/vsimem/merged.vrt', [r.GetDescription() for r in rasters], options=vrt_options)

    # Configurar opciones para la escritura final del ráster fusionado
    translate_options = gdal.TranslateOptions(format='JP2OpenJPEG', outputType=gdal.GDT_Byte)
    gdal.Translate(output_path, vrt, options=translate_options)

    # Limpiar recursos
    vrt = None
    for r in rasters:
        r = None

    print(f"Fusión completada: {output_path}")
    return output_path

def run_raster2pgsql(jp2_path, table_name):
    raster2pgsql_cmd = f"raster2pgsql -s 32719 -I -C -M -F -t 256x256 -N -9999 \"{jp2_path}\" {table_name} | psql -h {config['DB_HOST']} -d {config['DB_NAME']} -U {config['DB_USER']} -w"
    env = os.environ.copy()
    env['PGPASSWORD'] = config['DB_PASSWORD']
    print(f"Ejecutando comando: {raster2pgsql_cmd}")
    subprocess.run(raster2pgsql_cmd, shell=True, check=True, env=env)
    print(f"Ráster cargado en la tabla {table_name}.")

def update_table_with_geometries(table_name):
    connection = psycopg2.connect(dbname=config['DB_NAME'], user=config['DB_USER'], password=config['DB_PASSWORD'], host=config['DB_HOST'])
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"""
                ALTER TABLE {table_name} RENAME COLUMN rast TO geometria_raster;
                ALTER TABLE {table_name} ADD COLUMN id_comuna INTEGER;
                ALTER TABLE {table_name} ADD COLUMN tile_extent geometry(POLYGON, 32719);
                ALTER TABLE {table_name} ADD COLUMN raster_valued_extent geometry(MULTIPOLYGON, 32719);
            """)
            connection.commit()
            print(f"Columnas agregadas exitosamente en {table_name}.")

            cursor.execute(f"""
                UPDATE {table_name}
                SET
                    tile_extent = ST_ConvexHull(geometria_raster),
                    raster_valued_extent = ST_Multi(ST_ConvexHull(geometria_raster));
            """)
            connection.commit()
            print(f"Columnas de geometría actualizadas exitosamente en {table_name}.")
            
            cursor.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{table_name}_raster_valued_extent ON {table_name} USING GIST (raster_valued_extent);
            """)
            connection.commit()
            print(f"Índice GIST agregado exitosamente en {table_name}.")
            
            cursor.execute(f"""
                WITH intersections AS (
                    SELECT a.rid, c.objectid,
                           ST_Intersection(ST_Transform(c.geometria, 32719), a.raster_valued_extent) AS intersection
                    FROM {table_name} a
                    JOIN datos_maestros.dpa_comuna_subdere c
                    ON ST_Intersects(ST_Transform(c.geometria, 32719), a.raster_valued_extent)
                    WHERE ST_Area(ST_Intersection(ST_Transform(c.geometria, 32719), a.raster_valued_extent)) IS NOT NULL
                )
                UPDATE {table_name} a
                SET id_comuna = (
                    SELECT i.objectid
                    FROM intersections i
                    WHERE i.rid = a.rid
                    ORDER BY ST_Area(i.intersection) DESC
                    LIMIT 1
                )
                WHERE EXISTS (
                    SELECT 1
                    FROM intersections i
                    WHERE i.rid = a.rid
                );
            """)
            connection.commit()
            print(f"id_comuna actualizado exitosamente en {table_name}.")
    except psycopg2.Error as e:
        print(f"Error durante la operación en la base de datos: {e}")
        connection.rollback()
    finally:
        connection.close()
        print("Conexión a la base de datos cerrada.")

def main():
    # Configuración de URLs y nombres de tablas
    raster_configs = [
        {"url": "https://www.geoportal.cl/geoportal/catalog/download/50d4c190-6c3a-3b09-be2f-3d571960fea0", "table": "medio_fisico.dem_metropolitana", "merge": False},
        {"url": "https://www.geoportal.cl/geoportal/catalog/download/dd64b5cd-078f-3442-b599-83251a48311b", "table": "medio_fisico.dem_magallanes", "merge": True},
        {"url": "https://www.geoportal.cl/geoportal/catalog/download/f77f1be6-3dfc-37ce-a689-3f72022d301f", "table": "medio_fisico.dem_antofagasta", "merge": False}
    ]
    directory = "/home/dps_chanar/etl_raster"
    os.makedirs(directory, exist_ok=True)
    
    for raster_config in raster_configs:
        try:
            rar_url = raster_config["url"]
            table_name = raster_config["table"]
            merge_required = raster_config["merge"]
            rar_filename = os.path.basename(urlparse(rar_url).path)
            rar_path = os.path.join(directory, rar_filename)
            
            # Proceso de descarga y descompresión
            download_file(rar_url, rar_path)
            rar_path = ensure_rar_extension(rar_path)
            jp2_paths = extract_all_rars(rar_path, directory)
            
            # Fusión de rásteres si es necesario
            if merge_required:
                merged_raster_path = os.path.join(directory, f"{table_name}_merged.jp2")
                jp2_path = merge_rasters(jp2_paths, merged_raster_path)
            else:
                jp2_path = jp2_paths[0]  # Asumimos que solo hay un archivo JP2 por tabla
            
            # Carga en la base de datos
            run_raster2pgsql(jp2_path, table_name)
            update_table_with_geometries(table_name)
        except Exception as e:
            print(f"El proceso falló para {table_name}: {e}")

if __name__ == "__main__":
    main()
