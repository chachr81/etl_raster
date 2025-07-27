import os
import requests
import subprocess
from urllib.parse import urlparse
import psycopg2
from dotenv import dotenv_values

# Cargar configuración del entorno
config = dotenv_values("/home/dps_chanar/.env")

def download_file(url, destination):
    print(f"Descargando archivo desde {url}...")
    response = requests.get(url, stream=True)
    response.raise_for_status()  # Verifica si hubo algún error en la descarga
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

def extract_jp2_with_7z(rar_path, extract_to):
    print(f"Extrayendo el archivo .jp2 de {rar_path} en {extract_to} con 7z...")
    list_cmd = ['7z', 'l', rar_path]
    result = subprocess.run(list_cmd, capture_output=True, text=True, check=True)
    jp2_filename = None
    for line in result.stdout.splitlines():
        if line.strip().endswith('.jp2'):
            jp2_filename = line.split()[-1]
            break
    if not jp2_filename:
        raise FileNotFoundError("No se encontró ningún archivo .jp2 en el archivo RAR.")
    extract_cmd = ['7z', 'x', rar_path, f'-o{extract_to}', jp2_filename]
    subprocess.run(extract_cmd, check=True)
    print(f"Extracción completada: {jp2_filename}")
    return os.path.join(extract_to, jp2_filename)

def run_raster2pgsql(jp2_path):
    raster2pgsql_cmd = f"raster2pgsql -s 32719 -I -C -M -F -t 256x256 -N -9999 \"{jp2_path}\" medio_fisico.dem_antofagasta | psql -h {config['DB_HOST_P']} -d {config['DB_NAME_P']} -U {config['DB_USER_P']} -w"
    env = os.environ.copy()
    env['PGPASSWORD'] = config['DB_PASSWORD_P']
    print(f"Ejecutando comando: {raster2pgsql_cmd}")
    subprocess.run(raster2pgsql_cmd, shell=True, check=True, env=env)
    print("Comando ejecutado exitosamente, verifique la base de datos para confirmar la creación de la tabla.")

def update_table_with_geometries():
    connection = psycopg2.connect(dbname=config['DB_NAME_P'], user=config['DB_USER_P'], password=config['DB_PASSWORD_P'], host=config['DB_HOST_P'])
    try:
        with connection.cursor() as cursor:
            # RENAME, ADD COLUMN, and UPDATE operations with commits after each significant step.
            cursor.execute("""
                ALTER TABLE medio_fisico.dem_antofagasta RENAME COLUMN rast TO geometria_raster;
            """)
            connection.commit()
            print("Column renamed successfully.")

            cursor.execute("""
                ALTER TABLE medio_fisico.dem_antofagasta ADD COLUMN id_comuna INTEGER;
                ALTER TABLE medio_fisico.dem_antofagasta ADD COLUMN tile_extent geometry(POLYGON, 32719);
                ALTER TABLE medio_fisico.dem_antofagasta ADD COLUMN raster_valued_extent geometry(MULTIPOLYGON, 32719);
            """)
            connection.commit()
            print("Columns added successfully.")

            cursor.execute("""
                UPDATE medio_fisico.dem_antofagasta
                SET
                    tile_extent = ST_ConvexHull(geometria_raster),
                    raster_valued_extent = ST_Multi(ST_Envelope(geometria_raster));
            """)
            connection.commit()
            print("Geometry columns updated successfully.")

            cursor.execute("""
                UPDATE medio_fisico.dem_antofagasta
                SET id_comuna = (
                    SELECT c.objectid
                    FROM datos_maestros.dpa_comuna_subdere c
                    WHERE ST_Intersects(ST_Transform(c.geometria, 32719), ST_ConvexHull(ST_Transform(geometria_raster, 32719)))
                    ORDER BY ST_Area(ST_Intersection(ST_Transform(c.geometria, 32719), ST_ConvexHull(ST_Transform(geometria_raster, 32719)))) DESC
                    LIMIT 1
                );
            """)
            connection.commit()
            print("id_comuna updated successfully.")
    except psycopg2.Error as e:
        print(f"Error during database operation: {e}")
        connection.rollback()
    finally:
        connection.close()
        print("Database connection closed.")

def main():
    rar_url = "https://www.geoportal.cl/geoportal/catalog/download/f77f1be6-3dfc-37ce-a689-3f72022d301f"
    rar_filename = os.path.basename(urlparse(rar_url).path)
    directory = "/home/dps_chanar/etl_raster"
    rar_path = os.path.join(directory, rar_filename)
    os.makedirs(directory, exist_ok=True)
    try:
        download_file(rar_url, rar_path)
        rar_path = ensure_rar_extension(rar_path)
        jp2_path = extract_jp2_with_7z(rar_path, directory)
        run_raster2pgsql(jp2_path)
        update_table_with_geometries()
    except Exception as e:
        print(f"El proceso falló: {e}")

if __name__ == "__main__":
    main()
