import requests
import hashlib
import time
import pandas as pd
from google.cloud import bigquery
from google.cloud import secretmanager

def access_secret_version(project_id, secret_id, version_id="latest"):
    # Crear el cliente de Secret Manager
    client = secretmanager.SecretManagerServiceClient()

    # Configurar el nombre completo del secreto y la versión (por defecto 'latest')
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Acceder a la versión del secreto
    response = client.access_secret_version(request={"name": name})

    # El valor del secreto está en 'payload.data'
    secret_value = response.payload.data.decode("UTF-8")

    print(f"Valor del secreto '{secret_id}': {secret_value}")
    return secret_value

# Configura tus variables
project_id = "proyect-pma"
public_key_secret_id = "marvel_public_key"
private_key_secret_id = "marvel_private_key"

# Obtener la clave pública
public_key = access_secret_version(project_id, public_key_secret_id)

# Obtener la clave privada
private_key = access_secret_version(project_id, private_key_secret_id)



def generate_hash(ts, private_key, public_key):
    """Generar el hash md5 basado en ts + privateKey + publicKey"""
    to_hash = str(ts) + private_key + public_key
    return hashlib.md5(to_hash.encode()).hexdigest()

def fetch_all_comics(url, max_records=350):
    all_results = []
    limit = 100  
    offset = 0  

    while True:
        # Obtener timestamp dinámico
        ts = int(time.time())

        # Generar hash para la solicitud
        hash_value = generate_hash(ts, private_key, public_key)

        # Parámetros de la solicitud
        params = {
            'ts': ts,
            'apikey': public_key,
            'hash': hash_value,
            'limit': limit,  # Limitar a 100 resultados
            'offset': offset  # Empezar desde el resultado especificado
        }

        headers = {
            'Accept': '*/*',
            
        }

        try:
            # Realizar la solicitud GET
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            data = response.json()

            # Extraer los resultados
            results = data['data']['results']

            # Calcular cuántos registros más se pueden añadir sin superar el máximo
            remaining_space = max_records - len(all_results)

            # Si el número de resultados supera el límite que falta, solo añade lo necesario
            if len(results) > remaining_space:
                all_results.extend(results[:remaining_space])  # Añadir solo lo que falta
                break  # Romper el bucle cuando se alcanza el máximo de 350 registros
            else:
                all_results.extend(results)  # Añadir todos los resultados de esta página

            # Verificar si hay más páginas para obtener
            if len(results) < limit:
                break  # Romper el bucle si se obtuvieron menos de 100 resultados

            # Actualizar el offset para la siguiente página
            offset += limit

            # Detenerse si ya hemos alcanzado el máximo de 350 registros
            if len(all_results) >= max_records:
                break

        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            break  # Salir del bucle en caso de error

    return all_results

def create_dataframe_from_results(results):
    # Crear listas vacías para los campos deseados
    ids = []
    names = []
    descriptions = []

    # Iterar sobre los resultados y extraer la información
    for item in results:
        ids.append(item.get('id', 'N/A'))
        names.append(item.get('name', 'N/A'))

        # Verificar si 'description' es None, está vacío o no existe, y asignar 'No_description' en esos casos
        description = item.get('description')
        if not description:  # Esto cubre el caso de None y cadenas vacías ("")
            descriptions.append('No_description')
        else:
            descriptions.append(description)

    # Crear el DataFrame
    df = pd.DataFrame({
        'id': ids,
        'name': names,
        'description': descriptions
    })

    return df

def run_pipeline():
    # URL base de la API de Marvel
    base_url = 'http://gateway.marvel.com/v1/public/characters'

    # Obtener todos los resultados de todas las páginas, hasta un máximo de 350 registros
    results = fetch_all_comics(base_url, max_records=350)

    # Crear el DataFrame con todos los resultados obtenidos
    df = create_dataframe_from_results(results)

    print(df)


    #configuración de Bquery
    project_id = "proyect-pma"
    dataset_id = "Johnny_ETLs"
    table_id = "Marvel"

    # Configuración del trabajo de carga
    cliente = bigquery.Client(project=project_id)
    table_ref = cliente.dataset(dataset_id).table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.autodetect = True


    # Cargar el DataFrame a la tabla en BigQuery
    job = cliente.load_table_from_dataframe(df, table_ref, job_config=job_config)
        
    # job.result() # Esperar a que el trabajo se complete
    job.result()

    # Imprimir confirmación de éxito
    print(f"Datos cargados exitosamente en la tabla {table_id}")

def main(request):
    try:
        run_pipeline()
        return "corrio bien"
    except Exception as e:
        error_message=str(e)
        print(f"Error is: {error_message}")
        return "Error"

            