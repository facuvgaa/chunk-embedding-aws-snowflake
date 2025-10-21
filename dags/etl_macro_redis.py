from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from macro_embeddings_flow import main as embedding_main
from macro_flow_chunks import main as chunks_main
from airflow.decorators import dag, task
from datetime import timedelta
import pendulum
import os
import time
import redis
import hashlib


URLS_FILE = os.path.join(os.path.dirname(__file__), "urls.txt")

@dag(
    dag_id="macro_etl_redis_batch",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 9, 17, tz="America/Argentina/Tucuman"),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["etl", "redis", "batch"]
)
def macro_etl_dag():
    @task()
    def leer_urls_archivo():
        """Lee URLs desde archivo local"""
        try:
            with open(URLS_FILE, "r") as file:
                urls = [line.strip() for line in file.readlines() if line.strip()]
            print(f"📖 Leídas {len(urls)} URLs desde archivo")
            return urls
        except Exception as e:
            print(f"❌ Error leyendo archivo: {e}")
            return []

    @task()
    def filtrar_urls_no_procesadas(urls: list):
        """Filtra URLs que no han sido procesadas usando Redis"""
        try:
            # Conectar a Redis
            redis_client = redis.Redis(
                host=os.getenv('REDIS_HOST', 'localhost'),
                port=int(os.getenv('REDIS_PORT', 6379)),
                db=int(os.getenv('REDIS_DB', 0)),
                decode_responses=True
            )
            
            urls_no_procesadas = []
            for url in urls:
                # Crear hash de la URL para usar como clave
                url_hash = hashlib.md5(url.encode()).hexdigest()
                redis_key = f"processed_url:{url_hash}"
                
                # Verificar si ya fue procesada
                if not redis_client.exists(redis_key):
                    urls_no_procesadas.append(url)
                    print(f"✅ URL nueva: {url}")
                else:
                    print(f"⏭️ URL ya procesada: {url}")
            
            print(f"📊 Total URLs: {len(urls)}, Nuevas: {len(urls_no_procesadas)}")
            return urls_no_procesadas
            
        except Exception as e:
            print(f"❌ Error conectando a Redis: {e}")
            print("🔄 Procesando todas las URLs sin filtro")
            return urls

    @task()
    def marcar_urls_procesadas(urls: list):
        """Marca URLs como procesadas en Redis"""
        try:
            redis_client = redis.Redis(
                host=os.getenv('REDIS_HOST', 'localhost'),
                port=int(os.getenv('REDIS_PORT', 6379)),
                db=int(os.getenv('REDIS_DB', 0)),
                decode_responses=True
            )
            
            for url in urls:
                url_hash = hashlib.md5(url.encode()).hexdigest()
                redis_key = f"processed_url:{url_hash}"
                # Marcar como procesada con TTL de 30 días
                redis_client.setex(redis_key, 30 * 24 * 60 * 60, "processed")
                print(f"✅ Marcada como procesada: {url}")
            
            print(f"📝 Marcadas {len(urls)} URLs como procesadas")
            
        except Exception as e:
            print(f"❌ Error marcando URLs en Redis: {e}")

    @task()
    def dividir_urls_en_lotes(urls: list, tamano_lote: int = 3):
        """Divide las URLs en lotes de 3"""
        lotes = []
        for i in range(0, len(urls), tamano_lote):
            lote = urls[i:i + tamano_lote]
            lotes.append(lote)
            print(f"Lote {len(lotes)}: {lote}")
        return lotes

    @task()
    def procesar_lote_urls(lote_urls: list, numero_lote: int):
        """Procesa un lote de URLs (máximo 3)"""
        print(f"🔄 Procesando Lote {numero_lote}: {lote_urls}")
        
        s3_keys = []
        for i, url in enumerate(lote_urls):
            try:
                s3_key = chunks_main.MacroEtl(url)
                s3_keys.append(s3_key)
                print(f"Chunks guardados en: {s3_key} (URL {i+1}/{len(lote_urls)} del Lote {numero_lote})")
            except Exception as e:
                print(f"Error procesando URL {i+1} ({url}) del Lote {numero_lote}: {e}")
                continue
        
        if not s3_keys:
            raise ValueError(f"No se pudieron procesar URLs del Lote {numero_lote}")
        
        print(f"✅ Lote {numero_lote}: Procesadas {len(s3_keys)} URLs exitosamente")
        return s3_keys

    @task()
    def procesar_embeddings_lote(chunks_s3_paths: list, numero_lote: int):
        """Procesa embeddings para un lote de chunks"""
        print(f"🧠 Procesando embeddings del Lote {numero_lote}")
        
        embedding_urls = []
        for i, s3_path in enumerate(chunks_s3_paths):
            try:
                embedding_url = embedding_main.MacroEtlEmbedding(s3_path)
                embedding_urls.append(embedding_url)
                print(f"Embeddings guardados en: {embedding_url} (Chunk {i+1}/{len(chunks_s3_paths)} del Lote {numero_lote})")
            except Exception as e:
                print(f"Error procesando chunks {i+1} ({s3_path}) del Lote {numero_lote}: {e}")
                continue
        
        if not embedding_urls:
            raise ValueError(f"No se pudieron generar embeddings para el Lote {numero_lote}")
        
        print(f"✅ Lote {numero_lote}: Generados {len(embedding_urls)} embeddings exitosamente")
        return embedding_urls

    @task()
    def esperar_entre_lotes(numero_lote: int, tiempo_espera: int = 30):
        """Espera entre lotes para evitar sobrecarga"""
        print(f"⏳ Esperando {tiempo_espera} segundos antes del siguiente lote...")
        time.sleep(tiempo_espera)
        print(f"✅ Espera completada. Listo para procesar siguiente lote.")
        return f"Espera completada para lote {numero_lote}"

    # Flujo principal: Leer URLs, filtrar no procesadas y dividir en lotes
    urls_archivo = leer_urls_archivo()
    urls_no_procesadas = filtrar_urls_no_procesadas(urls_archivo)
    
    # Si no hay URLs nuevas, terminar el DAG
    if not urls_no_procesadas:
        print("🎉 No hay URLs nuevas para procesar")
        return
    
    lotes_urls = dividir_urls_en_lotes(urls_no_procesadas, tamano_lote=3)
    
    # Procesar cada lote secuencialmente
    tareas_lotes = []
    tareas_embeddings = []
    tareas_glue = []
    
    for i, lote in enumerate(lotes_urls, 1):
        # Task para procesar URLs del lote
        procesar_lote = procesar_lote_urls.override(task_id=f"procesar_lote_{i}")(lote, i)
        
        # Task para procesar embeddings del lote
        procesar_embeddings = procesar_embeddings_lote.override(task_id=f"embeddings_lote_{i}")(procesar_lote, i)
        
        # GlueJobOperator para el lote
        run_glue_lote = GlueJobOperator(
            task_id=f"glue_lote_{i}",
            job_name=os.getenv('AWS_GLUEJOB_NAME'),
            region_name=os.getenv('AWS_DEFAULT_REGION'),
            script_args={
                "--S3_KEY": "{{ ti.xcom_pull(task_ids='embeddings_lote_" + str(i) + "')[0] if ti.xcom_pull(task_ids='embeddings_lote_" + str(i) + "') else '' }}",
                "--SNOWFLAKE_TABLE": "DOCUMENT_CHUNKS",
                "--AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
                "--AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
                "--SNOWFLAKE_ORGANIZATION": os.getenv("SNOWFLAKE_ORGANIZATION"),
                "--SNOWFLAKE_PASSWORD": os.getenv("SNOWFLAKE_PASSWORD"),
                "--SNOWFLAKE_ACCOUNT": os.getenv("SNOWFLAKE_ACCOUNT"),
                "--SNOWFLAKE_DATABASE": os.getenv("SNOWFLAKE_DATABASE"),
                "--SNOWFLAKE_SCHEMA": os.getenv("SNOWFLAKE_SCHEMA"),
                "--SNOWFLAKE_WAREHOUSE": os.getenv("SNOWFLAKE_WAREHOUSE"),
                "--SNOWFLAKE_ROLE": os.getenv("SNOWFLAKE_ROLE"),
                "--SNOWFLAKE_USER": os.getenv("SNOWFLAKE_USER"),
                "--SNOWFLAKE_PAT": os.getenv("SNOWFLAKE_TOKEN"),
                "--SNOWFLAKE_TEMP": os.getenv("SNOWFLAKE_EMBED_TEMP")
            }
        )
        
        # Espera entre lotes (excepto para el último)
        if i < len(lotes_urls):
            espera = esperar_entre_lotes.override(task_id=f"espera_lote_{i}")(i, 30)
            run_glue_lote >> espera
        
        tareas_lotes.append(procesar_lote)
        tareas_embeddings.append(procesar_embeddings)
        tareas_glue.append(run_glue_lote)
    
    # Dependencias entre lotes
    for i in range(len(tareas_lotes)):
        if i == 0:
            # Primer lote: depende de la lectura de URLs
            urls >> tareas_lotes[i]
        else:
            # Lotes siguientes: dependen de la espera del lote anterior
            tareas_glue[i-1] >> tareas_lotes[i]
        
        # Dependencias dentro del lote
        tareas_lotes[i] >> tareas_embeddings[i] >> tareas_glue[i]
    
    # Marcar URLs como procesadas al final de todo el procesamiento
    marcar_procesadas = marcar_urls_procesadas.override(task_id="marcar_urls_procesadas")(urls_no_procesadas)
    
    # El último Glue job debe completarse antes de marcar como procesadas
    if tareas_glue:
        tareas_glue[-1] >> marcar_procesadas


dag = macro_etl_dag()
