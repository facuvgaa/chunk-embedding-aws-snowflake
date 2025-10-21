import sys
import ast
import boto3
import json
import pandas as pd
import numpy as np
import snowflake.connector
import uuid
from urllib.parse import urlparse
from awsglue.utils import getResolvedOptions

# Recibir parámetros desde Glue
args = getResolvedOptions(
    sys.argv,
    [
        "S3_KEY",
        "SNOWFLAKE_TABLE",
        "SNOWFLAKE_ORGANIZATION",
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_SCHEMA",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_ROLE",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PAT",
        "SNOWFLAKE_PASSWORD",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "SNOWFLAKE_TEMP"
    ]
)

# Configuración
sf_account = f"{args['SNOWFLAKE_ORGANIZATION']}-{args['SNOWFLAKE_ACCOUNT']}"
s3_url = args["S3_KEY"]
snowflake_table = args["SNOWFLAKE_TABLE"]
sf_database = args["SNOWFLAKE_DATABASE"]
sf_schema = args["SNOWFLAKE_SCHEMA"]
sf_warehouse = args["SNOWFLAKE_WAREHOUSE"]
sf_role = args["SNOWFLAKE_ROLE"]
sf_user = args["SNOWFLAKE_USER"]
sf_pat = args["SNOWFLAKE_PAT"]
sf_password = args["SNOWFLAKE_PASSWORD"]
sf_temp = args["SNOWFLAKE_TEMP"]

YOUR_AWS_KEY = args["AWS_ACCESS_KEY_ID"]
YOUR_AWS_SECRET = args["AWS_SECRET_ACCESS_KEY"]

full_table = f'"macro_db"."{sf_schema}"."{snowflake_table}"'

print("🚀 Configuración recibida:")
print(f"  • Archivo S3: {s3_url}")
print(f"  • Tabla destino: {full_table}")
print(f"  • Warehouse: {sf_warehouse}")
print(f"  • Role: {sf_role}")
print(f"  • Región: AWS_SA_EAST_1")
print(f"  • Fecha/Hora: 2025-09-21 03:20 AM -03")

# Descargar parquet desde S3
s3 = boto3.client("s3")
parsed = urlparse(s3_url)
bucket, key = parsed.netloc, parsed.path.lstrip("/")
local_file = "/tmp/input.parquet"
s3.download_file(bucket, key, local_file)
print(f"⬇️ Descargado {s3_url} a {local_file}")

# Cargar y transformar parquet
df = pd.read_parquet(local_file)
df = df.rename(columns={"text": "CHUNK_NAME"})
df["CHUNK_ID"] = [str(uuid.uuid4()) for _ in range(len(df))]
df["EMBED_TEMP"] = df["embedding"].apply(
    lambda x: list(map(float, ast.literal_eval(x))) if isinstance(x, str)
    else x.astype(float).tolist() if isinstance(x, np.ndarray)
    else [float(v) for v in x] if isinstance(x, list)
    else (_ for _ in ()).throw(ValueError(f"Tipo inesperado en embedding: {type(x)}"))
)
df["EMBED_TEMP"] = df["EMBED_TEMP"].apply(
    lambda x: x if len(x) == 768 else (_ for _ in ()).throw(ValueError(f"Embedding no tiene 768 dimensiones: {len(x)}"))
)
df["EMBED_TEMP"] = df["EMBED_TEMP"].apply(lambda x: json.dumps(x))
df = df[["CHUNK_ID", "CHUNK_NAME", "EMBED_TEMP"]]  # Ignorar source_pdf y EMBEDDING
tmp_file = "/tmp/input_parquet_ready.parquet"
df.to_parquet(tmp_file, engine="pyarrow", index=False)
print(f"✅ Parquet transformado: {tmp_file}")
print("🔹 Contenido del Parquet:")
print(pd.read_parquet(tmp_file).dtypes)
print(pd.read_parquet(tmp_file).head())
num_rows_input = len(df)
print(f"🔹 Filas en el Parquet: {num_rows_input}")

# Subir a S3
s3.upload_file(tmp_file, bucket, "input_parquet_ready.parquet")
print(f"⬆️ Subido a s3://{bucket}/input_parquet_ready.parquet")

# Conectar a Snowflake
conn = snowflake.connector.connect(
    account=sf_account,
    user=sf_user,
    password=sf_password,
    token=sf_pat,
    warehouse=sf_warehouse,
    schema=sf_schema,
    role=sf_role,
)
cursor = conn.cursor()
cursor.execute('USE DATABASE "macro_db"')
cursor.execute(f'USE SCHEMA "{sf_schema}"')

# Crear stage
stage_name = "TMP_STAGE"
cursor.execute(f"""
CREATE OR REPLACE STAGE {stage_name}
URL='s3://{bucket}/'
CREDENTIALS=(AWS_KEY_ID='{YOUR_AWS_KEY}' AWS_SECRET_KEY='{YOUR_AWS_SECRET}')
FILE_FORMAT = (TYPE = PARQUET)
""")

# COPY INTO (carga a CHUNK_ID, CHUNK_NAME, EMBED_TEMP)
copy_sql = f"""
COPY INTO "macro_db"."EMBEDDINGS"."DOCUMENT_CHUNKS"
  (CHUNK_ID, CHUNK_NAME, {sf_temp})
FROM (
  SELECT $1:CHUNK_ID::VARCHAR,
         $1:CHUNK_NAME::VARCHAR,
         PARSE_JSON($1:EMBED_TEMP)::VARIANT
  FROM @TMP_STAGE/input_parquet_ready.parquet
)
FILE_FORMAT = (TYPE = PARQUET)
ON_ERROR = 'CONTINUE';
"""
cursor.execute(copy_sql)
print(f"✅ Datos cargados en {full_table}")

# Verificar conteo de filas cargadas
cursor.execute(f"""
SELECT COUNT(*) AS row_count
FROM "macro_db"."EMBEDDINGS"."DOCUMENT_CHUNKS";
""")
row_count = cursor.fetchone()[0]
print(f"🔹 Filas cargadas en Snowflake: {row_count}")
if row_count >= num_rows_input:
    print(f"✅ Todas las {num_rows_input} filas del Parquet se cargaron correctamente")
else:
    print(f"⚠️ Solo {row_count} de {num_rows_input} filas se cargaron. Revisar historial de consultas.")

# Verificar datos cargados
cursor.execute(f"""
SELECT CHUNK_ID, CHUNK_NAME, {sf_temp}
FROM "macro_db"."EMBEDDINGS"."DOCUMENT_CHUNKS"
LIMIT 5;
""")
print("🔹 Datos cargados (primeras 5 filas):")
for row in cursor.fetchall():
    print(row)

# Consultar historial de COPY INTO para errores
cursor.execute(f"""
SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => '"macro_db"."EMBEDDINGS"."DOCUMENT_CHUNKS"',
    START_TIME => DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
))
WHERE STATUS != 'LOADED';
""")
errors = cursor.fetchall()
if errors:
    print("🔴 Errores en COPY INTO encontrados en el historial:")
    for error in errors:
        print(error)
else:
    print("✅ No se encontraron errores en el historial de COPY INTO")

# Cerrar conexión
cursor.close()
conn.close()
