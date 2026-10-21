"""
Pipeline ELT: Inside Airbnb  ->  Dagster+ (orquestación)  ->  BigQuery (data warehouse)

Flujo:
  1. EXTRACCIÓN   : se descargan 3 archivos de Inside Airbnb y se cargan "crudos" (todo como texto)
                    en el dataset  <TEAM_CODE>_raw  de BigQuery.
  2. TRANSFORMACIÓN: SQL que corre DENTRO de BigQuery (por eso es ELT, no ETL) y crea tablas
                    de staging limpias en el dataset  <TEAM_CODE>_dwh.
  3. MODELO       : esquema en estrella -> fact_listings + dim_barrio + dim_anfitrion + dim_tipo_alojamiento.
  4. CALIDAD      : asset checks de Dagster sobre la tabla de hechos.

Variables de entorno (se configuran en Dagster+ -> Deployment -> Environment variables):
  GCP_PROJECT       id del proyecto de Google Cloud (ej. dw-garcia-lopez)
  GCP_CREDENTIALS   llave JSON de la cuenta de servicio, codificada en base64
  TEAM_CODE         código del equipo (ej. dw_garcia_lopez_munoz). Solo letras, números y _
  URL_LISTINGS      URL de listings.csv.gz      (copiada de insideairbnb.com/get-the-data, Buenos Aires)
  URL_REVIEWS       URL de reviews.csv          (el resumido, no el .gz detallado)
  URL_NEIGHBOURHOODS URL de neighbourhoods.csv
"""

import os
from urllib.parse import quote

import pandas as pd
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetExecutionContext,
    AssetSelection,
    Backoff,
    Definitions,
    EnvVar,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    ScheduleDefinition,
    asset,
    asset_check,
    define_asset_job,
)
from dagster_gcp import BigQueryResource
from google.cloud import bigquery

# ---------------------------------------------------------------------------
# Configuración general
# ---------------------------------------------------------------------------
TEAM = os.getenv("TEAM_CODE", "dw_equipo")          # R7: código de equipo en todos los recursos
RAW = f"{TEAM}_raw"                                   # dataset con los datos tal como llegan
DWH = f"{TEAM}_dwh"                                   # dataset con staging + modelo en estrella
LOCATION = "US"

# Para no subir columnas que no usamos (el listings detallado trae ~75 columnas)
COLUMNAS_LISTINGS = {
    "id", "name", "host_id", "host_name", "host_since", "host_is_superhost",
    "neighbourhood_cleansed", "room_type", "accommodates", "bedrooms", "price",
    "number_of_reviews", "review_scores_rating", "availability_365",
    "latitude", "longitude", "last_scraped",
}

# Reintentos automáticos: si la descarga falla (red, servidor caído) Dagster reintenta 2 veces
REINTENTOS = RetryPolicy(max_retries=2, delay=30, backoff=Backoff.EXPONENTIAL)


# ---------------------------------------------------------------------------
# Funciones de apoyo
# ---------------------------------------------------------------------------
def _asegurar_dataset(client: bigquery.Client, nombre: str) -> None:
    ds = bigquery.Dataset(f"{client.project}.{nombre}")
    ds.location = LOCATION
    client.create_dataset(ds, exists_ok=True)


def _cargar_csv_a_raw(context, bq: BigQueryResource, url_env: str, tabla: str, usecols=None):
    """EXTRACCIÓN: lee el CSV desde la URL y lo carga tal cual (todo texto) en <TEAM>_raw."""
    # quote(): la URL de Buenos Aires tiene tildes ("autónoma") y hay que codificarlas
    url = quote(os.environ[url_env].strip(), safe=":/?&=%")
    context.log.info(f"Descargando {url}")
    df = pd.read_csv(
        url,
        dtype="string",                                   # todo como texto: los tipos se arreglan en SQL
        usecols=(lambda c: c in usecols) if usecols else None,
        storage_options={"User-Agent": "Mozilla/5.0"},    # algunos servidores bloquean el user-agent de Python
    )
    df.columns = [c.strip().lower() for c in df.columns]
    destino = f"{RAW}.{tabla}"
    with bq.get_client() as client:
        _asegurar_dataset(client, RAW)
        job = client.load_table_from_dataframe(
            df,
            f"{client.project}.{destino}",
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
        )
        job.result()  # espera a que termine el job de carga
    context.log.info(f"{len(df):,} filas cargadas en {destino}")
    return MaterializeResult(
        metadata={
            "filas": len(df),
            "tabla_bigquery": destino,
            "fuente": MetadataValue.url(url),
        }
    )


def _crear_tabla(context, bq: BigQueryResource, tabla: str, sql: str):
    """TRANSFORMACIÓN/CARGA: ejecuta un CREATE OR REPLACE TABLE ... AS SELECT dentro de BigQuery."""
    with bq.get_client() as client:
        _asegurar_dataset(client, DWH)
        consulta = sql.format(p=client.project, raw=RAW, dwh=DWH)
        context.log.info(consulta)
        client.query(consulta).result()
        filas = client.get_table(f"{client.project}.{DWH}.{tabla}").num_rows
    context.log.info(f"{filas:,} filas en {DWH}.{tabla}")
    return MaterializeResult(metadata={"filas": filas, "tabla_bigquery": f"{DWH}.{tabla}"})


def _consulta_escalar(bq: BigQueryResource, sql: str):
    with bq.get_client() as client:
        fila = list(client.query(sql.format(p=client.project, raw=RAW, dwh=DWH)).result())[0]
    return fila[0]


# ---------------------------------------------------------------------------
# 1. EXTRACCIÓN (fuente -> nube)
# ---------------------------------------------------------------------------
@asset(key_prefix=[TEAM], group_name=f"{TEAM}_extraccion", retry_policy=REINTENTOS,
       description="listings.csv.gz de Inside Airbnb cargado sin transformar en BigQuery")
def raw_listings(context: AssetExecutionContext, bigquery_dw: BigQueryResource):
    return _cargar_csv_a_raw(context, bigquery_dw, "URL_LISTINGS", "raw_listings", COLUMNAS_LISTINGS)


@asset(key_prefix=[TEAM], group_name=f"{TEAM}_extraccion", retry_policy=REINTENTOS,
       description="reviews.csv (una fila por reseña: listing_id, date) cargado sin transformar")
def raw_reviews(context: AssetExecutionContext, bigquery_dw: BigQueryResource):
    return _cargar_csv_a_raw(context, bigquery_dw, "URL_REVIEWS", "raw_reviews")


@asset(key_prefix=[TEAM], group_name=f"{TEAM}_extraccion", retry_policy=REINTENTOS,
       description="neighbourhoods.csv (catálogo de barrios) cargado sin transformar")
def raw_neighbourhoods(context: AssetExecutionContext, bigquery_dw: BigQueryResource):
    return _cargar_csv_a_raw(context, bigquery_dw, "URL_NEIGHBOURHOODS", "raw_neighbourhoods")


# ---------------------------------------------------------------------------
# 2. TRANSFORMACIONES (SQL dentro de BigQuery)
# ---------------------------------------------------------------------------
@asset(key_prefix=[TEAM], group_name=f"{TEAM}_transformacion", deps=[raw_listings],
       description="T1 conversión de tipos + T2 limpieza de nulos y duplicados + T3 columnas derivadas")
def stg_listings(context: AssetExecutionContext, bigquery_dw: BigQueryResource):
    sql = """
    CREATE OR REPLACE TABLE `{p}.{dwh}.stg_listings` AS
    SELECT
      SAFE_CAST(id AS INT64)                                        AS listing_id,
      name                                                          AS nombre,
      SAFE_CAST(host_id AS INT64)                                   AS host_id,
      COALESCE(host_name, 'Sin nombre')                             AS host_name,          -- T2 nulos
      SAFE_CAST(host_since AS DATE)                                 AS host_since,         -- T1 tipos
      host_is_superhost = 't'                                       AS es_superhost,       -- T1 texto -> BOOL
      TRIM(neighbourhood_cleansed)                                  AS barrio,
      room_type,
      SAFE_CAST(accommodates AS INT64)                              AS capacidad,
      COALESCE(CAST(SAFE_CAST(bedrooms AS FLOAT64) AS INT64), 0)    AS habitaciones,       -- T2 nulos
      SAFE_CAST(REGEXP_REPLACE(price, r'[$,]', '') AS NUMERIC)      AS precio,             -- T1 '$1,250.00' -> 1250.00
      SAFE_CAST(review_scores_rating AS FLOAT64)                    AS calificacion,
      SAFE_CAST(availability_365 AS INT64)                          AS disponibilidad_365,
      SAFE_CAST(latitude AS FLOAT64)                                AS latitud,
      SAFE_CAST(longitude AS FLOAT64)                               AS longitud,
      -- T3 columnas derivadas
      365 - SAFE_CAST(availability_365 AS INT64)                    AS noches_no_disponibles,
      -- rango por cuartiles: no depende de la moneda (pesos argentinos, mexicanos, dólares...)
      CASE NTILE(4) OVER (ORDER BY SAFE_CAST(REGEXP_REPLACE(price, r'[$,]', '') AS NUMERIC))
        WHEN 1 THEN '1. Económico'
        WHEN 2 THEN '2. Medio'
        WHEN 3 THEN '3. Alto'
        ELSE '4. Lujo'
      END                                                           AS rango_precio,
      SAFE_CAST(last_scraped AS DATE)                               AS fecha_extraccion
    FROM `{p}.{raw}.raw_listings`
    WHERE SAFE_CAST(id AS INT64) IS NOT NULL                                              -- T2 sin id = basura
      AND SAFE_CAST(REGEXP_REPLACE(price, r'[$,]', '') AS NUMERIC) > 0                    -- T2 sin precio no sirve para el análisis
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY last_scraped DESC) = 1            -- T2 duplicados
    """
    return _crear_tabla(context, bigquery_dw, "stg_listings", sql)


@asset(key_prefix=[TEAM], group_name=f"{TEAM}_transformacion", deps=[raw_reviews],
       description="T4 agregación: reseñas totales y de los últimos 12 meses por anuncio")
def stg_resenas_por_anuncio(context: AssetExecutionContext, bigquery_dw: BigQueryResource):
    sql = """
    CREATE OR REPLACE TABLE `{p}.{dwh}.stg_resenas_por_anuncio` AS
    WITH r AS (
      SELECT SAFE_CAST(listing_id AS INT64) AS listing_id,
             SAFE_CAST(date AS DATE)        AS fecha
      FROM `{p}.{raw}.raw_reviews`
      WHERE SAFE_CAST(listing_id AS INT64) IS NOT NULL
    ),
    corte AS (SELECT MAX(fecha) AS fecha_corte FROM r)
    SELECT
      r.listing_id,
      COUNT(*)                                                               AS total_resenas,
      COUNTIF(r.fecha > DATE_SUB(corte.fecha_corte, INTERVAL 12 MONTH))      AS resenas_12m,
      MIN(r.fecha)                                                           AS primera_resena,
      MAX(r.fecha)                                                           AS ultima_resena
    FROM r CROSS JOIN corte
    GROUP BY r.listing_id
    """
    return _crear_tabla(context, bigquery_dw, "stg_resenas_por_anuncio", sql)


# ---------------------------------------------------------------------------
# 3. MODELO EN ESTRELLA (dimensiones + hechos)
# ---------------------------------------------------------------------------
@asset(key_prefix=[TEAM], group_name=f"{TEAM}_modelo_estrella", deps=[raw_neighbourhoods],
       description="Dimensión barrio. Llave sustituta estable con FARM_FINGERPRINT")
def dim_barrio(context: AssetExecutionContext, bigquery_dw: BigQueryResource):
    sql = """
    CREATE OR REPLACE TABLE `{p}.{dwh}.dim_barrio` AS
    SELECT DISTINCT
      FARM_FINGERPRINT(TRIM(neighbourhood))            AS barrio_key,
      TRIM(neighbourhood)                              AS barrio,
      COALESCE(NULLIF(TRIM(neighbourhood_group), ''), 'Sin grupo') AS grupo_barrio
    FROM `{p}.{raw}.raw_neighbourhoods`
    WHERE neighbourhood IS NOT NULL
    """
    return _crear_tabla(context, bigquery_dw, "dim_barrio", sql)


@asset(key_prefix=[TEAM], group_name=f"{TEAM}_modelo_estrella", deps=[stg_listings],
       description="Dimensión anfitrión con antigüedad y número de anuncios")
def dim_anfitrion(context: AssetExecutionContext, bigquery_dw: BigQueryResource):
    sql = """
    CREATE OR REPLACE TABLE `{p}.{dwh}.dim_anfitrion` AS
    SELECT
      host_id                                               AS anfitrion_key,
      ANY_VALUE(host_name)                                  AS nombre_anfitrion,
      MIN(host_since)                                       AS anfitrion_desde,
      LOGICAL_OR(es_superhost)                              AS es_superhost,
      DATE_DIFF(MAX(fecha_extraccion), MIN(host_since), YEAR) AS antiguedad_anios,
      COUNT(*)                                              AS num_anuncios,
      COUNT(*) > 1                                          AS es_multianfitrion
    FROM `{p}.{dwh}.stg_listings`
    WHERE host_id IS NOT NULL
    GROUP BY host_id
    """
    return _crear_tabla(context, bigquery_dw, "dim_anfitrion", sql)


@asset(key_prefix=[TEAM], group_name=f"{TEAM}_modelo_estrella", deps=[stg_listings],
       description="Dimensión tipo de alojamiento, traducida al español")
def dim_tipo_alojamiento(context: AssetExecutionContext, bigquery_dw: BigQueryResource):
    sql = """
    CREATE OR REPLACE TABLE `{p}.{dwh}.dim_tipo_alojamiento` AS
    SELECT DISTINCT
      FARM_FINGERPRINT(room_type) AS tipo_key,
      room_type                   AS tipo_original,
      CASE room_type
        WHEN 'Entire home/apt' THEN 'Vivienda completa'
        WHEN 'Private room'    THEN 'Habitación privada'
        WHEN 'Shared room'     THEN 'Habitación compartida'
        WHEN 'Hotel room'      THEN 'Habitación de hotel'
        ELSE room_type
      END                         AS tipo_alojamiento
    FROM `{p}.{dwh}.stg_listings`
    WHERE room_type IS NOT NULL
    """
    return _crear_tabla(context, bigquery_dw, "dim_tipo_alojamiento", sql)


@asset(key_prefix=[TEAM], group_name=f"{TEAM}_modelo_estrella",
       deps=[stg_listings, stg_resenas_por_anuncio, dim_barrio, dim_anfitrion, dim_tipo_alojamiento],
       description="Tabla de hechos: un anuncio por fila, con medidas de precio, ocupación y reseñas (T5 join)")
def fact_listings(context: AssetExecutionContext, bigquery_dw: BigQueryResource):
    sql = """
    CREATE OR REPLACE TABLE `{p}.{dwh}.fact_listings` AS
    SELECT
      l.listing_id,
      l.host_id                                    AS anfitrion_key,
      FARM_FINGERPRINT(l.barrio)                   AS barrio_key,
      FARM_FINGERPRINT(l.room_type)                AS tipo_key,
      l.rango_precio,
      -- medidas
      l.precio,
      l.capacidad,
      l.habitaciones,
      l.calificacion,
      l.disponibilidad_365,
      l.noches_no_disponibles,
      ROUND(l.precio * l.noches_no_disponibles, 0) AS ingreso_potencial_anual,
      COALESCE(r.total_resenas, 0)                 AS total_resenas,
      COALESCE(r.resenas_12m, 0)                   AS resenas_12m,
      r.ultima_resena,
      l.latitud,
      l.longitud
    FROM `{p}.{dwh}.stg_listings` l
    LEFT JOIN `{p}.{dwh}.stg_resenas_por_anuncio` r USING (listing_id)    -- T5 unión entre fuentes
    """
    return _crear_tabla(context, bigquery_dw, "fact_listings", sql)


# ---------------------------------------------------------------------------
# 4. CONTROLES DE CALIDAD (asset checks) sobre la tabla de hechos
# ---------------------------------------------------------------------------
@asset_check(asset=fact_listings, blocking=True,
             description="No debe haber listing_id repetidos (la granularidad es 1 anuncio)")
def fact_sin_duplicados(bigquery_dw: BigQueryResource):
    dup = _consulta_escalar(bigquery_dw, """
        SELECT COUNT(*) - COUNT(DISTINCT listing_id) FROM `{p}.{dwh}.fact_listings`""")
    return AssetCheckResult(passed=dup == 0, metadata={"duplicados": int(dup)})


@asset_check(asset=fact_listings, description="La tabla de hechos debe tener al menos 1.000 filas")
def fact_minimo_filas(bigquery_dw: BigQueryResource):
    n = _consulta_escalar(bigquery_dw, "SELECT COUNT(*) FROM `{p}.{dwh}.fact_listings`")
    return AssetCheckResult(passed=n >= 1000, metadata={"filas": int(n)})


@asset_check(asset=fact_listings, description="Todos los precios deben ser mayores que 0")
def fact_precio_en_rango(bigquery_dw: BigQueryResource):
    malos = _consulta_escalar(bigquery_dw, """
        SELECT COUNTIF(precio <= 0) FROM `{p}.{dwh}.fact_listings`""")
    return AssetCheckResult(passed=malos == 0, severity=AssetCheckSeverity.WARN,
                            metadata={"fuera_de_rango": int(malos)})


@asset_check(asset=fact_listings, description="Integridad referencial: todo barrio_key existe en dim_barrio")
def fact_integridad_barrio(bigquery_dw: BigQueryResource):
    huerfanos = _consulta_escalar(bigquery_dw, """
        SELECT COUNT(*) FROM `{p}.{dwh}.fact_listings` f
        LEFT JOIN `{p}.{dwh}.dim_barrio` d USING (barrio_key)
        WHERE d.barrio_key IS NULL""")
    return AssetCheckResult(passed=huerfanos == 0, severity=AssetCheckSeverity.WARN,
                            metadata={"filas_huerfanas": int(huerfanos)})


# ---------------------------------------------------------------------------
# 5. EJECUCIÓN Y PROGRAMACIÓN
# ---------------------------------------------------------------------------
pipeline_job = define_asset_job(
    name=f"pipeline_{TEAM}",
    selection=AssetSelection.all(),
    description="Pipeline ELT completo: extracción -> staging -> modelo en estrella",
)

programacion_diaria = ScheduleDefinition(
    name=f"diario_{TEAM}",
    job=pipeline_job,
    cron_schedule="0 6 * * *",              # todos los días a las 6:00 a. m.
    execution_timezone="America/Bogota",
)

defs = Definitions(
    assets=[raw_listings, raw_reviews, raw_neighbourhoods,
            stg_listings, stg_resenas_por_anuncio,
            dim_barrio, dim_anfitrion, dim_tipo_alojamiento, fact_listings],
    asset_checks=[fact_sin_duplicados, fact_minimo_filas, fact_precio_en_rango, fact_integridad_barrio],
    jobs=[pipeline_job],
    schedules=[programacion_diaria],
    resources={
        "bigquery_dw": BigQueryResource(
            project=EnvVar("GCP_PROJECT"),
            location=LOCATION,
            gcp_credentials=EnvVar("GCP_CREDENTIALS"),   # llave en base64, nunca en el código
        )
    },
)
