from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

# El catálogo se inyecta desde la configuración del pipeline (electrocasa_dev / electrocasa_prod)
CATALOGO = spark.conf.get("catalogo", "electrocasa_dev")
LANDING_BASE = f"/Volumes/{CATALOGO}/default/vol_landing"

# 1. VENTAS (CSV con schema explícito)
schema_ventas = StructType([
    StructField("venta_id", StringType(), True),
    StructField("sucursal_id", StringType(), True),
    StructField("producto_id", StringType(), True),
    StructField("vendedor_id", StringType(), True),
    StructField("cantidad", StringType(), True),
    StructField("monto_total", StringType(), True),
    StructField("metodo_pago", StringType(), True),
    StructField("fecha_venta", StringType(), True),
    StructField("canal", StringType(), True),
])

@dp.table(name="bronze.ventas", comment="Ingesta raw de ventas por lotes vía Auto Loader")
def bronze_ventas():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(schema_ventas)
        .load(f"{LANDING_BASE}/ventas/")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_name"))
    )

# 2. EMPLEADOS (CSV con schema explícito)
schema_empleados = StructType([
    StructField("id_empleado", StringType(), True),
    StructField("nombre", StringType(), True),
    StructField("dni", StringType(), True),
    StructField("email", StringType(), True),
    StructField("salario", StringType(), True),
    StructField("sucursal_id", StringType(), True),
    StructField("cargo", StringType(), True),
    StructField("tipo_evento", StringType(), True),
    StructField("fecha_evento", StringType(), True),
    StructField("fecha_registro", StringType(), True),
])

@dp.table(name="bronze.empleados", comment="Ingesta raw de eventos de RR.HH. vía Auto Loader")
def bronze_empleados():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(schema_empleados)
        .load(f"{LANDING_BASE}/empleados/")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_name"))
    )

# 3. DEVOLUCIONES (CSV con schema explícito)
schema_devoluciones = StructType([
    StructField("devolucion_id", StringType(), True),
    StructField("venta_id", StringType(), True),
    StructField("sucursal_id", StringType(), True),
    StructField("producto_id", StringType(), True),
    StructField("motivo", StringType(), True),
    StructField("monto_reembolso", StringType(), True),
    StructField("fecha_devolucion", StringType(), True),
])

@dp.table(name="bronze.devoluciones", comment="Ingesta raw de devoluciones vía Auto Loader")
def bronze_devoluciones():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(schema_devoluciones)
        .load(f"{LANDING_BASE}/devoluciones/")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_name"))
    )

# 4. RESEÑAS (JSON Lines anidado con schema explícito)
schema_resenas = StructType([
    StructField("resena_id", StringType(), True),
    StructField("producto_id", StringType(), True),
    StructField("cliente_id", StringType(), True),
    StructField("calificacion", IntegerType(), True),
    StructField("comentario", StringType(), True),
    StructField("tags", ArrayType(StringType()), True),
    StructField("respuestas", ArrayType(StructType([
        StructField("autor", StringType(), True),
        StructField("texto", StringType(), True),
        StructField("fecha", StringType(), True),
    ])), True),
    StructField("fecha_resena", StringType(), True),
    StructField("metadata", StringType(), True),
])

@dp.table(name="bronze.resenas", comment="Ingesta raw de reseñas JSON Lines vía Auto Loader")
def bronze_resenas():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(schema_resenas)
        .load(f"{LANDING_BASE}/resenas/")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_name"))
    )