from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOGO = spark.conf.get("catalogo", "electrocasa_dev")

def monto_seguro(col_name):
    raw = F.regexp_replace(F.trim(F.col(col_name)), r"[^0-9,.\-]", "")
    norm = F.when(
        raw.rlike(r",\d{1,2}$"),
        F.regexp_replace(F.regexp_replace(raw, r"\.", ""), ",", ".")
    ).otherwise(F.regexp_replace(raw, ",", ""))
    return F.when(norm.rlike(r"^-?\d+(\.\d+)?$"), norm.cast("decimal(12,2)")).otherwise(F.lit(None).cast("decimal(12,2)"))

def fecha_segura(col_name):
    return F.coalesce(
        F.expr(f"try_to_timestamp(trim({col_name}))"),
        F.expr(f"try_to_timestamp(trim({col_name}), 'dd/MM/yyyy')"),
        F.expr(f"try_to_timestamp(trim({col_name}), 'dd/MM/yyyy HH:mm:ss')"),
        F.expr(f"try_to_timestamp(trim({col_name}), 'yyyy-MM-dd')")
    )


@dp.materialized_view(name="silver.sucursales", comment="Maestro limpio de 40 sucursales")
@dp.expect_or_fail("sucursal_id_valido", "sucursal_id IS NOT NULL AND nombre_sucursal IS NOT NULL")
def silver_sucursales():
    df = spark.read.table("bronze.sucursales")
    w = Window.partitionBy("sucursal_id").orderBy(F.col("_ingested_at").desc())
    return (
        df.withColumn("sucursal_id", F.trim(F.col("sucursal_id")))
        .withColumn("nombre_sucursal", F.trim(F.col("nombre_sucursal")))
        .withColumn("distrito", F.initcap(F.trim(F.col("distrito"))))
        .withColumn("ciudad", F.initcap(F.trim(F.col("ciudad"))))
        .withColumn("region", F.initcap(F.trim(F.col("region"))))
        .withColumn("fecha_apertura", F.coalesce(F.expr("try_to_date(trim(fecha_apertura))"), F.expr("try_to_date(trim(fecha_apertura), 'dd/MM/yyyy')")))
        .withColumn("_rn", F.row_number().over(w))
        .filter("_rn = 1")
        .drop("_rn", "_ingested_at", "_source_file")
    )


def limpiar_catalogo_base():
    df = spark.read.table("bronze.catalogo")
    cat_upper = F.upper(F.translate(F.trim(F.col("categoria")), "áéíóúÁÉÍÓÚ", "aeiouAEIOU"))
    cat_std = (
        F.when(cat_upper.like("%BLANCA%"), "LINEA_BLANCA")
        .when(cat_upper.like("%COCINA%"), "COCINA")
        .when(cat_upper.like("%ELECTRON%"), "ELECTRONICA")
        .when(cat_upper.like("%CLIMA%"), "CLIMATIZACION")
        .when(cat_upper.like("%ENTRETEN%"), "ENTRETENIMIENTO")
        .otherwise("OTROS")
    )
    return (
        df.withColumn("producto_id", F.trim(F.col("producto_id")))
        .withColumn("nombre_producto", F.trim(F.col("nombre_producto")))
        .withColumn("categoria", cat_std)
        .withColumn("marca", F.coalesce(F.nullif(F.trim(F.col("marca")), F.lit("")), F.lit("SIN_MARCA")))
        .withColumn("precio_lista", monto_seguro("precio_lista"))
        .withColumn("activo", F.when(F.lower(F.trim(F.col("activo"))).isin("true", "1", "si"), True).otherwise(False))
    )

@dp.materialized_view(name="silver.catalogo", comment="Catálogo limpio y deduplicado")
@dp.expect_or_drop("precio_valido", "producto_id IS NOT NULL AND precio_lista IS NOT NULL AND precio_lista > 0")
def silver_catalogo():
    df = limpiar_catalogo_base().filter("producto_id IS NOT NULL AND precio_lista IS NOT NULL AND precio_lista > 0")
    w = Window.partitionBy("producto_id").orderBy(F.col("_source_file").desc(), F.col("_ingested_at").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn", "_ingested_at", "_source_file")


def limpiar_ventas_base():
    df = spark.read.table("bronze.ventas")
    mp = F.upper(F.trim(F.col("metodo_pago")))

    mp_std = (
        F.when(mp.isin("EFECTIVO", "EFV"), "EFECTIVO")
        .when(mp.isin("TARJETA", "TARJETA DE CREDITO", "TC", "TD"), "TARJETA")
        .when(mp == "YAPE", "YAPE")
        .when(mp == "PLIN", "PLIN")
        .when(mp.isin("TRANSFERENCIA", "TRF"), "TRANSFERENCIA")
        .otherwise("OTRO")
    )
    canal_std = F.when(F.lower(F.trim(F.col("canal"))).like("%ecom%"), "ECOMMERCE").otherwise("TIENDA_FISICA")
    return (
        df.withColumn("venta_id", F.trim(F.col("venta_id")))
        .withColumn("sucursal_id", F.nullif(F.trim(F.col("sucursal_id")), F.lit("")))
        .withColumn("producto_id", F.trim(F.col("producto_id")))
        .withColumn("vendedor_id", F.nullif(F.trim(F.col("vendedor_id")), F.lit("")))
        .withColumn("cantidad", F.when(F.trim(F.col("cantidad")).rlike(r"^-?\d+$"), F.trim(F.col("cantidad")).cast("int")).otherwise(F.lit(None).cast("int")))
        .withColumn("monto_total", monto_seguro("monto_total"))
        .withColumn("metodo_pago", mp_std)
        .withColumn("fecha_venta", fecha_segura("fecha_venta"))
        .withColumn("canal", canal_std)
    )

@dp.materialized_view(name="silver.ventas", comment="Ventas limpias y deduplicadas")
@dp.expect_or_drop("monto_positivo", "venta_id IS NOT NULL AND sucursal_id IS NOT NULL AND cantidad > 0 AND monto_total > 0 AND fecha_venta IS NOT NULL")
def silver_ventas():

    df = limpiar_ventas_base()
    w = Window.partitionBy("venta_id").orderBy(F.col("_ingested_at").asc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn", "_ingested_at", "_source_file")


@dp.temporary_view(name="vw_devoluciones_enriquecidas")
def vw_devoluciones_enriquecidas():
    dev = (
        spark.read.table("bronze.devoluciones")
        .withColumn("devolucion_id", F.trim(F.col("devolucion_id")))
        .withColumn("venta_id", F.nullif(F.trim(F.col("venta_id")), F.lit("")))
        .withColumn("sucursal_id", F.trim(F.col("sucursal_id")))
        .withColumn("producto_id", F.trim(F.col("producto_id")))
        .withColumn("motivo", F.coalesce(F.nullif(F.trim(F.col("motivo")), F.lit("")), F.lit("NO_ESPECIFICADO")))
        .withColumn("monto_reembolso", monto_seguro("monto_reembolso"))
        .withColumn("fecha_devolucion", fecha_segura("fecha_devolucion"))
    )
    ventas = dp.read("silver.ventas").select(F.col("venta_id").alias("v_id"), F.col("monto_total").alias("monto_venta_origen"))
    return dev.join(ventas, dev.venta_id == ventas.v_id, "left").drop("v_id")

@dp.materialized_view(name="silver.devoluciones", comment="Devoluciones validadas contra venta origen")
@dp.expect_or_drop("reembolso_valido", "devolucion_id IS NOT NULL AND venta_id IS NOT NULL AND monto_venta_origen IS NOT NULL AND monto_reembolso >= 0 AND monto_reembolso <= monto_venta_origen AND fecha_devolucion IS NOT NULL")
def silver_devoluciones():
    df = dp.read("vw_devoluciones_enriquecidas")
    w = Window.partitionBy("devolucion_id").orderBy(F.col("_ingested_at").asc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn", "_ingested_at", "_source_file")

def limpiar_resenas_base():
    df = spark.read.table("bronze.resenas")
    return (
        df.withColumn("resena_id", F.trim(F.col("resena_id")))
        .withColumn("producto_id", F.trim(F.col("producto_id")))
        .withColumn("cliente_id", F.nullif(F.trim(F.col("cliente_id")), F.lit("")))
        .withColumn("comentario", F.nullif(F.trim(F.col("comentario")), F.lit("")))
        .withColumn("tags", F.coalesce(F.col("tags"), F.array().cast("array<string>")))
        .withColumn("fecha_resena", fecha_segura("fecha_resena"))
        .withColumn("metadata", F.expr("try_parse_json(metadata)"))
    )

@dp.materialized_view(name="silver.resenas", comment="Reseñas limpias con metadata como VARIANT")
@dp.expect_or_drop("calificacion_en_escala", "resena_id IS NOT NULL AND calificacion BETWEEN 1 AND 5 AND fecha_resena IS NOT NULL")
def silver_resenas():
    df = limpiar_resenas_base()
    w = Window.partitionBy("resena_id").orderBy(F.col("_ingested_at").asc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn", "_ingested_at", "_source_file")


def limpiar_tracking_base():
    df = spark.read.table("bronze.tracking_envios")
    est = F.lower(F.translate(F.trim(F.col("estado_entrega")), "áéíóúÁÉÍÓÚ", "aeiouAEIOU"))
    est_std = (
        F.when(est.like("%pend%"), "pendiente")
        .when(est.like("%transit%") | est.like("%camino%") | est.like("%ruta%"), "en_transito")
        .when(est.like("%entreg%"), "entregado")
        .when(est.like("%devol%") | est.like("%devuel%"), "devuelto")
        .otherwise("desconocido")
    )
    cour = F.lower(F.trim(F.col("courier")))
    courier_std = (
        F.when(cour.like("%olva%"), "Olva Courier")
        .when(cour.like("%shalom%"), "Shalom")
        .when(cour.like("%servientrega%"), "Servientrega")
        .otherwise(F.initcap(F.trim(F.col("courier"))))
    )
    return (
        df.withColumn("tracking_id", F.trim(F.col("tracking_id")))
        .withColumn("venta_id", F.trim(F.col("venta_id")))
        .withColumn("courier", courier_std)
        .withColumn("estado_entrega", est_std)
        .withColumn("sucursal_origen", F.trim(F.col("sucursal_origen")))
    )

@dp.materialized_view(name="silver.tracking_envios", comment="Eventos de despacho estandarizados y sin mensajes reprocesados")
@dp.expect_or_drop("estado_conocido", "evento_id IS NOT NULL AND estado_entrega IN ('pendiente', 'en_transito', 'entregado', 'devuelto')")
def silver_tracking():
    df = limpiar_tracking_base()

    w = Window.partitionBy("tracking_id", "estado_entrega").orderBy(F.col("fecha_modificacion").asc(), F.col("evento_id").asc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


@dp.temporary_view(name="vw_empleados_limpios")
@dp.expect_or_drop("evento_ubicable", "id_empleado IS NOT NULL AND fecha_evento IS NOT NULL")
def vw_empleados_limpios():
    df = spark.readStream.table("bronze.empleados")
    ev = F.upper(F.translate(F.trim(F.col("tipo_evento")), "áéíóúÁÉÍÓÚ", "aeiouAEIOU"))
    ev_std = (
        F.when(ev.like("%ALTA%") | ev.like("%INGRESO%"), "ALTA")
        .when(ev.like("%TRANSF%") | ev.like("%TRASLADO%"), "TRANSFERENCIA")
        .when(ev.like("%SUELDO%") | ev.like("%SALARIO%") | ev.like("%AUMENTO%"), "CAMBIO_SUELDO")
        .when(ev.like("%BAJA%") | ev.like("%CESE%") | ev.like("%RETIRO%"), "BAJA")
        .otherwise("OTRO")
    )
    sal = monto_seguro("salario")
    sal_norm = F.when(sal > 50000, (sal / 100).cast("decimal(12,2)")).otherwise(sal)
    f_ev = F.coalesce(F.expr("try_to_date(trim(fecha_evento))"), F.expr("try_to_date(trim(fecha_evento), 'dd/MM/yyyy')"))
    f_reg = fecha_segura("fecha_registro")

    return (
        df.withColumn("id_empleado", F.nullif(F.trim(F.col("id_empleado")), F.lit("")))
        .withColumn("nombre", F.initcap(F.trim(F.col("nombre"))))
        .withColumn("dni", F.nullif(F.trim(F.col("dni")), F.lit("")))
        .withColumn("email", F.lower(F.trim(F.col("email"))))
        .withColumn("salario", sal_norm)
        .withColumn("sucursal_id", F.trim(F.col("sucursal_id")))
        .withColumn("cargo", F.initcap(F.trim(F.col("cargo"))))
        .withColumn("tipo_evento", ev_std)
        .withColumn("fecha_evento", f_ev)
        .withColumn("fecha_registro", F.coalesce(f_reg, f_ev.cast("timestamp")))
        .withColumn("_seq_orden", F.struct(F.col("fecha_evento"), F.col("fecha_registro")))
        .select("id_empleado", "nombre", "dni", "email", "salario", "sucursal_id", "cargo", "tipo_evento", "fecha_evento", "fecha_registro", "_seq_orden")
    )

dp.create_streaming_table(
    name="silver.empleados",
    comment="Historia SCD Type 2 de empleados con enmascaramiento de DNI y Salario",
    schema=f"""
        id_empleado STRING NOT NULL,
        nombre STRING,
        dni STRING MASK {CATALOGO}.default.mask_dni,
        email STRING,
        salario DECIMAL(12,2) MASK {CATALOGO}.default.mask_salario,
        sucursal_id STRING,
        cargo STRING,
        tipo_evento STRING,
        fecha_evento DATE,
        fecha_registro TIMESTAMP,
        __START_AT STRUCT<fecha_evento: DATE, fecha_registro: TIMESTAMP>,
        __END_AT STRUCT<fecha_evento: DATE, fecha_registro: TIMESTAMP>
    """
)

dp.create_auto_cdc_flow(
    target="silver.empleados",
    source="vw_empleados_limpios",
    keys=["id_empleado"],
    sequence_by=F.col("_seq_orden"),
    stored_as_scd_type=2,
    except_column_list=["_seq_orden"]
)


@dp.materialized_view(name="silver.cuarentena", comment="Registro auditable de filas rechazadas por reglas de calidad")
def silver_cuarentena():
    q_ventas = (
        limpiar_ventas_base()
        .filter("venta_id IS NULL OR sucursal_id IS NULL OR cantidad IS NULL OR cantidad <= 0 OR monto_total IS NULL OR monto_total <= 0 OR fecha_venta IS NULL")
        .select(
            F.lit("ventas").alias("fuente"),
            F.lit("monto_positivo").alias("regla_fallida"),
            F.lit("ID/sucursal nulo, cantidad <= 0, monto <= 0 o fecha invalida").alias("motivo"),
            F.to_json(F.struct("venta_id", "sucursal_id", "producto_id", "cantidad", "monto_total", "fecha_venta")).alias("registro_crudo"),
            F.col("_source_file").alias("archivo_lote"),
            F.current_timestamp().alias("fecha_procesamiento")
        )
    )
    q_catalogo = (
        limpiar_catalogo_base()
        .filter("producto_id IS NULL OR precio_lista IS NULL OR precio_lista <= 0")
        .select(
            F.lit("catalogo").alias("fuente"),
            F.lit("precio_valido").alias("regla_fallida"),
            F.lit("producto_id nulo o precio_lista <= 0").alias("motivo"),
            F.to_json(F.struct("producto_id", "nombre_producto", "categoria", "precio_lista")).alias("registro_crudo"),
            F.col("_source_file").alias("archivo_lote"),
            F.current_timestamp().alias("fecha_procesamiento")
        )
    )
    q_empleados = (
        spark.read.table("bronze.empleados")
        .withColumn("f_ev", F.coalesce(F.expr("try_to_date(trim(fecha_evento))"), F.expr("try_to_date(trim(fecha_evento), 'dd/MM/yyyy')")))
        .filter("trim(id_empleado) IS NULL OR trim(id_empleado) = '' OR f_ev IS NULL")
        .select(
            F.lit("empleados").alias("fuente"),
            F.lit("evento_ubicable").alias("regla_fallida"),
            F.lit("id_empleado o fecha_evento nulo/invalido").alias("motivo"),
            F.to_json(F.struct("id_empleado", "nombre", "tipo_evento", "fecha_evento", "fecha_registro")).alias("registro_crudo"),
            F.col("_source_file").alias("archivo_lote"),
            F.current_timestamp().alias("fecha_procesamiento")
        )
    )
    q_resenas = (
        limpiar_resenas_base()
        .filter("resena_id IS NULL OR calificacion IS NULL OR calificacion NOT BETWEEN 1 AND 5 OR fecha_resena IS NULL")
        .select(
            F.lit("resenas").alias("fuente"),
            F.lit("calificacion_en_escala").alias("regla_fallida"),
            F.lit("calificacion fuera de rango 1-5 o fecha invalida").alias("motivo"),
            F.to_json(F.struct("resena_id", "producto_id", "calificacion", "fecha_resena")).alias("registro_crudo"),
            F.col("_source_file").alias("archivo_lote"),
            F.current_timestamp().alias("fecha_procesamiento")
        )
    )
    q_devoluciones = (
        dp.read("vw_devoluciones_enriquecidas")
        .filter("devolucion_id IS NULL OR venta_id IS NULL OR monto_venta_origen IS NULL OR monto_reembolso IS NULL OR monto_reembolso < 0 OR monto_reembolso > monto_venta_origen OR fecha_devolucion IS NULL")
        .select(
            F.lit("devoluciones").alias("fuente"),
            F.lit("reembolso_valido").alias("regla_fallida"),
            F.lit("venta inexistente, monto_reembolso < 0 o mayor a venta origen").alias("motivo"),
            F.to_json(F.struct("devolucion_id", "venta_id", "monto_reembolso", "monto_venta_origen", "fecha_devolucion")).alias("registro_crudo"),
            F.col("_source_file").alias("archivo_lote"),
            F.current_timestamp().alias("fecha_procesamiento")
        )
    )
    q_tracking = (
        limpiar_tracking_base()
        .filter("evento_id IS NULL OR estado_entrega NOT IN ('pendiente', 'en_transito', 'entregado', 'devuelto')")
        .select(
            F.lit("tracking_envios").alias("fuente"),
            F.lit("estado_conocido").alias("regla_fallida"),
            F.lit("estado_entrega fuera del conjunto cerrado").alias("motivo"),
            F.to_json(F.struct("evento_id", "tracking_id", "venta_id", "estado_entrega")).alias("registro_crudo"),
            F.lit("Azure SQL dbo.TrackingEnvios").alias("archivo_lote"),
            F.current_timestamp().alias("fecha_procesamiento")
        )
    )
    return (
        q_ventas.unionByName(q_catalogo)
        .unionByName(q_empleados)
        .unionByName(q_resenas)
        .unionByName(q_devoluciones)
        .unionByName(q_tracking)
    )