from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ==============================================================================
# PREGUNTA 1: Ventas netas por sucursal y mes (marzo a octubre 2026)
# ==============================================================================
@dp.materialized_view(name="gold.ventas_netas_sucursal_mes", comment="P1: Ventas netas, devoluciones, cantidad y ticket promedio por sucursal y mes")
def gold_p1_ventas_netas():
    ventas_mes = (
        dp.read("silver.ventas")
        .filter("fecha_venta >= '2026-03-01' AND fecha_venta < '2026-11-01'")
        .withColumn("mes", F.date_format("fecha_venta", "yyyy-MM"))
        .groupBy("sucursal_id", "mes")
        .agg(
            F.sum("monto_total").cast("decimal(14,2)").alias("ventas_brutas"),
            F.countDistinct("venta_id").alias("cantidad_ventas")
        )
    )
    dev_mes = (
        dp.read("silver.devoluciones")
        .filter("fecha_devolucion >= '2026-03-01' AND fecha_devolucion < '2026-11-01'")
        .withColumn("mes", F.date_format("fecha_devolucion", "yyyy-MM"))
        .groupBy("sucursal_id", "mes")
        .agg(F.sum("monto_reembolso").cast("decimal(14,2)").alias("devoluciones"))
    )
    suc = dp.read("silver.sucursales").select("sucursal_id", "nombre_sucursal", "ciudad", "region")

    return (
        ventas_mes.join(dev_mes, ["sucursal_id", "mes"], "full_outer")
        .join(suc, "sucursal_id", "left")
        .na.fill({"ventas_brutas": 0, "devoluciones": 0, "cantidad_ventas": 0})
        .withColumn("ventas_netas", (F.col("ventas_brutas") - F.col("devoluciones")).cast("decimal(14,2)"))
        .withColumn(
            "ticket_promedio",
            F.when(F.col("cantidad_ventas") > 0, (F.col("ventas_brutas") / F.col("cantidad_ventas")).cast("decimal(12,2)"))
            .otherwise(F.lit(0).cast("decimal(12,2)"))
        )
        .select("sucursal_id", "nombre_sucursal", "ciudad", "region", "mes", "ventas_brutas", "devoluciones", "ventas_netas", "cantidad_ventas", "ticket_promedio")
        .orderBy("mes", "sucursal_id")
    )

# ==============================================================================
# PREGUNTA 2: Top 10 productos más vendidos y Top 10 mayor tasa de devolución
# ==============================================================================
@dp.materialized_view(name="gold.top_productos", comment="P2: Top 10 productos por unidades vendidas y Top 10 por tasa de devolución")
def gold_p2_productos():
    v_prod = (
        dp.read("silver.ventas")
        .filter("fecha_venta >= '2026-03-01' AND fecha_venta < '2026-11-01'")
        .groupBy("producto_id")
        .agg(
            F.sum("cantidad").alias("unidades_vendidas"),
            F.countDistinct("venta_id").alias("cantidad_ventas")
        )
    )
    d_prod = (
        dp.read("silver.devoluciones")
        .filter("fecha_devolucion >= '2026-03-01' AND fecha_devolucion < '2026-11-01'")
        .groupBy("producto_id")
        .agg(F.countDistinct("devolucion_id").alias("cantidad_devoluciones"))
    )
    cat = dp.read("silver.catalogo").select("producto_id", "nombre_producto", "categoria", "marca")

    metricas = (
        v_prod.join(d_prod, "producto_id", "left")
        .join(cat, "producto_id", "inner")
        .na.fill({"cantidad_devoluciones": 0})
        .withColumn("tasa_devolucion", F.round(F.col("cantidad_devoluciones") / F.col("cantidad_ventas"), 4))
    )

    top_vendidos = (
        metricas.orderBy(F.col("unidades_vendidas").desc())
        .limit(10)
        .withColumn("ranking_tipo", F.lit("TOP_10_MAS_VENDIDOS"))
    )
    top_devueltos = (
        metricas.orderBy(F.col("tasa_devolucion").desc(), F.col("cantidad_devoluciones").desc())
        .limit(10)
        .withColumn("ranking_tipo", F.lit("TOP_10_TASA_DEVOLUCION"))
    )
    return top_vendidos.unionByName(top_devueltos)

# ==============================================================================
# PREGUNTA 3: Dotación de empleados activos al último día de cada mes
# ==============================================================================
@dp.materialized_view(name="gold.dotacion_mensual", comment="P3: Empleados activos por sucursal al cierre de cada mes (marzo a octubre 2026)")
def gold_p3_dotacion():
    cierres = spark.createDataFrame(
        [("2026-03-31",), ("2026-04-30",), ("2026-05-31",), ("2026-06-30",),
         ("2026-07-31",), ("2026-08-31",), ("2026-09-30",), ("2026-10-31",)],
        ["fecha_corte_str"]
    ).withColumn("fecha_corte", F.to_date("fecha_corte_str")).withColumn("mes", F.date_format("fecha_corte", "yyyy-MM"))

    emp = dp.read("silver.empleados")
    # Una versión SCD2 está vigente en fecha_corte si su fecha_evento <= fecha_corte
    # y la siguiente versión (__END_AT.fecha_evento) es nula o > fecha_corte, y el evento no es BAJA
    activos = (
        cierres.join(
            emp,
            (F.col("__START_AT.fecha_evento") <= F.col("fecha_corte")) &
            (F.col("__END_AT.fecha_evento").isNull() | (F.col("__END_AT.fecha_evento") > F.col("fecha_corte"))) &
            (F.col("tipo_evento") != "BAJA"),
            "inner"
        )
        .groupBy("mes", "fecha_corte", "sucursal_id")
        .agg(F.countDistinct("id_empleado").alias("empleados_activos"))
    )
    suc = dp.read("silver.sucursales").select("sucursal_id", "nombre_sucursal", "region")
    return activos.join(suc, "sucursal_id", "left").orderBy("mes", "sucursal_id")

# ==============================================================================
# PREGUNTA 4: Ventas por vendedor con sucursal histórica (Point-in-Time SCD2)
# ==============================================================================
@dp.materialized_view(name="gold.ventas_vendedor", comment="P4: Ventas de tienda física por vendedor y sucursal histórica en la fecha de venta")
def gold_p4_ventas_vendedor():
    ventas_fis = (
        dp.read("silver.ventas")
        .filter("canal = 'TIENDA_FISICA' AND fecha_venta >= '2026-03-01' AND fecha_venta < '2026-11-01'")
        .withColumn("fecha_venta_dia", F.to_date("fecha_venta"))
        .withColumn("mes", F.date_format("fecha_venta", "yyyy-MM"))
    )
    emp = dp.read("silver.empleados").filter("tipo_evento != 'BAJA'")

    joined = ventas_fis.join(
        emp,
        (ventas_fis.vendedor_id == emp.id_empleado) &
        (F.col("__START_AT.fecha_evento") <= F.col("fecha_venta_dia")) &
        (F.col("__END_AT.fecha_evento").isNull() | (F.col("__END_AT.fecha_evento") > F.col("fecha_venta_dia"))),
        "left"
    )

    return (
        joined.withColumn("sucursal_historica", F.coalesce(emp.sucursal_id, F.lit("SIN_ASIGNAR")))
        .groupBy("vendedor_id", "sucursal_historica", "mes")
        .agg(
            F.sum("monto_total").cast("decimal(14,2)").alias("monto_ventas"),
            F.countDistinct("venta_id").alias("cantidad_ventas")
        )
        .orderBy("mes", "sucursal_historica", F.col("monto_ventas").desc())
    )

# ==============================================================================
# PREGUNTA 5: Porcentaje de reseñas negativas (1 o 2) por categoría y mes
# ==============================================================================
@dp.materialized_view(name="gold.resenas_negativas", comment="P5: Porcentaje de reseñas negativas por categoría del catálogo y mes (con detalle de origen VARIANT)")
def gold_p5_resenas():
    res = (
        dp.read("silver.resenas")
        .filter("fecha_resena >= '2026-03-01' AND fecha_resena < '2026-11-01'")
        .withColumn("mes", F.date_format("fecha_resena", "yyyy-MM"))
    )
    # Inner join excluye del numerador y denominador productos que no existen en el catálogo
    cat = dp.read("silver.catalogo").select("producto_id", "categoria")

    return (
        res.join(cat, "producto_id", "inner")
        .groupBy("categoria", "mes")
        .agg(
            F.count("*").alias("total_resenas"),
            F.sum(F.when(F.col("calificacion").isin(1, 2), 1).otherwise(0)).alias("resenas_negativas")
        )
        .withColumn("porcentaje_negativas", F.round((F.col("resenas_negativas") / F.col("total_resenas")) * 100, 2))
        .orderBy("mes", "categoria")
    )

# ==============================================================================
# PREGUNTA 6: Despachos por courier (tiempo promedio a entregado y % devueltos)
# ==============================================================================
@dp.materialized_view(name="gold.despachos_courier", comment="P6: Tiempo promedio de entrega en horas y porcentaje de envíos devueltos por courier")
def gold_p6_despachos():
    trk = dp.read("silver.tracking_envios")

    por_envio = (
        trk.groupBy("courier", "tracking_id")
        .agg(
            F.min(F.when(F.col("estado_entrega") == "pendiente", F.col("fecha_modificacion"))).alias("ts_pendiente"),
            F.min(F.when(F.col("estado_entrega") == "entregado", F.col("fecha_modificacion"))).alias("ts_entregado"),
            F.max(F.when(F.col("estado_entrega") == "devuelto", 1).otherwise(0)).alias("es_devuelto")
        )
        .withColumn("es_entregado", F.when(F.col("ts_entregado").isNotNull(), 1).otherwise(0))
        .withColumn(
            "horas_entrega",
            F.when(
                F.col("ts_pendiente").isNotNull() & F.col("ts_entregado").isNotNull(),
                (F.unix_timestamp("ts_entregado") - F.unix_timestamp("ts_pendiente")) / 3600.0
            )
        )
    )

    return (
        por_envio.groupBy("courier")
        .agg(
            F.round(F.avg(F.when(F.col("es_entregado") == 1, F.col("horas_entrega"))), 2).alias("tiempo_promedio_horas"),
            F.sum("es_entregado").alias("envios_entregados"),
            F.sum("es_devuelto").alias("envios_devueltos"),
            F.sum(F.when((F.col("es_entregado") == 1) | (F.col("es_devuelto") == 1), 1).otherwise(0)).alias("envios_estado_final")
        )
        .withColumn(
            "porcentaje_devueltos",
            F.when(F.col("envios_estado_final") > 0, F.round((F.col("envios_devueltos") / F.col("envios_estado_final")) * 100, 2))
            .otherwise(F.lit(0.0))
        )
        .orderBy("courier")
    )