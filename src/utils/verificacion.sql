


SELECT _source_file, COUNT(*) AS filas FROM electrocasa_dev.bronze.ventas   GROUP BY 1 ORDER BY 1;
SELECT _source_file, COUNT(*) AS filas FROM electrocasa_dev.bronze.catalogo GROUP BY 1 ORDER BY 1;


SELECT metodo_pago, COUNT(*) FROM electrocasa_dev.silver.ventas           GROUP BY 1 ORDER BY 1;
SELECT categoria,   COUNT(*) FROM electrocasa_dev.silver.catalogo         GROUP BY 1 ORDER BY 1;
SELECT tipo_evento, COUNT(*) FROM electrocasa_dev.silver.empleados        GROUP BY 1 ORDER BY 1;
SELECT courier,     COUNT(*) FROM electrocasa_dev.silver.tracking_envios  GROUP BY 1 ORDER BY 1;


SELECT tracking_id, estado_entrega, COUNT(*) c
FROM electrocasa_dev.silver.tracking_envios
GROUP BY ALL HAVING c > 1;


SELECT fuente, regla_fallida, archivo_lote, COUNT(*) AS filas
FROM electrocasa_dev.silver.cuarentena
GROUP BY ALL ORDER BY fuente, archivo_lote;


DESCRIBE HISTORY electrocasa_dev.bronze.tracking_envios;
