# ElectroCasa Lakehouse

Plataforma de datos sobre Databricks (Unity Catalog) para ElectroCasa, una cadena de 40 sucursales de electrodomésticos en Perú. Integra 6 fuentes y una tabla de referencia en un modelo Medallion (Bronze → Silver → Gold) que responde 6 preguntas de negocio para el período **marzo–octubre 2026**.

**Stack:** Lakeflow Spark Declarative Pipelines · Auto Loader · COPY INTO · Lakeflow Connect · Lakeflow Jobs · Declarative Automation Bundles · Unity Catalog

---

## 1. Arquitectura

```text
 vol_landing (CSV/JSON)                         Azure SQL (dbo.TrackingEnvios)
   │                    │                                │
 COPY INTO          Auto Loader                  Lakeflow Connect (query-based)
 catalogo,          ventas, empleados,           cursor: fecha_modificacion
 sucursales         resenas, devoluciones        PK: evento_id
   │                    │                                │
   └──────────► BRONZE (7 tablas + _ingested_at, _source_file) ◄┘
                          │
                SILVER  limpieza · estandarización · dedup · expectativas
                        SCD2 empleados (AUTO CDC) · MASK dni/salario
                        silver.cuarentena (rechazos consultables)
                          │
                GOLD    6 materialized views (una por pregunta)
                        + empleados_vigentes (MASK) + resenas_negativas_origen (VARIANT)
                          │
                VALIDACIÓN  conteos + rechazos + falla si Gold vacío
```

### Orden del job `electrocasa_orquestacion`

| # | Tarea | Tipo | Depende de |
|---|---|---|---|
| 1 | `tarea_1_copy_into` | Notebook (COPY INTO) | — |
| 2 | `tarea_2_ingest_tracking` | Pipeline (Lakeflow Connect) | — |
| 3 | `tarea_3_pipeline_medallion` | Pipeline (Bronze/Silver/Gold) | 1 y 2 |
| 4 | `tarea_4_validacion` | Notebook | 3 |

El conector de Tracking y el pipeline principal corren en secuencia, porque Free Edition permite solo una pipeline activa por tipo.

| Configuración | Valor | Por qué |
|---|---|---|
| Trigger | **Llegada de archivo** en `/Volumes/<catalogo>/default/vol_landing/` | Los lotes llegan en fechas irregulares (mayo, agosto, octubre). Un horario fijo correría en vacío casi siempre. |
| Reintentos | 2 en ingesta, 1 en el pipeline, 0 en validación | Las fallas de ingesta suelen ser transitorias (red, SQL). Si la validación falla es porque falta un dato, y reintentar no lo arregla. |
| Alertas | Correo al fallar y al terminar bien (`alert_email`) | Aviso inmediato si un lote no se procesó. |
| Concurrencia | `max_concurrent_runs: 1` | Evita dos corridas de la misma pipeline a la vez (límite de Free Edition). |

> En el target `dev` (`mode: development`) el bundle deja el trigger en pausa, por eso los lotes se corrieron con `bundle run`.

---

## 2. Estructura del repositorio

```text
├── databricks.yml                  # Bundle: targets dev / prod
├── resources/
│   ├── electrocasa_pipeline.yml    # Pipeline Medallion
│   └── electrocasa_job.yml         # Job de 4 tareas
├── src/electrocasa/
│   ├── transformations/
│   │   ├── 01_bronze.py            # Auto Loader
│   │   ├── 02_silver.py            # Limpieza, expectativas, SCD2, cuarentena
│   │   └── 03_gold.py              # 6 preguntas de negocio + masking + VARIANT
│   └── utils/                      # Consultas de verificación y notas de las funciones de limpieza
├── notebooks/
│   ├── 00_setup.ipynb              # Catálogo, schemas, volume, máscaras, GRANT/REVOKE
│   ├── 01_copy_into_bronze.ipynb   # COPY INTO idempotente
│   └── 02_validacion.ipynb         # Conteos, rechazos, falla si Gold vacío
├── contratos/                      # 7 contratos de datos (YAML)
└── img/                            # Evidencias de ejecución
```

---

## 3. Despliegue

1. Crear los grupos `electrocasa_ingenieria`, `electrocasa_analistas` y `electrocasa_auditoria` en **Settings > Identity and access > Groups**, y agregarse a `electrocasa_ingenieria`.
2. Ejecutar `notebooks/00_setup.ipynb` con `catalogo = electrocasa_dev` (y luego `electrocasa_prod`).
3. Crear la Connection de Unity Catalog a la Azure SQL (la contraseña se guarda en la Connection, no en el código) y el pipeline de ingesta de Tracking. Poner su ID en `tracking_pipeline_id` de `databricks.yml`.
4. Desplegar y correr el bundle:

```bash
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run electrocasa_orquestacion -t dev
```

5. Subir los archivos de cada lote a `/Volumes/electrocasa_dev/default/vol_landing/<fuente>/` y correr el job. Repetir con los lotes 2 y 3.

| Target | Catálogo | Modo |
|---|---|---|
| `dev` | `electrocasa_dev` | development |
| `prod` | `electrocasa_prod` | production |

**Evidencia de despliegue** (`databricks bundle deploy -t dev`):

![bundle deploy](img/bundle_deploy.png)

---

## 4. Ingesta

| Fuente | Método | Justificación |
|---|---|---|
| Ventas, Devoluciones, Empleados | Auto Loader (Streaming Table) | Llegan por lotes incrementales. Auto Loader procesa solo los archivos nuevos y el pipeline gestiona el checkpoint. |
| Reseñas | Auto Loader (JSON Lines) | Llega por lotes. Se lee con `tags` como `array<string>`, `respuestas` como `array<struct<autor,texto,fecha>>` y `metadata` como texto (en Silver pasa a `VARIANT`). |
| Catálogo | COPY INTO | Llega como snapshots completos y poco frecuentes (2). COPY INTO es idempotente: un archivo ya cargado no se vuelve a cargar. |
| Sucursales | COPY INTO | Es un archivo único de referencia. La carga es idempotente. |
| Tracking | Lakeflow Connect query-based | Obligatorio. Cursor `fecha_modificacion`, PK `evento_id` y solo las columnas necesarias. Destino: `bronze.tracking_envios`. |

- Las fuentes CSV usan un schema explícito (sin `inferSchema`).
- Las columnas de auditoría son `_ingested_at` (momento de ingesta) y `_source_file` (tomada de `_metadata.file_name`, nunca de `input_file_name()`).
- El **identificador de lote** viaja en el nombre del archivo (`ventas_lote_02_2026-08-31.csv`, `catalogo_productos_2026-08-01.json`), así que `_source_file` identifica el archivo y el lote a la vez.
- Bronze no aplica lógica de negocio ni particiona por fecha de negocio: guarda todo como texto, tal como llega.

### Corridas del conector de Tracking

| Corrida | Filas leídas | Resultado |
|---|---|---|
| 1 (carga inicial) | 15 139 | Trae todos los eventos |
| 2 (sin cambios) | 0 | No recarga nada: la ingesta es incremental por el cursor |

`bronze.tracking_envios` se mantiene en **15 139 filas** en las corridas de los lotes 1, 2 y 3: el conector no vuelve a cargar los eventos.

**Evidencia:** `DESCRIBE HISTORY electrocasa_dev.bronze.tracking_envios`

| Versión | Timestamp (UTC) | Operación | Métricas |
|---|---|---|---|
| 0 | 2026-11-24 03:21:34 | CREATE TABLE | — |
| 1 | 2026-11-24 03:21:35 | DLT SETUP | pipeline `c2ea1eb1-…` |
| 2 | 2026-11-24 03:21:57 | **MERGE** (`evento_id <=> evento_id`) | `numSourceRows = 15139`, `numTargetRowsInserted = 15139`, `numTargetRowsUpdated = 0` |

- La primera corrida deja un `MERGE` con las 15 139 filas.
- Las corridas siguientes del conector (una por cada corrida del job) **no agregan ninguna versión**: no hay filas con `fecha_modificacion` mayor al último valor leído.
- Eso demuestra que la ingesta es incremental por el cursor y no una recarga completa.

---

## 5. Silver: limpieza y calidad

**Normalizaciones:**

- **Fechas:** pasan a `TIMESTAMP`/`DATE`. Se aceptan los formatos ISO, `dd/MM/yyyy` y `dd/MM/yyyy HH:mm:ss`.
- **Montos:** coma decimal y punto de miles (`1.234,50`), y precios con moneda, pasan a `decimal(12,2)`.
- **Conjuntos cerrados:**

| Campo | Variantes en la fuente | Valores en Silver |
|---|---|---|
| `metodo_pago` | 17 (`EFV`, `TC`, `td`, `Tarjeta de credito`, `TRF`, `Yape`, `PLIN`…) | `EFECTIVO`, `TARJETA`, `YAPE`, `PLIN`, `TRANSFERENCIA` |
| `categoria` | 17 (`Línea blanca`, `linea_blanca`, `CLIMATIZACION`…) | `LINEA_BLANCA`, `COCINA`, `ELECTRONICA`, `CLIMATIZACION`, `ENTRETENIMIENTO` |
| `tipo_evento` | 13 (`alta`, `Alta`, `Cambio Salario`, `cese`…) | `ALTA`, `TRANSFERENCIA`, `CAMBIO_SUELDO`, `BAJA` (`cese` → `BAJA`) |
| `estado_entrega` | `En camino`, `EN_CAMINO`, `en_transito`… | `pendiente`, `en_transito`, `entregado`, `devuelto` |
| `courier` | `Olva`, `Olva Courier`, casing variable | `Olva Courier`, `Shalom`, `Servientrega` |
| `canal` | `tienda_fisica`, `ecommerce` | `TIENDA_FISICA`, `ECOMMERCE` |

Verificación de `metodo_pago`: las 17 variantes quedan en exactamente 5 valores. La suma (3 488 + 3 468 + 3 610 + 3 506 + 3 416 = **17 488**) es igual al total de `silver.ventas`, así que ninguna venta quedó como "OTRO".

![metodo_pago en Silver](img/silver_metodo_pago.png)
- **Casing:** `ciudad`, `distrito` y `region` de Sucursales.
- **Deduplicación:** `row_number()` por la llave. Las reingestas no duplican ventas, devoluciones ni reseñas. Los productos repetidos del catálogo quedan una sola vez, con la versión del último snapshot.

### Expectativas y política

| Fuente | Regla | Política | Por qué |
|---|---|---|---|
| Sucursales | `sucursal_id_valido` | **fail** | Es la dimensión base de todas las preguntas. Si falta, nada cuadra. |
| Ventas | `monto_positivo` (monto > 0, cantidad > 0, ids y fecha válidos) | drop | Una venta inválida distorsiona las preguntas 1, 2 y 4. Se descarta y queda en cuarentena. |
| Devoluciones | `reembolso_valido` (0 ≤ reembolso ≤ monto de la venta, venta existente) | drop | Se valida en la vista `vw_devoluciones_enriquecidas`, que hace el join con ventas. |
| Reseñas | `calificacion_en_escala` (1–5) | drop | El enunciado pide excluirlas del numerador y del denominador. |
| Catálogo | `precio_valido` (> 0) | drop | Un precio inválido no sirve como maestro. |
| Empleados | `evento_ubicable` (id y fecha no nulos) | drop | Sin ellos el evento no se puede ubicar en el SCD2. |
| Tracking | `estado_conocido` (pendiente, en_transito, entregado, devuelto) | drop | Un estado desconocido no aporta a la pregunta 6. |

Todos los rechazos van a **`silver.cuarentena`** con `fuente`, `regla_fallida`, `motivo`, `registro_crudo` (JSON), `archivo_lote` y `fecha_procesamiento`.

- **La expectativa es la que descarta.** Las tablas de Silver no aplican un filtro previo con la misma condición, así que las métricas de calidad del pipeline registran cuántas filas descartó cada regla.
- **Excepción: Catálogo.** Primero se filtran los precios inválidos y después se deduplica. Así, si un producto del snapshot de marzo aparece dos veces y una de las versiones tiene precio inválido, se conserva la válida.

### SCD Type 2 de Empleados

- Se implementa con `dp.create_auto_cdc_flow(stored_as_scd_type=2)` y `keys=["id_empleado"]`.
- `sequence_by = struct(fecha_evento, fecha_registro)`, tomado de la fuente y no de `current_timestamp()`:
  - `fecha_evento` ordena la historia.
  - `fecha_registro` desempata dos eventos del mismo día.
- Los eventos tardíos que llegan en un lote posterior se ubican en su lugar de la historia.
- Las versiones quedan con `__START_AT` y `__END_AT`. Gold busca la versión vigente a una fecha (preguntas 3 y 4).

### Decisiones propias

| Tema | Decisión |
|---|---|
| Devoluciones con venta inexistente | Se descartan y van a cuarentena (regla `reembolso_valido`). |
| Ventas o reseñas con producto fuera del catálogo | Se conservan en Silver y se excluyen en Gold con un inner join contra el catálogo (preguntas 2 y 5). |
| Tracking con venta inexistente | Se conserva. La pregunta 6 se mide por envío, no por venta. |
| Mensajes reprocesados de Tracking | Se deduplica por `(tracking_id, estado_entrega)` y se conserva el primer registro por `fecha_modificacion`. El reproceso llega con otro `evento_id` y otra `fecha_modificacion`. |
| Mismo `dni` en dos empleados | Se conservan los dos. La llave es `id_empleado`, no el `dni`. |
| Salarios fuera de rango | Un salario > 50 000 se toma como error de escala y se divide entre 100. En la fuente, los salarios normales van de S/ 1 201 a S/ 10 387 (gerentes) y los 18 fuera de rango van de 170 908 a 310 523, es decir, el mismo patrón con dos decimales de más. |
| Productos descontinuados (`activo=false`) | Se conservan marcados. Siguen teniendo ventas y reseñas históricas. |
| Máscara | `dni` → `***`, `salario` → `NULL`, para quien no pertenece a `electrocasa_ingenieria`. |
| Modo del conector | **SCD Type 1** (MERGE por `evento_id`). Como la llave es `evento_id` y no `tracking_id`, cada cambio de estado queda como una fila propia y no se pierde historia. Además, si un evento se corrige en la fuente, el MERGE lo actualiza en vez de duplicarlo. Los mensajes reprocesados traen otro `evento_id`, así que se eliminan en Silver. |

---

## 6. Conteos por lote

### Bronze y Silver

| Tabla | Lote 1 | Lote 2 | Lote 3 |
|---|---|---|---|
| `bronze.ventas` | 6 783 | 13 622 | 18 240 |
| `bronze.catalogo` | 2 305 | 4 705 | 4 705 |
| `bronze.empleados` | 2 257 | 3 524 | 4 448 |
| `bronze.resenas` | 5 292 | 9 280 | 9 280 |
| `bronze.devoluciones` | 1 387 | 3 091 | 4 217 |
| `bronze.sucursales` | 40 | 40 | 40 |
| `bronze.tracking_envios` | 15 139 | 15 139 | 15 139 |
| `silver.ventas` | 6 604 | 13 118 | 17 488 |
| `silver.catalogo` | 2 238 | 2 393 | 2 393 |
| `silver.empleados` (versiones SCD2) | 2 239 | 3 495 | 4 410 |
| ↳ versiones cerradas (`__END_AT` no nulo) | 1 019 | 2 081 | 2 911 |
| `silver.resenas` | 5 226 | 9 097 | 9 097 |
| `silver.devoluciones` | 1 301 | 2 877 | 3 908 |
| `silver.sucursales` | 40 | 40 | 40 |
| `silver.tracking_envios` | 15 139 | 15 139 | 15 139 → **15 049** (*) |
| `silver.cuarentena` | 392 | 796 | 1 004 |

(*) Después de corregir la deduplicación de Tracking, Silver elimina **90 mensajes reprocesados** (15 139 − 15 049).

### Gold

| Tabla | Lote 1 | Lote 2 | Lote 3 |
|---|---|---|---|
| `gold.ventas_netas_sucursal_mes` | 120 | 240 | 320 |
| `gold.top_productos` | 20 | 20 | 20 |
| `gold.dotacion_mensual` | 320 | 320 | 320 |
| `gold.ventas_vendedor` | 2 011 | 4 081 | 5 505 |
| `gold.resenas_negativas` | 25 | 40 | 40 |
| `gold.despachos_courier` | 4 | 4 | 3 (*) |
| `gold.empleados_vigentes` | — | — | 1 138 |
| `gold.resenas_negativas_origen` | — | — | 24 |

(*) 3 couriers después de estandarizar "Olva" / "Olva Courier". Las dos últimas tablas se agregaron en la corrida final; la validación también confirma **0 estados duplicados** en `silver.tracking_envios`.

Evidencia (salida de `tarea_4_validacion`):

![Validación lote 1](img/validacion_lote1.png)
![Validación lote 2](img/validacion_lote2.png)
![Validación lote 3](img/validacion_lote3.png)
![Validación final con Tracking deduplicado](img/validacion_final.png)

**Lectura de los conteos:**

- `gold.ventas_netas_sucursal_mes` crece 120 → 240 → 320 filas (40 sucursales × 3, 6 y 8 meses): cada lote agrega sus meses.
- `silver.catalogo` queda en 2 393 aunque `bronze.catalogo` acumule los dos snapshots: los productos repetidos quedan una sola vez.
- `bronze.catalogo` no cambia en el lote 3 (4 705 = 2 305 del snapshot de marzo + 2 400 del de agosto). Que COPY INTO no vuelva a cargar archivos confirma que es idempotente.
- `bronze.ventas` suma exactamente los 3 archivos (6 783 + 6 839 + 4 618 = 18 240). Silver queda en 17 488 porque descarta las ventas que vuelven a llegar repetidas en el lote siguiente y las que van a cuarentena.
- Bronze coincide exactamente con las filas de los archivos fuente en todas las fuentes. Por ejemplo, empleados: 2 257 + 1 267 + 924 = 4 448, y devoluciones: 1 387 + 1 704 + 1 126 = 4 217.
- Reseñas no cambia en el lote 3 (9 280 / 9 097) porque solo tiene 2 lotes: no se reprocesan archivos.
- `silver.empleados` (4 410) = `bronze.empleados` (4 448) − 38 eventos en cuarentena: cada evento válido genera una versión SCD2.
- Las versiones cerradas del SCD2 aumentan con cada lote (1 019 → 2 081 → 2 911): las transferencias y los eventos tardíos cierran la versión anterior.

![Bronze catálogo por archivo](img/bronze_catalogo_por_archivo.png)
![Bronze ventas por archivo](img/bronze_ventas_por_archivo.png)

### Registros rechazados (`silver.cuarentena`, acumulado)

| Fuente | Regla | Lote 1 | Lote 2 | Lote 3 |
|---|---|---:|---:|---:|
| ventas | `monto_positivo` | 179 | 386 | 520 |
| devoluciones | `reembolso_valido` | 86 | 184 | 249 |
| resenas | `calificacion_en_escala` | 66 | 103 | 103 |
| catalogo | `precio_valido` | 43 | 94 | 94 |
| empleados | `evento_ubicable` | 18 | 29 | 38 |
| tracking_envios | `estado_conocido` | 0 | 0 | 0 |
| **Total** | | **392** | **796** | **1 004** |

Tracking no tiene rechazos: todos sus estados quedaron estandarizados al conjunto cerrado.

---

## 7. Respuestas de negocio (Gold)

| # | Pregunta | Tabla Gold |
|---|---|---|
| 1 | Ventas netas por sucursal y mes | `gold.ventas_netas_sucursal_mes` |
| 2 | Top 10 por unidades y top 10 por tasa de devolución | `gold.top_productos` |
| 3 | Dotación al cierre de cada mes | `gold.dotacion_mensual` |
| 4 | Ventas por vendedor con la sucursal histórica (`SIN_ASIGNAR` si no hay versión vigente) | `gold.ventas_vendedor` |
| 5 | Porcentaje de reseñas negativas por categoría y mes | `gold.resenas_negativas` |
| 6 | Despachos por courier | `gold.despachos_courier` |
| — | Empleados vigentes con máscara (evidencia de masking) | `gold.empleados_vigentes` |
| — | Reto VARIANT: % de reseñas negativas por origen (`metadata:origen`) | `gold.resenas_negativas_origen` |

### Pregunta 1: ventas netas por sucursal y mes

- **El mes con mayores ventas netas** es el de **S14 ElectroCasa Pueblo Libre en abril 2026**: S/ 356 937.92 brutos − S/ 47 431.13 en devoluciones = **S/ 309 506.79 netos**.
- En el top 10 hay sucursales de las 4 regiones (Lima, Centro, Norte, Sur). Las devoluciones restan entre un 7 % y un 20 % de las ventas brutas; S25 Cayma (abril) es la más afectada.

![Pregunta 1](img/gold_p1.png)

### Pregunta 2: top 10 por unidades y top 10 por tasa de devolución

- La tabla une los dos rankings, marcados en la columna `ranking_tipo`.
- Los más vendidos rondan las 25–27 unidades en el período. Entre ellos, **PRD-00125 (Aire acondicionado Bosch Lite 667)** tiene 6 devoluciones en 16 ventas (37.5 %).
- El top por tasa de devolución lo forman productos con pocas ventas y tasa del 100 % (1 devolución en 1 venta). Es una tasa poco representativa porque la muestra es chica.

![Pregunta 2](img/gold_p2.png)

### Pregunta 3: dotación al cierre de cada mes

- Al 31-oct-2026 lideran **S31 Victor Larco (39 empleados activos)**, S38 Huancayo (38) y S10 Chorrillos (37).
- El conteo usa la versión SCD2 vigente en cada fecha de corte, así que cada empleado transferido se cuenta en la sucursal donde estaba ese día.

![Pregunta 3](img/gold_p3.png)

### Pregunta 4: ventas por vendedor (sucursal histórica)

- El mes de mayor facturación de un vendedor es el de **EMP-01086 en S21, abril 2026**: S/ 67 529.68 en 9 ventas.
- `sucursal_historica` es la sucursal que tenía el vendedor en la fecha de cada venta según el SCD2, no la actual. Si no hay una versión vigente en esa fecha, la venta va a `SIN_ASIGNAR`.

![Pregunta 4](img/gold_p4.png)

### Pregunta 5: % de reseñas negativas por categoría y mes

- **Climatización** tiene los peores meses: 21.97 % en mayo y 21.94 % en julio. Le siguen Cocina (21.19 % en mayo) y Línea Blanca (21.07 % en octubre).
- En general, alrededor de 1 de cada 5 reseñas es negativa (calificación 1 o 2). Las reseñas con calificación inválida o con un producto fuera del catálogo no entran en el cálculo.

![Pregunta 5](img/gold_p5.png)

### Pregunta 6: despachos por courier

| Courier | Horas promedio (pendiente → entregado) | Entregados | Devueltos | Estado final | % devueltos |
|---|---:|---:|---:|---:|---:|
| Shalom | **102.43** | 1 399 | 85 | 1 484 | 5.73 |
| Olva Courier | 103.86 | 1 391 | 85 | 1 476 | 5.76 |
| Servientrega | 103.83 | 1 363 | 113 | 1 476 | **7.66** |

![Pregunta 6](img/gold_p6_despachos.png)

**Lectura:**

- Shalom es el más rápido (102.4 h) y el que menos devuelve (5.73 %).
- Los tres couriers tardan casi lo mismo (unas 102–104 h, alrededor de 4,3 días).
- Servientrega devuelve bastante más que los otros dos (7.66 % contra unos 5.7 %).
- "Olva" y "Olva Courier" ahora se cuentan como un solo courier.

### Reto VARIANT: % de reseñas negativas por origen

- `metadata` cambia de estructura según el origen. Por ejemplo, `{"origen":"web","navegador":"safari"}` o `{"origen":"app","app":{"version":"5.0.1","so":"ios"}}`.
- Gold lee el campo con `metadata:origen::string` sobre el `VARIANT`, sin aplanar el JSON.
- **App** concentra el mayor volumen de reseñas (unas 520–585 al mes) y tuvo el pico de negatividad: **21.18 % en mayo**.
- **Tienda** tiene menos reseñas, y en marzo la menor negatividad (10.89 %).

![Reto VARIANT](img/gold_variant_origen.png)

---

## 8. Gobierno y seguridad

### Permisos (`00_setup.ipynb`)

| Grupo | Acceso |
|---|---|
| `electrocasa_ingenieria` | `ALL PRIVILEGES` sobre bronze, silver y gold; lectura y escritura en `vol_landing` |
| `electrocasa_analistas` | `USE SCHEMA` + `SELECT` sobre gold |
| `electrocasa_auditoria` | `USE SCHEMA` + `SELECT` sobre gold; lineage en Catalog Explorer |

### Demostración de REVOKE

```sql
USE CATALOG IDENTIFIER(:catalogo);

GRANT  SELECT ON SCHEMA silver TO `electrocasa_analistas`;
SHOW GRANTS ON SCHEMA silver;   -- antes
REVOKE SELECT ON SCHEMA silver FROM `electrocasa_analistas`;
SHOW GRANTS ON SCHEMA silver;   -- después
```

**1. Antes (después del GRANT indebido):** `electrocasa_analistas` tiene `SELECT` sobre silver, un privilegio que no le corresponde.

| Principal | ActionType | ObjectType | ObjectKey |
|---|---|---|---|
| electrocasa_analistas | SELECT | SCHEMA | electrocasa_dev.silver |
| electrocasa_ingenieria | ALL PRIVILEGES | SCHEMA | electrocasa_dev.silver |

![SHOW GRANTS antes](img/grants_antes.png)

**2. REVOKE ejecutado:**

![REVOKE](img/revoke_ok.png)

**3. Después del REVOKE:** solo queda ingeniería; `electrocasa_analistas` ya no tiene acceso a silver.

| Principal | ActionType | ObjectType | ObjectKey |
|---|---|---|---|
| electrocasa_ingenieria | ALL PRIVILEGES | SCHEMA | electrocasa_dev.silver |

![SHOW GRANTS después](img/show_grants_despues.png)

### Auditoría (`system.access`)

```sql
GRANT USE CATALOG ON CATALOG system TO `electrocasa_auditoria`;
GRANT USE SCHEMA ON SCHEMA system.access TO `electrocasa_auditoria`;
GRANT SELECT ON TABLE system.access.audit TO `electrocasa_auditoria`;
GRANT SELECT ON TABLE system.access.table_lineage TO `electrocasa_auditoria`;
GRANT SELECT ON TABLE system.access.column_lineage TO `electrocasa_auditoria`;
```

**Error obtenido (Free Edition):**

```text
[UNAUTHORIZED_ACCESS] Unauthorized access:
PERMISSION_DENIED: User does not have MANAGE on Catalog 'system'. SQLSTATE: 42501
```

![Error GRANT system](img/grant_system_error.png)

**Alternativa:** como ni el administrador del workspace tiene `MANAGE` sobre `system`, la evidencia de auditoría es la pestaña **Lineage** de Catalog Explorer.

El lineage de `gold.ventas_vendedor` muestra que se calcula desde `silver.ventas` (materialized view) y `silver.empleados` (streaming table SCD2). Muestra también las columnas de cada tabla y el dueño de cada objeto. Es justo la trazabilidad que necesita el grupo de auditoría.

![Lineage](img/lineage.png)

### Masking

- `mask_dni` y `mask_salario` usan `is_account_group_member('electrocasa_ingenieria')`.
- La máscara se declara con `MASK` en el schema de `silver.empleados`, dentro del pipeline.
- El dueño del pipeline es miembro de `electrocasa_ingenieria`, así que las tablas derivadas conservan los valores reales.
- `gold.empleados_vigentes` se deriva de `silver.empleados` y vuelve a declarar `MASK` en su schema. Así ingeniería ve los valores reales, y analistas y auditoría (que tienen `SELECT` sobre gold) ven `***` y `NULL`.

```sql
SELECT id_empleado, dni, salario,
       is_account_group_member('electrocasa_ingenieria') AS soy_ingenieria
FROM electrocasa_dev.gold.empleados_vigentes LIMIT 5;
```

**Resultado:** el usuario que consulta es miembro de `electrocasa_ingenieria` (`soy_ingenieria = true`) y ve el **DNI y el salario reales** en una tabla derivada de Gold. Eso confirma que el pipeline, cuyo dueño está en el grupo, no guardó valores ya enmascarados. Un usuario fuera del grupo vería `dni = ***` y `salario = NULL`.

![mask](img/mask.png)

---

## 9. Monitoreo y compute

```sql
SELECT timestamp, event_type, message
FROM event_log(TABLE(electrocasa_dev.silver.ventas))
WHERE event_type IN ('flow_progress', 'update_progress')
ORDER BY timestamp DESC;
```

**Resultado:** el event log del pipeline registra cada flow con su estado (`PLANNING` → `STARTING` → `RUNNING` → `COMPLETED`) y sus métricas de tiempo. Por ejemplo, `silver.cuarentena` terminó *COMPLETED*, y luego arrancaron `gold.ventas_netas_sucursal_mes` y `gold.top_productos`. Se usó el event log porque es la fuente de monitoreo que se puede consultar directamente sobre las tablas del pipeline.

![event log](img/event_log.png)

**Monitoreo de la corrida del pipeline (UI de Jobs & Pipelines, target `prod`):** la corrida quedó *Completed*, con 0 errores y 0 advertencias. Se ven el grafo Bronze → Silver → Gold, los registros de salida por tabla y las expectativas cumplidas ("1 met").

![Pipeline prod](img/pipeline_prod_run.png)

| Recurso | Compute | Por qué |
|---|---|---|
| Pipeline Medallion | Serverless | Corre por lote, con datos chicos y sin cluster que mantener. |
| Conector de Tracking | Serverless | Es el único modo que admite Lakeflow Connect. |
| Notebooks (COPY INTO y validación) | Serverless (job compute) | Tareas cortas; no se usan clusters all-purpose. |

---

## 10. Retos extra

- **VARIANT en Reseñas:** `metadata` se guarda como `VARIANT` en `silver.resenas` (con `try_parse_json`). Gold lo usa en `gold.resenas_negativas_origen`, que agrupa por `metadata:origen` (app, web y tienda) y por mes.
- **Cuarentena con trazabilidad:** cada rechazo guarda la fuente, la regla, el motivo, el registro crudo, el archivo o lote y la fecha de procesamiento.
  - **Reprocesamiento:** Bronze conserva los datos crudos, y `silver.cuarentena` y las tablas de Silver son materialized views calculadas desde Bronze. Para reprocesar un rechazo, se corrige el dato en origen (un archivo corregido que llega en un lote nuevo) o se ajusta la regla de limpieza. En el siguiente refresh del pipeline, la fila se vuelve a evaluar: si ya cumple, pasa a Silver y sale de la cuarentena, sin tocar a mano ninguna tabla.
  - Consulta para auditar los rechazos de un lote:

    ```sql
    SELECT fuente, regla_fallida, archivo_lote, COUNT(*) AS filas
    FROM electrocasa_dev.silver.cuarentena
    GROUP BY ALL ORDER BY fuente, archivo_lote;
    ```

