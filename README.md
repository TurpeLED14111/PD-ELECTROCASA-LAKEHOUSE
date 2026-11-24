# PD-ELECTROCASA-LAKEHOUSE
Proyecto de Databricks 
Aquí tienes el **`README.md`** completo, profesional y listo para copiar y pegar directamente en la raíz de tu repositorio **`PD-ELECTROCASA-LAKEHOUSE`**. Solo reemplaza los campos marcados con **`[COMPLETAR]`** con tus conteos reales de `tarea_4_validacion` y los resultados del SQL Editor.

```markdown
# 🏠 ElectroCasa Lakehouse — Arquitectura Medallion en Databricks

Implementación end-to-end de una plataforma de datos moderna tipo **Lakehouse** para **ElectroCasa** sobre **Databricks (Unity Catalog)**, diseñada bajo la **Arquitectura Medallion (Bronze $\rightarrow$ Silver $\rightarrow$ Gold)**, orquestada e industrializada como código mediante **Databricks Asset Bundles (DAB)** con entornos aislados (`dev` y `prod`).

---

## 📌 1. Visión General y Arquitectura

El proyecto integra **7 fuentes heterogéneas** (archivos CSV, JSON semiestructurados y base de datos transaccional Azure SQL) bajo un enfoque de **ingesta incremental**, **calidad de datos auditable con cuarentena**, **historial SCD Type 2**, **gobernanza de datos sensibles (PII)** y **vistas analíticas de negocio**.

```text
                       ┌──────────────────────────────────────────┐
                       │          FUENTES DE DATOS (7)            │
                       ├────────────────────┬─────────────────────┤
                       │   vol_landing      │   Azure SQL Server  │
                       │ (CSV / JSON Lotes) │ (dbo.TrackingEnvios)│
                       └─────────┬──────────┴──────────┬──────────┘
                                 │                     │
              ┌──────────────────┴─────────┐           │
              ▼                            ▼           ▼
   ┌────────────────────┐       ┌──────────────────┐  ┌──────────────────────────┐
   │  tarea_1_copy_into │       │   Auto Loader    │  │ tarea_2_ingest_tracking  │
   │    (COPY INTO)     │       │   (01_bronze.py) │  │    (Lakeflow Connect)    │
   │ catalogo,sucursales│       │ ventas,empleados │  │  tracking_envios (CDC)   │
   └─────────┬──────────┘       │resenas,devoluc.  │  └────────────┬─────────────┘
             │                  └────────┬─────────┘               │
             └───────────────────────────┼─────────────────────────┘
                                         ▼
              ┌────────────────────────────────────────────────────┐
              │       CAPA BRONZE (electrocasa_<env>.bronze)       │
              │  7 tablas crudas + metadatos de auditoría          │
              └──────────────────────────┬─────────────────────────┘
                                         │
                                         ▼
              ┌────────────────────────────────────────────────────┐
              │       CAPA SILVER (electrocasa_<env>.silver)       │
              │  • 02_silver.py (Lakeflow Declarative Pipelines)   │
              │  • Limpieza, estandarización y deduplicación       │
              │  • SCD Type 2 (AUTO CDC) + MASK en silver.empleados│
              │  • Tipo VARIANT en silver.resenas                  │
              │  • Expectativas + tabla auditable silver.cuarentena│
              └──────────────────────────┬─────────────────────────┘
                                         │
                                         ▼
              ┌────────────────────────────────────────────────────┐
              │        CAPA GOLD (electrocasa_<env>.gold)          │
              │  • 03_gold.py — 6 Materialized Views de negocio    │
              └──────────────────────────┬─────────────────────────┘
                                         │
                                         ▼
              ┌────────────────────────────────────────────────────┐
              │       VALIDACIÓN AUTOMATIZADA (tarea_4_validacion) │
              │  • Conteos > 0 en 21 tablas, SCD2 y Cuarentena     │
              └────────────────────────────────────────────────────┘

```

---

## 📁 2. Estructura del Repositorio

```text
PD-ELECTROCASA-LAKEHOUSE/
├── databricks.yml                            # Configuración principal de Databricks Asset Bundles (dev / prod)
├── resources/
│   ├── electrocasa_pipeline.yml              # Definición del Lakeflow Declarative Pipeline (Medallion)
│   └── electrocasa_job.yml                   # Definición del Job orquestador de 4 tareas
├── notebooks/
│   ├── 00_setup.ipynb                        # Provisionamiento de catálogos, esquemas, volúmenes y funciones MASK
│   ├── 01_copy_into_bronze.ipynb             # Ingesta incremental idempotente (COPY INTO) de catálogo y sucursales
│   └── 02_validacion.ipynb                   # Pruebas automáticas de calidad, conteos, SCD2 y cuarentena
├── src/
│   └── electrocasa/
│       └── transformations/
│           ├── 01_bronze.py                  # Ingesta incremental con Auto Loader (ventas, empleados, reseñas, devoluciones)
│           ├── 02_silver.py                  # Reglas de limpieza, expectativas, SCD Type 2, VARIANT y cuarentena
│           └── 03_gold.py                    # 6 Vistas Materializadas analíticas de negocio
├── contratos/                                # 7 Contratos de datos en YAML (esquemas, SLA y reglas de calidad)
│   ├── catalogo.yml
│   ├── devoluciones.yml
│   ├── empleados.yml
│   ├── resenas.yml
│   ├── sucursales.yml
│   ├── tracking_envios.yml
│   └── ventas.yml
└── README.md                                 # Documentación técnica y de negocio del proyecto

```

---

## ⚙️ 3. Decisiones Técnicas y Arquitectura

| Componente | Tecnología Aplicada | Justificación Técnica |
| --- | --- | --- |
| **Ingesta de Archivos Frecuentes** | **Auto Loader (`cloudFiles`)** en `01_bronze.py` | Procesa incrementalmente solo los archivos nuevos en `vol_landing` (Lotes 1, 2 y 3) manteniendo estado mediante *checkpoints* internos sin reprocesar lotes previos. |
| **Ingesta de Maestros/Catálogos** | **`COPY INTO`** en `01_copy_into_bronze.ipynb` | Carga idempotente ideal para archivos maestros (`sucursales` y `catalogo`), garantizando que archivos ya ingeridos no se dupliquen en corridas posteriores. |
| **Ingesta Transaccional (Azure SQL)** | **Lakeflow Connect (Change Tracking)** | Captura cambios incrementales desde `dbo.TrackingEnvios` hacia `bronze.tracking_envios` con trazabilidad completa en el historial Delta (`DESCRIBE HISTORY`). |
| **Calidad de Datos y Cuarentena** | **`@dp.expect_or_drop` / `@dp.expect_or_fail` + `silver.cuarentena**` | Bloquea corrupción de datos en la capa Silver (`sucursales` falla si pierde clave; resto descarta inválidos) mientras consolida **todos los registros rechazados** en `silver.cuarentena` con fuente, regla fallida, motivo, registro crudo en JSON y archivo origen. |
| **Validación Cruzada** | **Vista temporal `vw_devoluciones_enriquecidas**` | Valida integridad referencial y lógica de negocio en `silver.devoluciones` contra `silver.ventas` asegurando que la venta exista y `0 <= monto_reembolso <= monto_venta_origen`. |
| **Datos Semiestructurados** | **Tipo `VARIANT` (`try_parse_json`)** | Almacena la columna `metadata` de `silver.resenas` en formato nativo `VARIANT` de Databricks, permitiendo consultas flexibles sobre esquemas cambiantes con alto rendimiento. |
| **Historia de Empleados** | **SCD Type 2 con `dp.create_auto_cdc_flow**` | Mantiene trazabilidad completa de altas, traslados, cambios salariales y bajas en `silver.empleados`, ordenados por la estructura compuesta `(fecha_evento, fecha_registro)` generando intervalos `__START_AT` y `__END_AT`. |
| **Gobernanza y Privacidad (PII)** | **Column Masking en Unity Catalog (`MASK`)** | Protege las columnas `dni` y `salario` en `silver.empleados` mediante las funciones `mask_dni` y `mask_salario` declaradas en el esquema de la tabla streaming. |
| **CI/CD e Infraestructura como Código** | **Databricks Asset Bundles (DAB)** | Versiona y despliega pipeline y job de forma reproducible en `dev` (`electrocasa_dev`) y `prod` (`electrocasa_prod`) con `workspace.root_path` aislado en producción. |

---

## 🔄 4. Orquestación del Job (`electrocasa_orquestacion`)

El Job definido en `resources/electrocasa_job.yml` coordina la ejecución completa en 3 etapas dependientes:

1. **Etapa 1 (En paralelo):**
* `tarea_1_copy_into`: Ejecuta `notebooks/01_copy_into_bronze.ipynb` con parámetro `catalogo`.
* `tarea_2_ingest_tracking`: Dispara el pipeline de Lakeflow Connect hacia `bronze.tracking_envios`.


2. **Etapa 2 (Dependiente de Etapa 1):**
* `tarea_3_pipeline_medallion`: Ejecuta el Lakeflow Declarative Pipeline (`01_bronze.py` $\rightarrow$ `02_silver.py` $\rightarrow$ `03_gold.py`) sobre cómputo Serverless.


3. **Etapa 3 (Dependiente de Etapa 2):**
* `tarea_4_validacion`: Ejecuta `notebooks/02_validacion.ipynb`, verificando que las **21 tablas/vistas** tengan filas (`> 0`), que existan intervalos históricos cerrados (`__END_AT IS NOT NULL`) en `silver.empleados` y desplegando el resumen de `silver.cuarentena`.



---

## 🚀 5. Guía de Despliegue y Ejecución

### Prerrequisitos

1. Ejecutar `notebooks/00_setup.ipynb` configurando el widget `catalogo` en `electrocasa_dev` y luego en `electrocasa_prod` (crea esquemas `bronze`, `silver`, `gold`, volumen `vol_landing` y funciones `mask_dni` y `mask_salario`).
2. Configurar el `tracking_pipeline_id` correspondiente en `databricks.yml`.
3. Cargar los archivos de datos en `/Volumes/<catalogo>/default/vol_landing/` organizados por subcarpeta (`catalogo/`, `sucursales/`, `ventas/`, `empleados/`, `resenas/`, `devoluciones/`).

### Comandos CLI (Databricks Asset Bundles)

```bash
# 1. Ubicarse en la raíz del repositorio
cd /Workspace/Users/rensoturpolb@gmail.com/PD-ELECTROCASA-LAKEHOUSE

# 2. Validar y desplegar en Desarrollo (dev)
databricks bundle validate -t dev
databricks bundle deploy -t dev

# 3. Ejecutar carga incremental (Lote 1, luego Lote 2, luego Lote 3)
databricks bundle run electrocasa_orquestacion -t dev

# 4. Desplegar y ejecutar en Producción (prod)
databricks bundle deploy -t prod
databricks bundle run electrocasa_orquestacion -t prod

```

---

## 📊 6. Evolución por Lotes y Calidad de Datos

### Evolución de Filas por Lote (`electrocasa_dev`)

| Capa / Tabla | Lote 1 (Inicial - Mar 2026) | Lote 2 (Incremental - Ago 2026) | Lote 3 (Cierre y Tardíos - Oct 2026) |
| --- | --- | --- | --- |
| `bronze.ventas` | `[COMPLETAR]` | `[COMPLETAR]` | `[COMPLETAR]` |
| `bronze.catalogo` | `[COMPLETAR]` | `[COMPLETAR]` | `[COMPLETAR]` |
| `bronze.empleados` | `[COMPLETAR]` | `[COMPLETAR]` | `[COMPLETAR]` |
| `bronze.resenas` | `[COMPLETAR]` | `[COMPLETAR]` | `[COMPLETAR]` |
| `bronze.devoluciones` | `[COMPLETAR]` | `[COMPLETAR]` | `[COMPLETAR]` |
| `bronze.tracking_envios` | `[COMPLETAR]` | `[COMPLETAR]` | `[COMPLETAR]` |
| `bronze.sucursales` | `[COMPLETAR]` | `[COMPLETAR]` | `[COMPLETAR]` |
| `silver.ventas` | `[COMPLETAR]` | `[COMPLETAR]` | `[COMPLETAR]` |
| `silver.catalogo` | `[COMPLETAR]` | `[COMPLETAR]` | `[COMPLETAR]` |
| `silver.empleados` (Total / Cerrados `__END_AT`) | `[COMPLETAR]` | `[COMPLETAR]` | `[COMPLETAR]` |
| `silver.resenas` | `[COMPLETAR]` | `[COMPLETAR]` | `[COMPLETAR]` |
| `silver.devoluciones` | `[COMPLETAR]` | `[COMPLETAR]` | `[COMPLETAR]` |
| `silver.tracking_envios` | `[COMPLETAR]` | `[COMPLETAR]` | `[COMPLETAR]` |
| `silver.sucursales` | `[COMPLETAR]` | `[COMPLETAR]` | `[COMPLETAR]` |
| `silver.cuarentena` | `[COMPLETAR]` | `[COMPLETAR]` | `[COMPLETAR]` |

### Resumen de Cuarentena (`silver.cuarentena`)

| Fuente | Regla Fallida | Motivo Principal | Filas Rechazadas (Final) |
| --- | --- | --- | --- |
| `ventas` | `monto_positivo` | ID/sucursal nulo, cantidad $\le 0$, monto $\le 0$ o fecha inválida | `[COMPLETAR]` |
| `devoluciones` | `reembolso_valido` | Venta inexistente o monto de reembolso mayor a la venta origen | `[COMPLETAR]` |
| `resenas` | `calificacion_en_escala` | Calificación fuera de rango `1-5` o fecha inválida | `[COMPLETAR]` |
| `catalogo` | `precio_valido` | `producto_id` nulo o `precio_lista <= 0` | `[COMPLETAR]` |
| `empleados` | `evento_ubicable` | `id_empleado` o `fecha_evento` nulo/inválido | `[COMPLETAR]` |
| `tracking_envios` | `estado_conocido` | `estado_entrega` fuera del catálogo cerrado | `[COMPLETAR]` |

---

## 🏆 7. Respuestas de Negocio — Capa Gold (Marzo – Octubre 2026)

Consultas ejecutadas sobre el catálogo `electrocasa_dev.gold` (replicado en `electrocasa_prod.gold`):

1. **Ventas Netas por Sucursal y Mes (`gold.ventas_netas_sucursal_mes`):**
* `[COMPLETAR: Menciona la sucursal líder en ventas netas, el mes pico y el impacto de devoluciones]`


2. **Top Productos (`gold.top_productos`):**
* `[COMPLETAR: Lista los 3-5 productos/categorías con mayor volumen o ingresos netos]`


3. **Dotación Mensual Histórica (`gold.dotacion_mensual`):**
* `[COMPLETAR: Describe cómo evolucionó la dotación activa gracias al SCD Type 2 en silver.empleados]`


4. **Desempeño por Vendedor (`gold.ventas_vendedor`):**
* `[COMPLETAR: Destaca los vendedores con mayor facturación y ticket promedio]`


5. **Reseñas Negativas (`gold.resenas_negativas`):**
* `[COMPLETAR: Indica los productos o motivos recurrentes detectados en reseñas con baja calificación y su metadata VARIANT]`


6. **Eficiencia de Despachos por Courier (`gold.despachos_courier`):**
* `[COMPLETAR: Compara tasa de entregas exitosas, devoluciones y tiempos promedio por courier]`



---

## 🛡️ 8. Gobernanza, Auditoría y Evidencias

### 8.1 Enmascaramiento de Columnas Sensibles (`MASK`)

Consulta de verificación sobre `electrocasa_prod.silver.empleados`:

```sql
SELECT id_empleado, nombre, dni, salario, sucursal_id, cargo
FROM electrocasa_prod.silver.empleados
LIMIT 10;

```

* **Resultado esperado:** Los usuarios autorizados ven los valores íntegros, mientras que los perfiles sin privilegios reciben `dni` y `salario` enmascarados automáticamente por Unity Catalog sin duplicar tablas.

### 8.2 Historial de Ingesta CDC (`DESCRIBE HISTORY`)

```sql
DESCRIBE HISTORY electrocasa_dev.bronze.tracking_envios;

```

* `[COMPLETAR: Resume las versiones/operaciones registradas en el Delta Log tras las corridas incrementales]`

---

## 🧠 9. Retos Técnicos Resueltos y Lecciones Aprendidas

Durante la construcción y despliegue del Lakehouse se resolvieron los siguientes retos reales de ingeniería:

1. **Uso de literales en `F.nullif()` en PySpark:** En Databricks Runtime, `F.nullif(col1, col2)` interpreta el segundo argumento como nombre de columna; pasar `""` provocaba `UNRESOLVED_COLUMN`. Se estandarizó a `F.nullif(F.trim(F.col(...)), F.lit(""))` en toda la capa Silver.
2. **Orden de evaluación de expectativas en Lakeflow Declarative Pipelines:** El decorador `@dp.expect_or_drop` evalúa las condiciones sobre el DataFrame de salida de la función. En `silver_devoluciones`, eliminar `monto_venta_origen` antes de retornar causaba fallo de resolución; conservar la columna resolvió la validación cruzada contra `silver.ventas`.
3. **Aislamiento de despliegues en Producción con DAB:** Al pasar de `dev` a `mode: production`, Databricks exige declarar explícitamente `workspace.root_path` dentro del target `prod` en `databricks.yml` para impedir despliegues duplicados entre usuarios y garantizar gobernanza de despliegues.
4. **Despliegue CLI frente a UI:** Utilizar `databricks bundle validate/deploy/run` desde la Web Terminal permitió diagnosticar al instante advertencias de esquema YAML, bloqueos de *sync snapshots* y errores de indentación con trazabilidad completa.

---

**Autor:** Renzo Turpo

**Repositorio:** `TurpeLED14111/PD-ELECTROCASA-LAKEHOUSE`

**Stack:** Azure Databricks · Unity Catalog · Lakeflow Declarative Pipelines · Auto Loader · Lakeflow Connect · Databricks Asset Bundles · PySpark · Delta Lake

```

---

### Últimos 2 pasos para entregarlo impecable:
1. Abre **`README.md`** en tu Git Folder, pega este contenido y rellena los campos **`[COMPLETAR]`** con los números que te arrojó `tarea_4_validacion` y tus consultas del SQL Editor.
2. Haz **Commit & Push** final hacia `main` en GitHub para que tu repositorio quede 100 % actualizado y listo para evaluación.

Si quieres pegarme aquí tus conteos de los lotes o los resultados de las 6 consultas Gold, te los acomodo directo en las tablas del `README.md` para que no tengas ni que editarlas.

```