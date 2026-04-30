# POC Databricks Free: selective rebuild por lineage BFS

## 1. Objetivo del POC

Este POC demuestra de punta a punta que, cuando `gold.v_sales_summary` tiene un
defecto de calidad, podemos corregir el dato o transformación upstream y
recalcular solamente la rama mínima de lineage que alimenta esa Gold view,
evitando recomputar `gold.v_inventory_summary`.

Pruebas que deja resueltas:

1. Solo se ejecuta la rama necesaria para el target Gold seleccionado.
2. La Gold de inventory no se recalcula aunque comparta `core.dim_date`.
3. El lineage se puede resolver con:
   - tabla de system lineage si existe
   - `workspace.audit.lineage_edges` como fallback
4. La calidad Gold queda visible en tablas/auditoría aunque DLT expectations no
   estén disponibles.

## 2. Estructura recomendada

```text
.
|-- databricks.yml
|-- resources/jobs/
|-- src/lineage_poc/
|-- notebooks/
|   |-- _common/
|   |-- 00_admin/
|   |-- ops/
|   |-- raw/dml/
|   |-- stage/dml/
|   |-- core/dml/
|   |-- curated/dml/
|   `-- gold/dml/
`-- docs/
```

## 3. Prerrequisitos

1. Tener acceso a un workspace Databricks Free Edition.
2. Tener Databricks CLI reciente con soporte de bundles.
3. Tener un PAT o autenticación válida.
4. Trabajar en la rama `develop`.

## 4. Configuración del CLI

```bash
databricks configure --profile DEFAULT
```

Si quieres reproducir el scaffolding desde una carpeta vacía:

```bash
databricks bundle init default-python
```

Este repositorio ya viene inicializado, así que `init` solo es necesario si
quieres recrearlo desde cero en otro directorio.

## 5. Supuestos prácticos del POC

1. Catalog por defecto: `workspace`
2. Schemas creados por bootstrap:
   - `workspace.raw`
   - `workspace.stage`
   - `workspace.core`
   - `workspace.curated`
   - `workspace.gold`
   - `workspace.audit`
3. Volumen preferido:
   - `workspace.audit.poc_volume`
4. Landing preferido:
   - `/Volumes/workspace/audit/poc_volume/landing/raw/sales_orders/`
   - `/Volumes/workspace/audit/poc_volume/landing/raw/inventory_moves/`
5. Fallback si el volume no está disponible:
   - `dbfs:/tmp/databricks_lineage_poc/...`
6. Fallback adicional para ingestión:
   - `workspace.audit.source_seed_records`

## 6. Orden exacto de ejecución manual

1. `notebooks/00_admin/00_bootstrap_poc.py`
2. `notebooks/00_admin/01_generate_source_data.py`
3. `notebooks/raw/dml/SALES/ORDERS/sales_orders.py`
4. `notebooks/raw/dml/INVENTORY/MOVES/inventory_moves.py`
5. `notebooks/stage/dml/SALES/ORDERS/sales_orders_clean.py`
6. `notebooks/stage/dml/INVENTORY/MOVES/inventory_moves_clean.py`
7. `notebooks/core/dml/SHARED/CALENDAR/dim_date.py`
8. `notebooks/core/dml/SALES/FACT/fact_sales.py`
9. `notebooks/core/dml/INVENTORY/FACT/fact_inventory.py`
10. `notebooks/curated/dml/kpi/sales_kpis.py`
11. `notebooks/curated/dml/kpi/inventory_kpis.py`
12. `notebooks/gold/dml/consumer/v_sales_summary.py`
13. `notebooks/gold/dml/consumer/v_inventory_summary.py`
14. `notebooks/00_admin/06_run_sanity_checks.py`

## 7. Bundle y Workflows

Validar y desplegar:

```bash
databricks bundle validate -t dev --profile DEFAULT
databricks bundle deploy -t dev --profile DEFAULT
```

Ejecutar por CLI:

```bash
databricks bundle run generate_lineage_yaml -t dev --profile DEFAULT
databricks bundle run load_lineage_sales_summary -t dev --profile DEFAULT
```

También puedes abrir Workflows UI y lanzar manualmente:

1. `Generate_Lineage_YAML`
2. `Load_Lineage_Sales_Summary`
3. `POC_End_To_End_Demo`

Uso recomendado:

1. Usa la ejecución manual por notebooks cuando quieras capturas limpias y
   evidencia paso a paso.
2. Usa `POC_End_To_End_Demo` cuando quieras una ejecución reproducible que
   encadene baseline correcto, inyección del defecto, fallo de calidad,
   corrección, generación del YAML y selective rebuild de sales.

## 8. Primer ciclo con defecto

1. Ejecuta `01_generate_source_data.py` con `INJECT_DEFECTS=true`.
2. Ejecuta el pipeline base completo.
3. Consulta `workspace.audit.quality_results`.
4. Verás fallos en la rama de sales y ninguna anomalía en inventory.

## 9. Corrección upstream y selective rebuild

1. Ejecuta `05_fix_sales_source.py`.
2. Ejecuta `90_generate_lineage_yaml.py` con:
   - `TARGET_TABLE_NAME = gold.v_sales_summary`
   - `MAX_HOPS = 6`
   - `NOTEBOOK_BASE = notebooks`
   - `TEST_RANKS =`
   - `TEST_LIMIT_PER_RANK = 0`
3. Ejecuta el job `Load_Lineage_Sales_Summary`.
4. Revisa:
   - `workspace.audit.run_log`
   - `workspace.audit.quality_results`
   - `workspace.audit.lineage_bfs_results`
   - `workspace.audit.lineage_job_candidates`

Resultado esperado:

1. `gold.v_sales_summary` queda corregida.
2. No aparecen tareas inventory en `audit.run_log` para ese run.
3. `gold.v_inventory_summary` no se recomputa.

## 10. Smoke test del orquestador

Ejecuta `90_generate_lineage_yaml.py` con:

1. `TEST_RANKS = 3,4`
2. `TEST_LIMIT_PER_RANK = 1`

En modo smoke el generador limita las tareas por rank y no añade el target final
ni la validación de calidad, para mantener una ejecución coherente.

## 11. Secuencia del job end-to-end

El workflow `POC_End_To_End_Demo` ejecuta estas fases:

1. bootstrap de objetos del POC
2. generación de fuente baseline sin defectos
3. ejecución completa raw a gold para sales e inventory
4. validación de baseline limpio para sales
5. inyección del defecto en la fuente de sales
6. reconstrucción solo de la rama de sales para reproducir el fallo
7. validación del estado fallido de calidad
8. corrección de la fuente de sales
9. generación del YAML de selective rebuild
10. ejecución solo de la rama selectiva de sales
11. validación final de la reparación

Se puede añadir sin romper nada porque no sustituye los jobs existentes ni cambia
los notebooks base. Solo los orquesta en una secuencia más larga.

## 12. Consultas de validación

### Conteo de edges por hop

```sql
SELECT hop, COUNT(*) AS edges
FROM workspace.audit.lineage_bfs_results
GROUP BY hop
ORDER BY hop;
```

### Candidatos por rank

```sql
SELECT from_rank, COUNT(*) AS candidates
FROM workspace.audit.lineage_job_candidates
GROUP BY from_rank
ORDER BY from_rank;
```

### Tareas realmente ejecutadas

```sql
SELECT event_time, task_name, materialized_object, status
FROM workspace.audit.run_log
WHERE target_table = 'workspace.gold.v_sales_summary'
ORDER BY event_time;
```

### Demostrar que inventory no corrió

```sql
SELECT *
FROM workspace.audit.run_log
WHERE target_table = 'workspace.gold.v_sales_summary'
  AND task_name LIKE '%inventory%';
```

Debe devolver cero filas para el selective rebuild de sales.

## 13. Fallbacks implementados

1. Lineage:
   - Preferido: `system.access.table_lineage`
   - Fallback: `workspace.audit.lineage_edges`
2. Ingestión:
   - Preferido: Auto Loader
   - Fallback: batch JSON
   - Último fallback: seed inline desde `workspace.audit.source_seed_records`
3. Calidad Gold:
   - Opcional: notebook DLT/Lakeflow expectations
   - Default first-time-works: framework notebook con `workspace.audit.quality_results`

## 14. Fuentes oficiales verificadas

1. Free Edition limitations:
   https://learn.microsoft.com/azure/databricks/getting-started/free-edition-limitations
2. System lineage table:
   https://docs.databricks.com/aws/en/admin/system-tables/lineage
3. Workspace files:
   https://docs.databricks.com/aws/en/files/workspace
4. Bundle examples:
   https://docs.databricks.com/aws/en/dev-tools/bundles/examples
5. Bundle job tutorial:
   https://docs.databricks.com/aws/en/dev-tools/bundles/jobs-tutorial
6. Lakeflow expectations:
   https://docs.databricks.com/aws/en/ldp/expectations
