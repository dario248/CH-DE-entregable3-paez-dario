# CoderHouse - Data Engineering

## Entregable N° 3 - Darío Páez

## Extracción de Datos
Se extraen datos de la **API pública del gobierno de Argentina**. Sin embargo, a diferencia del entregable N°2, se extraen datos de dos tablas diferentes:

- Serie de tiempo de **tipo de cambio dólar vendedor** del Banco Nación en pesos, con frecuencia diaria. [LINK](https://datos.gob.ar/series/api/series/?ids=168.1_T_CAMBIOR_D_0_0_26&start_date=2018-07&limit=5000)
- Serie de tiempo de **Indice de Precios al Consumidor Nacional (IPC)**, que se mide de forma mensual. [LINK](https://datos.gob.ar/series/api/series/?ids=148.3_INUCLEONAL_DICI_M_19) 

Se decide utilizar estas dos tablas por estar más actualizadas y disponer de una conexión directa con la API. Con el objetivo de evitar la carga de datos usando archivos **.csv**

## DAG

La DAG que orquesta las tareas se encuentra definida en el script `dags/etl_entregable3.py`. Consiste de una secuencia de tareas cuyo orden de prioridad se explica a continuación:

1. **PythonOperator** -> obtiene la variable `process_date` como el tiempo actual y lo envía a al entorno de Airflow para que esté disponible para ser accedida por otras tareas.
2. **SQLExecuteQueryOperator** -> ejecuta una query de SQL para crear la tabla donde se guardaran los datos obtenidos de la API tipo de cambio.
3. **SQLExecuteQueryOperator** -> ejecuta una query de SQL para crear la tabla donde se guardaran los datos obtenidos de la API de indice de precios al consumidor (IPC)
4. **SQLExecuteQueryOperator** -> ejecuta una query de SQL para crear la tabla donde se guardara un analisis comparativo del tipo de cambio y el IPC.
5. **SQLExecuteQueryOperator** -> ejecuta una query para limpiar registros que tengan el mismo process_date que al momento de ejecución.
6. **SparkSubmitOperator** -> ejecuta un **ETL** de spark definido en el script ``scripts/ETL_entregable3.py``


## ETL

- Extracción de datos de las dos APIs mencionadas y se instancian los DataFrames de Spark con los datos obtenidos
- Para poder combinar los datos en la tabla de analisis, es necesario convertir la columna `date` para que sea del tipo `datetime.date`. 
- Una vez ejecutado eso, se realiza una agregación de la tabla `tipo_de_cambio` de forma mensual, obteniendo el promedio del tipo de cambio vendedor.
- Luego, se realiza un merge entre ambas tablas obteniendo la tabla `df_analisis`
- Las tres tablas son cargadas en la base de datos de Redshift después de agregarles una columna con la variable `process_date`