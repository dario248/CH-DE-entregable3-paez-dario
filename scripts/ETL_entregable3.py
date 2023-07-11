# Este script está pensado para correr en Spark y hacer el proceso de ETL de la tabla users

import requests
from datetime import datetime, timedelta
from os import environ as env

from pyspark.sql.functions import col, lit, trunc, mean, asc
from pyspark.sql.types import DateType

from commons import ETL_Spark

class ETL_Entregable3(ETL_Spark):
    def __init__(self, job_name=None):
        super().__init__(job_name)
        self.process_date = datetime.now().strftime("%Y-%m-%d")
        self.url_tipo_cambio = "https://apis.datos.gob.ar/series/api/series/?ids=168.1_T_CAMBIOR_D_0_0_26&limit=5000&start_date=2018-07&format=json"
        self.url_ipc = "https://apis.datos.gob.ar/series/api/series/?ids=148.3_INUCLEONAL_DICI_M_19&limit=5000&format=json"

    def run(self):
        process_date = "2023-07-09"  # datetime.now().strftime("%Y-%m-%d")
        self.execute(process_date)

    def extract(self):
        """
        Extrae datos de la API
        """
        print(">>> [E] Extrayendo datos de la API Tipo de cambio...")

        response_tipo_cambio = requests.get(self.url_tipo_cambio)
        if response_tipo_cambio.status_code == 200:
            data_tipo_cambio = response_tipo_cambio.json()["data"]
            print(data_tipo_cambio)
        else:
            print("Error al extraer datos de la API")
            data_tipo_cambio = []
            raise Exception("Error al extraer datos de la API")

        print(">>> [E] Extrayendo datos de la API IPC...")

        response_ipc = requests.get(self.url_ipc)
        if response_ipc.status_code == 200:
            data_ipc = response_ipc.json()["data"]
            print(data_ipc)
        else:
            print("Error al extraer datos de la API")
            data_ipc = []
            raise Exception("Error al extraer datos de la API")
        
        print(">>> [E] Creando Dataframes de Spark...")
        
        df_tipo_cambio = self.spark.createDataFrame(data_tipo_cambio, ['date', 'tipo_cambio_bna_vendedor'])
        df_ipc = self.spark.createDataFrame(data_ipc, ['date', 'ipc'])

        df_tipo_cambio.printSchema()
        df_ipc.printSchema()

        dataframes = {'tipo_de_cambio': df_tipo_cambio,
                      'ipc': df_ipc}
        
        return dataframes

    def transform(self, dfs_original):
        """
        Transforma los datos
        """
        print(">>> [T] Transformando datos...")
        df_tipo_cambio = dfs_original['tipo_de_cambio']
        df_ipc = dfs_original['ipc']

        # Conversión de string fecha a datetime
        df_ipc = df_ipc.withColumn('date', col('date').cast(DateType()))

        # Procesamiento de tipo de cambio
        df_tipo_cambio = df_tipo_cambio.withColumn('date', col('date').cast(DateType()))

        # Pasar las fechas a mensuales
        df_tipo_cambio = df_tipo_cambio.withColumn('date', trunc(col('date'), "Month"))

        # Agrupar por mes, calcular el promedio de tipo de cambio y ordenar de forma ascendente
        df_cambio_mensual = df_tipo_cambio.groupBy('date')\
            .agg(mean('tipo_cambio_bna_vendedor'))\
                .orderBy(asc('date'))\
                    .withColumnRenamed('avg(tipo_cambio_bna_vendedor)', 'tipo_cambio_bna_vendedor')

        # Comparación de IPC con media de tipo de cambio vendedor
        df_analisis = df_cambio_mensual.join(df_ipc, on='date', how='inner').orderBy(asc('date'))
        df_analisis.printSchema()

        dfs_original['analisis'] = df_analisis

        return dfs_original

    def load(self, df_final):
        """
        Carga los datos transformados en Redshift
        """
        print(">>> [L] Cargando datos en Redshift...")
        df_tipo_cambio = df_final['tipo_de_cambio']
        df_ipc = df_final['ipc']
        df_analisis = df_final['analisis']

        # add process_date column
        df_tipo_cambio = df_tipo_cambio.withColumn("process_date", lit(self.process_date))
        df_ipc = df_ipc.withColumn("process_date", lit(self.process_date))
        df_analisis = df_analisis.withColumn("process_date", lit(self.process_date))

        df_tipo_cambio.write \
            .format("jdbc") \
            .option("url", env['REDSHIFT_URL']) \
            .option("dbtable", f"{env['REDSHIFT_SCHEMA']}.tipo_de_cambio") \
            .option("user", env['REDSHIFT_USER']) \
            .option("password", env['REDSHIFT_PASSWORD']) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        
        df_ipc.write \
            .format("jdbc") \
            .option("url", env['REDSHIFT_URL']) \
            .option("dbtable", f"{env['REDSHIFT_SCHEMA']}.ipc_nacional") \
            .option("user", env['REDSHIFT_USER']) \
            .option("password", env['REDSHIFT_PASSWORD']) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        
        df_analisis.write \
            .format("jdbc") \
            .option("url", env['REDSHIFT_URL']) \
            .option("dbtable", f"{env['REDSHIFT_SCHEMA']}.analisis_economico") \
            .option("user", env['REDSHIFT_USER']) \
            .option("password", env['REDSHIFT_PASSWORD']) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        
        print(">>> [L] Datos cargados exitosamente")


if __name__ == "__main__":
    print("Corriendo script")
    etl = ETL_Entregable3()
    etl.run()