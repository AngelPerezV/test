# -*- coding: utf-8 -*-

from string import Template
from subprocess import Popen, PIPE
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as sf
from pyspark.sql.window import Window
import pandas as pd
from impala.dbapi import connect
from pathlib2 import Path
from pyspark.sql import Row
import sys
import time
import os
import warnings
import config
from src.mod import bnmxspark


def validar_info(simplicity):
    #simplicity.start_step(0, "Input validation")

    tablas_mis_dt = [
        config.tbl_coll_colltr_act,
        config.tbl_als_txn,
        config.tbl_m75_dt,
        config.tbl_ecs_txn,
        config.tbl_plastic
    ]

    tablas_fec_informacion = [config.tbl_s11l_txn]

    intentos = 0
    intentos_insumos = 0
    status = 0
    status_a = 0
    max_intentos = 3

    print("Máximo de intentos:", max_intentos)

    while intentos < max_intentos:
        for tabla in tablas_mis_dt:
            simplicity.validate_tables(tabla, f"mis_dt = '{config.mis_dt.strftime('%Y-%m-%d')}'")

        for tabla in tablas_fec_informacion:
            simplicity.validate_tables(tabla, f"fec_informacion = '{config.mis_dt.strftime('%Y-%m-%d')}'")

        x_od_3 = simplicity.spark.table(config.tbl_s404_3_txn).select(sf.max("fec_informacion").alias("fec_informacion_3")).distinct()
        x_od_2 = simplicity.spark.table(config.tbl_s404_2_txn).select(sf.max("fec_informacion").alias("fec_informacion_2")).distinct()
        x_od_1 = simplicity.spark.table(config.tbl_s404_1_txn).select(sf.max("fec_informacion").alias("fec_informacion_1")).distinct()

        fechas = x_od_3.union(x_od_2).union(x_od_1).collect()
        od = [row[0] for row in fechas if row[0] is not None]

        if len(od) == 3:
            simplicity.validate_tables(config.tbl_s404_3_txn, f"fec_informacion = '{od[0]}'")
            simplicity.validate_tables(config.tbl_s404_2_txn, f"fec_informacion = '{od[1]}'")
            simplicity.validate_tables(config.tbl_s404_1_txn, f"fec_informacion = '{od[2]}'")

        control = len(simplicity.gbl_df[simplicity.gbl_df.status == 'Ok'])

        if control > 0:
            print("Tablas validadas correctamente")
            status = 1
            simplicity.tables_validate("Validación de tablas")
            intentos = 3
        else:
            print("Error en validación de tablas")
            tablas_error = simplicity.gbl_df[simplicity.gbl_df.status == 'Error']
            print(tablas_error)
            status = 0
            simplicity.tables_validate("Validación de tablas con errores")
            simplicity.gbl_df.drop(simplicity.gbl_df.index, inplace=True)
            time.sleep(1800)
            intentos += 1

    while intentos_insumos < max_intentos:
        print(f"Intento de validación de insumos: {intentos_insumos + 1}")
        simplicity.read_files_and_collect_details(config.folder_inputs, config.files_config)
        errores = len(simplicity.details_df[simplicity.details_df.status == 'Error: file not found'])

        if errores > 0:
            print("Errores encontrados en archivos de insumo")
            status_a = 0
            simplicity.details_df.drop(simplicity.details_df.index, inplace=True)
            intentos_insumos += 1
            time.sleep(600)
        else:
            simplicity.insumos_validate()
            status_a = 1
            intentos_insumos = max_intentos

    status_f = status and status_a
    print("Resultado final de la validación:", status_f)
    return status_f


def DLakeReplace(self, query_name, dlake_tbl: str):
    """
    Inserta o sobreescribe datos en una tabla del Data Lake.

    Args:
        query_name: Nombre de la vista temporal en Spark (str) o un DataFrame de Spark.
        dlake_tbl (str): Nombre de la tabla destino en el Data Lake.
    """
    start_time = dt.datetime.now()
    try:
        # Si query_name es el nombre de una vista Spark
        if isinstance(query_name, str):
            rows = self.spark.table(query_name).count()
            self.spark.table(query_name)\
                .write.mode("overwrite")\
                .insertInto(dlake_tbl, overwrite=True)

        # Si query_name es un DataFrame de Spark
        elif isinstance(query_name, pyspark_df):
            rows = query_name.count()
            query_name\
                .write.mode("overwrite")\
                .insertInto(dlake_tbl, overwrite=True)

        else:
            raise TypeError("query_name debe ser un nombre de vista o un DataFrame de Spark")

        # Log de éxito
        self.write_log(f"Datos insertados en {dlake_tbl} ({rows} registros)", "INFO")

    except Exception as e:
        # Captura y registra el error completo
        error_msg = f"Error inesperado en DLakeReplace: {str(e)}\n{traceback.format_exc()}"
        self.write_log(error_msg, "ERROR")
        # Relanza una excepción con mensaje más amigable
        raise Exception(f"No se pudo insertar la tabla {dlake_tbl} en el Datalake.")



    def pandas_to_spark(self, pandas_df: pd.DataFrame, temp_view_name: str = None) -> pyspark_df:
        """
        Convierte un DataFrame de pandas a un DataFrame de Spark,
        limpiando y casteando columnas numéricas para compatibilidad.
        """
        try:
            # Limpieza y conversión previa de columnas numéricas (elimina caracteres no numéricos)
            for col in pandas_df.columns:
                if pandas_df[col].dtype == object:
                    # Quita todo excepto dígitos, punto y signo negativo
                    cleaned = pandas_df[col].astype(str).str.replace(r"[^0-9.\-]", "", regex=True)
                    # Intenta convertir a numérico, si falla deja el original
                    pandas_df[col] = pd.to_numeric(cleaned, errors='ignore')

            # Convertir a lista de registros y dejar que Spark infiera el schema
            records = pandas_df.to_dict(orient="records")
            spark_df = self.spark.createDataFrame(records)

            if temp_view_name:
                view_name = temp_view_name.split(".")[-1]
                spark_df.createOrReplaceTempView(view_name)

            return spark_df

        except Exception as e:
            self.write_log(f"Error en pandas_to_spark para vista '{temp_view_name}': {str(e)}", "ERROR")
            raise

        except Exception as e:
            self.write_log(f"Error en pandas_to_spark para vista '{temp_view_name}': {str(e)}", "ERROR")
            raise