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
