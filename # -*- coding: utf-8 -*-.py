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

    def pandas_to_spark(self, pandas_df: pd.DataFrame, temp_view_name: str = None) -> pyspark_df:
        """
        Convierte un DataFrame de pandas a un DataFrame de Spark,
        limpiando y casteando columnas numéricas, y manejando DataFrames vacíos.
        """
        from pyspark.sql.types import StructField, StructType

        def map_dtype(dtype):
            dtype_str = str(dtype)
            if "datetime" in dtype_str:
                return StringType()
            elif "int" in dtype_str:
                return LongType()
            elif "float" in dtype_str:
                return FloatType()
            else:
                return StringType()

        try:
            # Limpieza y conversión previa de columnas numéricas (elimina caracteres no numéricos)
            for col in pandas_df.columns:
                if pandas_df[col].dtype == object:
                    cleaned = pandas_df[col].astype(str).str.replace(r"[^0-9.\-]", "", regex=True)
                    pandas_df[col] = pd.to_numeric(cleaned, errors='ignore')

            # Manejo de DataFrame vacío: inferir schema y crear DataFrame vacío
            if pandas_df.empty:
                struct_fields = []
                for col in pandas_df.columns:
                    struct_fields.append(StructField(col, map_dtype(pandas_df[col].dtype), True))
                schema = StructType(struct_fields)
                empty_rdd = self.spark.sparkContext.emptyRDD()
                spark_df = self.spark.createDataFrame(empty_rdd, schema)
                if temp_view_name:
                    view_name = temp_view_name.split(".")[-1]
                    spark_df.createOrReplaceTempView(view_name)
                return spark_df

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

    def define_schema(self, pandas_df: pd.DataFrame) -> StructType:
        """
        Genera un StructType de Spark a partir de los dtypes de pandas.
        """
        fields = []
        for col, dtype in zip(pandas_df.columns, pandas_df.dtypes):
            dt_str = str(dtype)
            if "datetime" in dt_str:
                typ = StringType()
            elif "int" in dt_str:
                typ = LongType()
            elif "float" in dt_str:
                typ = FloatType()
            else:
                typ = StringType()
            fields.append(StructField(col, typ, True))
        return StructType(fields)

    def pandas_to_spark(self, pandas_df: pd.DataFrame, temp_view_name: str = None, schema: StructType = None) -> pyspark_df:
        """
        Convierte un DataFrame de pandas a Spark, limpiando numéricos y aplicando un esquema opcional.
        """
        # Limpieza básica de obj -> numérico
        obj_cols = pandas_df.select_dtypes(include="object").columns
        for c in obj_cols:
            s = pandas_df[c].astype(str).str.replace(r"[^0-9.\-]", "", regex=True)
            pandas_df[c] = pd.to_numeric(s, errors="ignore")

        # Habilitar Arrow para rendimiento
        self.spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

        # Determinar esquema
        final_schema = schema or self.define_schema(pandas_df)

        # Crear Spark DataFrame
        spark_df = self.spark.createDataFrame(pandas_df, schema=final_schema)

        # Crear vista temporal si aplica
        if temp_view_name:
            view = temp_view_name.split(".")[-1]
            spark_df.createOrReplaceTempView(view)
        return spark_df
    

    def pandas_to_spark(self, pandas_df: pd.DataFrame, temp_view_name: str = None, schema: StructType = None) -> pyspark_df:
        """
        Convierte un DataFrame de pandas a Spark:
          - Limpia columnas numéricas en pandas
          - Usa Arrow para inferir esquema si no se provee uno
          - Usa records+schema si se provee
        """
        # 1. Limpieza básica: columnas object a numérico en pandas
        obj_cols = pandas_df.select_dtypes(include="object").columns
        for c in obj_cols:
            cleaned = pandas_df[c].astype(str).str.replace(r"[^0-9.\-]", "", regex=True)
            pandas_df[c] = pd.to_numeric(cleaned, errors="ignore")

        # 2. Habilitar Arrow para mejora de rendimiento en inferencia
        self.spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

        # 3. Crear DataFrame de Spark
        if schema:
            # Convertir a lista de dicts y aplicar schema
            records = pandas_df.to_dict(orient="records")
            spark_df = self.spark.createDataFrame(records, schema=schema)
        else:
            # Dejar que Spark (con Arrow) infiera automáticamente el esquema
            spark_df = self.spark.createDataFrame(pandas_df)

        # 4. Registrar vista temporal si se indicó
        if temp_view_name:
            view = temp_view_name.split(".")[-1]
            spark_df.createOrReplaceTempView(view)

        return spark_df
    
    def pandas_to_spark(self, pandas_df: pd.DataFrame, temp_view_name: str = None, schema: StructType = None) -> pyspark_df:
        """
        Convierte un DataFrame de pandas a Spark:
          - Limpia columnas numéricas en pandas
          - Genera esquema si no se provee uno con define_schema
          - Crea DataFrame usando lista de registros + esquema para evitar iteritems issues
        """
        # Limpieza básica: columnas object a numérico en pandas
        obj_cols = pandas_df.select_dtypes(include="object").columns
        for c in obj_cols:
            cleaned = pandas_df[c].astype(str).str.replace(r"[^0-9.\-]", "", regex=True)
            pandas_df[c] = pd.to_numeric(cleaned, errors="ignore")

        # Determinar esquema final
        final_schema = schema or self.define_schema(pandas_df)

        # Convertir a lista de registros y crear DataFrame con esquema explícito
        records = pandas_df.to_dict(orient="records")
        spark_df = self.spark.createDataFrame(records, schema=final_schema)

        # Registrar vista temporal si se indicó
        if temp_view_name:
            view = temp_view_name.split(".")[-1]
            spark_df.createOrReplaceTempView(view)

        return spark_df

    # ... resto de métodos (validate_tables, read_files_and_collect_details, etc.) ...
def define_schema_from_pandas(pandas_df: pd.DataFrame) -> StructType:
    """
    Crea un StructType de Spark a partir de los dtypes de un DataFrame de pandas,
    usando zip sobre pandas_df.columns y pandas_df.dtypes.
    """
    mapping = {
        "object": StringType(),
        "int64": LongType(),
        "int32": IntegerType(),
        "float64": DoubleType(),
        "float32": FloatType(),
        "bool": BooleanType(),
        "datetime64[ns]": TimestampType(),
    }

    fields = []
    for col, dtype in zip(pandas_df.columns, pandas_df.dtypes.astype(str)):
        spark_type = mapping.get(dtype, StringType())
        fields.append(StructField(col, spark_type, True))

    return StructType(fields)


def pandas_to_spark_with_schema(self, pandas_df: pd.DataFrame, temp_view_name: str = None) -> pyspark_df:
    """
    Convierte un pandas.DataFrame a Spark DataFrame:
      1) Infiriendo esquema con define_schema_from_pandas().
      2) Usando to_dict(orient='records') para evitar iteritems internos.
      3) Opcionalmente registra como vista temporal.
    """
    # 1) Infiero el esquema
    schema = define_schema_from_pandas(pandas_df)

    # 2) Limpio NaNs y convierto todo a str para evitar conflictos
    df_clean = pandas_df.fillna("").astype(str)

    # 3) Paso a lista de registros
    records = df_clean.to_dict(orient="records")

    # 4) Creo el DataFrame de Spark con el esquema explícito
    spark_df = self.spark.createDataFrame(records, schema=schema)

    # 5) Registro como vista si piden nombre
    if temp_view_name:
        view = temp_view_name.split(".")[-1]
        spark_df.createOrReplaceTempView(view)

    return spark_df




def pandas_to_spark(self,
                        pandas_df: pd.DataFrame,
                        temp_view_name: str = None) -> pyspark_df:
        """
        Convierte un pandas.DataFrame a Spark DataFrame con casting estricto:
          - Infiriendo esquema con define_schema_from_pandas()
          - Casteando cada valor Python al tipo exacto
          - Evitando pasar strings a columnas numéricas
        """
        # 1) Infiero esquema desde pandas
        schema = self.define_schema_from_pandas(pandas_df)

        # 2) Limpio NaNs → None para que Spark los interprete como NULL
        df_clean = pandas_df.where(pd.notnull(pandas_df), None)

        # 3) Construyo registros casteados
        records = []
        for row in df_clean.to_dict(orient="records"):
            rec = {}
            for field in schema.fields:
                name = field.name
                dtype = field.dataType
                val = row.get(name)
                # NULL
                if val is None:
                    rec[name] = None
                # Enteros
                elif isinstance(dtype, (IntegerType, LongType)):
                    try:
                        rec[name] = int(val)
                    except:
                        rec[name] = None
                # Flotantes
                elif isinstance(dtype, (FloatType, DoubleType)):
                    try:
                        rec[name] = float(val)
                    except:
                        rec[name] = None
                # Booleanos
                elif isinstance(dtype, BooleanType):
                    rec[name] = bool(val)
                # Timestamps: esperamos un datetime o string ISO
                elif isinstance(dtype, TimestampType):
                    rec[name] = val  # Spark aceptará str ISO o datetime
                # Cualquier otro tipo → string
                else:
                    rec[name] = str(val)
            records.append(rec)

        # 4) Creo el DataFrame de Spark con el esquema explícito
        try:
            spark_df = self.spark.createDataFrame(records, schema=schema)
        except Exception as e:
            self.write_log(f"Error en pandas_to_spark para vista '{temp_view_name}': {str(e)}", "ERROR")
            raise

        # 5) Registro como vista temporal si se indicó nombre
        if temp_view_name:
            view = temp_view_name.split(".")[-1]
            spark_df.createOrReplaceTempView(view)

        return spark_df



def pandas_to_spark(self, pandas_df: pd.DataFrame, temp_view_name: str = None) -> pyspark_df:
        """
        Convierte un pandas.DataFrame a un Spark DataFrame:
          - Convierte columas numéricas de forma vectorizada (sin loops por fila).
          - Usa Apache Arrow para acelerar la transferencia.
          - Maneja DataFrames vacíos sin fallar.
          - No emplea .iteritems() en ningún momento.
        """
        # 1) Vectorizamos el casteo en pandas
        for col, dtype in pandas_df.dtypes.items():  # items() es seguro en pandas 2.x
            if pd.api.types.is_integer_dtype(dtype):
                pandas_df[col] = pd.to_numeric(pandas_df[col], errors="coerce").astype("Int64")
            elif pd.api.types.is_float_dtype(dtype):
                pandas_df[col] = pd.to_numeric(pandas_df[col], errors="coerce")
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                pandas_df[col] = pd.to_datetime(pandas_df[col], errors="coerce")

        # 3) Crear el DataFrame de Spark
        if pandas_df.shape[0] == 0:
            # DataFrame vacío: inferir esquema y crear RDD vacío
            schema = self.define_schema_from_pandas(pandas_df)
            spark_df = self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(), schema)
        else:
            # Dataset no vacío: Spark infiere el esquema automáticamente con Arrow
            spark_df = self.spark.createDataFrame(pandas_df)

        # 4) Registrar vista temporal si se pide
        if temp_view_name:
            view = temp_view_name.split(".")[-1]
            spark_df.createOrReplaceTempView(view)

        return spark_df

    from pyspark.sql import Row

class bnmxspark:
    # … tus __init__, session(), write_log(), define_schema_from_pandas() …

    def pandas_to_spark(self, pandas_df, temp_view_name=None):
        """
        Convierte un pandas.DataFrame a Spark DataFrame SIN usar iteritems/items
        ni createDataFrame(pandas_df) directo. Usa RDD + esquema explícito.
        """
        # 1) Vectorizar casteo en pandas
        for col, dtype in zip(pandas_df.columns, pandas_df.dtypes):
            if pd.api.types.is_integer_dtype(dtype):
                pandas_df[col] = pd.to_numeric(pandas_df[col], errors="coerce").astype("Int64")
            elif pd.api.types.is_float_dtype(dtype):
                pandas_df[col] = pd.to_numeric(pandas_df[col], errors="coerce")
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                pandas_df[col] = pd.to_datetime(pandas_df[col], errors="coerce")

        # 2) Inferir esquema con tu método (que usa zip, no items)
        schema = self.define_schema_from_pandas(pandas_df)

        # 3) Preparar los datos como lista de tuplas
        #    - pandas_df.fillna(None) para convertir NaN a None
        pdf = pandas_df.where(pd.notnull(pandas_df), None)
        records = [tuple(row) for row in pdf.itertuples(index=False, name=None)]

        # 4) Crear un RDD desde las tuplas
        rdd = self.spark.sparkContext.parallelize(records)

        # 5) Crear DataFrame de Spark usando el esquema explícito
        spark_df = self.spark.createDataFrame(rdd, schema)

        # 6) Registrar vista temporal si se indicó nombre
        if temp_view_name:
            view = temp_view_name.split(".")[-1]
            spark_df.createOrReplaceTempView(view)

        return spark_df
