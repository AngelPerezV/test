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


def DLakeReplace(self, query_name, dlake_tbl: str, partition_cols: list = None):
    """
    Inserta o sobreescribe datos en una tabla existente del Data Lake usando INSERT OVERWRITE.
    
    Args:
        query_name: nombre de la vista temporal en Spark (str) o un DataFrame de Spark
        dlake_tbl:  tabla destino en formato "db.schema.tabla"
        partition_cols: lista de columnas de partición (ej. ["anio","mes"]) si es particionada
    """
    # 1. Obtén el DataFrame
    if isinstance(query_name, str):
        df = self.spark.table(query_name)
    else:
        df = query_name

    # 2. Coalesce para ajustar particiones
    df = df.coalesce(self.SparkPartitions)

    # 3. Registra vista temporal de trabajo
    tmp_view = "_tmp_replace"
    df.createOrReplaceTempView(tmp_view)

    # 4. Construye la sentencia SQL de INSERT OVERWRITE
    if partition_cols:
        # Si la tabla está particionada
        part_clause = "PARTITION(" + ", ".join(partition_cols) + ")"
        sql = f"""
        INSERT OVERWRITE TABLE {dlake_tbl}
        {part_clause}
        SELECT * FROM {tmp_view}
        """
    else:
        # Sobrescribe toda la tabla
        sql = f"""
        INSERT OVERWRITE TABLE {dlake_tbl}
        SELECT * FROM {tmp_view}
        """

    # 5. Ejecuta el INSERT OVERWRITE
    try:
        self.write_log(f"Ejecutando: {sql}", "INFO")
        self.spark.sql(sql)
        self.write_log(f"Datos insertados en {dlake_tbl} exitosamente.", "INFO")
    except Exception as e:
        msg = f"No se pudo insertar en la tabla {dlake_tbl}: {str(e)}"
        self.write_log(msg, "ERROR")
        raise
from pyspark.sql import DataFrame

def safe_dlake_replace(df: DataFrame,
                       spark,
                       SparkPartitions: int,
                       dlake_tbl: str,
                       partition_cols: list = None):
    """
    Inserta o sobreescribe datos en una tabla existente usando INSERT OVERWRITE,
    validando antes que df sea un Spark DataFrame y SparkPartitions sea un int > 0.

    Args:
        df             : DataFrame de Spark a escribir.
        spark          : SparkSession activa.
        SparkPartitions: número de particiones a usar en coalesce.
        dlake_tbl      : tabla destino en formato "db.schema.tabla".
        partition_cols : lista de columnas de partición si la tabla está particionada.
    """
    # 1) Validar inputs
    if not isinstance(df, DataFrame):
        raise TypeError(f"Esperaba un Spark DataFrame, no {type(df)}")
    if not isinstance(SparkPartitions, int) or SparkPartitions <= 0:
        raise ValueError(f"SparkPartitions inválido: {SparkPartitions!r}")

    # 2) Reducir particiones de forma segura
    df2 = df.coalesce(SparkPartitions)

    # 3) Registrar vista temporal
    tmp_view = "_tmp_replace"
    df2.createOrReplaceTempView(tmp_view)

    # 4) Generar SQL de INSERT OVERWRITE
    if partition_cols:
        part_clause = "PARTITION(" + ", ".join(partition_cols) + ")"
        sql = f"""
            INSERT OVERWRITE TABLE {dlake_tbl}
            {part_clause}
            SELECT * FROM {tmp_view}
        """
    else:
        sql = f"""
            INSERT OVERWRITE TABLE {dlake_tbl}
            SELECT * FROM {tmp_view}
        """

    # 5) Ejecutar y devolver mensaje
    print(f"Ejecutando:\n{sql}")
    spark.sql(sql)
    print(f"✅ Datos insertados en {dlake_tbl} using coalesce({SparkPartitions})")

# — Ejemplo de uso en tu notebook — 
# (ajusta spark, df_test, SparkPartitions y dlake_tbl a tu entorno)

# df_test = spark.read.table("origen_intermedio")
# safe_dlake_replace(
#     df=df_test,
#     spark=spark,
#     SparkPartitions=10,
#     dlake_tbl="mx.tabla", 
#     partition_cols=None
# )

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def safe_dlake_replace_adapt(df: DataFrame,
                              spark,
                              SparkPartitions: int,
                              dlake_tbl: str):
    """
    Reemplaza los datos de una tabla Hive no particionada, adaptando el esquema automáticamente.

    Args:
        df             : Spark DataFrame de entrada.
        spark          : SparkSession activa.
        SparkPartitions: Número de particiones para coalesce.
        dlake_tbl      : Nombre de la tabla Hive (formato db.tabla).
    """
    # 1. Validaciones básicas
    if not isinstance(df, DataFrame):
        raise TypeError(f"Esperaba un Spark DataFrame, no {type(df)}")
    if not isinstance(SparkPartitions, int) or SparkPartitions <= 0:
        raise ValueError(f"SparkPartitions inválido: {SparkPartitions!r}")

    # 2. Leer esquema de la tabla destino
    try:
        schema_info = spark.sql(f"DESCRIBE {dlake_tbl}").toPandas()
        hive_schema = schema_info[~schema_info["col_name"].str.contains("#")][["col_name", "data_type"]]
    except Exception as e:
        print(f"❌ Error leyendo el esquema de {dlake_tbl}: {str(e)}")
        raise

    # 3. Cast automático del DataFrame
    cols_casted = []
    for row in hive_schema.itertuples(index=False):
        colname, datatype = row.col_name, row.data_type
        if colname in df.columns:
            cols_casted.append(col(colname).cast(datatype).alias(colname))
        else:
            raise ValueError(f"La columna '{colname}' no está presente en el DataFrame.")

    df_casted = df.select(*cols_casted).coalesce(SparkPartitions)
    df_casted.createOrReplaceTempView("_tmp_replace")

    # 4. Ejecutar INSERT OVERWRITE
    sql_text = f"""
        INSERT OVERWRITE TABLE {dlake_tbl}
        SELECT * FROM _tmp_replace
    """

    try:
        print(f"Ejecutando SQL:\n{sql_text.strip()}")
        spark.sql(sql_text)
        print(f"✅ Datos insertados correctamente en {dlake_tbl} con coalesce({SparkPartitions})")
    except Exception as e:
        print(f"❌ Error en INSERT OVERWRITE: {str(e)}")
        raise
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def safe_dlake_replace_adapt(df: DataFrame,
                              spark,
                              SparkPartitions: int,
                              dlake_tbl: str):
    """
    Inserta datos en una tabla Hive no particionada,
    adaptando automáticamente el esquema (corrige diferencias de nombre, tipo, puntos o mayúsculas).
    
    Args:
        df             : Spark DataFrame.
        spark          : SparkSession activa.
        SparkPartitions: Número de particiones a aplicar con coalesce.
        dlake_tbl      : Nombre de la tabla destino, formato "base.tabla".
    """
    if not isinstance(df, DataFrame):
        raise TypeError(f"Se esperaba un Spark DataFrame, no {type(df)}.")
    if not isinstance(SparkPartitions, int) or SparkPartitions <= 0:
        raise ValueError(f"SparkPartitions inválido: {SparkPartitions!r}")

    # 1. Obtener el esquema de la tabla destino desde Hive
    try:
        schema_info = spark.sql(f"DESCRIBE {dlake_tbl}").toPandas()
        hive_schema = schema_info[~schema_info["col_name"].str.contains("#")][["col_name", "data_type"]]
    except Exception as e:
        raise RuntimeError(f"❌ Error al describir la tabla {dlake_tbl}: {e}")

    # 2. Preparar columnas del DataFrame para hacer match con el esquema Hive
    #    (usando claves sin puntos y en minúscula para robustez)
    df_cols = {c.lower().split('.')[-1]: c for c in df.columns}
    cols_casted = []

    for row in hive_schema.itertuples(index=False):
        colname_hive = row.col_name.lower()
        hive_type = row.data_type

        match_col = df_cols.get(colname_hive)

        if not match_col:
            raise ValueError(f"Columna '{row.col_name}' del esquema Hive no encontrada en el DataFrame.\n"
                             f"Columnas del DF: {list(df.columns)}")

        # Casteamos y renombramos a como lo espera Hive
        cols_casted.append(col(match_col).cast(hive_type).alias(row.col_name))

    # 3. Crear DataFrame casted y reducir particiones
    df_casted = df.select(*cols_casted).coalesce(SparkPartitions)

    # 4. Crear vista temporal
    tmp_view = "_tmp_replace"
    df_casted.createOrReplaceTempView(tmp_view)

    # 5. Ejecutar INSERT OVERWRITE
    sql_text = f"""
        INSERT OVERWRITE TABLE {dlake_tbl}
        SELECT * FROM {tmp_view}
    """
    try:
        print(f"Ejecutando SQL:\n{sql_text.strip()}")
        spark.sql(sql_text)
        print(f"✅ Datos insertados en {dlake_tbl} con coalesce({SparkPartitions}) correctamente.")
    except Exception as e:
        raise RuntimeError(f"❌ Error al insertar en {dlake_tbl}:\n{str(e)}")

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

class bnmxspark:
    # ... tus otros métodos como __init__, session(), write_log(), etc.

    def DLakeReplace(self, temp_view_or_df, dlake_tbl: str, partition_cols: list = None):
        """
        Inserta o sobreescribe datos en una tabla del Data Lake.
        Adapta automáticamente el esquema y soporta tablas particionadas.

        Args:
            temp_view_or_df: Nombre de vista temporal de Spark o un DataFrame de Spark.
            dlake_tbl       : Tabla destino en formato "base.tabla".
            partition_cols  : Lista de columnas de partición, si aplica.
        """
        # 1. Obtener DataFrame
        if isinstance(temp_view_or_df, str):
            df = self.spark.table(temp_view_or_df)
        elif isinstance(temp_view_or_df, DataFrame):
            df = temp_view_or_df
        else:
            raise TypeError("temp_view_or_df debe ser el nombre de una vista temporal o un Spark DataFrame.")

        # 2. Obtener esquema de la tabla destino
        try:
            schema_info = self.spark.sql(f"DESCRIBE {dlake_tbl}").toPandas()
            hive_schema = schema_info[~schema_info["col_name"].str.contains("#")][["col_name", "data_type"]]
        except Exception as e:
            self.write_log(f"Error describiendo la tabla {dlake_tbl}: {e}", "ERROR")
            raise

        # 3. Preparar columnas del DataFrame para castear
        df_cols = {c.lower().split('.')[-1]: c for c in df.columns}
        cols_casted = []

        for row in hive_schema.itertuples(index=False):
            colname_hive = row.col_name.lower()
            hive_type = row.data_type
            match_col = df_cols.get(colname_hive)

            if not match_col:
                raise ValueError(f"Columna '{row.col_name}' de Hive no encontrada en el DataFrame: {list(df.columns)}")

            cols_casted.append(col(match_col).cast(hive_type).alias(row.col_name))

        df_casted = df.select(*cols_casted).coalesce(self.SparkPartitions)

        # 4. Crear vista temporal
        tmp_view = "_tmp_replace"
        df_casted.createOrReplaceTempView(tmp_view)

        # 5. Armar SQL dinámico
        if partition_cols:
            part_clause = "PARTITION(" + ", ".join(partition_cols) + ")"
            sql_text = f"""
                INSERT OVERWRITE TABLE {dlake_tbl}
                {part_clause}
                SELECT * FROM {tmp_view}
            """
        else:
            sql_text = f"""
                INSERT OVERWRITE TABLE {dlake_tbl}
                SELECT * FROM {tmp_view}
            """

        # 6. Ejecutar
        try:
            self.write_log(f"Ejecutando SQL:\n{sql_text.strip()}", "INFO")
            self.spark.sql(sql_text)
            self.write_log(f"✅ Datos insertados en {dlake_tbl} correctamente con coalesce({self.SparkPartitions})", "INFO")
        except Exception as e:
            self.write_log(f"Error en DLakeReplace para {dlake_tbl}: {str(e)}", "ERROR")
            raise

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

class bnmxspark:
    # ... otros métodos como __init__, session(), write_log() ...

    def DLakeReplace(self, temp_view_or_df, dlake_tbl: str, partition_cols: list = None, debug_schema: bool = False):
        """
        Inserta o sobreescribe datos en una tabla del Data Lake.
        Adapta automáticamente el esquema y soporta tablas particionadas.
        
        Args:
            temp_view_or_df: Nombre de vista temporal en Spark o un DataFrame.
            dlake_tbl       : Nombre de la tabla Hive destino (formato db.tabla).
            partition_cols  : Columnas de partición, si aplica.
            debug_schema    : Si True, imprime esquema Hive vs DataFrame antes de insertar.
        """
        # 1. Obtener el DataFrame
        if isinstance(temp_view_or_df, str):
            df = self.spark.table(temp_view_or_df)
        elif isinstance(temp_view_or_df, DataFrame):
            df = temp_view_or_df
        else:
            raise TypeError("temp_view_or_df debe ser una vista temporal o un DataFrame de Spark.")

        if df is None:
            raise ValueError("El DataFrame obtenido es None. Verifica si existe la vista temporal correctamente.")

        # 2. Leer esquema de la tabla destino
        try:
            schema_info = self.spark.sql(f"DESCRIBE {dlake_tbl}").toPandas()
            hive_schema = schema_info[~schema_info["col_name"].str.contains("#")][["col_name", "data_type"]]
        except Exception as e:
            self.write_log(f"Error describiendo la tabla {dlake_tbl}: {e}", "ERROR")
            raise

        # 3. Preparar columnas casteadas
        df_cols = {c.lower().split('.')[-1]: c for c in df.columns}
        cols_casted = []

        for row in hive_schema.itertuples(index=False):
            colname_hive = row.col_name.lower()
            hive_type = row.data_type
            match_col = df_cols.get(colname_hive)

            if not match_col:
                raise ValueError(f"Columna '{row.col_name}' de Hive no encontrada en el DataFrame: {list(df.columns)}")

            cols_casted.append(col(match_col).cast(hive_type).alias(row.col_name))

        df_casted = df.select(*cols_casted).coalesce(self.SparkPartitions)

        # 4. Opcional: Imprimir el esquema si debug_schema=True
        if debug_schema:
            print("\n=== Esquema Hive (tabla destino) ===")
            print(hive_schema)
            print("\n=== Esquema del DataFrame casteado ===")
            df_casted.printSchema()
            print("\n")

        # 5. Crear vista temporal de trabajo
        tmp_view = "_tmp_replace"
        df_casted.createOrReplaceTempView(tmp_view)

        # 6. Construir SQL de INSERT OVERWRITE
        if partition_cols:
            part_clause = "PARTITION(" + ", ".join(partition_cols) + ")"
            sql_text = f"""
                INSERT OVERWRITE TABLE {dlake_tbl}
                {part_clause}
                SELECT * FROM {tmp_view}
            """
        else:
            sql_text = f"""
                INSERT OVERWRITE TABLE {dlake_tbl}
                SELECT * FROM {tmp_view}
            """

        # 7. Ejecutar
        try:
            self.write_log(f"Ejecutando SQL:\n{sql_text.strip()}", "INFO")
            self.spark.sql(sql_text)
            self.write_log(f"✅ Datos insertados en {dlake_tbl} correctamente usando coalesce({self.SparkPartitions})", "INFO")
        except Exception as e:
            self.write_log(f"Error en DLakeReplace para {dlake_tbl}: {str(e)}", "ERROR")
            raise

%macro validar_convertir(vars=);
  /* DATA step: lee SEGUROS y escribe VARIABLESCORREGIDAS2 */
  data VariablesCorregidas2;
    set seguros;

    /* Recorre cada nombre en vars= */
    %let i = 1;
    %let v = %scan(&vars, &i, %str( ));
    %do %while(&v ne);

      /* 
         INPUT(...,??best12.) devuelve missing si hay cualquier no-dígito;
         sólo convierte las vars que resultan 100% numéricas.
      */
      _tmp = input(&v, ?? best12.);

      if not missing(_tmp) then do;
        /* Creación de la versión numérica, con formato entero */
        &v._num = _tmp;
        format &v._num 8.;
        drop &v;                     /* Elimina la versión carácter */
        rename &v._num = &v;         /* Renombra la nueva var */
      end;
      else drop _tmp;                /* Si no era totalmente numérica, la deja como estaba */

      /* Siguiente variable */
      %let i = %eval(&i + 1);
      %let v = %scan(&vars, &i, %str( ));
    %end;
  run;
%mend validar_convertir;

/* —— Ejemplo de uso —— */
%validar_convertir(vars=id age cod_postal salario);


%macro validar_convertir(vars=   /* ej: id   age   salario */
                        ,fmts=   /* ej: 8.  8.2  comma12. */
                        );
  %let n = %sysfunc(countw(&vars));

  data VariablesCorregidas2;
    set seguros;

    %do i = 1 %to &n;
      %let v   = %scan(&vars, &i, %str( ));
      %let fmt = %scan(&fmts, &i, %str( ));

      /* conversión silenciosa */
      _tmp = input(&v, ?? &fmt);

      if not missing(_tmp) then do;
        &v._num = _tmp;
        /* aplicamos el formato específico de esta var */
        format &v._num &fmt;
        drop &v _tmp;
        rename &v._num = &v;
      end;
      else drop _tmp;
    %end;
  run;
%mend validar_convertir;

/* Ejemplo: id→8., age→8.2, salario→comma12. */
%validar_convertir(
  vars=id age salario,
  fmts=8. 8.2 comma12.
);


def DLakeReplace(self, temp_view_or_df, dlake_tbl: str, partition_cols: list = None, debug_schema: bool = False):
    """
    Inserta o sobreescribe datos en una tabla del Data Lake, en modo seguro.
    Si el DataFrame no existe, el proceso se detiene.
    """
    # Obtener el DataFrame
    if isinstance(temp_view_or_df, str):
        df = self.spark.table(temp_view_or_df)
    elif isinstance(temp_view_or_df, DataFrame):
        df = temp_view_or_df
    else:
        raise TypeError("temp_view_or_df debe ser una vista temporal o un DataFrame.")

    if df is None:
        raise ValueError(f"❌ El DataFrame para {temp_view_or_df} no existe o es None. Revisa el flujo anterior.")

    # Obtener esquema destino
    schema_info = self.spark.sql(f"DESCRIBE {dlake_tbl}").toPandas()
    hive_schema = schema_info[~schema_info["col_name"].str.contains("#")][["col_name", "data_type"]]

    # Cast automático
    df_cols = {c.lower().split('.')[-1]: c for c in df.columns}
    cols_casted = []
    for row in hive_schema.itertuples(index=False):
        colname_hive = row.col_name.lower()
        hive_type = row.data_type
        match_col = df_cols.get(colname_hive)

        if not match_col:
            raise ValueError(f"❌ Columna '{row.col_name}' de Hive no encontrada en el DataFrame.")

        cols_casted.append(col(match_col).cast(hive_type).alias(row.col_name))

    df_casted = df.select(*cols_casted).coalesce(self.SparkPartitions)

    # Debug schema si se solicita
    if debug_schema:
        print("\n=== Esquema Hive ===")
        print(hive_schema)
        print("\n=== Esquema DF casteado ===")
        df_casted.printSchema()
        print("\n")

    # Crear vista temporal
    tmp_view = "_tmp_replace"
    df_casted.createOrReplaceTempView(tmp_view)

    # INSERT
    if partition_cols:
        part_clause = "PARTITION(" + ", ".join(partition_cols) + ")"
        sql_text = f"INSERT OVERWRITE TABLE {dlake_tbl} {part_clause} SELECT * FROM {tmp_view}"
    else:
        sql_text = f"INSERT OVERWRITE TABLE {dlake_tbl} SELECT * FROM {tmp_view}"

    self.write_log(f"Ejecutando SQL:\n{sql_text.strip()}", "INFO")
    self.spark.sql(sql_text)
    self.write_log(f"✅ Datos insertados en {dlake_tbl}", "INFO")


from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def dlake_replace_test(spark, temp_view_or_df, dlake_tbl: str, partition_cols: list = None, strict: bool = True, debug_schema: bool = False, spark_partitions: int = 1):
    """
    Reemplaza datos en una tabla Hive usando INSERT OVERWRITE.
    
    Args:
        spark:             SparkSession activa.
        temp_view_or_df:   Nombre de vista temporal (str) o DataFrame de Spark.
        dlake_tbl:         Tabla destino en Hive (ej. "db.tabla").
        partition_cols:    Columnas de partición si aplica (lista).
        strict:            Si True, falla si no encuentra el DataFrame.
        debug_schema:      Si True, imprime comparación de esquemas.
        spark_partitions:  Número de particiones a coalescear antes de insertar.
    """
    # 1. Obtener DataFrame
    if isinstance(temp_view_or_df, str):
        try:
            df = spark.table(temp_view_or_df)
        except Exception as e:
            df = None
    elif isinstance(temp_view_or_df, DataFrame):
        df = temp_view_or_df
    else:
        raise TypeError("temp_view_or_df debe ser un nombre de vista temporal (str) o un Spark DataFrame.")
p
    # 2. Validar existencia de df
    if df is None:
        msg = f"⚠️ Advertencia: DataFrame para {temp_view_or_df} es None."
        if strict:
            raise ValueError(f"❌ {msg}")
        else:
            print(msg)
            return

    # 3. Obtener esquema de Hive
    schema_info = spark.sql(f"DESCRIBE {dlake_tbl}").toPandas()
    hive_schema = schema_info[~schema_info["col_name"].str.contains("#")][["col_name", "data_type"]]

    # 4. Cast de columnas
    df_cols = {c.lower().split('.')[-1]: c for c in df.columns}
    cols_casted = []
    for row in hive_schema.itertuples(index=False):
        colname_hive = row.col_name.lower()
        hive_type = row.data_type
        match_col = df_cols.get(colname_hive)

        if not match_col:
            raise ValueError(f"❌ Columna '{row.col_name}' de Hive no encontrada en DataFrame: {list(df.columns)}")

        cols_casted.append(col(match_col).cast(hive_type).alias(row.col_name))

    df_casted = df.select(*cols_casted).coalesce(spark_partitions)

    # 5. Debug opcional
    if debug_schema:
        print("\n=== Esquema Hive ===")
        print(hive_schema)
        print("\n=== Esquema DF casteado ===")
        df_casted.printSchema()
        print("\n")

    # 6. Crear vista temporal de trabajo
    tmp_view = "_tmp_replace"
    df_casted.createOrReplaceTempView(tmp_view)

    # 7. Construir SQL
    if partition_cols:
        part_clause = "PARTITION(" + ", ".join(partition_cols) + ")"
        sql_text = f"INSERT OVERWRITE TABLE {dlake_tbl} {part_clause} SELECT * FROM {tmp_view}"
    else:
        sql_text = f"INSERT OVERWRITE TABLE {dlake_tbl} SELECT * FROM {tmp_view}"

    # 8. Ejecutar
    try:
        print(f"Ejecutando SQL:\n{sql_text.strip()}\n")
        spark.sql(sql_text)
        print(f"✅ Datos insertados exitosamente en {dlake_tbl}")
    except Exception as e:
        print(f"❌ Error en DLakeReplace: {e}")
        raise

import subprocess
import os

def dlake_replace_hdfs(spark, temp_view_or_df, dlake_tbl: str, temp_path_hdfs: str = "/tmp/dlake_temp", mode: str = "overwrite"):
    """
    Reemplaza una tabla del Data Lake usando HDFS subprocess + Spark write, basado en vista o DataFrame.
    
    Args:
        spark:             SparkSession activa.
        temp_view_or_df:   Nombre de vista temporal en Spark o un DataFrame.
        dlake_tbl:         Nombre de tabla destino, formato 'db.tabla'.
        temp_path_hdfs:    Ruta temporal en HDFS (default: '/tmp/dlake_temp').
        mode:              'overwrite' (borra destino) o 'append' (solo agrega sin borrar destino).
    """
    # 1. Obtener el DataFrame
    if isinstance(temp_view_or_df, str):
        try:
            df = spark.table(temp_view_or_df)
        except Exception as e:
            raise ValueError(f"No se pudo encontrar la vista temporal '{temp_view_or_df}': {e}")
    else:
        df = temp_view_or_df

    # 2. Escribir DataFrame a HDFS temporal en formato parquet
    temp_write_path = os.path.join(temp_path_hdfs, dlake_tbl.replace('.', '_'))
    print(f"Escribiendo datos a HDFS temporal: {temp_write_path}")

    try:
        df.write.mode("overwrite").parquet(temp_write_path)
        print(f"✅ Datos escritos en {temp_write_path}")
    except Exception as e:
        raise ValueError(f"Error escribiendo el DataFrame a HDFS temporal: {e}")

    # 3. Construir ruta final basada en hive warehouse
    target_path_hdfs = f"/user/hive/warehouse/{dlake_tbl.replace('.', '.db.')}/"

    print(f"Destino final en HDFS: {target_path_hdfs}")

    # 4. Mover datos usando hdfs dfs
    try:
        if mode == "overwrite":
            delete_cmd = f"hdfs dfs -rm -r -skipTrash {target_path_hdfs}"
            print(f"Ejecutando: {delete_cmd}")
            subprocess.run(delete_cmd, shell=True, check=False)  # No truena si no existe (primera vez)

        move_cmd = f"hdfs dfs -mv {temp_write_path} {target_path_hdfs}"
        print(f"Ejecutando: {move_cmd}")
        subprocess.run(move_cmd, shell=True, check=True)

        print(f"✅ Datos movidos a {target_path_hdfs} correctamente.")

    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Error en operaciones HDFS:\n{e}")
import subprocess
import os
from pyspark.sql import DataFrame

def dlake_replace_hdfs(spark, temp_view_or_df, dlake_tbl: str, temp_path_hdfs: str = "/tmp/dlake_temp"):
    """
    Reemplaza todos los datos en una tabla del Lake usando escritura a HDFS + hdfs dfs -mv.

    Args:
        spark:             SparkSession activa.
        temp_view_or_df:   Nombre de vista temporal en Spark o un DataFrame.
        dlake_tbl:         Nombre de tabla destino, formato 'db.tabla'.
        temp_path_hdfs:    Ruta temporal en HDFS (default: '/tmp/dlake_temp').
    """
    # 1. Obtener el DataFrame
    if isinstance(temp_view_or_df, str):
        df = spark.table(temp_view_or_df)
    elif isinstance(temp_view_or_df, DataFrame):
        df = temp_view_or_df
    else:
        raise TypeError("temp_view_or_df debe ser nombre de vista o un DataFrame.")

    # 2. Escribir a carpeta temporal
    temp_write_path = os.path.join(temp_path_hdfs, dlake_tbl.replace('.', '_'))
    print(f"Escribiendo Parquet temporal en {temp_write_path}")
    df.write.mode("overwrite").parquet(temp_write_path)

    # 3. Construir destino en warehouse
    target_path_hdfs = f"/user/hive/warehouse/{dlake_tbl.replace('.', '.db.')}/"

    # 4. Borrar el contenido anterior (pero NO la tabla Hive como metadata)
    print(f"Borrando archivos anteriores en {target_path_hdfs} (pero NO la tabla)")
    delete_cmd = f"hdfs dfs -rm -r -skipTrash {target_path_hdfs}*"
    subprocess.run(delete_cmd, shell=True, check=False)

    # 5. Mover nuevos archivos al destino
    move_cmd = f"hdfs dfs -mv {temp_write_path}/* {target_path_hdfs}"
    print(f"Ejecutando: {move_cmd}")
    move_result = subprocess.run(move_cmd, shell=True, capture_output=True, text=True)

    if move_result.returncode != 0:
        print(f"❌ Error moviendo archivos:\nSTDOUT:\n{move_result.stdout}\nSTDERR:\n{move_result.stderr}")
        raise RuntimeError(f"Error moviendo {temp_write_path} a {target_path_hdfs}")

    print(f"✅ Reemplazo exitoso de datos en {dlake_tbl}.")


import subprocess
import os
from pyspark.sql import DataFrame

def safe_dlake_replace_adapt(spark, temp_view_or_df, dlake_tbl: str, temp_path_hdfs: str = "/tmp/dlake_temp"):
    """
    Reemplaza de forma segura los datos de una tabla en HDFS.
    Si falla el proceso, no se borran datos productivos.

    Args:
        spark:             SparkSession activa.
        temp_view_or_df:   Nombre de vista temporal en Spark o un DataFrame.
        dlake_tbl:         Nombre de la tabla destino (formato: 'db.tabla').
        temp_path_hdfs:    Carpeta temporal HDFS donde escribir antes de mover.
    """
    # 1. Obtener DataFrame
    if isinstance(temp_view_or_df, str):
        df = spark.table(temp_view_or_df)
    elif isinstance(temp_view_or_df, DataFrame):
        df = temp_view_or_df
    else:
        raise TypeError("temp_view_or_df debe ser nombre de vista o un Spark DataFrame.")

    # 2. Definir rutas
    temp_write_path = os.path.join(temp_path_hdfs, dlake_tbl.replace('.', '_'))
    final_target_path = f"/user/hive/warehouse/{dlake_tbl.replace('.', '.db.')}/"

    print(f"Escribiendo temporalmente en: {temp_write_path}")

    try:
        # 3. Escribir DataFrame en formato parquet
        df.write.mode("overwrite").parquet(temp_write_path)
        print(f"✅ Parquet temporal escrito en {temp_write_path}")

        # 4. Borrar datos anteriores del destino (sin borrar metadata Hive)
        print(f"Eliminando datos antiguos de: {final_target_path}")
        delete_cmd = f"hdfs dfs -rm -r -skipTrash {final_target_path}*"
        subprocess.run(delete_cmd, shell=True, check=False, capture_output=True)

        # 5. Mover nuevos archivos al destino
        print(f"Moviendo datos nuevos a: {final_target_path}")
        move_cmd = f"hdfs dfs -mv {temp_write_path}/* {final_target_path}"
        move_result = subprocess.run(move_cmd, shell=True, capture_output=True, text=True)

        if move_result.returncode != 0:
            print(f"❌ Error moviendo archivos:\nSTDOUT:\n{move_result.stdout}\nSTDERR:\n{move_result.stderr}")
            raise RuntimeError(f"Error moviendo {temp_write_path} a {final_target_path}")

        print(f"✅ Reemplazo exitoso de datos en {dlake_tbl}")

    except Exception as e:
        print(f"❌ Error crítico en safe_dlake_replace_adapt: {str(e)}")
        raise

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

class bnmxspark:
    # … tus métodos __init__, session(), write_log(), etc. …

    def DLakeReplace(self,
                     temp_view_or_df,
                     dlake_tbl: str,
                     partition_cols: list = None,
                     debug_schema: bool = False):
        """
        Inserta o sobreescribe datos en una tabla Hive existente.
        Adapta automáticamente el esquema y soporta particiones.

        Args:
          temp_view_or_df: Nombre de vista temporal (str) o DataFrame de Spark.
          dlake_tbl      : Tabla destino en formato "db.tabla".
          partition_cols : Lista de columnas de partición si aplica.
          debug_schema   : Si True, imprime esquema Hive vs DataFrame antes de insertar.
        """
        # 1) Obtener DataFrame
        if isinstance(temp_view_or_df, str):
            df = self.spark.table(temp_view_or_df)
        elif isinstance(temp_view_or_df, DataFrame):
            df = temp_view_or_df
        else:
            raise TypeError("temp_view_or_df debe ser nombre de vista o un Spark DataFrame.")

        if df is None:
            raise ValueError(f"El DataFrame para {temp_view_or_df} es None. Verifica que exista la vista o variable.")

        # 2) Leer esquema de Hive
        schema_df = self.spark.sql(f"DESCRIBE {dlake_tbl}").toPandas()
        hive_schema = schema_df[~schema_df["col_name"].str.startswith("#")][["col_name","data_type"]]

        # 3) Preparar cast y alias de columnas
        df_cols = {c.lower().split('.')[-1]: c for c in df.columns}
        casted = []
        for name, dtype in hive_schema.itertuples(index=False):
            key = name.lower()
            if key not in df_cols:
                raise ValueError(f"Columna '{name}' del esquema Hive no encontrada en el DataFrame: {df.columns}")
            casted.append(col(df_cols[key]).cast(dtype).alias(name))

        # 4) Seleccionar, castear y coalesce
        df2 = df.select(*casted).coalesce(self.SparkPartitions)

        # 5) Debug de esquemas opcional
        if debug_schema:
            print("=== Esquema Hive destino ===")
            print(hive_schema.to_string(index=False))
            print("\n=== Esquema DataFrame casteado ===")
            df2.printSchema()
            print()

        # 6) Vista temporal y SQL dinámico
        tmp = "_tmp_replace"
        df2.createOrReplaceTempView(tmp)

        if partition_cols:
            part = "PARTITION(" + ", ".join(partition_cols) + ")"
            sql = f"INSERT OVERWRITE TABLE {dlake_tbl} {part} SELECT * FROM {tmp}"
        else:
            sql = f"INSERT OVERWRITE TABLE {dlake_tbl} SELECT * FROM {tmp}"

        # 7) Ejecutar
        self.write_log(f"Ejecutando SQL:\n{sql}", "INFO")
        self.spark.sql(sql)
        self.write_log(f"✅ Datos insertados en {dlake_tbl} usando coalesce({self.SparkPartitions})", "INFO")
        from pyspark.sql import DataFrame
from pyspark.sql.functions import col

class bnmxspark:
    # … tus __init__, session(), write_log(), etc. …

    def DLakeReplace(self,
                     temp_view_or_df,
                     dlake_tbl: str,
                     partition_cols: list = None,
                     debug_schema: bool = False):
        """
        Inserta o sobreescribe datos en una tabla Hive existente,
        adaptando el esquema y reparticionando (sin usar coalesce).

        Args:
          temp_view_or_df: Nombre de vista temporal (str) o DataFrame de Spark.
          dlake_tbl      : Tabla destino en formato "db.tabla".
          partition_cols : Lista de columnas de partición si aplica.
          debug_schema   : Si True, imprime esquema Hive vs DataFrame antes de insertar.
        """
        # 1) Obtener DataFrame
        if isinstance(temp_view_or_df, str):
            df = self.spark.table(temp_view_or_df)
        elif isinstance(temp_view_or_df, DataFrame):
            df = temp_view_or_df
        else:
            raise TypeError("temp_view_or_df debe ser nombre de vista o un Spark DataFrame.")

        # 2) Validar
        if df is None:
            raise ValueError(f"El DataFrame para {temp_view_or_df} es None.")

        # 3) Leer esquema de destino en Hive
        schema_df = self.spark.sql(f"DESCRIBE {dlake_tbl}").toPandas()
        hive_schema = schema_df[~schema_df["col_name"].str.startswith("#")][["col_name","data_type"]]

        # 4) Preparar cast y alias
        df_cols = {c.lower().split('.')[-1]: c for c in df.columns}
        casted = []
        for name, dtype in hive_schema.itertuples(index=False):
            key = name.lower()
            if key not in df_cols:
                raise ValueError(f"Columna '{name}' de Hive no encontrada en el DataFrame: {df.columns}")
            casted.append(col(df_cols[key]).cast(dtype).alias(name))

        # 5) Reparticionar en lugar de coalesce
        df2 = df.select(*casted).repartition(self.SparkPartitions)

        # 6) Debug de esquemas opcional
        if debug_schema:
            print("=== Esquema Hive destino ===")
            print(hive_schema.to_string(index=False))
            print("\n=== Esquema DF casteado & reparticionado ===")
            df2.printSchema()
            print()

        # 7) Crear vista y armar SQL
        tmp = "_tmp_replace"
        df2.createOrReplaceTempView(tmp)

        if partition_cols:
            part = "PARTITION(" + ", ".join(partition_cols) + ")"
            sql = f"INSERT OVERWRITE TABLE {dlake_tbl} {part} SELECT * FROM {tmp}"
        else:
            sql = f"INSERT OVERWRITE TABLE {dlake_tbl} SELECT * FROM {tmp}"

        # 8) Ejecutar
        self.write_log(f"Ejecutando SQL:\n{sql}", "INFO")
        self.spark.sql(sql)
        self.write_log(f"✅ Datos insertados en {dlake_tbl} con reparticion({self.SparkPartitions})", "INFO")



 from pyspark.sql import DataFrame
from pyspark.sql.functions import col

class bnmxspark:
    # ... tu __init__, session(), write_log(), etc.

    def DLakeReplace(self, temp_view_or_df, dlake_tbl: str, partition_cols: list = None):
        """
        Inserta o sobreescribe datos en una tabla del Data Lake de forma segura.
        
        Args:
            temp_view_or_df: Nombre de vista temporal de Spark (str) o un DataFrame de Spark.
            dlake_tbl: Tabla destino en formato "base.tabla".
            partition_cols: Lista de columnas de partición, si aplica.
        """
        try:
            # 1. Obtener el DataFrame
            if isinstance(temp_view_or_df, str):
                df = self.spark.table(temp_view_or_df)
            elif isinstance(temp_view_or_df, DataFrame):
                df = temp_view_or_df
            else:
                raise TypeError("temp_view_or_df debe ser el nombre de una vista temporal o un Spark DataFrame.")

            # 2. Validar si tiene datos
            count_df = df.count()
            if count_df == 0:
                self.write_log(f"❗ Advertencia: El DataFrame está vacío. No se realizará el INSERT.", "WARNING")
                return
            else:
                self.write_log(f"👍 El DataFrame tiene {count_df} filas. Procediendo a insertarlo en {dlake_tbl}.", "INFO")

            # 3. Obtener esquema de destino
            schema_info = self.spark.sql(f"DESCRIBE {dlake_tbl}").toPandas()
            hive_schema = schema_info[~schema_info["col_name"].str.contains("#")][["col_name", "data_type"]]

            # 4. Castear columnas
            df_cols = {c.lower().split('.')[-1]: c for c in df.columns}
            cols_casted = []

            for row in hive_schema.itertuples(index=False):
                colname_hive = row.col_name.lower()
                hive_type = row.data_type
                match_col = df_cols.get(colname_hive)

                if not match_col:
                    raise ValueError(f"Columna '{row.col_name}' de Hive no encontrada en el DataFrame: {list(df.columns)}")
                
                cols_casted.append(col(match_col).cast(hive_type).alias(row.col_name))

            df_casted = df.select(*cols_casted)
            tmp_view = "_tmp_replace"
            df_casted.createOrReplaceTempView(tmp_view)

            # 5. Crear el SQL dinámico
            if partition_cols:
                part_clause = "PARTITION(" + ", ".join(partition_cols) + ")"
                sql_text = f"""
                    INSERT OVERWRITE TABLE {dlake_tbl}
                    {part_clause}
                    SELECT * FROM {tmp_view}
                """
            else:
                sql_text = f"""
                    INSERT OVERWRITE TABLE {dlake_tbl}
                    SELECT * FROM {tmp_view}
                """

            # 6. Ejecutar el SQL
            self.write_log(f"Ejecutando SQL:\n{sql_text.strip()}", "INFO")
            self.spark.sql(sql_text)
            self.write_log(f"✅ Datos insertados en {dlake_tbl} exitosamente ({count_df} registros).", "INFO")

        except Exception as e:
            msg = f"❌ Error en DLakeReplace para {dlake_tbl}: {str(e)}"
            self.write_log(msg, "ERROR")
            raise

from pyspark.sql.functions import col, regexp_replace, when
from pyspark.sql import DataFrame as pyspark_df

def DLake_Replace(self, temp_view_or_df, dlake_tbl: str, *argsv, debug_schema: bool = True):
    """
    Inserta o sobreescribe datos en una tabla Hive existente. Adapta automáticamente el esquema y soporta particiones.

    Args:
        temp_view_or_df (str | pyspark.sql.DataFrame): Nombre de vista temporal o un DataFrame de Spark.
        dlake_tbl (str): Tabla destino en formato "db.tabla".
        *argsv: Argumentos adicionales.
        debug_schema (bool): Si True, imprime esquema Hive vs DataFrame antes de insertar.
    """
    # 1. Obtener DataFrame
    if isinstance(temp_view_or_df, str):
        df = self.spark.table(temp_view_or_df)
    elif isinstance(temp_view_or_df, pyspark_df):
        df = temp_view_or_df
    else:
        raise TypeError("temp_view_or_df debe ser nombre de vista o un Spark DataFrame.")

    if df is None:
        raise ValueError("El DataFrame es None. Verifica que exista la vista o variable.")

    # 2. Validar si tiene datos
    count_df = df.count()
    if count_df == 0:
        self.write_log("! Advertencia: El DataFrame está vacío. No se realizará el INSERT.", "WARNING")
        return
    else:
        self.write_log(f"& El DataFrame tiene {count_df} filas. Procediendo a insertarlo en {dlake_tbl}.", "INFO")

    # 3. Leer esquema de Hive
    schema_df = self.spark.sql(f"DESCRIBE {dlake_tbl}").toPandas()
    hive_schema = schema_df[~schema_df["col_name"].str.startswith("#")][["col_name", "data_type"]]

    # 4. Preparar cast y alias de columnas
    patron_decimal = r"^-?\d+(\.\d+)?$"
    df_cols = {c.lower(): c for c in df.columns}
    casted = []

    for name, dtype in hive_schema.itertuples(index=False):
        key = name.lower()
        if key not in df_cols:
            raise ValueError(f"Columna '{name}' del esquema Hive no encontrada en el DataFrame: {df.columns}")

        spark_col = col(df_cols[key])
        clean_col = regexp_replace(spark_col, ",", "")
        clean_col = regexp_replace(clean_col, " ", "")

        if dtype.lower() in ("float", "double", "decimal(18,12)", "decimal(38,18)"):
            casted_col = when(clean_col.rlike(patron_decimal), clean_col.cast(dtype)).otherwise(None).alias(name)
        else:
            casted_col = clean_col.cast(dtype).alias(name)

        casted.append(casted_col)

    # 5. Aplicar transformación
    df2 = df.select(*casted)

    # 6. Debug opcional
    if debug_schema:
        self.write_log(f"Esquema Hive: {hive_schema}", "DEBUG")
        self.write_log(f"Esquema DataFrame: {df2.printSchema()}", "DEBUG")

    # 7. Vista temporal y SQL dinámico
    tmp = "_tmp_replace"
    df2.createOrReplaceTempView(tmp)

    try:
        if len(argsv) > 0:
            df2.write.mode("overwrite").format("parquet").insertInto(dlake_tbl, overwrite=False)
        else:
            df2.write.mode("overwrite").insertInto(dlake_tbl, overwrite=True)
    except Exception as e:
        msg = f"X Error en DLake_Replace para {dlake_tbl}: {str(e)}"
        self.write_log(msg, "ERROR")
        raise

    self.write_log(f"Datos insertados en [{dlake_tbl}] exitosamente ({count_df} registros).", "INFO")
import os

# === CONFIGURACIÓN ===
ruta = r"C:\Datos"  # Cambia esto a tu carpeta de trabajo
nombre_base_1 = "DAD"
nombre_base_2 = "INFO"
nombre_salida = "concatenado"

# === Listar archivos en la carpeta ===
archivos = os.listdir(ruta)

# Filtrar archivos que empiezan con DAD_
archivos_dad = [f for f in archivos if f.startswith(nombre_base_1 + "_") and f.endswith(".txt")]

for archivo_dad in archivos_dad:
    # Extraer la fecha desde el nombre del archivo (lo que está después de "_")
        try:
                fecha = archivo_dad.split("_")[1].replace(".txt", "")
                    except IndexError:
                            continue  # Si el nombre no tiene formato correcto, saltar

                                archivo_info = f"{nombre_base_2}_{fecha}.txt"
                                    path_dad = os.path.join(ruta, archivo_dad)
                                        path_info = os.path.join(ruta, archivo_info)

                                            if archivo_info in archivos:
                                                    print(f"Concatenando {archivo_dad} + {archivo_info}")
                                                            path_salida = os.path.join(ruta, f"{nombre_salida}_{fecha}.txt")

                                                                    with open(path_salida, "w", encoding="utf-8") as salida:
                                                                                with open(path_dad, "r", encoding="utf-8") as f1:
                                                                                                salida.write(f1.read())
                                                                                                            with open(path_info, "r", encoding="utf-8") as f2:
                                                                                                                            salida.write(f2.read())
                                                                                                                                else:
                                                                                                                                        print(f"No se encontró {archivo_info} para la fecha {fecha}")

                                                                                                                                        print("\n✅ Listo. Archivos combinados por fecha.")
                                                                                                                                        @echo off
                                                                                                                                        setlocal enabledelayedexpansion

                                                                                                                                        REM === CONFIGURACIÓN DE VARIABLES ===
                                                                                                                                        set "ruta=C:\Datos"
                                                                                                                                        set "base_1=DAD"
                                                                                                                                        set "base_2=INFO"
                                                                                                                                        set "base_out=concatenado"

                                                                                                                                        REM Cambiar al directorio donde están los archivos
                                                                                                                                        cd /d "%ruta%"

                                                                                                                                        REM Recorrer todos los archivos que empiecen con base_1
                                                                                                                                        for %%f in ("%base_1%_*.txt") do (
                                                                                                                                            REM Obtener el nombre sin extensión
                                                                                                                                                set "filename=%%~nf"

                                                                                                                                                    REM Extraer la fecha (todo después del guión bajo "_")
                                                                                                                                                        for /f "tokens=1* delims=_" %%a in ("!filename!") do set "fecha=%%b"

                                                                                                                                                            REM Construir el nombre del archivo 2
                                                                                                                                                                set "archivo2=%base_2%_!fecha!.txt"
                                                                                                                                                                    set "archivo1=%%~nxf"
                                                                                                                                                                        set "salida=%base_out%_!fecha!.txt"

                                                                                                                                                                            REM Verificar si el segundo archivo existe
                                                                                                                                                                                if exist "!archivo2!" (
                                                                                                                                                                                        echo Concatenando !archivo1! + !archivo2! → !salida!

                                                                                                                                                                                                REM Reiniciar control de encabezado
                                                                                                                                                                                                        set header=

                                                                                                                                                                                                                REM Escribir encabezado y contenido de archivo1
                                                                                                                                                                                                                        for /f "delims=" %%l in ('type "!archivo1!"') do (
                                                                                                                                                                                                                                    if not defined header (
                                                                                                                                                                                                                                                    echo %%l > "!salida!"
                                                                                                                                                                                                                                                                    set header=1
                                                                                                                                                                                                                                                                                ) else (
                                                                                                                                                                                                                                                                                                echo %%l >> "!salida!"
                                                                                                                                                                                                                                                                                                            )
                                                                                                                                                                                                                                                                                                                    )

                                                                                                                                                                                                                                                                                                                            REM Escribir solo contenido SIN encabezado del archivo2
                                                                                                                                                                                                                                                                                                                                    set "first_line=1"
                                                                                                                                                                                                                                                                                                                                            for /f "delims=" %%l in ('type "!archivo2!"') do (
                                                                                                                                                                                                                                                                                                                                                        if defined first_line (
                                                                                                                                                                                                                                                                                                                                                                        set "first_line="
                                                                                                                                                                                                                                                                                                                                                                                    ) else (
                                                                                                                                                                                                                                                                                                                                                                                                    echo %%l >> "!salida!"
                                                                                                                                                                                                                                                                                                                                                                                                                )
                                                                                                                                                                                                                                                                                                                                                                                                                        )
                                                                                                                                                                                                                                                                                                                                                                                                                            )
                                                                                                                                                                                                                                                                                                                                                                                                                            )

                                                                                                                                                                                                                                                                                                                                                                                                                            echo.
                                                                                                                                                                                                                                                                                                                                                                                                                            echo ✅ Listo. Archivos concatenados en: %ruta%
                                                                                                                                                                                                                                                                                                                                                                                                                            pausel


                                                                                                                                                                                                                                          # --- PASO 1: Marcar las sesiones que tienen una fila 'pop' con '450' ---
window_spec_idsesion = Window.partitionBy("idsesion")
df_session_marked = df.withColumn(
    "has_pop_450",
    sum(when((col("service") == "pop") & (col("comments") == "450"), 1).otherwise(0)).over(window_spec_idsesion) > 0
)

# --- PASO 2: Preparar los conteos y marcadores dentro de las sesiones válidas ---
df_relevant_rows = df_session_marked.filter(col("has_pop_450"))

df_grouped = df_relevant_rows.groupBy("idsesion").agg(
    sum(when((col("service") == "pop") & (col("comments") == "450"), 1).otherwise(0)).alias("count_pop_450"),
    sum(when(col("service") == "otp", 1).otherwise(0)).alias("has_otp"),
    collect_list(when(col("service") == "otp", col("comments"))).alias("otp_comments_list"),
    sum(when(col("service") == "aut", 1).otherwise(0)).alias("has_aut"),
    collect_list(when(col("service") == "aut", col("comments"))).alias("aut_comments_list"),
    sum(when(~col("service").isin("pop", "otp", "aut"), 1).otherwise(0)).alias("count_other_services")
)

df_grouped = df_grouped.withColumn(
    "otp_comment",
    array_distinct(df_grouped["otp_comments_list"]).getItem(0)
).withColumn(
    "aut_comment",
    array_distinct(df_grouped["aut_comments_list"]).getItem(0)
).drop("otp_comments_list", "aut_comments_list")

print("\nSesiones agrupadas con marcadores de patrón (antes del filtro final):")
df_grouped.show(truncate=False)

# --- PASO 3: Filtrar los grupos de idsesion que cumplen el patrón deseado ---
df_filtered_groups = df_grouped.filter(
    (col("count_pop_450") >= 1) &
    (col("has_otp") >= 1) &
    (col("count_other_services") == 0) & # No debe haber otros servicios no deseados
    (
        ( (col("otp_comment").isin("succes", "ok")) & (col("has_aut") == 0) ) | # OTP succes/ok Y NO hay AUT
        ( (col("otp_comment") == "fail") & (col("has_aut") >= 1) & (col("aut_comment").isin("fail", "succes", "not_found")) ) # OTP fail Y HAY AUT válido
    )
).select("idsesion")

print("\nIDs de sesiones que cumplen el patrón de grupo:")
df_filtered_groups.show()

# --- PASO 4: Unir los idsesion válidos con el DataFrame original y aplicar filtro de servicio/comentario ---
final_result_df = df.join(df_filtered_groups, on="idsesion", how="inner").filter(
    ( (col("service") == "pop") & (col("comments") == "450") ) | # Para 'pop', comments debe ser '450'
    ( (col("service").isin("otp", "aut")) & (col("comments").isin("fail", "succes", "ok", "not_found")) ) # Para 'otp'/'aut', comments pueden ser estos
)

print("\nResultado final que cumple con el patrón de grupo y los comentarios individuales:")
final_result_df.show(truncate=False)                


def comment_like(column, value):
    return column.like(f"%{value}%")

# 1. Marcar cada pop 450 como un potencial inicio de segmento y asignar un 'segment_id'
#    Usamos la columna 'timestamp' para ordenar la ventana.
window_spec_session_order = Window.partitionBy("idsesion").orderBy("timestamp")

df_segments = df.withColumn(
    "is_pop_450_start",
    when((col("service") == "pop") & (comment_like(col("comments"), "450")), 1).otherwise(0)
).withColumn(
    "segment_id",
    sum(col("is_pop_450_start")).over(window_spec_session_order)
).filter(col("segment_id") > 0) # Solo consideramos filas que vienen después de (o son) un pop 450

print("\nDataFrame con segment_id (ordenado por timestamp):")
df_segments.show(truncate=False)

# 2. Para cada segmento, recolectar los servicios y comentarios para evaluación
#    Agrupamos por (idsesion, segment_id)
df_evaluated_segments = df_segments.groupBy("idsesion", "segment_id").agg(
    first(when((col("service") == "pop") & comment_like(col("comments"), "450"), 1)).alias("segment_starts_with_pop_450"),
    
    collect_list(col("service")).alias("segment_services"),
    collect_list(col("comments")).alias("segment_comments"),
    
    sum(when(col("service") == "otp", 1).otherwise(0)).alias("segment_has_otp"),
    sum(when(col("service") == "aut", 1).otherwise(0)).alias("segment_has_aut"),
    
    first(when(col("service") == "otp", col("comments"))).alias("segment_otp_comment"),
    first(when(col("service") == "aut", col("comments"))).alias("segment_aut_comment"),

    sum(when(~col("service").isin("pop", "otp", "aut"), 1).otherwise(0)).alias("segment_count_other_services")
)

print("\nSegmentos agrupados con sus propiedades (evaluadas por timestamp):")
df_evaluated_segments.show(truncate=False)

# 3. Evaluar los patrones para CADA SEGMENTO
df_valid_segments = df_evaluated_segments.withColumn(
    "is_segment_valid",
    (col("segment_starts_with_pop_450") == 1) &
    (col("segment_count_other_services") == 0) &
    (
        # PATRÓN 1: OTP succes/ok (y si hay AUT, debe ser válido, pero es opcional)
        (
            (comment_like(col("segment_otp_comment"), "succes") | comment_like(col("segment_otp_comment"), "ok")) &
            (col("segment_has_otp") >= 1) &
            (
                (col("segment_has_aut") == 0) |
                (
                    comment_like(col("segment_aut_comment"), "fail") |
                    comment_like(col("segment_aut_comment"), "succes") |
                    comment_like(col("segment_aut_comment"), "not_found") |
                    comment_like(col("segment_aut_comment"), "authentication finished") # Agregado
                )
            )
        ) |
        # PATRÓN 2: OTP fail Y HAY AUT válido
        (
            comment_like(col("segment_otp_comment"), "fail") &
            (col("segment_has_aut") >= 1) &
            (
                comment_like(col("segment_aut_comment"), "fail") |
                comment_like(col("segment_aut_comment"), "succes") |
                comment_like(col("segment_aut_comment"), "not_found") |
                comment_like(col("segment_aut_comment"), "authentication finished") # Agregado
            ) &
            (col("segment_has_otp") >= 1)
        ) |
        # NUEVO PATRÓN 3: Solo pop con 450, sin OTP ni AUT en ESTE SEGMENTO
        (
            (col("segment_has_otp") == 0) &
            (col("segment_has_aut") == 0)
        )
    )
).filter(col("is_segment_valid") == True).select("idsesion", "segment_id")

print("\nIDs de segmentos válidos (determinados por timestamp):")
df_valid_segments.show(truncate=False)

# 4. Obtener los idsesion únicos que tienen AL MENOS UN segmento válido
valid_idsesions = df_valid_segments.select("idsesion").distinct()

print("\nIDs de sesiones que contienen al menos un segmento válido:")
valid_idsesions.show()

# 5. Unir con el DataFrame original para obtener todas las filas de esos idsesion válidos,
#    Y aplicar el filtro de comentarios individuales (que sigue siendo por contenido)

final_result_df = df.join(valid_idsesions, on="idsesion", how="inner").filter(
    ( (col("service") == "pop") & (comment_like(col("comments"), "450")) ) |
    (
        col("service").isin("otp", "aut") &
        (
            comment_like(col("comments"), "fail") |
            comment_like(col("comments"), "succes") |
            comment_like(col("comments"), "ok") |
            comment_like(col("comments"), "not_found") |
            comment_like(col("comments"), "authentication finished") # Agregado para AUT
        )
    )
)

print("\nResultado final con lógica de segmentos (usando timestamp):")
final_result_df.show(truncate=False)