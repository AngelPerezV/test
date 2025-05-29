# Clase bnmxspark refactorizada

import os
import logging
import traceback
import pandas as pd
import datetime as dt
from datetime import datetime
from string import Template
from pathlib import Path
from pyspark import SparkConf, SparkFiles
from pyspark.sql import SparkSession, DataFrame as pyspark_df
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, FloatType
from src.mod import email as bpa_email
import config

class bnmxspark():
    def __init__(self):
        self._timestamp = dt.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self.gbl_df = pd.DataFrame(columns=["count_tables", "ctrl_fig", "status", "comments", "table", "where_syntax"])
        self.control_table_df = None
        self.details_df = None

        log_folder = Path(config.log_folder_results)
        log_folder.mkdir(parents=True, exist_ok=True)
        log_filename = log_folder / f"log_{config.SparkAppName}_{self._timestamp}.log"

        logging.basicConfig(
            filename=str(log_filename),
            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
            level=logging.INFO
        )

        self.logger = logging.getLogger(config.SparkAppName)
        self.LogFile = str(log_filename)

        self.write_log(f"{config.SparkAppName} está levantando el entorno PySpark...")
        self.write_log(f"{config.SparkAppName} está generando la SparkSession...")

        self.spark = self.session()
        self.context = self.spark.sparkContext

    def write_log(self, message: str, level: str = "INFO"):
        if level == 'DEBUG':
            self.logger.debug(message)
        elif level == 'INFO':
            self.logger.info(message)
        elif level == 'WARNING':
            self.logger.warning(message)
        elif level == 'ERROR':
            self.logger.error(message)
        elif level == 'CRITICAL':
            self.logger.critical(message)

    def session(self):
        performance = config.SparkPerformance.lower()
        if performance == "high":
            spark_cores = "8"
            spark_memory = "12g"
            spark_partitions = 80
        elif performance == "medium":
            spark_cores = "6"
            spark_memory = "8g"
            spark_partitions = 60
        elif performance == "low":
            spark_cores = "4"
            spark_memory = "6g"
            spark_partitions = 56
        else:
            raise ValueError(f"SparkPerformance inválido: {config.SparkPerformance}")

        self.conf = SparkConf()
        self.conf.setAppName(config.SparkAppName)
        self.conf.set("spark.ui.port", str(config.spark_port))
        self.conf.set("spark.sql.shuffle.partitions", str(spark_partitions))
        self.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        self.conf.set("spark.eventLog.enabled", "false")
        self.conf.set("spark.scheduler.mode", "FAIR")
        self.conf.set("spark.driver.cores", spark_cores)
        self.conf.set("spark.driver.memory", spark_memory)
        self.conf.set("spark.executor.cores", spark_cores)
        self.conf.set("spark.executor.memory", spark_memory)
        self.conf.set("spark.yarn.queue", config.SPARK_QUEUE)
        self.conf.set("spark.executor.memoryOverhead", "2g")
        self.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")
        self.conf.set("hive.exec.dynamic.partition", "true")
        self.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
        self.conf.set("hive.resultset.use.unique.column.names", "false")
        self.conf.set("spark.sql.inMemoryColumnarStorage.compressed", "true")
        self.conf.set("spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled", "true")

        try:
            spark = SparkSession.builder.config(conf=self.conf).enableHiveSupport().getOrCreate()
            return spark
        except Exception as e:
            error_msg = traceback.format_exc()
            self.write_log(f"Error creando SparkSession: {error_msg}", "ERROR")
            bpa_email.emailSession(config.ProcessName, self._timestamp, error_msg, self.LogFile)
            raise

    def pandas_to_spark(self, pandas_df: pd.DataFrame, temp_view_name: str = None) -> pyspark_df:
        def map_dtype(dtype):
            if dtype == 'datetime64[ns]':
                return StringType()
            elif dtype == 'int64':
                return LongType()
            elif dtype == 'int32':
                return IntegerType()
            elif dtype == 'float64':
                return FloatType()
            else:
                return StringType()

        struct_fields = [StructField(col, map_dtype(str(dtype)), True) for col, dtype in zip(pandas_df.columns, pandas_df.dtypes)]
        schema = StructType(struct_fields)
        spark_df = self.spark.createDataFrame(pandas_df.astype(str), schema=schema)

        if temp_view_name:
            view_name = temp_view_name.split(".")[-1]
            spark_df.createOrReplaceTempView(view_name)

        return spark_df

    def validate_tables(self, dlake_tbl: str, where_syntax: str):
        try:
            self.write_log(f"Validando tabla: {dlake_tbl}")
            query = Template("SELECT COUNT(*) AS RECORDS FROM $dlake_tbl WHERE $where_syntax")
            qry_exe = query.substitute(dlake_tbl=dlake_tbl, where_syntax=where_syntax)
            df_ctrl = self.spark.sql(qry_exe)
            ctrl_fig = df_ctrl.first()["RECORDS"] if df_ctrl.head(1) else 0

            review_status = "Ok" if ctrl_fig > 0 else "Error"
            review_comments = "-" if ctrl_fig > 0 else "No records available"

            file_info = {
                "count_tables": len(self.gbl_df) + 1,
                "ctrl_fig": ctrl_fig,
                "status": review_status,
                "comments": review_comments,
                "table": dlake_tbl,
                "where_syntax": where_syntax
            }

            self.gbl_df.loc[len(self.gbl_df)] = file_info
            return review_status

        except Exception as e:
            self.write_log(f"Error al validar tabla {dlake_tbl}: {str(e)}", "ERROR")
            raise

    def start_step(self, number_step: int, description: str):
        if self.control_table_df is None:
            self.control_table_df = pd.DataFrame(columns=["Step", "Description", "Start", "End", "Execution time (s)", "Rows", "State", "Comment"])

        new_row = {
            "Step": number_step,
            "Description": description,
            "Start": datetime.now(),
            "End": None,
            "Execution time (s)": 0.0,
            "Rows": 0,
            "State": "Not Completed",
            "Comment": ""
        }

        self.control_table_df.loc[len(self.control_table_df)] = new_row
        self.write_log(f"Step {number_step} - '{description}' iniciado.", "INFO")

    def end_step(self, number_step: int, rows: int, comment: str):
        idx = self.control_table_df[self.control_table_df["Step"] == number_step].index
        if not idx.empty:
            start_time = self.control_table_df.at[idx[0], "Start"]
            end_time = datetime.now()
            exec_time = (end_time - start_time).total_seconds()

            self.control_table_df.at[idx[0], "End"] = end_time
            self.control_table_df.at[idx[0], "Execution time (s)"] = exec_time
            self.control_table_df.at[idx[0], "Rows"] = rows
            self.control_table_df.at[idx[0], "State"] = "Finished"
            self.control_table_df.at[idx[0], "Comment"] = comment

            description = self.control_table_df.at[idx[0], "Description"]
            self.write_log(f"Step {number_step} - '{description}' finalizado: {comment}", "INFO")
        else:
            self.write_log(f"No se encontró el paso {number_step} para finalizar.", "WARNING")

    def SendStartEmail(self):
        self.write_log("Enviando correo de inicio del proceso.")
        bpa_email.SendStartEmail(config.ProcessName, self.LogFile)
        self.write_log("Correo de inicio enviado.")

    def SendErrorEmail(self, err_msg: str = ""):
        self.write_log("Enviando correo de error.")
        bpa_email.SendErrorEmail(config.ProcessName, self.LogFile, err_msg)
        self.write_log("Correo de error enviado.")

    def SendControlEmail(self, controls: list = [], attachments: list = []):
        self.write_log("Preparando envío de correo de control.")
        bpa_email.SendControlEmail(config.ProcessName, self.LogFile, controls, attachments)
        self.write_log("Correo de control enviado.")

    def report_final(self, evidences: list = [], attachments: list = []):
        self.write_log(f"{config.SparkAppName} - Enviando correo final.", "INFO")
        bpa_email.report_final(config.ProcessName, self.LogFile, evidences, attachments)
        self.write_log("Correo final enviado.")

    def DLakeReplace(self, query_name, dlake_tbl: str):
        """
        Inserta o sobreescribe datos en una tabla del Data Lake.

        Args:
            query_name: Nombre de la vista temporal en Spark o un DataFrame de Spark.
            dlake_tbl (str): Nombre de la tabla destino en el Datalake.
        """
        start_time = dt.datetime.now()
        try:
            if isinstance(query_name, str):
                rows = self.spark.table(query_name).count()
                self.spark.table(query_name).write.mode("overwrite").insertInto(dlake_tbl, overwrite=True)
            elif isinstance(query_name, pyspark_df):
                rows = query_name.count()
                query_name.write.mode("overwrite").insertInto(dlake_tbl, overwrite=True)
            else:
                raise TypeError("query_name debe ser un nombre de vista o un DataFrame de Spark")

            # Aquí podrías guardar métricas si tienes algo como self.TblMetrics()
            self.write_log(f"Datos insertados en {dlake_tbl} ({rows} registros)", "INFO")

        except Exception as e:
            error_msg = f"Error inesperado en DLakeReplace: {str(e)}\n{traceback.format_exc()}"
            self.write_log(error_msg, "ERROR")
            raise Exception(f"No se pudo insertar la tabla {dlake_tbl} en el Datalake.")
        
    def tables_validate(self, msg_notif: str = ""):
        """ Envía el correo de validación de las tablas con las cifras de control generadas.

            Args:
            msg_notif (str): Mensaje adicional que se incluirá en el correo.

            Returns:
            None
        """
        self.write_log("Preparando envío de correo de validación de tablas.", "INFO")
        bpa_email.tables_validate(config.ProcessName, self.gbl_df, self.LogFile, msg_notif)
        self.write_log("Correo de validación de tablas enviado.", "INFO")



    def read_files_and_collect_details(self, hdfs_directory: str, files_config: dict): 
        """ Lee archivos desde HDFS, obtiene metadatos, valida archivos esperados y genera vistas temporales.

            Args:
            hdfs_directory (str): Ruta HDFS donde están los archivos.
            files_config (dict): Diccionario de configuración por archivo esperado.

            Returns:
            None. Actualiza self.details_df
        """

    self.write_log("Iniciando lectura y validación de archivos desde HDFS")

    # Obtener listado de archivos en el directorio
    try:
        hdfs_ls_cmd = f"hdfs dfs -ls {hdfs_directory}"
        result = subprocess.run(hdfs_ls_cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.returncode != 0:
            raise Exception(f"Error al listar archivos: {result.stderr}")

        found_files = {}
        for line in result.stdout.splitlines():
            parts = line.strip().split()
            if len(parts) >= 8:
                file_path = parts[-1]
                filename = os.path.basename(file_path)
                modified = f"{parts[5]} {parts[6]}"
                found_files[filename] = {
                    "path": file_path,
                    "last_modified": modified
                }
    except Exception as e:
        self.write_log(f"Fallo al obtener listado de archivos: {str(e)}", "ERROR")
        raise

    file_details_list = []

    for filename, file_cfg in files_config.items():
        self.write_log(f"Procesando archivo: {filename}")
        config_format = file_cfg.get("format", "csv").lower()
        delimiter = file_cfg.get("delimiter", ",")
        sheet_name = file_cfg.get("sheet", None)
        spark_view = file_cfg.get("spark_name", filename.split(".")[0])

        detail = {
            "prefix": filename.split(".")[0],
            "format": config_format,
            "path": "N/A",
            "name": filename,
            "last_modified": "N/A",
            "size_in_mb": "N/A",
            "rows": "N/A",
            "sheets": sheet_name or "N/A",
            "status": "Error: file not found"
        }

        if filename in found_files:
            file_path = found_files[filename]["path"]
            detail["path"] = file_path
            detail["last_modified"] = found_files[filename]["last_modified"]

            try:
                # Obtener tamaño en MB
                hdfs_du_cmd = f"hdfs dfs -du -s {file_path}"
                du_result = subprocess.run(hdfs_du_cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                if du_result.returncode == 0:
                    size_bytes = int(du_result.stdout.strip().split()[0])
                    detail["size_in_mb"] = round(size_bytes / (1024 * 1024), 2)

                if config_format in ["csv", "txt"]:
                    df = self.spark.read.option("header", "true").option("delimiter", delimiter).csv(file_path)
                    detail["rows"] = df.count()
                    df.createOrReplaceTempView(spark_view)
                    detail["status"] = "ok"

                elif config_format in ["xls", "xlsx"]:
                    self.spark.sparkContext.addFile(file_path)
                    df_pd = pd.read_excel(SparkFiles.get(filename), engine="openpyxl", sheet_name=sheet_name).astype(str)
                    df_spk = self.pandas_to_spark(df_pd, spark_view)
                    detail["rows"] = df_spk.count()
                    detail["status"] = "ok"

                else:
                    detail["status"] = "unsupported file type"

            except Exception as e:
                detail["status"] = f"Error: {str(e)}"
                self.write_log(f"Error procesando {filename}: {str(e)}", "ERROR")
        else:
            self.write_log(f"Archivo esperado no encontrado: {filename}", "WARNING")

        self.write_log(f"Resultado [{filename}]: {detail['status']} | Filas: {detail['rows']}")
        file_details_list.append(detail)

    self.details_df = pd.DataFrame(file_details_list)
    self.write_log("Archivo de detalles actualizado en self.details_df")

    def insumos_validate(self, msg_notif: str = ""):
            """
                Valida el estado de los archivos cargados y envía notificación por correo si hay errores.

                Args:
                msg_notif (str): Mensaje adicional para incluir en la notificación.

                Returns:
                None
                """
    self.write_log("Validando archivos de insumo.")

    errores = len(self.details_df[self.details_df["status"].str.contains("Error", na=False)])
    tipo_unsupported = len(self.details_df[self.details_df["status"].str.contains("unsupported file type", na=False)])

    if errores > 0 or tipo_unsupported > 0:
        status = "Phase Pre-Execution: Warning"
        self.write_log(f"Errores detectados en archivos de insumo: {errores + tipo_unsupported}", "WARNING")
        self.spark.stop()
    else:
        status = "Phase Pre-Execution: Completed"
        self.write_log("Archivos de insumo validados correctamente.")

    bpa_email.email_insumos_validate(
        process_name=config.ProcessName,
        msg=msg_notif,
        df=self.details_df,
        logfile=self.LogFile,
        status=status
    )

    self.write_log("Correo de validación de insumos enviado.")

    def read_files_and_collect_details(self, hdfs_directory: str, files_config: dict):

        """ Lee archivos desde HDFS, obtiene metadatos, valida archivos esperados y genera vistas temporales.

        Args:
        hdfs_directory (str): Ruta HDFS donde están los archivos.
        files_config (dict): Diccionario de configuración por archivo esperado.

            Returns:
        None. Actualiza self.details_df
        """

        self.write_log("Iniciando lectura y validación de archivos desde HDFS")

    # Obtener listado de archivos en el directorio
    try:
        hdfs_ls_cmd = f"hdfs dfs -ls {hdfs_directory}"
        result = subprocess.run(hdfs_ls_cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.returncode != 0:
            raise Exception(f"Error al listar archivos: {result.stderr}")

        found_files = {}
        for line in result.stdout.splitlines():
            parts = line.strip().split()
            if len(parts) >= 8:
                file_path = parts[-1]
                filename = os.path.basename(file_path)
                modified = f"{parts[5]} {parts[6]}"
                found_files[filename] = {
                    "path": file_path,
                    "last_modified": modified
                }
    except Exception as e:
        self.write_log(f"Fallo al obtener listado de archivos: {str(e)}", "ERROR")
        raise

    file_details_list = []

    for filename, file_cfg in files_config.items():
        self.write_log(f"Procesando archivo: {filename}")
        config_format = file_cfg.get("format", "csv").lower()
        delimiter = file_cfg.get("delimiter", ",")
        sheet_name = file_cfg.get("sheet", None)
        spark_view = file_cfg.get("spark_name", filename.split(".")[0])

        detail = {
            "prefix": filename.split(".")[0],
            "format": config_format,
            "path": "N/A",
            "name": filename,
            "last_modified": "N/A",
            "size_in_mb": "N/A",
            "rows": "N/A",
            "sheets": sheet_name or "N/A",
            "status": "Error: file not found"
        }

        if filename in found_files:
            file_path = found_files[filename]["path"]
            detail["path"] = file_path
            detail["last_modified"] = found_files[filename]["last_modified"]

            try:
                # Obtener tamaño en MB
                hdfs_du_cmd = f"hdfs dfs -du -s {file_path}"
                du_result = subprocess.run(hdfs_du_cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                if du_result.returncode == 0:
                    size_bytes = int(du_result.stdout.strip().split()[0])
                    detail["size_in_mb"] = round(size_bytes / (1024 * 1024), 2)

                if config_format in ["csv", "txt"]:
                    df = self.spark.read.option("header", "true").option("delimiter", delimiter).csv(file_path)
                    detail["rows"] = df.count()
                    df.createOrReplaceTempView(spark_view)
                    detail["status"] = "ok"

                elif config_format in ["xls", "xlsx"]:
                    self.spark.sparkContext.addFile(file_path)
                    df_pd = pd.read_excel(SparkFiles.get(filename), engine="openpyxl", sheet_name=sheet_name).astype(str)

                    # Detectar y limpiar contenido XML en columnas
                    for col in df_pd.columns:
                        if df_pd[col].str.contains(r"<[^>]+>", regex=True).any():
                            self.write_log(f"Advertencia: Se detectó contenido XML en columna '{col}' del archivo {filename}. Será limpiado.", "WARNING")
                            df_pd[col] = df_pd[col].str.replace(r"<[^>]+>", "", regex=True)

                    df_spk = self.pandas_to_spark(df_pd, spark_view)
                    detail["rows"] = df_spk.count()
                    detail["status"] = "ok"

                else:
                    detail["status"] = "unsupported file type"

            except Exception as e:
                detail["status"] = f"Error: {str(e)}"
                self.write_log(f"Error procesando {filename}: {str(e)}", "ERROR")
        else:
            self.write_log(f"Archivo esperado no encontrado: {filename}", "WARNING")

        self.write_log(f"Resultado [{filename}]: {detail['status']} | Filas: {detail['rows']}")
        file_details_list.append(detail)

    self.details_df = pd.DataFrame(file_details_list)
    self.write_log("Archivo de detalles actualizado en self.details_df")

import ast
import os

def buscar_iteritems_en_archivo(path):
    with open(path, "r", encoding="utf-8") as file:
        tree = ast.parse(file.read(), filename=path)

    errores = []

    class IteritemsVisitor(ast.NodeVisitor):
        def visit_Attribute(self, node):
            if node.attr == "iteritems":
                errores.append((node.lineno, node.col_offset))
            self.generic_visit(node)

    IteritemsVisitor().visit(tree)

    return errores

# Ruta al archivo que quieres analizar
archivo_a_revisar = "src/mod/bnmxspark.py"

errores = buscar_iteritems_en_archivo(archivo_a_revisar)

if errores:
    print(f"Se encontraron usos de `.iteritems()` en {archivo_a_revisar}:")
    for lineno, col in errores:
        print(f"  - Línea {lineno}, columna {col}")
else:
    print("¡Perfecto! No se encontró ninguna referencia a `.iteritems()` en el archivo.")

... def read_files_and_collect_details(self, hdfs_directory: str, files_config: dict): """ Lee archivos desde HDFS, obtiene metadatos, valida archivos esperados y genera vistas temporales.

Args:
        hdfs_directory (str): Ruta HDFS donde están los archivos.
        files_config (dict): Diccionario de configuración por archivo esperado.

    Returns:
        None. Actualiza self.details_df
    """
    import subprocess
    import pandas as pd
    from pyspark import SparkFiles

    self.write_log("Iniciando lectura y validación de archivos desde HDFS")

    # Obtener listado de archivos en el directorio
    try:
        hdfs_ls_cmd = f"hdfs dfs -ls {hdfs_directory}"
        result = subprocess.run(hdfs_ls_cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.returncode != 0:
            raise Exception(f"Error al listar archivos: {result.stderr}")

        found_files = {}
        for line in result.stdout.splitlines():
            parts = line.strip().split()
            if len(parts) >= 8:
                file_path = parts[-1]
                filename = os.path.basename(file_path)
                modified = f"{parts[5]} {parts[6]}"
                found_files[filename] = {
                    "path": file_path,
                    "last_modified": modified
                }
    except Exception as e:
        self.write_log(f"Fallo al obtener listado de archivos: {str(e)}", "ERROR")
        raise

    file_details_list = []

    for filename, file_cfg in files_config.items():
        self.write_log(f"Procesando archivo: {filename}")

        file_ext = filename.split(".")[-1].lower()
        config_format = file_ext if file_ext in ["csv", "txt", "xls", "xlsx"] else "csv"
        delimiter = file_cfg.get("delimiter", ",")
        sheet_name = file_cfg.get("sheet", None)
        spark_view = file_cfg.get("spark_name", filename.split(".")[0])

        detail = {
            "prefix": filename.split(".")[0],
            "format": config_format,
            "path": "N/A",
            "name": filename,
            "last_modified": "N/A",
            "size_in_mb": "N/A",
            "rows": "N/A",
            "sheets": sheet_name or "N/A",
            "status": "Error: file not found"
        }

        if filename in found_files:
            file_path = found_files[filename]["path"]
            detail["path"] = file_path
            detail["last_modified"] = found_files[filename]["last_modified"]

            try:
                # Obtener tamaño en MB
                hdfs_du_cmd = f"hdfs dfs -du -s {file_path}"
                du_result = subprocess.run(hdfs_du_cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                if du_result.returncode == 0:
                    size_bytes = int(du_result.stdout.strip().split()[0])
                    detail["size_in_mb"] = round(size_bytes / (1024 * 1024), 2)

                if config_format in ["csv", "txt"]:
                    df = self.spark.read.option("header", "true").option("delimiter", delimiter).csv(file_path)
                    detail["rows"] = df.count()
                    df.createOrReplaceTempView(spark_view)
                    detail["status"] = "ok"

                elif config_format in ["xls", "xlsx"]:
                    self.spark.sparkContext.addFile(f"hdfs://{hdfs_directory}/{filename}")
                    local_path = SparkFiles.get(filename)

                    df_pd = pd.read_excel(local_path, engine="openpyxl", sheet_name=sheet_name).astype(str)

                    try:
                        for col in df_pd.columns:
                            if df_pd[col].str.contains(r"<[^>]+>", regex=True).any():
                                self.write_log(f"Advertencia: Se detectó contenido XML en columna '{col}' del archivo {filename}. Será limpiado.", "WARNING")
                                df_pd[col] = df_pd[col].str.replace(r"<[^>]+>", "", regex=True)
                    except Exception as e:
                        self.write_log(f"Error al limpiar contenido XML en {filename}: {str(e)}", "ERROR")
                        detail["status"] = f"Error limpiando XML: {str(e)}"
                        file_details_list.append(detail)
                        continue

                    df_spk = self.pandas_to_spark(df_pd, spark_view)
                    detail["rows"] = df_spk.count()
                    detail["status"] = "ok"

                else:
                    detail["status"] = "unsupported file type"

            except Exception as e:
                detail["status"] = f"Error: {str(e)}"
                self.write_log(f"Error procesando {filename}: {str(e)}", "ERROR")
        else:
            self.write_log(f"Archivo esperado no encontrado: {filename}", "WARNING")

        self.write_log(f"Resultado [{filename}]: {detail['status']} | Filas: {detail['rows']}")
        file_details_list.append(detail)

    self.details_df = pd.DataFrame(file_details_list)
    self.write_log("Archivo de detalles actualizado en self.details_df")

    def read_files_and_collect_details(self, hdfs_directory: str, files_config: dict): 
    """ Lee archivos desde HDFS, obtiene metadatos, valida archivos esperados y genera vistas temporales.

            Args:
        hdfs_directory (str): Ruta HDFS donde están los archivos.
        files_config (dict): Diccionario de configuración por archivo esperado.

            Returns:
        None. Actualiza self.details_df
    """
    import subprocess
    import pandas as pd
    from pyspark import SparkFiles

    self.write_log("Iniciando lectura y validación de archivos desde HDFS")

    # Obtener listado de archivos en el directorio
    try:
        hdfs_ls_cmd = f"hdfs dfs -ls {hdfs_directory}"
        result = subprocess.run(hdfs_ls_cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.returncode != 0:
            raise Exception(f"Error al listar archivos: {result.stderr}")

        found_files = {}
        for line in result.stdout.splitlines():
            parts = line.strip().split()
            if len(parts) >= 8:
                file_path = parts[-1]
                filename = os.path.basename(file_path)
                modified = f"{parts[5]} {parts[6]}"
                found_files[filename] = {
                    "path": file_path,
                    "last_modified": modified
                }
    except Exception as e:
        self.write_log(f"Fallo al obtener listado de archivos: {str(e)}", "ERROR")
        raise

    file_details_list = []

    for filename, file_cfg in files_config.items():
        self.write_log(f"Procesando archivo: {filename}")

        file_ext = filename.split(".")[-1].lower()
        config_format = file_ext if file_ext in ["csv", "txt", "xls", "xlsx"] else "csv"
        delimiter = file_cfg.get("delimiter", ",")
        sheet_name = file_cfg.get("sheet", None)
        spark_view = file_cfg.get("spark_name", filename.split(".")[0])

        detail = {
            "prefix": filename.split(".")[0],
            "format": config_format,
            "path": "N/A",
            "name": filename,
            "last_modified": "N/A",
            "size_in_mb": "N/A",
            "rows": "N/A",
            "sheets": sheet_name or "N/A",
            "status": "Error: file not found"
        }

        if filename in found_files:
            file_path = found_files[filename]["path"]
            detail["path"] = file_path
            detail["last_modified"] = found_files[filename]["last_modified"]

            try:
                # Obtener tamaño en MB
                hdfs_du_cmd = f"hdfs dfs -du -s {file_path}"
                du_result = subprocess.run(hdfs_du_cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                if du_result.returncode == 0:
                    size_bytes = int(du_result.stdout.strip().split()[0])
                    detail["size_in_mb"] = round(size_bytes / (1024 * 1024), 2)

                if config_format in ["csv", "txt"]:
                    df = self.spark.read.option("header", "true").option("delimiter", delimiter).csv(file_path)
                    detail["rows"] = df.count()
                    df.createOrReplaceTempView(spark_view)
                    detail["status"] = "ok"

                elif config_format in ["xls", "xlsx"]:
                    self.spark.sparkContext.addFile(f"hdfs://{hdfs_directory}/{filename}")
                    local_path = SparkFiles.get(filename)

                    df_pd = pd.read_excel(local_path, engine="openpyxl", sheet_name=sheet_name).astype(str)

                    try:
                        for col in df_pd.columns:
                            if df_pd[col].str.contains(r"<[^>]+>", regex=True).any():
                                self.write_log(f"Advertencia: Se detectó contenido XML en columna '{col}' del archivo {filename}. Será limpiado.", "WARNING")
                                df_pd[col] = df_pd[col].str.replace(r"<[^>]+>", "", regex=True)
                    except Exception as e:
                        self.write_log(f"Error al limpiar contenido XML en {filename}: {str(e)}", "ERROR")
                        detail["status"] = f"Error limpiando XML: {str(e)}"
                        file_details_list.append(detail)
                        continue

                    df_spk = self.pandas_to_spark(df_pd, spark_view)
                    detail["rows"] = df_spk.count()
                    detail["status"] = "ok"

                else:
                    detail["status"] = "unsupported file type"

            except Exception as e:
                detail["status"] = f"Error: {str(e)}"
                self.write_log(f"Error procesando {filename}: {str(e)}", "ERROR")
        else:
            self.write_log(f"Archivo esperado no encontrado: {filename}", "WARNING")

        self.write_log(f"Resultado [{filename}]: {detail['status']} | Filas: {detail['rows']}")
        file_details_list.append(detail)

    self.details_df = pd.DataFrame(file_details_list)
    self.write_log("Archivo de detalles actualizado en self.details_df")

... def read_files_and_collect_details(self, hdfs_directory: str, files_config: dict): """ Lee archivos desde HDFS, obtiene metadatos, valida archivos esperados y genera vistas temporales.

Args:
        hdfs_directory (str): Ruta HDFS donde están los archivos.
        files_config (dict): Diccionario de configuración por archivo esperado.

    Returns:
        None. Actualiza self.details_df
    """
    import subprocess
    import pandas as pd
    from pyspark import SparkFiles

    self.write_log("Iniciando lectura y validación de archivos desde HDFS")

    # Obtener listado de archivos en el directorio
    try:
        hdfs_ls_cmd = f"hdfs dfs -ls {hdfs_directory}"
        result = subprocess.run(hdfs_ls_cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.returncode != 0:
            raise Exception(f"Error al listar archivos: {result.stderr}")

        found_files = {}
        for line in result.stdout.splitlines():
            parts = line.strip().split()
            if len(parts) >= 8:
                file_path = parts[-1]
                filename = os.path.basename(file_path)
                modified = f"{parts[5]} {parts[6]}"
                found_files[filename] = {
                    "path": file_path,
                    "last_modified": modified
                }
    except Exception as e:
        self.write_log(f"Fallo al obtener listado de archivos: {str(e)}", "ERROR")
        raise

    file_details_list = []

    for filename, file_cfg in files_config.items():
        self.write_log(f"Procesando archivo: {filename}")

        file_ext = filename.split(".")[-1].lower()
        config_format = file_ext if file_ext in ["csv", "txt", "xls", "xlsx"] else "csv"
        delimiter = file_cfg.get("delimiter", ",")
        sheet_name = file_cfg.get("sheet", None)
        spark_view = file_cfg.get("spark_name", filename.split(".")[0])

        detail = {
            "prefix": filename.split(".")[0],
            "format": config_format,
            "path": "N/A",
            "name": filename,
            "last_modified": "N/A",
            "size_in_mb": "N/A",
            "rows": "N/A",
            "sheets": sheet_name or "N/A",
            "status": "Error: file not found"
        }

        if filename in found_files:
            file_path = found_files[filename]["path"]
            detail["path"] = file_path
            detail["last_modified"] = found_files[filename]["last_modified"]

            try:
                # Obtener tamaño en MB
                hdfs_du_cmd = f"hdfs dfs -du -s {file_path}"
                du_result = subprocess.run(hdfs_du_cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                if du_result.returncode == 0:
                    size_bytes = int(du_result.stdout.strip().split()[0])
                    detail["size_in_mb"] = round(size_bytes / (1024 * 1024), 2)

                if config_format in ["csv", "txt"]:
                    df = self.spark.read.option("header", "true").option("delimiter", delimiter).csv(file_path)
                    detail["rows"] = df.count()
                    df.createOrReplaceTempView(spark_view)
                    detail["status"] = "ok"

                elif config_format in ["xls", "xlsx"]:
                    self.spark.sparkContext.addFile(f"hdfs://{hdfs_directory}/{filename}")
                    local_path = SparkFiles.get(filename)

                    df_pd = pd.read_excel(local_path, engine="openpyxl", sheet_name=sheet_name).astype(str)

                    try:
                        for col in df_pd.columns:
                            if df_pd[col].str.contains(r"<[^>]+>", regex=True).any():
                                self.write_log(f"Advertencia: Se detectó contenido XML en columna '{col}' del archivo {filename}. Será limpiado.", "WARNING")
                                df_pd[col] = df_pd[col].str.replace(r"<[^>]+>", "", regex=True)
                    except Exception as e:
                        self.write_log(f"Error al limpiar contenido XML en {filename}: {str(e)}", "ERROR")
                        detail["status"] = f"Error limpiando XML: {str(e)}"
                        file_details_list.append(detail)
                        continue

                    df_spk = self.pandas_to_spark(df_pd, spark_view)
                    detail["rows"] = df_spk.count()
                    detail["status"] = "ok"

                else:
                    detail["status"] = "unsupported file type"

            except Exception as e:
                detail["status"] = f"Error: {str(e)}"
                self.write_log(f"Error procesando {filename}: {str(e)}", "ERROR")
        else:
            self.write_log(f"Archivo esperado no encontrado: {filename}", "WARNING")

        self.write_log(f"Resultado [{filename}]: {detail['status']} | Filas: {detail['rows']}")
        file_details_list.append(detail)

    self.details_df = pd.DataFrame(file_details_list)
    self.write_log("Archivo de detalles actualizado en self.details_df")

def pandas_to_spark(self, pandas_df: pd.DataFrame, temp_view_name: str = None) -> pyspark_df:
    from pyspark.sql.types import StructField, StructType

    def map_dtype(dtype):
        dtype = str(dtype)
        if "datetime" in dtype:
            return StringType()
        elif "int64" in dtype:
            return LongType()
        elif "int32" in dtype:
            return IntegerType()
        elif "float" in dtype:
            return FloatType()
        else:
            return StringType()

    try:
        struct_fields = []
        for col, dtype in zip(pandas_df.columns, pandas_df.dtypes):
            struct_fields.append(StructField(col, map_dtype(dtype), True))

        schema = StructType(struct_fields)
        spark_df = self.spark.createDataFrame(pandas_df.astype(str), schema=schema)

        if temp_view_name:
            view_name = temp_view_name.split(".")[-1]
            spark_df.createOrReplaceTempView(view_name)

        return spark_df

    except Exception as e:
        self.write_log(f"Error en pandas_to_spark para vista '{temp_view_name}': {str(e)}", "ERROR")
        raise

def read_files_and_collect_details(self, hdfs_directory: str, files_config: dict):
    """
    Lee archivos desde HDFS, obtiene metadatos, valida archivos esperados y genera vistas temporales.

    Args:
        hdfs_directory (str): Ruta HDFS donde están los archivos.
        files_config (dict): Diccionario de configuración por archivo esperado.

    Returns:
        None. Actualiza self.details_df
    """
    import os
    import subprocess
    import pandas as pd
    from pyspark import SparkFiles

    self.write_log("Iniciando lectura y validación de archivos desde HDFS")

    # Obtener listado de archivos en el directorio
    try:
        hdfs_ls_cmd = f"hdfs dfs -ls {hdfs_directory}"
        result = subprocess.run(hdfs_ls_cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.returncode != 0:
            raise Exception(f"Error al listar archivos: {result.stderr}")

        found_files = {}
        for line in result.stdout.splitlines():
            parts = line.strip().split()
            if len(parts) >= 8:
                file_path = parts[-1]
                filename = os.path.basename(file_path)
                modified = f"{parts[5]} {parts[6]}"
                found_files[filename] = {
                    "path": file_path,
                    "last_modified": modified
                }
    except Exception as e:
        self.write_log(f"Fallo al obtener listado de archivos: {str(e)}", "ERROR")
        raise

    file_details_list = []

    for filename, file_cfg in files_config.items():
        self.write_log(f"Procesando archivo: {filename}")

        file_ext = filename.split(".")[-1].lower()
        config_format = file_ext if file_ext in ["csv", "txt", "xls", "xlsx"] else "csv"
        delimiter = file_cfg.get("delimiter", ",")
        sheet_name = file_cfg.get("sheet", None)
        spark_view = file_cfg.get("spark_name", filename.split(".")[0])

        detail = {
            "prefix": filename.split(".")[0],
            "format": config_format,
            "path": "N/A",
            "name": filename,
            "last_modified": "N/A",
            "size_in_mb": "N/A",
            "rows": "N/A",
            "sheets": sheet_name or "Todas",
            "status": "Error: file not found"
        }

        if filename in found_files:
            file_path = found_files[filename]["path"]
            detail["path"] = file_path
            detail["last_modified"] = found_files[filename]["last_modified"]

            try:
                # Obtener tamaño en MB
                hdfs_du_cmd = f"hdfs dfs -du -s {file_path}"
                du_result = subprocess.run(hdfs_du_cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                if du_result.returncode == 0:
                    size_bytes = int(du_result.stdout.strip().split()[0])
                    detail["size_in_mb"] = round(size_bytes / (1024 * 1024), 2)

                if config_format in ["csv", "txt"]:
                    df = self.spark.read.option("header", "true").option("delimiter", delimiter).csv(file_path)
                    detail["rows"] = df.count()
                    df.createOrReplaceTempView(spark_view)
                    detail["status"] = "ok"

                elif config_format in ["xls", "xlsx"]:
                    self.spark.sparkContext.addFile(f"hdfs://{hdfs_directory}/{filename}")
                    local_path = SparkFiles.get(filename)

                    all_sheets = pd.read_excel(local_path, engine="openpyxl", sheet_name=None)
                    combined_df = pd.DataFrame()

                    for sheet, sheet_df in all_sheets.items():
                        sheet_df["__sheet_name"] = sheet  # opcional para identificar de dónde viene
                        combined_df = pd.concat([combined_df, sheet_df], ignore_index=True)

                    combined_df = combined_df.astype(str)

                    for col in combined_df.columns:
                        if combined_df[col].str.contains(r"<[^>]+>", regex=True).any():
                            self.write_log(f"Advertencia: Se detectó contenido XML en columna '{col}' del archivo {filename}. Será limpiado.", "WARNING")
                            combined_df[col] = combined_df[col].str.replace(r"<[^>]+>", "", regex=True)

                    df_spk = self.pandas_to_spark(combined_df, spark_view)
                    detail["rows"] = df_spk.count()
                    detail["status"] = "ok"

                else:
                    detail["status"] = "unsupported file type"

            except Exception as e:
                detail["status"] = f"Error: {str(e)}"
                self.write_log(f"Error procesando {filename}: {str(e)}", "ERROR")
        else:
            self.write_log(f"Archivo esperado no encontrado: {filename}", "WARNING")

        self.write_log(f"Resultado [{filename}]: {detail['status']} | Filas: {detail['rows']}")
        file_details_list.append(detail)

    self.details_df = pd.DataFrame(file_details_list)
    self.write_log("Archivo de detalles actualizado en self.details_df")



def Historica(conex, fechas_iniciales, fechas_finales, diaria, mensual, campoFecha, processdate):
    hoy = datetime.date.today()  # Fecha del día de hoy
    ocho_mes = hoy.day in range(7, 16)  # Validar que la fecha de hoy esté entre el 7 y el 15 del mes
    rangos_max = conex.spark.table(mensual).select('*')  # Obtener el mes máximo de la tabla mensual
    r_max = rangos_max.selectExpr("sf-max(processdate) as max_date").collect()[0]["max_date"]
    format = "%Y-%m-%d"

    try:
        print("Intentando obtener fecha máxima")
        print("r_max:", r_max)
        d = datetime.datetime.strptime(r_max, format)
    except Exception as e:
        print('Excepción al obtener fecha máxima:', e)
        r_max = datetime.date.today() + relativedelta(months=-1)
        d = datetime.datetime.strptime(r_max, format)
    
    print("Mes actual:", d.month, "Mes de hoy:", hoy.month)
    mes_pasado = d.month

    if ocho_mes:
        if (d.month == 12 and hoy.month == 1) or (d.month == hoy.month):
            print("Escribiendo tablas para el mes actual")
            for i, j in zip(range(len(fechas_iniciales)), range(len(fechas_finales))):
                if fechas_iniciales[i].month == mes_pasado:
                    start_date = fechas_iniciales[i]
                    end_date = fechas_finales[j]
                    print("Iniciando consulta para:", start_date, "-", end_date)
                    consulta_tablas(conex, start_date, end_date, diaria, mensual, campoFecha)
                else:
                    print("La tabla ya estaba actualizada")
            
            # Verificar y realizar ingesta para meses anteriores si no están actualizados
            meses_faltantes = [mes_pasado - 1, mes_pasado - 2]  # Mes anterior y anteanterior
            for mes in meses_faltantes:
                if mes >= 1:
                    print(f"Verificando ingesta para el mes {mes}")
                    fecha_inicial = datetime.date(hoy.year, mes, 1)
                    fecha_final = datetime.date(hoy.year, mes, 30)  # Asumiendo mes completo por simplicidad
                    print("Iniciando consulta para:", fecha_inicial, "-", fecha_final)
                    consulta_tablas(conex, fecha_inicial, fecha_final, diaria, mensual, campoFecha)
        else:
            print("No se hace nada")
    else:
        print("No se hace nada porque no está en el rango de fechas esperado")

def consulta_tablas(conex, start_date, end_date, diaria, mensual, campoFecha):
    tabla = conex.spark.table(diaria).select("*").where(f"{campoFecha} between '{start_date}' and '{end_date}'")
    conex.DLake_Replace(tabla, mensual)

import datetime
from dateutil.relativedelta import relativedelta

def Historica(conex, diaria, mensual, campoFecha, processdate):
    hoy = datetime.date.today()
    format = "%Y-%m-%d"

    try:
        r_max = conex.spark.table(mensual).selectExpr(f"max({processdate}) as max_date").collect()[0]["max_date"]
        ultima_fecha = datetime.datetime.strptime(r_max, format).date()
    except:
        print("No se encontró fecha máxima, se asumirá un inicio manual.")
        ultima_fecha = datetime.date(hoy.year, hoy.month, 1) - relativedelta(months=6)

    # Crear lista de meses faltantes desde la última fecha hasta el mes actual
    mes_actual = datetime.date(hoy.year, hoy.month, 1)
    fecha_iteracion = ultima_fecha + relativedelta(months=1)

    while fecha_iteracion < mes_actual:
        inicio_mes = fecha_iteracion
        fin_mes = (inicio_mes + relativedelta(months=1)) - datetime.timedelta(days=1)

        print(f"Ingestando datos del {inicio_mes} al {fin_mes}")
        consulta_tablas(conex, inicio_mes, fin_mes, diaria, mensual, campoFecha)

        fecha_iteracion += relativedelta(months=1)

def consulta_tablas(conex, start_date, end_date, diaria, mensual, campoFecha):
    tabla = conex.spark.table(diaria).select("*").where(f"{campoFecha} BETWEEN '{start_date}' AND '{end_date}'")
    conex.DLake_Replace(tabla, mensual)


import datetime
from dateutil.relativedelta import relativedelta

def Historica(conex, fechas_iniciales, fechas_finales, diaria, mensual, campoFecha, processdate):
    hoy = datetime.date.today()  # Fecha actual
    print(f"Hoy: {hoy}")

    ocho_mes = hoy.day in range(7, 16)  # Ejecutar entre el 7 y el 15 del mes

    # Obtener la fecha máxima ingestada (último processdate en tabla mensual)
    try:
        r_max_row = conex.spark.table(mensual).selectExpr(f"max({processdate}) as max_date").collect()
        r_max = r_max_row[0]["max_date"]

        if isinstance(r_max, str):
            r_max = datetime.datetime.strptime(r_max, "%Y-%m-%d").date()
        elif isinstance(r_max, datetime.datetime):
            r_max = r_max.date()
        elif isinstance(r_max, datetime.date):
            pass  # ya está bien
        else:
            raise ValueError("Tipo de dato inesperado en fecha máxima")

    except Exception as e:
        print("No se pudo obtener la fecha máxima, usando mes anterior como fallback:", e)
        r_max = hoy + relativedelta(months=-1)

    print("Última fecha ingestada:", r_max)

    mes_pasado = r_max.month
    año_pasado = r_max.year

    # Si estamos en la ventana de días (7 al 15)
    if ocho_mes:
        # Si aún no se ha ingestido el mes actual
        if (r_max.month == 12 and hoy.month == 1 and hoy.year > r_max.year) or \
           (r_max.month < hoy.month or (r_max.month == hoy.month and hoy.year > r_max.year)):

            print("Ingestando tabla mensual del mes anterior...")

            for i, j in zip(range(len(fechas_iniciales)), range(len(fechas_finales))):
                if fechas_iniciales[i].month == mes_pasado:
                    start_date = fechas_iniciales[i]
                    end_date = fechas_finales[j]
                    print(f"Ingestando: {start_date} - {end_date}")
                    consulta_tablas(conex, start_date, end_date, diaria, mensual, campoFecha)

            # 🔁 Extra: intentar ingestar también meses anteriores faltantes
            meses_faltantes = [(hoy.year, hoy.month - 1), (hoy.year, hoy.month - 2)]
            for año, mes in meses_faltantes:
                if mes <= 0:
                    mes += 12
                    año -= 1

                ya_ingestado = (mes == mes_pasado and año == año_pasado)
                if not ya_ingestado:
                    try:
                        fecha_inicio = datetime.date(año, mes, 1)
                        fecha_fin = fecha_inicio + relativedelta(day=31)
                        print(f"Intentando ingestar: {fecha_inicio} - {fecha_fin}")
                        consulta_tablas(conex, fecha_inicio, fecha_fin, diaria, mensual, campoFecha)
                    except Exception as e:
                        print(f"No se pudo ingestar mes {mes}/{año}:", e)
        else:
            print("La tabla ya estaba actualizada")
    else:
        print("No es fecha de ejecución (fuera del rango 7 al 15)")


def consulta_tablas(conex, start_date, end_date, diaria, mensual, campoFecha):
    tabla = conex.spark.table(diaria).select("*").where(
        f"{campoFecha} BETWEEN '{start_date}' AND '{end_date}'"
    )
    print(f"Reemplazando datos en tabla mensual entre {start_date} y {end_date}")
    conex.DLake_Replace(tabla, mensual)


import datetime
from dateutil.relativedelta import relativedelta

def Historica(conex, diaria, mensual, campoFecha, processdate):
    hoy = datetime.datetime.today().date()  # Corrección: obtener la fecha actual correctamente
    format = "%Y-%m-%d"

    # Obtener el mes anterior y el mes anteanterior
    mes_anterior = hoy.replace(day=1) - relativedelta(months=1)
    mes_anteanterior = hoy.replace(day=1) - relativedelta(months=2)

    print("Mes anterior:", mes_anterior.strftime("%B %Y"))
    print("Mes anteanterior:", mes_anteanterior.strftime("%B %Y"))

    # Obtener fechas ya presentes en la tabla mensual
    try:
        fechas_ingestadas = conex.spark.table(mensual).select(processdate).distinct().toPandas()
        fechas_ingestadas[processdate] = pd.to_datetime(fechas_ingestadas[processdate])
        meses_ingestados = fechas_ingestadas[processdate].dt.to_period("M").unique().tolist()
        meses_ingestados = [str(m) for m in meses_ingestados]
    except Exception as e:
        print("No se pudo leer la tabla mensual o está vacía:", e)
        meses_ingestados = []

    def ingesta_mes(fecha_base):
        mes_str = fecha_base.strftime("%Y-%m")
        if mes_str not in meses_ingestados:
            start_date = fecha_base.replace(day=1)
            end_date = (start_date + relativedelta(months=1)) - datetime.timedelta(days=1)
            print(f"Iniciando ingesta de {mes_str} desde {start_date} hasta {end_date}")
            
            # Validamos si hay datos disponibles en la tabla diaria
            tabla_filtrada = conex.spark.table(diaria).where(
                f"{campoFecha} between '{start_date}' and '{end_date}'"
            )
            if tabla_filtrada.take(1):  # Si hay al menos una fila
                conex.DLake_Replace(tabla_filtrada, mensual)
                print(f"Ingesta exitosa para {mes_str}")
            else:
                print(f"No hay datos disponibles en diaria para {mes_str}, se omite.")
        else:
            print(f"El mes {mes_str} ya fue ingestado, se omite.")

    # Ingestar mes anterior
    ingesta_mes(mes_anterior)

    # Solo intentar mes anteanterior si no está presente
    if mes_anteanterior.strftime("%Y-%m") not in meses_ingestados:
        ingesta_mes(mes_anteanterior)

def ingesta_mes(fecha_base):
    mes_str = fecha_base.strftime("%Y-%m")
    if mes_str not in meses_ingestados:
        start_date = fecha_base.replace(day=1)
        end_date = (start_date + relativedelta(months=1)) - datetime.timedelta(days=1)
        print(f"Iniciando ingesta de {mes_str} desde {start_date} hasta {end_date}")

        # Leer tabla diaria
        tabla_filtrada = conex.spark.table(diaria).where(
            f"{campoFecha} between '{start_date}' and '{end_date}'"
        )

        if tabla_filtrada.take(1):  # Si hay al menos una fila
            # Eliminar columna processdate si ya existe, para evitar duplicados
            if processdate in tabla_filtrada.columns:
                tabla_filtrada = tabla_filtrada.drop(processdate)
            
            # Agregar columna de fecha de proceso
            tabla_filtrada = tabla_filtrada.withColumn(processdate, sf.lit(str(datetime.datetime.today().date())))
            
            # Reemplazar en tabla mensual
            conex.DLake_Replace(tabla_filtrada, mensual)
            print(f"Ingesta exitosa para {mes_str}")
        else:
            print(f"No hay datos disponibles en diaria para {mes_str}, se omite.")
    else:
        print(f"El mes {mes_str} ya fue ingestado, se omite.")

def Historica(conex, fechas_iniciales, fechas_finales, diaria, mensual, campoFecha, processdate):
    hoy = datetime.datetime.today().date()
    ocho_mes = hoy.day in range(7, 16)  # Solo ejecutar entre el día 7 y 15

    # Obtener meses ya existentes en la tabla mensual
    try:
        rangos_max = conex.spark.table(mensual).select(processdate)
        meses_ingestados = rangos_max.select(
            sf.date_format(processdate, "yyyy-MM")
        ).distinct().rdd.map(lambda x: x[0]).collect()
    except Exception as e:
        print("No se pudo leer la tabla mensual, se asume vacía.")
        print("Error:", e)
        meses_ingestados = []

    print("Meses ya ingresados en la tabla mensual:", meses_ingestados)

    # Obtener mes anterior (abril si estamos en mayo) y mes ante-anterior (marzo)
    mes_actual = hoy.replace(day=1)
    mes_anterior = mes_actual - relativedelta(months=1)
    mes_anteanterior = mes_actual - relativedelta(months=2)

    def ingesta_mes(fecha_base):
        mes_str = fecha_base.strftime("%Y-%m")
        if mes_str not in meses_ingestados:
            start_date = fecha_base.replace(day=1)
            end_date = (start_date + relativedelta(months=1)) - datetime.timedelta(days=1)
            print(f"Iniciando ingesta de {mes_str} desde {start_date} hasta {end_date}")

            tabla_filtrada = conex.spark.table(diaria).where(
                f"{campoFecha} between '{start_date}' and '{end_date}'"
            )

            if tabla_filtrada.take(1):  # Si hay datos
                # Eliminar 'processdate' si ya existe
                if processdate in tabla_filtrada.columns:
                    tabla_filtrada = tabla_filtrada.drop(processdate)
                
                # Agregar columna de fecha de proceso
                hoy_str = str(datetime.datetime.today().date())
                tabla_filtrada = tabla_filtrada.withColumn(processdate, sf.lit(hoy_str))

                # Reemplazar en tabla mensual
                conex.DLake_Replace(tabla_filtrada, mensual)
                print(f"Ingesta exitosa para {mes_str}")
            else:
                print(f"No hay datos en diaria para {mes_str}, se omite.")
        else:
            print(f"El mes {mes_str} ya fue ingresado, se omite.")

    if ocho_mes:
        ingesta_mes(mes_anterior)       # Intentar abril (si estamos en mayo)
        ingesta_mes(mes_anteanterior)   # Solo si marzo no está
    else:
        print("No está en el rango de fechas (7-15), no se hace nada.")

    import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as sf

def Historica(conex, fechas_iniciales, fechas_finales, diaria, mensual, campoFecha, processdate):
    hoy = datetime.datetime.today().date()
    ocho_mes = hoy.day in range(7, 16)  # Solo ejecutar entre el día 7 y 15

    # Obtener meses ya existentes en la tabla mensual
    try:
        rangos_max = conex.spark.table(mensual).select(processdate)
        meses_ingestados = rangos_max.select(
            sf.date_format(processdate, "yyyy-MM")
        ).distinct().rdd.map(lambda x: x[0]).collect()
    except Exception as e:
        print("No se pudo leer la tabla mensual, se asume vacía.")
        print("Error:", e)
        meses_ingestados = []

    print("Meses ya ingresados en la tabla mensual:", meses_ingestados)

    # Obtener mes anterior y ante-anterior
    mes_actual = hoy.replace(day=1)
    mes_anterior = mes_actual - relativedelta(months=1)
    mes_anteanterior = mes_actual - relativedelta(months=2)

    def ingesta_mes(fecha_base):
        mes_str = fecha_base.strftime("%Y-%m")
        if mes_str not in meses_ingestados:
            start_date = fecha_base.replace(day=1)
            end_date = (start_date + relativedelta(months=1)) - datetime.timedelta(days=1)
            print(f"Iniciando ingesta de {mes_str} desde {start_date} hasta {end_date}")

            tabla_filtrada = conex.spark.table(diaria).where(
                f"{campoFecha} between '{start_date}' and '{end_date}'"
            )

            if tabla_filtrada.take(1):  # Si hay datos
                # Eliminar 'processdate' si ya existe, sin importar formato
                columnas_actuales = [col.lower() for col in tabla_filtrada.columns]
                if processdate.lower() in columnas_actuales:
                    print(f"Columna {processdate} ya existe, se eliminará.")
                    tabla_filtrada = tabla_filtrada.drop(processdate)

                # Agregar columna de fecha de proceso
                hoy_str = str(datetime.datetime.today().date())
                tabla_filtrada = tabla_filtrada.withColumn(processdate, sf.lit(hoy_str))

                # Reemplazar en tabla mensual
                conex.DLake_Replace(tabla_filtrada, mensual)
                print(f"Ingesta exitosa para {mes_str}")
            else:
                print(f"No hay datos en diaria para {mes_str}, se omite.")
        else:
            print(f"El mes {mes_str} ya fue ingresado, se omite.")

    if ocho_mes:
        ingesta_mes(mes_anterior)       # Intentar abril (si estamos en mayo)
        ingesta_mes(mes_anteanterior)   # Solo si marzo no está
    else:
        print("No está en el rango de fechas (7-15), no se hace nada.")

import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as sf

def Historica(conex, fechas_iniciales, fechas_finales, diaria, mensual, campoFecha, processdate):
    hoy = datetime.date.today()
    ocho_mes = hoy.day in range(7, 16)

    try:
        r_max = conex.spark.table(mensual).selectExpr(f"max({processdate}) as max_date").collect()[0]["max_date"]
        d = datetime.datetime.strptime(r_max, "%Y-%m-%d").date()
    except Exception as e:
        print("Error al obtener fecha máxima:", e)
        d = hoy + relativedelta(months=-1)

    mes_actual = hoy.month
    anio_actual = hoy.year

    # Definimos los meses a validar: anteanterior y anterior
    meses = [
        (anio_actual, mes_actual - 2),  # mes anteanterior
        (anio_actual, mes_actual - 1),  # mes anterior
    ]

    # Ajustamos si los meses quedan en valores negativos
    meses = [(a - 1 if m <= 0 else a, m + 12 if m <= 0 else m) for a, m in meses]

    if ocho_mes:
        insertar_anteriores = []
        for anio, mes in meses:
            existe = conex.spark.table(mensual).filter(
                f"month({processdate}) = {mes} and year({processdate}) = {anio}"
            ).count() > 0
            insertar_anteriores.append((anio, mes, not existe))

        # Solo insertar mes anteanterior si también falta el mes anterior
        if insertar_anteriores[1][2]:  # Si mes anterior no existe
            for idx in [0, 1]:  # Anteanterior y anterior
                anio, mes, debe_insertar = insertar_anteriores[idx]
                if debe_insertar:
                    start_date = datetime.date(anio, mes, 1)
                    end_date = (start_date + relativedelta(months=1)) - datetime.timedelta(days=1)

                    tabla_filtrada = conex.spark.table(diaria).filter(
                        f"{campoFecha} between '{start_date}' and '{end_date}'"
                    )

                    if idx == 0:  # Mes anteanterior
                        process_date_value = (end_date + relativedelta(months=1)).replace(day=1) - datetime.timedelta(days=1)
                    else:  # Mes anterior
                        process_date_value = end_date

                    print(f"Ingestando mes {mes}/{anio} con processdate: {process_date_value}")
                    tabla_final = tabla_filtrada.withColumn(processdate, sf.lit(str(process_date_value)))
                    conex.DLake_Replace(tabla_final, mensual)
                else:
                    print(f"Ya existe información para {mes}/{anio}, no se ingesta.")
        else:
            print("El mes anterior ya existe, no se insertará el anteanterior.")
    else:
        print("No estás entre el 7 y el 15 del mes. No se hace nada.")
#dinamico

SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT INTO tabla_destino PARTITION (processdate)
SELECT columna1, columna2, columna3, processdate
FROM tabla_origen
WHERE condiciones;
#no dinamico
INSERT INTO tabla_destino PARTITION (processdate='2025-04-30')
SELECT columna1, columna2, columna3  -- sin processdate
FROM tabla_origen
WHERE condiciones;

def read_files_and_collect_details(self, hdfs_directory: str, files_config: dict):
    """
    Lee archivos desde HDFS, obtiene metadatos, valida archivos esperados y genera vistas temporales.

    Args:
        hdfs_directory (str): Ruta HDFS donde están los archivos.
        files_config (dict): Diccionario de configuración por archivo esperado:
            {
              "archivo.xlsx": {"delimiter": ",", "sheet": "Hoja1", "spark_name": "vista1"},
              "otro.csv":   {"delimiter": ";",               "spark_name": "vista2"}
            }
    Returns:
        None. Actualiza self.details_df con un DataFrame de pandas.
    """
    import os
    import subprocess
    import pandas as pd
    from pyspark import SparkFiles

    self.write_log("Iniciando lectura y validación de archivos desde HDFS")

    # 1) Listar archivos en HDFS
    try:
        hdfs_ls_cmd = f"hdfs dfs -ls {hdfs_directory}"
        result = subprocess.run(hdfs_ls_cmd.split(), stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, text=True)
        if result.returncode != 0:
            raise Exception(f"Error al listar archivos: {result.stderr}")

        found_files = {}
        for line in result.stdout.splitlines():
            parts = line.strip().split()
            if len(parts) >= 8:
                file_path = parts[-1]
                filename = os.path.basename(file_path)
                modified = f"{parts[5]} {parts[6]}"
                found_files[filename] = {"path": file_path, "last_modified": modified}
    except Exception as e:
        self.write_log(f"Fallo al obtener listado de archivos: {e}", "ERROR")
        raise

    file_details_list = []

    # 2) Procesar cada archivo según files_config
    for filename, cfg in files_config.items():
        self.write_log(f"Procesando archivo: {filename}")

        file_ext = filename.split(".")[-1].lower()
        fmt = file_ext if file_ext in ["csv", "txt", "xls", "xlsx"] else "csv"
        delim      = cfg.get("delimiter",    ",")
        sheet_name = cfg.get("sheet",        None)
        spark_view = cfg.get("spark_name", filename.split(".")[0])

        detail = {
            "prefix":       filename.split(".")[0],
            "format":       fmt,
            "path":         "N/A",
            "name":         filename,
            "last_modified":"N/A",
            "size_in_mb":   "N/A",
            "rows":         "N/A",
            "sheets":       sheet_name or "Todas",
            "status":       "Error: file not found"
        }

        # Si existe en HDFS
        if filename in found_files:
            file_path = found_files[filename]["path"]
            detail["path"]          = file_path
            detail["last_modified"] = found_files[filename]["last_modified"]

            try:
                # 3) Tamaño en MB
                hdfs_du = f"hdfs dfs -du -s {file_path}"
                du_res = subprocess.run(hdfs_du.split(), stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE, text=True)
                if du_res.returncode == 0:
                    b = int(du_res.stdout.strip().split()[0])
                    detail["size_in_mb"] = round(b/1024/1024,2)

                # 4) Lectura según formato
                if fmt in ["csv", "txt"]:
                    df = (self.spark.read
                          .option("header","true")
                          .option("delimiter",delim)
                          .csv(file_path))
                    detail["rows"] = df.count()
                    df.createOrReplaceTempView(spark_view)
                    detail["status"] = "ok"

                else:  # Excel
                    # Traer al nodo local
                    self.spark.sparkContext.addFile(file_path)
                    local_path = SparkFiles.get(filename)

                    # LEER SOLO HOJA ESPECÍFICA O TODAS
                    if sheet_name:
                        pdf = pd.read_excel(local_path,
                                             sheet_name=sheet_name,
                                             engine="openpyxl")
                        pdf["__sheet_name"] = sheet_name
                    else:
                        all_sheets = pd.read_excel(local_path,
                                                   sheet_name=None,
                                                   engine="openpyxl")
                        pdf = pd.concat(
                            [df.assign(__sheet_name=name)
                             for name,df in all_sheets.items()],
                            ignore_index=True
                        )

                    # Forzar a texto y limpiar HTML/XML
                    pdf = pdf.astype(str)
                    for col in pdf.columns:
                        if pdf[col].str.contains(r"<[^>]+>", regex=True).any():
                            self.write_log(f"Advertencia: limpiando HTML en '{col}'", "WARNING")
                            pdf[col] = pdf[col].str.replace(r"<[^>]+>", "", regex=True)

                    # Crear DataFrame Spark
                    sdf = self.pandas_to_spark(pdf, spark_view)
                    detail["rows"] = sdf.count()
                    detail["status"] = "ok"

            except Exception as e:
                detail["status"] = f"Error: {e}"
                self.write_log(f"Error procesando {filename}: {e}", "ERROR")

        else:
            self.write_log(f"Archivo esperado no encontrado: {filename}", "WARNING")

        self.write_log(f"Resultado [{filename}]: {detail['status']} | Filas: {detail['rows']}")
        file_details_list.append(detail)

    # 5) Guardar resumen
    import pandas as pd
    self.details_df = pd.DataFrame(file_details_list)
    self.write_log("Archivo de detalles actualizado en self.details_df")

def read_files_and_collect_details(self, hdfs_directory: str, files_config: dict):
    """
    Lee archivos desde HDFS, obtiene metadatos, valida archivos esperados y genera vistas temporales.
    """
    import os
    import subprocess
    import pandas as pd
    from pyspark import SparkFiles

    self.write_log("Iniciando lectura y validación de archivos desde HDFS")

    # 1) Listar archivos en HDFS
    try:
        hdfs_ls_cmd = f"hdfs dfs -ls {hdfs_directory}"
        result = subprocess.run(hdfs_ls_cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.returncode != 0:
            raise Exception(f"Error al listar archivos: {result.stderr}")

        found_files = {}
        for line in result.stdout.splitlines():
            parts = line.strip().split()
            if len(parts) >= 8:
                file_path = parts[-1]
                filename = os.path.basename(file_path)
                modified = f"{parts[5]} {parts[6]}"
                found_files[filename] = {
                    "path": file_path,
                    "last_modified": modified
                }
    except Exception as e:
        self.write_log(f"Fallo al obtener listado de archivos: {e}", "ERROR")
        raise

    file_details_list = []

    # 2) Procesar cada archivo según files_config
    for filename, file_cfg in files_config.items():
        self.write_log(f"Procesando archivo: {filename}")

        file_ext     = filename.split(".")[-1].lower()
        config_format= file_ext if file_ext in ["csv", "txt", "xls", "xlsx"] else "csv"
        delimiter    = file_cfg.get("delimiter", ",")
        sheet_name   = file_cfg.get("sheet", None)
        spark_view   = file_cfg.get("spark_name", filename.split(".")[0])

        detail = {
            "prefix": filename.split(".")[0],
            "format": config_format,
            "path": "N/A",
            "name": filename,
            "last_modified": "N/A",
            "size_in_mb": "N/A",
            "rows": "N/A",
            "sheets": sheet_name or "Todas",
            "status": "Error: file not found"
        }

        if filename in found_files:
            file_path = found_files[filename]["path"]
            detail["path"]          = file_path
            detail["last_modified"] = found_files[filename]["last_modified"]

            try:
                # 3) Tamaño en MB
                hdfs_du_cmd = f"hdfs dfs -du -s {file_path}"
                du_result   = subprocess.run(hdfs_du_cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                if du_result.returncode == 0:
                    size_bytes = int(du_result.stdout.strip().split()[0])
                    detail["size_in_mb"] = round(size_bytes / (1024 * 1024), 2)

                # 4) Lectura según formato
                if config_format in ["csv", "txt"]:
                    df = ( self.spark.read
                           .option("header", "true")
                           .option("delimiter", delimiter)
                           .csv(file_path) )
                    detail["rows"] = df.count()
                    df.createOrReplaceTempView(spark_view)
                    detail["status"] = "ok"

                else:  # Excel
                    # bajar al nodo local
                    self.spark.sparkContext.addFile(file_path)
                    local_path = SparkFiles.get(filename)

                    if sheet_name:
                        # leer solo la hoja indicada
                        combined_df = pd.read_excel(local_path, sheet_name=sheet_name, engine="openpyxl")
                        combined_df["__sheet_name"] = sheet_name
                    else:
                        # leer todas las hojas
                        all_sheets = pd.read_excel(local_path, sheet_name=None, engine="openpyxl")
                        combined_df = pd.DataFrame()
                        for sheet, sheet_df in all_sheets.items():
                            sheet_df["__sheet_name"] = sheet
                            combined_df = pd.concat([combined_df, sheet_df], ignore_index=True)

                    # convertir a string y limpiar HTML/XML
                    combined_df = combined_df.astype(str)
                    for col in combined_df.columns:
                        if combined_df[col].str.contains(r"<[^>]+>", regex=True).any():
                            self.write_log(f"Advertencia: limpiando HTML en '{col}'", "WARNING")
                            combined_df[col] = combined_df[col].str.replace(r"<[^>]+>", "", regex=True)

                    df_spk = self.pandas_to_spark(combined_df, spark_view)
                    detail["rows"]   = df_spk.count()
                    detail["status"] = "ok"

            except Exception as e:
                detail["status"] = f"Error: {e}"
                self.write_log(f"Error procesando {filename}: {e}", "ERROR")

        else:
            self.write_log(f"Archivo esperado no encontrado: {filename}", "WARNING")

        self.write_log(f"Resultado [{filename}]: {detail['status']} | Filas: {detail['rows']}")
        file_details_list.append(detail)

    # 5) Guardar resumen en pandas DataFrame
    import pandas as pd
    self.details_df = pd.DataFrame(file_details_list)
    self.write_log("Archivo de detalles actualizado en self.details_df")
