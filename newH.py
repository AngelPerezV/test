def Historica(conex, fechas_iniciales, fechas_finales, diaria, mensual, campofecha, processdate):
    """
    Gestiona la ingesta de datos históricos mensuales, asegurando que los meses
    faltantes se procesen en secuencia (hasta un máximo de 2 por ejecución).

    Args:
        conex: Objeto de conexión a Spark.
        fechas_iniciales (dict): Diccionario de fechas de inicio de los rangos.
        fechas_finales (dict): Diccionario de fechas de fin de los rangos.
        diaria (str): Nombre de la tabla diaria.
        mensual (str): Nombre de la tabla mensual.
        campofecha (str): Columna de fecha en la tabla diaria.
        processdate (str): Columna de fecha de procesamiento en la tabla mensual.
    """
    hoy = datetime.today().date()
    formats_to_try = [
        '%Y-%m-%d', '%Y/%m/%d', '%Y%m%d',
        '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f'
    ]

    # Regla de negocio: La ejecución solo ocurre entre los días 7 y 15 del mes.
    if not (7 <= hoy.day <= 15):
        conex.write_log(f"No estás en rango de ejecución mensual (día {hoy.day}). Se ejecuta solo del día 7 al 15.", "INFO")
        return

    # --- Bucle para procesar hasta 2 meses faltantes en una sola ejecución ---
    for i in range(2):
        conex.write_log(f"Iniciando ciclo de verificación #{i+1} para la ingesta mensual.", "INFO")

        # 1. Obtener la fecha máxima ya procesada en la tabla mensual
        max_processed_date = None
        try:
            rangos_max = conex.spark.table(mensual).selectExpr(f"max({processdate}) as max_date").collect()
            if rangos_max and rangos_max[0] and rangos_max[0][0]:
                r_max = rangos_max[0][0]
                max_processed_date = _parse_date(r_max, formats_to_try)
                if not max_processed_date:
                    conex.write_log(f"La fecha máxima procesada '{r_max}' no coincide con los formatos esperados.", "ERROR")
                    raise ValueError(f"Formato de fecha de processdate no reconocido: '{r_max}'")
        except Exception as e:
            conex.write_log(f"No se pudo obtener la fecha máxima procesada. Asumiendo tabla vacía. Error: {e}", "WARNING")
            max_processed_date = None # Aseguramos que es None si hay error

        # 2. Determinar el mes que necesita ser procesado
        month_to_process_start = None
        if max_processed_date:
            last_day_of_max_month = date(max_processed_date.year, max_processed_date.month,
                                         calendar.monthrange(max_processed_date.year, max_processed_date.month)[1])
            if max_processed_date >= last_day_of_max_month:
                month_to_process_start = last_day_of_max_month + relativedelta(days=1)
                conex.write_log(f"Mes {max_processed_date.strftime('%Y-%m')} ya procesado. Siguiente a considerar: {month_to_process_start.strftime('%Y-%m')}.", "INFO")
            else:
                month_to_process_start = date(max_processed_date.year, max_processed_date.month, 1)
                conex.write_log(f"Mes {max_processed_date.strftime('%Y-%m')} incompleto. Se procesará para completarlo.", "INFO")
        else:
            # Si la tabla está vacía, se inicia con el mes antepasado.
            month_to_process_start = (hoy - relativedelta(months=2)).replace(day=1)
            conex.write_log(f"Tabla mensual vacía. Iniciando ingesta desde {month_to_process_start.strftime('%Y-%m')}.", "INFO")

        # 3. Validar si el mes a procesar es el actual o futuro
        if month_to_process_start.year == hoy.year and month_to_process_start.month == hoy.month:
            conex.write_log(f"El mes a procesar ({month_to_process_start.strftime('%Y-%m')}) es el mes en curso. No se procesará. Proceso finalizado.", "INFO")
            break # Salir del bucle, ya no hay más meses pasados que procesar.
        if month_to_process_start > hoy:
            conex.write_log(f"El mes a procesar ({month_to_process_start.strftime('%Y-%m')}) es futuro. No se procesará. Proceso finalizado.", "INFO")
            break # Salir del bucle.

        # 4. Encontrar el rango de fechas de los diccionarios de entrada
        found_range_to_process = False
        sorted_initial_keys = sorted(fechas_iniciales.keys())

        for initial_key in sorted_initial_keys:
            expected_final_key = initial_key.replace('first', 'last')
            if expected_final_key not in fechas_finales:
                msg = f"La clave '{expected_final_key}' no se encontró en 'fechas_finales' para emparejar con '{initial_key}'."
                conex.write_log(msg, "ERROR")
                raise ValueError(msg)

            start_date_candidate = _parse_date(fechas_iniciales[initial_key], formats_to_try)
            end_date_candidate = _parse_date(fechas_finales[expected_final_key], formats_to_try)

            if not start_date_candidate or not end_date_candidate:
                msg = f"No se pudo parsear una de las fechas para las claves {initial_key}/{expected_final_key}."
                conex.write_log(msg, "ERROR")
                raise TypeError(msg)

            if (start_date_candidate.month == month_to_process_start.month and
                start_date_candidate.year == month_to_process_start.year):
                
                conex.write_log(f"Ejecutando ingesta mensual para el rango: {start_date_candidate} a {end_date_candidate}", "INFO")
                
                # Se agrega una columna de partición para una escritura segura y eficiente.
                partition_value = start_date_candidate.strftime('%Y-%m')
                consulta_tablas(conex, start_date_candidate, end_date_candidate, diaria, mensual, campofecha, partition_value)
                
                found_range_to_process = True
                break
        
        if not found_range_to_process:
            conex.write_log(f"No se encontró un rango de fechas para el mes {month_to_process_start.strftime('%Y-%m')}. Proceso finalizado.", "WARNING")
            break # Si no encontramos el mes, detenemos el bucle.
            
# --- Función de Consulta Refactorizada ---
def consulta_tablas(conex, start_date, end_date, diaria, mensual, campoFecha, partition_value):
    """
    Consulta la tabla diaria, procesa los datos y los inserta en la tabla mensual
    sobrescribiendo la partición correspondiente.

    Args:
        conex: Objeto de conexión a Spark.
        start_date (date): Fecha de inicio del rango a procesar.
        end_date (date): Fecha de fin del rango a procesar.
        diaria (str): Nombre de la tabla diaria de origen.
        mensual (str): Nombre de la tabla mensual de destino.
        campoFecha (str): Nombre de la columna de fecha para filtrar.
        partition_value (str): Valor para la partición (ej. '2025-08').
    """
    conex.write_log(f"[consulta_tablas] Procesando desde {start_date} hasta {end_date} para la tabla [{mensual}]", "INFO")
    
    try:
        # Consulta para extraer los datos del mes completo
        tabla = conex.spark.table(diaria).where(F.col(campoFecha).between(start_date, end_date))

        # Añadir la columna de partición con el valor del mes que se está procesando
        # Se asume que la tabla mensual está particionada por 'partition_month' (o un nombre similar).
        # AJUSTA 'partition_month' AL NOMBRE REAL DE TU COLUMNA DE PARTICIÓN.
        tabla = tabla.withColumn("partition_month", F.lit(partition_value))

        # Lógica de cifras de control (código corregido y funcional)
        # Nota: La lógica específica puede requerir ajustes según el esquema de tu tabla.
        if "UNP" in mensual: # Ejemplo de condición más robusta
            cifras_control = tabla.agg(
                F.min("BusinessActivityDate").alias("min_acty_dt"),
                F.max("BusinessActivityDate").alias("max_acty_dt"),
                F.count("TotalPromisesNumber").alias("Registros")
            )
            conex.report_final([("Promesas no cumplidas M", cifras_control)])
        elif "RAK" in mensual:
            cifras_control = tabla.agg(
                F.min("ProcessDate").alias("Fecha_minima"),
                F.max("ProcessDate").alias("Fecha_maxima"),
                F.count("EmployeeRankingNumber").alias("Registros")
            )
            conex.report_final([("Ranking M", cifras_control)])
        else:
             cifras_control = tabla.agg(
                F.min("BusinessActivityDate").alias("min_acty_dt"),
                F.max("BusinessActivityDate").alias("max_acty_dt"),
                F.count("TotalPromisesNumber").alias("Registros")
            )
             conex.report_final([("Promesas cumplidas M", cifras_control)])


        # Ingesta de datos usando replaceWhere para seguridad y atomicidad
        # Esto sobrescribe únicamente la partición del mes en proceso.
        conex.write_log(f"Iniciando escritura en [{mensual}] para la partición [{partition_value}]...", "INFO")
        conex.DLake_Replace(tabla, mensual, f"partition_month = '{partition_value}'")
        conex.write_log(f"Escritura completada exitosamente en [{mensual}] para la partición [{partition_value}].", "INFO")

    except Exception as e:
        error_msg = traceback.format_exc()
        conex.write_log(f"Error en [consulta_tablas]: {error_msg}", "ERROR")
        # SendErrorEmail(f"Error en el proceso de ingesta mensual: {e}") # Descomentar si se usa
        raise e # Relanzar la excepción para detener el proceso si es crítico