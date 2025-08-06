# ==============================================================================
#  Dialer_Functions.py - Versión Final
#  Funciones ETL para el procesamiento de datos del Dialer en PySpark.
# ==============================================================================

# --- Imports Necesarios ---


# ==============================================================================
#  FUNCIONES AUXILIARES (Helpers)
# ==============================================================================

def _procesar_jerarquia(df_base, tab_name, columns_to_select, subset_duplicates, classification=None):
    """
    Función auxiliar para procesar una sección de la jerarquía.
    Filtra, selecciona, ordena y elimina duplicados.
    """
    df_filtered = df_base.filter(col('tab') == tab_name)
    if classification:
        df_filtered = df_filtered.filter(col('classification') == classification)

    # Ordenar por fecha para quedarse con el registro más reciente al eliminar duplicados
    df_processed = df_filtered.orderBy(col('process_date').desc()) \
                              .dropDuplicates(subset=subset_duplicates)

    # Seleccionar y transformar las columnas finales
    df_final = df_processed.select(columns_to_select)
    return df_final

def _prepare_interaction_agent_detail(df_int_agent_det, timezone_offset_hours=6):
    """
    Prepara y agrega el DataFrame de InteractionAgentDetail.
    Esta lógica es compartida por los flujos de inbound, outbound y staff.
    """
    print("Preparando datos de InteractionAgentDetail...")
    df_agent_details = df_int_agent_det.filter(col('agenttime') > 0) \
        .withColumn('interactionstartdt', col('interactionstartdt') - F.expr(f'INTERVAL {timezone_offset_hours} HOURS')) \
        .withColumn('user_id', lower(col('user_id'))) \
        .withColumn('record_date', to_date('interactionstartdt')) \
        .withColumn('hour', hour('interactionstartdt')) \
        .withColumn('minute', minute('interactionstartdt')) \
        .withColumn('interval_hour', concat(col('hour'), when(col('minute') < 30, ":00").otherwise(":30")))

    df_agent_agg = df_agent_details.withColumn('LLAVE_INT_AGENT_DET', concat_ws('-', 'record_date', 'interval_hour', 'seqnum', 'user_id')) \
        .groupBy('LLAVE_INT_AGENT_DET') \
        .agg(
            sum('agenttime').alias('agenttime'), sum('previewtime').alias('previewtime'),
            sum('activetime').alias('activetime'), sum('wraptime').alias('wraptime'), sum('holdtime').alias('holdtime')
        )
    return df_agent_agg

# ==============================================================================
#  FUNCIONES DE PROCESAMIENTO PRINCIPALES
# ==============================================================================

def catalogo_dialer(df_cat_dialer):
    """
    Ordena el DataFrame del catálogo del dialer por la fecha de procesamiento.
    """
    try:
        print("Función para obtener el Catálogo del Dialer actualizado...")
        # Se usa orderBy para ordenar el DataFrame de más nuevo a más viejo en PySpark.
        df_cat_dialer_sorted = df_cat_dialer.orderBy(col('process_date').desc())
        return df_cat_dialer_sorted
    except Exception as e:
        print(f"Error al ordenar el catálogo del dialer: {e}")
        return df_cat_dialer

# ==============================================================================
#  FUNCIÓN 2: JERARQUIA DIALER (Refactorizada)
# ==============================================================================

def _procesar_jerarquia(df_base, tab_name, columns_to_select, subset_duplicates, classification=None):
    """
    Función auxiliar (privada) para procesar una sección de la jerarquía.
    Filtra, selecciona, ordena y elimina duplicados.
    """
    df_filtered = df_base.filter(col('tab') == tab_name)
    if classification:
        df_filtered = df_filtered.filter(col('classification') == classification)

    # Ordenar por fecha para quedarse con el registro más reciente al eliminar duplicados
    df_processed = df_filtered.orderBy(col('process_date').desc()) \
                              .dropDuplicates(subset=subset_duplicates)

    # Seleccionar y transformar las columnas finales
    df_final = df_processed.select(columns_to_select)
    
    return df_final

def jerarquia_dialer(df_jerarquia_dialer):
    """
    Función principal que divide el DataFrame de jerarquía en múltiples 
    DataFrames específicos usando una función auxiliar.
    """
    try:
        print("Iniciando la función para procesar la Jerarquía del Dialer...")

        # --- 1. Report Groups to Super Groups ---
        cols_rg_sg = [
            col('reportnamemasterid'), col('reportname'), col('supergroupmasterid'), col('supergroupname'),
            to_date(col('startdate'),"dd/MM/yyyy").alias('startdate_sg'),
            to_date(col('stopdate'),"dd/MM/yyyy").alias('stopdate_sg'),
            to_date(col('process_date'),"yyyy-MM-dd").alias('process_date')
        ]
        subset_rg_sg = ['reportnamemasterid', 'reportname', 'supergroupmasterid', 'supergroupname', 'startdate_sg']
        jerarquia_dialer_hist_rg_sg = _procesar_jerarquia(df_jerarquia_dialer, 'Report Groups to Super Groups', cols_rg_sg, subset_rg_sg)
        
        # --- 2. Inbound: LOB Group - ReportGrp ---
        cols_lg_rg_ib = [
            col('lobmasterid'), col('groupname'), to_date(col('lobtorgstartdate'),"dd/MM/yyyy").alias('lobtorgstartdate'),
            to_date(col('lobtorgstopdate'),"dd/MM/yyyy").alias('lobtorgstopdate'), col('reportnamemasterid'),
            col('reportname'), to_date(col('rgstartdate'),"dd/MM/yyyy").alias('rgstartdate'),
            to_date(col('process_date'),"yyyy-MM-dd").alias('process_date')
        ]
        subset_lg_rg_ib = ['lobmasterid', 'groupname', 'lobtorgstartdate', 'reportnamemasterid', 'reportname']
        jerarquia_dialer_hist_lg_rg_ib = _procesar_jerarquia(df_jerarquia_dialer, 'LOB Group - ReportGrp', cols_lg_rg_ib, subset_lg_rg_ib, classification='IB Service')

        # --- 3. Inbound: LOBs to Service Ids ---
        cols_lob_sid = [
            col('lobmasterid'), col('groupname'), to_date(col('startdate'),"dd/MM/yyyy").alias('startdate_serviceid'),
            to_date(col('stopdate'),"dd/MM/yyyy").alias('stopdate_serviceid'), col('uip_inst'),
            col('uip_inst_serviceid'), col('serviceid'),
            to_date(col('process_date'),"yyyy-MM-dd").alias('process_date')
        ]
        subset_lob_sid = ['lobmasterid', 'groupname', 'startdate', 'serviceid']
        jerarquia_dialer_hist_lob_sid = _procesar_jerarquia(df_jerarquia_dialer, 'LOBs to Service Ids', cols_lob_sid, subset_lob_sid, classification='IB Service')
        
        # --- 4. Staff: LOBs to Service Ids ---
        # Reutiliza la misma definición de columnas que el #3
        jerarquia_dialer_hist_lob_sid_staff = jerarquia_dialer_hist_lob_sid

        # --- 5. Outbound: LOB Group - ReportGrp ---
        cols_lg_rg_ob = [
            col('lobmasterid'), col('groupname'), to_date(col('lobtorgstartdate'),"dd/MM/yyyy").alias('lobtorgstartdate'),
            to_date(col('lobtorgstopdate'),"dd/MM/yyyy").alias('lobtorgstopdate'), col('reportnamemasterid'),
            col('reportname'), to_date(col('rgstartdate'),"dd/MM/yyyy").alias('rgstartdate'),
            to_date(col('process_date'),"yyyy-MM-dd").alias('process_date')
        ]
        subset_lg_rg_ob = ['lobmasterid', 'groupname', 'lobtorgstartdate', 'reportnamemasterid', 'reportname']
        jerarquia_dialer_hist_lg_rg_ob = _procesar_jerarquia(df_jerarquia_dialer, 'LOB Group - ReportGrp', cols_lg_rg_ob, subset_lg_rg_ob, classification='List')

        # --- 6. Outbound: LOBs to ALMLists ---
        cols_lob_alm = [
            col('lobmasterid'), col('groupname'), col('uip_inst'), col('listname'),
            to_date(col('listname_startdate'),"dd/MM/yyyy").alias('listname_startdate'),
            to_date(col('listname_stopdate'),"dd/MM/yyyy").alias('listname_stopdate'),
            to_date(col('process_date'),"yyyy-MM-dd").alias('process_date')
        ]
        subset_lob_alm = ['lobmasterid', 'groupname', 'uip_inst', 'listname', 'listname_startdate']
        jerarquia_dialer_hist_lob_alm = _procesar_jerarquia(df_jerarquia_dialer, 'LOBs to ALMLists', cols_lob_alm, subset_lob_alm)

        # --- 7. Outbound: ALMList Active Goals ---
        cols_alm_active = [
            col('listname').alias('listname_active'), to_date(col('updatedate'),"dd/MM/yyyy").alias('updatedate_active'),
            col('goallow').alias('goallow_active'), col('goalhigh').alias('goalhigh_active'),
            to_date(col('startdate'),"dd/MM/yyyy").alias('startdate_active'),
            to_date(col('stopdate'),"dd/MM/yyyy").alias('stopdate_active'),
            to_date(col('process_date'),"yyyy-MM-dd").alias('process_date')
        ]
        subset_alm_active = ['listname_active', 'updatedate_active', 'startdate_active']
        jerarquia_dialer_hist_alm_active = _procesar_jerarquia(df_jerarquia_dialer, 'ALMList Active Goals', cols_alm_active, subset_alm_active)

        # --- 8. Staff: LOB Group - ReportGrp ---
        # Reutiliza la misma definición de columnas que el #2 y #5
        jerarquia_dialer_hist_lg_rg_staff = _procesar_jerarquia(df_jerarquia_dialer, 'LOB Group - ReportGrp', cols_lg_rg_ob, subset_lg_rg_ob) # Sin clasificación específica

        # --- 9. NICEMU-WorkSeg-ReportGrp Config ---
        cols_mu_rg = [
            col('reportnamemasterid'), col('reportname'), col('mu_id'), col('nicemu'),
            to_date(col('nicemu_startdate'),"dd/MM/yyyy").alias('nicemu_startdate'),
            to_date(col('nicemu_stopdate'),"dd/MM/yyyy").alias('nicemu_stopdate'),
            to_date(col('process_date'),"yyyy-MM-dd").alias('process_date')
        ]
        subset_mu_rg = ['reportnamemasterid', 'reportname', 'mu_id', 'nicemu', 'nicemu_startdate']
        jerarquia_dialer_hist_mu_rg = _procesar_jerarquia(df_jerarquia_dialer, 'NICEMU-WorkSeg-ReportGrp Config', cols_mu_rg, subset_mu_rg)

        # --- 10. Staff: Forecast Group to Report Group ---
        cols_fg_rg_staff = [
            col('reportnamemasterid'), col('reportname'), col('fcstgrpid'), col('forecast_group_code'),
            to_date(col('fcst_group_code_startdate'),"dd/MM/yyyy").alias('fcst_group_code_startdate'),
            to_date(col('fcst_group_code_stopdate'),"dd/MM/yyyy").alias('fcst_group_code_stopdate'),
            to_date(col('process_date'),"yyyy-MM-dd").alias('process_date')
        ]
        subset_fg_rg_staff = ['reportnamemasterid', 'reportname', 'fcstgrpid', 'forecast_group_code', 'fcst_group_code_startdate']
        jerarquia_dialer_hist_fg_rg_staff = _procesar_jerarquia(df_jerarquia_dialer, 'Forecast Group to Report Group', cols_fg_rg_staff, subset_fg_rg_staff)
        
        print("Procesamiento de jerarquía completado.")
        
        # Se retorna una tupla con todos los DataFrames procesados, manteniendo la estructura original.
        return (
            jerarquia_dialer_hist_rg_sg, 
            jerarquia_dialer_hist_lg_rg_ib, 
            jerarquia_dialer_hist_lob_sid, 
            jerarquia_dialer_hist_lob_sid_staff,
            jerarquia_dialer_hist_lg_rg_ob, 
            jerarquia_dialer_hist_lob_alm, 
            jerarquia_dialer_hist_alm_active,
            jerarquia_dialer_hist_lg_rg_staff,
            jerarquia_dialer_hist_mu_rg, 
            jerarquia_dialer_hist_fg_rg_staff
        )

    except Exception as e:
        print(f"Error fatal al procesar la jerarquía del dialer: {e}")
        import traceback
        traceback.print_exc()
        # Devuelve una tupla de 10 Nones si hay un error para evitar fallos en el desempaquetado.
        return (None,) * 10

def nice_dialer(df_nice_agent_info, df_nice_active_forecast, df_nice_agent_adherence_summary):
    """
    Procesa los DataFrames de NICE, quedándose con el registro más reciente de cada entidad.
    """
    try:
        print("Iniciando el procesamiento de DataFrames de NICE...")
        
        # --- 1. NICE Agent Info (último registro por agente) ---
        subset_agent_info = ['externalid'] # La clave única del agente
        df_nice_agent_info_p = df_nice_agent_info.orderBy(col('process_date').desc()) \
                                                 .dropDuplicates(subset=subset_agent_info) \
                                                 .select('date', 'muid', 'externalid')

        # --- 2. NICE Active Forecast (último forecast por 'ctid' y 'period') ---
        subset_forecast = ['ctid', 'date', 'period'] # Clave única del forecast
        df_nice_active_forecast_p = df_nice_active_forecast.orderBy(col('process_date').desc()) \
                                                           .dropDuplicates(subset=subset_forecast) \
                                                           .select('date', 'period', 'ctid', 'ctname', 'fcstcontactsreceived', 'festaht', 'fcstreq', 'schedopen')

        # --- 3. NICE Agent Adherence Summary (último registro por agente y fecha) ---
        subset_adherence = ['externalid', 'date'] # Clave única de la adherencia
        df_nice_agent_adherence_summary_p = df_nice_agent_adherence_summary.orderBy(col('process_date').desc()) \
                                                                           .dropDuplicates(subset=subset_adherence) \
                                                                           .select('date', 'muid', 'attribute', 'totalact', 'totalsched', 'unitmanager', 'logonid', 'externalid')

        print("Procesamiento de NICE completado.")
        return df_nice_agent_info_p, df_nice_active_forecast_p, df_nice_agent_adherence_summary_p

    except Exception as e:
        print(f"Error fatal durante el procesamiento de NICE: {e}")
        traceback.print_exc()
        return None, None, None

def data_inbound(df_acd_call_det, df_int_agent_det, jerarquia_dialer_hist_rg_sg, jerarquia_dialer_hist_lg_rg_ib, jerarquia_dialer_hist_lob_sid):
    """
    Procesa y enriquece los datos de llamadas inbound cruzando información
    de ACD, agentes y jerarquías del dialer.
    """
    try:
        print("Iniciando el procesamiento de datos Inbound...")
        TIMEZONE_OFFSET_HOURS = 6

        # --- 1. Preparar datos de ACDCallDetail ---
        print("Procesando ACDCallDetail...")
        df_acd_calls = df_acd_call_det.filter(col('callstartdt') != col('callenddt')) \
            .select(
                col('seqnum'),
                col('service_id'),
                lower(col('user_id')).alias('user_id'),
                col('calltypeid'),
                col('callactionid'),
                (col('callstartdt') - F.expr(f'INTERVAL {TIMEZONE_OFFSET_HOURS} HOURS')).alias('callstartdt'),
                (col('queuestartdt') - F.expr(f'INTERVAL {TIMEZONE_OFFSET_HOURS} HOURS')).alias('queuestartdt'),
                (col('queueenddt') - F.expr(f'INTERVAL {TIMEZONE_OFFSET_HOURS} HOURS')).alias('queueenddt'),
                (col('conncleardt') - F.expr(f'INTERVAL {TIMEZONE_OFFSET_HOURS} HOURS')).alias('conncleardt')
            ).withColumn('hour', hour('callstartdt')) \
             .withColumn('minute', minute('callstartdt')) \
             .withColumn('interval_hour', concat(col('hour'), when(col('minute') < 30, ":00").otherwise(":30"))) \
             .withColumn('record_date', to_date('callstartdt')) \
             .withColumn('queuetime', col('queueenddt').cast("double") - col('queuestartdt').cast("double")) \
             .withColumn('agentime_calc', col('conncleardt').cast("double") - col('queueenddt').cast("double"))

        # Limpiar fechas y tiempos calculados
        df_acd_calls_cleaned = df_acd_calls.select(
            "interval_hour", "record_date", "service_id", "seqnum", "user_id", "calltypeid", "callactionid",
            when(year('queuestartdt') <= 1900, None).otherwise(col('queuestartdt')).alias('queuestartdt'),
            when(year('queueenddt') <= 1900, None).otherwise(col('queueenddt')).alias('queueenddt'),
            when(year('conncleardt') <= 1900, None).otherwise(col('conncleardt')).alias('conncleardt'),
            "queuetime", "agentime_calc"
        ).withColumn('queuetime', when(col('queuestartdt').isNull() | col('queueenddt').isNull(), 0).otherwise(col('queuetime'))) \
         .withColumn('agentime_calc', when(col('queueenddt').isNull() | col('conncleardt').isNull(), 0).otherwise(col('agentime_calc'))) \
         .select("interval_hour", "record_date", "service_id", "seqnum", "user_id", "calltypeid", "callactionid", "queuetime", "agentime_calc") \
         .na.fill('') # Manera eficiente de reemplazar todos los nulos por strings vacíos

        # --- 2. Preparar datos de InteractionAgentDetail (usando la función auxiliar) ---
        df_agent_agg = _prepare_interaction_agent_detail(df_int_agent_det, TIMEZONE_OFFSET_HOURS)

        # --- 3. Unir ACDCallDetail con InteractionAgentDetail ---
        print("Uniendo datos de llamadas y agentes...")
        df_calls_enriched = df_acd_calls_cleaned.withColumn('LLAVE_ACD_CALL', concat_ws('-', 'record_date', 'interval_hour', 'seqnum', 'user_id')) \
            .join(df_agent_agg, col('LLAVE_ACD_CALL') == col('LLAVE_INT_AGENT_DET'), 'left') \
            .drop('LLAVE_ACD_CALL', 'LLAVE_INT_AGENT_DET') \
            .na.fill(0, subset=['agenttime', 'previewtime', 'activetime', 'wraptime', 'holdtime']) # Rellenar nulos de la unión con 0

        # --- 4. Enriquecer con Jerarquías ---
        print("Enriqueciendo con jerarquías del dialer...")
        
        # Join con LOBs to Service IDs
        df_final = df_calls_enriched.join(jerarquia_dialer_hist_lob_sid,
                                  (df_calls_enriched['service_id'] == jerarquia_dialer_hist_lob_sid['serviceid']) &
                                  (df_calls_enriched['record_date'] >= jerarquia_dialer_hist_lob_sid['startdate_serviceid']) &
                                  (when(jerarquia_dialer_hist_lob_sid['stopdate_serviceid'].isNull(), True)
                                   .otherwise(df_calls_enriched['record_date'] <= jerarquia_dialer_hist_lob_sid['stopdate_serviceid'])), 'left') \
            .select(df_calls_enriched["*"], jerarquia_dialer_hist_lob_sid['lobmasterid'])

        # Join con LOB Group to ReportGrp
        df_final = df_final.join(jerarquia_dialer_hist_lg_rg_ib,
                         (df_final['lobmasterid'] == jerarquia_dialer_hist_lg_rg_ib['lobmasterid']) &
                         (df_final['record_date'] >= jerarquia_dialer_hist_lg_rg_ib['lobtorgstartdate']) &
                         (when(jerarquia_dialer_hist_lg_rg_ib['lobtorgstopdate'].isNull(), True)
                          .otherwise(df_final['record_date'] <= jerarquia_dialer_hist_lg_rg_ib['lobtorgstopdate'])), 'left') \
            .select(df_final["*"], jerarquia_dialer_hist_lg_rg_ib['reportnamemasterid'], jerarquia_dialer_hist_lg_rg_ib['reportname'])

        # Join con Report Groups to Super Groups
        df_final = df_final.join(jerarquia_dialer_hist_rg_sg,
                         (df_final['reportnamemasterid'] == jerarquia_dialer_hist_rg_sg['reportnamemasterid']) &
                         (df_final['record_date'] >= jerarquia_dialer_hist_rg_sg['startdate_sg']) &
                         (when(jerarquia_dialer_hist_rg_sg['stopdate_sg'].isNull(), True)
                          .otherwise(df_final['record_date'] <= jerarquia_dialer_hist_rg_sg['stopdate_sg'])), 'left') \
            .select(df_final["*"], jerarquia_dialer_hist_rg_sg['supergroupname'])

        # --- 5. Limpieza y Selección Final ---
        print("Realizando limpieza final...")
        uip_inbound_d = df_final.withColumn('month', month('record_date')) \
            .select(
                "interval_hour", "record_date", "supergroupname", "reportname", "service_id", "user_id",
                "calltypeid", "callactionid", "queuetime", "agentime_calc", "agenttime",
                "previewtime", "activetime", "wraptime", "holdtime", "month"
            ).withColumn('supergroupname', when(col('supergroupname').isNull() | (col('supergroupname') == ''), "Sin Asignar").otherwise(col('supergroupname'))) \
             .withColumn('reportname', when(col('reportname').isNull() | (col('reportname') == ''), "Sin Asignar").otherwise(col('reportname'))) \
             .na.fill('') # Rellenar cualquier nulo restante
        
        print("Procesamiento Inbound completado.")
        return uip_inbound_d

    except Exception as e:
        print(f"Error fatal en la función data_inbound: {e}")
        import traceback
        traceback.print_exc()
        return None

def data_outbound(df_contact_event, df_int_agent_det, df_cat_dialer, jerarquia_dialer_hist_lob_alm, 
                  jerarquia_dialer_hist_alm_active, jerarquia_dialer_hist_lg_rg_ob, jerarquia_dialer_hist_rg_sg):
    """
    Procesa y enriquece los datos de llamadas outbound.
    """
    try:
        print("Iniciando el procesamiento de datos Outbound...")
        TIMEZONE_OFFSET_HOURS = 6

        # --- 1. Preparar datos de ContactEvent ---
        print("Procesando ContactEvent...")
        df_contacts = df_contact_event \
            .withColumn('time_of_contact', col('time_of_contact') - F.expr(f'INTERVAL {TIMEZONE_OFFSET_HOURS} HOURS')) \
            .withColumn('agent_login_name', lower(col('agent_login_name'))) \
            .withColumn('hour', hour('time_of_contact')) \
            .withColumn('minute', minute('time_of_contact')) \
            .withColumn('interval_hour', concat(col('hour'), when(col('minute') < 30, ":00").otherwise(":30"))) \
            .withColumn('record_date', to_date('time_of_contact')) \
            .select("interval_hour", "record_date", "contact_list_name", "response_status", "agent_login_name", "total_number_of_records", "seqnum")

        # Calcular máximos en un solo paso
        df_max_records = df_contacts.groupBy("record_date", "contact_list_name").agg(
            max("total_number_of_records").alias("max_total_number_of_records"),
            max(when(col("response_status") != "", col("total_number_of_records"))).alias("max_total_number_of_records_not_emp_res_sta")
        )

        # Unir los máximos calculados
        df_contacts = df_contacts.join(df_max_records, ["record_date", "contact_list_name"], "left")

        # --- 2. Preparar datos de InteractionAgentDetail (usando la función auxiliar) ---
        df_agent_agg = _prepare_interaction_agent_detail(df_int_agent_det, TIMEZONE_OFFSET_HOURS)

        # --- 3. Unir ContactEvent con InteractionAgentDetail ---
        print("Uniendo datos de contactos y agentes...")
        df_outbound_base = df_contacts.withColumn('LLAVE_CONTACT_EVENT', concat_ws('-', 'record_date', 'interval_hour', 'seqnum', 'agent_login_name')) \
            .join(df_agent_agg, col('LLAVE_CONTACT_EVENT') == col('LLAVE_INT_AGENT_DET'), 'left') \
            .drop('LLAVE_CONTACT_EVENT', 'LLAVE_INT_AGENT_DET')

        # --- 4. Preparar y unir con Catalogo Dialer ---
        print("Uniendo con Catálogo Dialer...")
        df_cat_saturacion = df_cat_dialer.groupBy(trim(col("aspect_list_name")).alias("aspect_list_name")) \
                                         .agg(max("saturacion").alias("saturacion"))

        df_cat_disp = df_cat_dialer.select(
            trim(col('disposition')).alias('disposition'), 'attempt', 'penetration', 'connection',
            'direct_contact', 'promise', 'abandon', trim(col('group_disposition')).alias('group_disposition')
        )

        df_outbound_base = df_outbound_base.join(df_cat_saturacion, df_outbound_base['contact_list_name'] == df_cat_saturacion['aspect_list_name'], 'left') \
                                           .join(df_cat_disp, df_outbound_base['response_status'] == df_cat_disp['disposition'], 'left') \
                                           .drop('aspect_list_name', 'disposition')

        # --- 5. Unir con todas las Jerarquías ---
        print("Uniendo con Jerarquías...")
        df_final = df_outbound_base.join(jerarquia_dialer_hist_lob_alm.select('lobmasterid', 'groupname', 'listname').distinct(),
                                     df_outbound_base['contact_list_name'] == jerarquia_dialer_hist_lob_alm['listname'], 'left') \
                               .join(jerarquia_dialer_hist_alm_active.select('listname_active', 'goalhigh_active').distinct(),
                                     df_outbound_base['contact_list_name'] == jerarquia_dialer_hist_alm_active['listname_active'], 'left')

        # Join con jerarquías que dependen de la fecha
        df_final = df_final.join(jerarquia_dialer_hist_lg_rg_ob,
                                 (df_final['lobmasterid'] == jerarquia_dialer_hist_lg_rg_ob['lobmasterid']) &
                                 (df_final['record_date'] >= jerarquia_dialer_hist_lg_rg_ob['lobtorgstartdate']) &
                                 (when(jerarquia_dialer_hist_lg_rg_ob['lobtorgstopdate'].isNull(), True)
                                  .otherwise(df_final['record_date'] <= jerarquia_dialer_hist_lg_rg_ob['lobtorgstopdate'])), 'left')

        df_final = df_final.join(jerarquia_dialer_hist_rg_sg,
                                 (df_final['reportnamemasterid'] == jerarquia_dialer_hist_rg_sg['reportnamemasterid']) &
                                 (df_final['record_date'] >= jerarquia_dialer_hist_rg_sg['startdate_sg']) &
                                 (when(jerarquia_dialer_hist_rg_sg['stopdate_sg'].isNull(), True)
                                  .otherwise(df_final['record_date'] <= jerarquia_dialer_hist_rg_sg['stopdate_sg'])), 'left')

        # --- 6. Limpieza y Selección Final ---
        print("Realizando limpieza y selección final...")
        uip_outbound_d = df_final.withColumn('month', month('record_date')) \
            .withColumn('supergroupname', when(col('supergroupname').isNull() | (col('supergroupname') == ''), "Sin Asignar").otherwise(col('supergroupname'))) \
            .withColumn('reportname', when(col('reportname').isNull() | (col('reportname') == ''), "Sin Asignar").otherwise(col('reportname'))) \
            .select(
                'interval_hour', 'record_date', 'supergroupname', 'reportname', 'contact_list_name', 'agent_login_name',
                col('max_total_number_of_records').cast(IntegerType()),
                col('max_total_number_of_records_not_emp_res_sta').cast(IntegerType()),
                col('agenttime').cast(IntegerType()), 'previewtime', 'activetime', 'wraptime', 'holdtime',
                col('saturacion').cast(IntegerType()), 'attempt', 'penetration', 'connection', 'direct_contact',
                'promise', 'abandon', 'group_disposition', col('goalhigh_active').cast(FloatType()), 'month'
            ).na.fill(0).na.fill("").sort('record_date', ascending=True)

        print("Procesamiento Outbound completado.")
        return uip_outbound_d

    except Exception as e:
        print(f"Error fatal en la función data_outbound: {e}")
        import traceback
        traceback.print_exc()
        return None

def data_staff(df_agent_act_sum, df_int_agent_det, jerarquia_dialer_hist_rg_sg, 
               jerarquia_dialer_hist_mu_rg, df_nice_agent_info):
    """
    Procesa y enriquece los datos de actividad de agentes (staff).
    """
    try:
        print("Iniciando el procesamiento de datos de Staff...")
        TIMEZONE_OFFSET_HOURS = 6

        # --- 1. Preparar datos de AgentActivitySummary ---
        print("Procesando AgentActivitySummary...")
        df_activity = df_agent_act_sum.filter(col('service_id') == 0) \
            .withColumn('begintimeperioddt', col('begintimeperioddt') - F.expr(f'INTERVAL {TIMEZONE_OFFSET_HOURS} HOURS')) \
            .withColumn('user_id', lower(col('user_id'))) \
            .withColumn('record_date', to_date('begintimeperioddt')) \
            .withColumn('hour', hour('begintimeperioddt')) \
            .withColumn('minute', minute('begintimeperioddt')) \
            .withColumn('interval_hour', concat(col('hour'), when(col('minute') < 30, ":00").otherwise(":30"))) \
            .groupBy('interval_hour', 'record_date', 'user_id', 'service_id') \
            .agg(
                sum('totallogintime').alias('totallogintime'), sum('totalidletime').alias('totalidletime'),
                sum('totalnotreadytime').alias('totalnotreadytime'), sum('totalgaptime').alias('totalgaptime'),
                sum('totalparkidletime').alias('totalparkidletime'), sum('totalparktime').alias('totalparktime')
            )

        # --- 2. Preparar datos de InteractionAgentDetail (usando la función auxiliar) ---
        # Nota: La llave de esta agregación no usa 'seqnum', por lo que se ajusta.
        df_agent_agg_staff = _prepare_interaction_agent_detail(df_int_agent_det, TIMEZONE_OFFSET_HOURS) \
            .withColumn("LLAVE_INT_AGENT_DET", concat_ws('-', col("record_date"), col("interval_hour"), col("user_id"))) \
            .groupBy("LLAVE_INT_AGENT_DET") \
            .agg(
                sum('agenttime').alias('agenttime'), sum('activetime').alias('activetime'),
                sum('wraptime').alias('wraptime'), sum('previewtime').alias('previewtime'),
                sum('holdtime').alias('holdtime')
            )

        # --- 3. Unir Activity con Interaction Details ---
        print("Uniendo datos de actividad e interacciones...")
        df_staff_base = df_activity.withColumn('LLAVE_AGENT_ACT', concat_ws('-', 'record_date', 'interval_hour', 'user_id')) \
            .join(df_agent_agg_staff, col('LLAVE_AGENT_ACT') == col('LLAVE_INT_AGENT_DET'), 'left') \
            .drop('LLAVE_AGENT_ACT', 'LLAVE_INT_AGENT_DET')

        # --- 4. Preparar y unir con Nice Agent Info (Lógica Simplificada) ---
        print("Procesando y uniendo con Nice Agent Info...")
        window_spec = Window.partitionBy(col("agent_info_id")).orderBy(col("date_nice_agent_info").desc(), col("process_date").desc())
        
        df_nice_latest = df_nice_agent_info \
            .withColumn('date_nice_agent_info', to_date(col('date'), "yyyyMMdd")) \
            .withColumn('agent_info_id', lower(col('externalid'))) \
            .withColumn('row_num', row_number().over(window_spec)) \
            .filter(col('row_num') == 1) \
            .select(
                'muid', 
                'agent_info_id', 
                concat_ws('-', 'date_nice_agent_info', 'agent_info_id').alias('LLAVE_NICE_AGENT_INFO')
            )

        df_staff_base = df_staff_base.withColumn('LLAVE_STAFF', concat_ws('-', 'record_date', 'user_id')) \
            .join(df_nice_latest, col('LLAVE_STAFF') == col('LLAVE_NICE_AGENT_INFO'), 'left') \
            .drop('LLAVE_STAFF', 'LLAVE_NICE_AGENT_INFO')

        # --- 5. Unir con Jerarquías ---
        print("Uniendo con jerarquías...")
        df_final = df_staff_base.join(jerarquia_dialer_hist_mu_rg,
                                 (df_staff_base['muid'] == jerarquia_dialer_hist_mu_rg['mu_id']) &
                                 (df_staff_base['record_date'] >= jerarquia_dialer_hist_mu_rg['nicemu_startdate']) &
                                 (when(jerarquia_dialer_hist_mu_rg['nicemu_stopdate'].isNull(), True)
                                  .otherwise(df_staff_base['record_date'] <= jerarquia_dialer_hist_mu_rg['nicemu_stopdate'])), 'left')

        df_final = df_final.join(jerarquia_dialer_hist_rg_sg,
                                 (df_final['reportnamemasterid'] == jerarquia_dialer_hist_rg_sg['reportnamemasterid']) &
                                 (df_final['record_date'] >= jerarquia_dialer_hist_rg_sg['startdate_sg']) &
                                 (when(jerarquia_dialer_hist_rg_sg['stopdate_sg'].isNull(), True)
                                  .otherwise(df_final['record_date'] <= jerarquia_dialer_hist_rg_sg['stopdate_sg'])), 'left')

        # --- 6. Limpieza y Selección Final ---
        print("Realizando limpieza final...")
        uip_staff_d = df_final.withColumn('month', month('record_date')) \
            .withColumn('supergroupname', when(col('supergroupname').isNull() | (col('supergroupname') == ''), "Sin Asignar").otherwise(col('supergroupname'))) \
            .withColumn('reportname', when(col('reportname').isNull() | (col('reportname') == ''), "Sin Asignar").otherwise(col('reportname'))) \
            .select(
                'interval_hour', col('record_date').cast(DateType()), 'supergroupname', 'reportname', 'user_id',
                col('service_id').cast(IntegerType()), col('totallogintime').cast(IntegerType()),
                col('totalidletime').cast(IntegerType()), col('totalnotreadytime').cast(IntegerType()),
                col('totalgaptime').cast(IntegerType()), col('totalparkidletime').cast(IntegerType()),
                col('totalparktime').cast(IntegerType()), col('agenttime').cast(IntegerType()),
                'previewtime', 'activetime', 'wraptime', 'holdtime', col('month').cast(IntegerType())
            ).na.fill(0).na.fill("").sort('record_date', ascending=True)

        print("Procesamiento de Staff completado.")
        return uip_staff_d

    except Exception as e:
        print(f"Error fatal en la función data_staff: {e}")
        import traceback
        traceback.print_exc()
        return None

def data_forecast(jerarquia_dialer_hist_rg_sg, jerarquia_dialer_hist_fg_rg_staff, df_nice_active_forecast):
    """
    Procesa y enriquece los datos de pronóstico (forecast) de NICE.
    """
    try:
        print("Iniciando el procesamiento de datos de Forecast...")

        # --- 1. Preparar y limpiar datos de NiceActiveForecast ---
        # Se obtiene el registro más reciente para cada pronóstico usando una Window Function.
        # Esto reemplaza el método anterior de groupBy + join + window.
        window_spec = Window.partitionBy("date_nice_active_fcst", "ctid") \
                              .orderBy(col("process_date").desc())

        df_forecast_latest = df_nice_active_forecast \
            .withColumn("date_nice_active_fcst", to_date(col("date"), "yyyyMMdd")) \
            .withColumn("period_nice_active_fcst", date_format(to_timestamp(col("period"), "HH:mm"), "HH:mm")) \
            .withColumn("row_num", row_number().over(window_spec)) \
            .filter(col("row_num") == 1) \
            .select(
                "date_nice_active_fcst",
                "period_nice_active_fcst",
                "ctid",
                col("fcstcontactsreceived").cast(FloatType()),
                col("fcstaht").cast(FloatType()),
                col("fcstreq").cast(FloatType()),
                col("schedopen").cast(FloatType())
            )

        # --- 2. Enriquecer con Jerarquías ---
        print("Uniendo forecast con jerarquías...")

        # Join con Forecast Group to Report Group
        df_enriched = df_forecast_latest.join(jerarquia_dialer_hist_fg_rg_staff,
            (df_forecast_latest['ctid'] == jerarquia_dialer_hist_fg_rg_staff['fcstgrpid']) &
            (df_forecast_latest['date_nice_active_fcst'] >= jerarquia_dialer_hist_fg_rg_staff['fcst_group_code_startdate']) &
            (when(jerarquia_dialer_hist_fg_rg_staff['fcst_group_code_stopdate'].isNull(), True)
             .otherwise(df_forecast_latest['date_nice_active_fcst'] <= jerarquia_dialer_hist_fg_rg_staff['fcst_group_code_stopdate'])),
            'inner'
        ).select(df_forecast_latest["*"], col("reportnamemasterid"), col("reportname"))

        # Join con Report Group to Super Groups
        df_final = df_enriched.join(jerarquia_dialer_hist_rg_sg,
            (df_enriched['reportnamemasterid'] == jerarquia_dialer_hist_rg_sg['reportnamemasterid']) &
            (df_enriched['date_nice_active_fcst'] >= jerarquia_dialer_hist_rg_sg['startdate_sg']) &
            (when(jerarquia_dialer_hist_rg_sg['stopdate_sg'].isNull(), True)
             .otherwise(df_enriched['date_nice_active_fcst'] <= jerarquia_dialer_hist_rg_sg['stopdate_sg'])),
            'inner'
        ).select(df_enriched["*"], col("supergroupname"))


        # --- 3. Limpieza y Selección Final ---
        print("Realizando limpieza y selección final...")
        uip_nice_active_forecast_d = df_final \
            .withColumn("month", month("date_nice_active_fcst").cast(IntegerType())) \
            .select(
                "period_nice_active_fcst",
                col("date_nice_active_fcst").cast(DateType()),
                "supergroupname",
                "reportname",
                "fcstcontactsreceived",
                "fcstaht",
                "fcstreq",
                "schedopen",
                "month"
            ) \
            .na.fill("") \
            .sort("date_nice_active_fcst", ascending=True)

        print("Procesamiento de Forecast completado.")
        return uip_nice_active_forecast_d

    except Exception as e:
        print(f"Error fatal en la función data_forecast: {e}")
        import traceback
        traceback.print_exc()
        return None

def data_nice_agent_adherence_summary(df_nice_agent_adherence_summary, jerarquia_dialer_hist_mu_rg, jerarquia_dialer_hist_rg_sg):
    """
    Procesa y enriquece los datos de adherencia de agentes de NICE.
    """
    try:
        print("Iniciando el procesamiento de datos de Adherencia de Agentes...")

        # --- 1. Preparar y limpiar datos de Adherence Summary ---
        # Usamos una sola Window Function para obtener el registro más reciente,
        # lo cual es más eficiente que el método original de groupBy + join + window.
        window_spec = Window.partitionBy("date_nice_agent_adh_summary", "muid", "externalid", "attribute") \
                              .orderBy(col("process_date").desc())

        df_adherence_latest = df_nice_agent_adherence_summary \
            .withColumn("date_nice_agent_adh_summary", to_date(col("date"), "yyyyMMdd")) \
            .withColumn("attribute", trim(col("attribute"))) \
            .withColumn("total_active", (col("totalact") / 3600).cast(FloatType())) \
            .withColumn("total_scheduled", (col("totalsched") / 3600).cast(FloatType())) \
            .withColumn("unitmanagerid", lower(substring(col("unitmanager"), -7, 7))) \
            .withColumn("logonid", lower(col("logonid"))) \
            .withColumn("row_num", row_number().over(window_spec)) \
            .filter(col("row_num") == 1) \
            .select(
                "date_nice_agent_adh_summary", "muid", "attribute", "total_active", "total_scheduled",
                "unitmanager", "unitmanagerid", "logonid", "externalid"
            )

        # --- 2. Enriquecer con Jerarquías ---
        print("Uniendo adherencia con jerarquías...")

        # Join con NICEMU-WorkSeg-ReportGrp Config
        df_enriched = df_adherence_latest.join(jerarquia_dialer_hist_mu_rg,
            (df_adherence_latest['muid'] == jerarquia_dialer_hist_mu_rg['mu_id']) &
            (df_adherence_latest['date_nice_agent_adh_summary'] >= jerarquia_dialer_hist_mu_rg['nicemu_startdate']) &
            (when(jerarquia_dialer_hist_mu_rg['nicemu_stopdate'].isNull(), True)
             .otherwise(df_adherence_latest['date_nice_agent_adh_summary'] <= jerarquia_dialer_hist_mu_rg['nicemu_stopdate'])),
            'inner'
        ).select(df_adherence_latest["*"], col("reportnamemasterid"), col("reportname"))

        # Join con Report Group to Super Groups
        df_final = df_enriched.join(jerarquia_dialer_hist_rg_sg,
            (df_enriched['reportnamemasterid'] == jerarquia_dialer_hist_rg_sg['reportnamemasterid']) &
            (df_enriched['date_nice_agent_adh_summary'] >= jerarquia_dialer_hist_rg_sg['startdate_sg']) &
            (when(jerarquia_dialer_hist_rg_sg['stopdate_sg'].isNull(), True)
             .otherwise(df_enriched['date_nice_agent_adh_summary'] <= jerarquia_dialer_hist_rg_sg['stopdate_sg'])),
            'inner'
        ).select(df_enriched["*"], col("supergroupname"))

        # --- 3. Limpieza y Selección Final ---
        print("Realizando limpieza y selección final...")
        uip_nice_agent_adherence_summary_d = df_final \
            .withColumn("month", month("date_nice_agent_adh_summary").cast(IntegerType())) \
            .select(
                col("date_nice_agent_adh_summary").cast(DateType()),
                "supergroupname",
                "reportname",
                "unitmanager",
                "unitmanagerid",
                "logonid",
                "attribute",
                "total_active",
                "total_scheduled",
                "month"
            ) \
            .na.fill("") \
            .sort("date_nice_agent_adh_summary", ascending=True)

        print("Procesamiento de Adherencia completado.")
        return uip_nice_agent_adherence_summary_d

    except Exception as e:
        print(f"Error fatal en la función data_nice_agent_adherence_summary: {e}")
        import traceback
        traceback.print_exc()
        return None
    



    # =======================================================
# SCRIPT PRINCIPAL - Orquestador del Proceso Dialer
# =======================================================

import sys
from datetime import datetime
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import max, col

# --- Importar módulos personalizados y de configuración ---
import config as cf
import Dialer_Functions as df

# Asumo que tienes estas librerías personalizadas en tu entorno
# from CitiPy import CitiPyLogger, CitiPyMail, CitiPySpark, notification_mail

def principal():
    """
    Función principal que orquesta todo el proceso ETL de Dialer.
    """
    # --- 1. CONFIGURACIÓN INICIAL ---
    # En un entorno real, descomentarías estas líneas
    # logger = CitiPyLogger()
    # mail = CitiPyMail()
    begin_time = datetime.now()
    
    # Simulación de logs y notificaciones para que puedas probarlo
    print(f"[{begin_time.strftime('%Y-%m-%d %H:%M:%S')}] INFO: << Beginning execution process {cf.processName} >>")
    # mail.send_notification(cf.sender, cf.receivers, cf.subject, 'Execution process has begun.')

    spark = None # Inicializar spark a None para el bloque finally
    try:
        # --- 2. INICIALIZAR SPARK SESSION ---
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] INFO: Stage << Initializing SparkSession >>")
        
        # En un entorno real, usarías tu librería personalizada
        # spark = CitiPySpark().spark
        
        # Para desarrollo y pruebas, puedes usar una sesión de Spark estándar
        spark = SparkSession.builder.appName(cf.processName).enableHiveSupport().getOrCreate()
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] INFO: Stage << SparkSession successfully connected >>")

        # --- 3. LECTURA DE DATOS DE ENTRADA ---
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] INFO: Stage << Reading input tables >>")
        
        dataframes = {}
        missing_tables = []
        for table_name in cf.input_table_list:
            try:
                schema = cf.dic_schema_table.get(table_name, "default") # Usar .get para seguridad
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] INFO: Reading table {schema}.{table_name}...")
                dataframes[table_name] = spark.sql(f"SELECT * FROM {schema}.{table_name}")
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] INFO: Successfully read table {table_name}")
            except Exception as e:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ERROR: Failed to read table {table_name}: {e}")
                missing_tables.append(table_name)

        if missing_tables:
            error_msg = f"Could not continue. Missing tables: {missing_tables}"
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] FATAL: {error_msg}")
            # logger.end_process(mail, error_msg)
            sys.exit(1)
            
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] INFO: All input tables read successfully.")

        # --- 4. PROCESAMIENTO PRINCIPAL ---
        # Asignar DataFrames leídos a variables con nombres claros
        df_contact_event = dataframes[cf.input_table_list[0]]
        df_int_agent_det = dataframes[cf.input_table_list[1]]
        df_acd_call_det = dataframes[cf.input_table_list[2]]
        df_agent_act_sum = dataframes[cf.input_table_list[3]]
        df_cat_dialer = dataframes[cf.input_table_list[4]]
        df_jerarquia_dialer = dataframes[cf.input_table_list[5]]
        df_nice_active_forecast = dataframes[cf.input_table_list[6]]
        df_nice_agent_adherence_summary = dataframes[cf.input_table_list[7]]
        df_nice_agent_info = dataframes[cf.input_table_list[8]]

        # Pre-filtrar catálogos por la fecha de proceso más reciente
        max_cat_date = df_cat_dialer.select(max('process_date')).first()[0]
        df_cat_dialer = df_cat_dialer.filter(col('process_date') == max_cat_date)

        max_jerarquia_date = df_jerarquia_dialer.select(max('process_date')).first()[0]
        df_jerarquia_dialer = df_jerarquia_dialer.filter(col('process_date') == max_jerarquia_date)

        # Ejecutar las funciones de transformación de Dialer_Functions.py
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] INFO: Executing Dialer ETL functions...")
        
        (j_rg_sg, j_lg_rg_ib, j_lob_sid, j_lob_sid_staff, j_lg_rg_ob, 
         j_lob_alm, j_alm_active, j_lg_rg_staff, j_mu_rg, j_fg_rg_staff) = df.jerarquia_dialer(df_jerarquia_dialer)

        df_cat_dialer_proc = df.catalogo_dialer(df_cat_dialer)
        
        table_inbound = df.data_inbound(df_acd_call_det, df_int_agent_det, j_rg_sg, j_lg_rg_ib, j_lob_sid)
        table_outbound = df.data_outbound(df_contact_event, df_int_agent_det, df_cat_dialer_proc, j_lob_alm, j_alm_active, j_lg_rg_ob, j_rg_sg)
        table_staff = df.data_staff(df_agent_act_sum, df_int_agent_det, j_rg_sg, j_mu_rg, df_nice_agent_info)
        table_nice_act_forecast = df.data_forecast(j_rg_sg, j_fg_rg_staff, df_nice_active_forecast)
        table_nice_adh_attr_summary = df.data_nice_agent_adherence_summary(df_nice_agent_adherence_summary, j_mu_rg, j_rg_sg)
        
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] INFO: ETL functions executed successfully.")

        # --- 5. ESCRITURA DE RESULTADOS EN HIVE ---
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] INFO: Stage << Inserting results into Hive >>")
        
        output_dataframes = {
            "table_nice_adh_attr_summary": table_nice_adh_attr_summary,
            "table_staff": table_staff,
            "table_nice_act_forecast": table_nice_act_forecast,
            "table_inbound": table_inbound,
            "table_outbound": table_outbound,
        }
        
        not_inserted = []
        for df_key, df_to_write in output_dataframes.items():
            if df_to_write is None:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] WARNING: DataFrame for '{df_key}' is None. Skipping insertion.")
                not_inserted.append(cf.output_table_mapping.get(df_key, df_key))
                continue
            try:
                table_name = cf.output_table_mapping[df_key]
                full_table_name = f"{cf.schema_final_table}.{table_name}"
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] INFO: Inserting into {full_table_name}...")
                df_to_write.write.mode("overwrite").insertInto(full_table_name)
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] INFO: Successfully inserted into {full_table_name}")
                df_to_write.unpersist()
            except Exception as e:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ERROR: Failed to insert into {table_name}: {e}")
                not_inserted.append(table_name)
        
        if not_inserted:
            error_msg = f"Could not insert into the following tables: {not_inserted}"
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] FATAL: {error_msg}")
            # logger.end_process(mail, error_msg)
            sys.exit(1)
            
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] INFO: All tables inserted successfully.")

        # --- 6. CÁLCULO DE CIFRAS CONTROL ---
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] INFO: Stage << Calculating control figures >>")
        
        control_figures_data = [
            ("Adherence Summary", table_nice_adh_attr_summary.filter(col("date_nice_agent_adh_summary") >= cf.filter_date_control_figures).count()),
            ("Staff", table_staff.filter(col("record_date") >= cf.filter_date_control_figures).count()),
            ("Forecast", table_nice_act_forecast.filter(col("date_nice_active_fcst") >= cf.filter_date_control_figures).count()),
            ("Inbound", table_inbound.filter(col("record_date") >= cf.filter_date_control_figures).count()),
            ("Outbound", table_outbound.count())
        ]
        
        DF_Control = spark.createDataFrame(control_figures_data, ["DataFrame", "Registros"])
        print("Control Figures:")
        DF_Control.show(truncate=False)
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] INFO: Control figures calculated successfully.")

    except Exception as e:
        # --- MANEJO DE ERRORES GENERALES ---
        error_msg = f"An unexpected error occurred in the main process: {e}"
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] FATAL: {error_msg}")
        traceback.print_exc()
        # logger.end_process(mail, error_msg)
        sys.exit(1)
        
    finally:
        # --- 7. FINALIZACIÓN DEL PROCESO ---
        end_time = datetime.now()
        total_time = end_time - begin_time
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] INFO: Ending execution of {cf.processName}")
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] INFO: Total execution time: {total_time.seconds} seconds")
        
        # Enviar notificación final
        # notification_mail(cf.idProcess, cf.processName, cf.sender, cf.receivers, cf.fileName_results, cf.nameLog, DF_Control)
        
        if spark:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] INFO: Stopping SparkSession.")
            spark.stop()

if __name__ == "__main__":
    principal()