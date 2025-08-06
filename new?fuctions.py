from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import (
    col, lit, when, to_timestamp, to_date, hour, minute, concat_ws,
    lower, max, sum, trim, month, date_format, row_number, substring
)
from pyspark.sql.types import DateType, IntegerType, FloatType
import pyspark.sql.functions as F
from typing import Dict, Tuple

# ==============================================================================
#                               FUNCIONES AUXILIARES
# ==============================================================================

def _fill_nulls(df: DataFrame) -> DataFrame:
    """
    Rellena los valores nulos en un DataFrame con cadenas vacías para evitar errores de tipo en joins.
    """
    return df.select(*[when(col(c).isNull(), '').otherwise(col(c)).alias(c) for c in df.columns])

def _process_datetime_columns(df: DataFrame, time_col: str, alias_map: dict) -> DataFrame:
    """
    Función auxiliar para procesar las columnas de fecha y hora.
    Ajusta la zona horaria, extrae el intervalo de 30 minutos y crea la fecha.
    """
    return df.withColumn(
        time_col, to_timestamp(col(time_col) - F.expr('INTERVAL 6 HOURS'))
    ).withColumn(
        'record_date', to_date(col(time_col))
    ).withColumn(
        'interval_hour', concat_ws(
            ':',
            hour(col(time_col)),
            when(minute(col(time_col)) < 30, lit('00')).otherwise(lit('30'))
        )
    ).select(
        'interval_hour', 'record_date', *[col(c).alias(alias_map.get(c, c)) for c in df.columns if c not in [time_col]]
    )

def _process_catalogo_dialer(df_cat_dialer: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    Procesa el catálogo del dialer para obtener las tablas de saturación y disposiciones
    que se usarán en el procesamiento de outbound.
    """
    df_cat_dialer_list = df_cat_dialer.select(
        trim(col('aspect_list_name')).alias('aspect_list_name'), col('saturacion')
    ).groupBy('aspect_list_name').agg(
        max(col('saturacion')).alias('saturacion')
    ).select('aspect_list_name', 'saturacion')

    df_cat_dialer_disp = df_cat_dialer.select(
        trim(col('disposition')).alias('disposition'),
        col('attempt'), col('penetration'), col('connection'),
        col('direct_contact'), col('promise'), col('abandon'),
        trim(col('group_disposition')).alias('group_disposition')
    )
    
    return df_cat_dialer_list, df_cat_dialer_disp

# ==============================================================================
#                          FUNCIONES DE PROCESAMIENTO
# ==============================================================================

def jerarquia_dialer(df_jerarquia_dialer: DataFrame) -> Tuple:
    """
    Procesa un DataFrame maestro de jerarquías para crear DataFrames específicos
    basados en la columna 'tab', eliminando duplicados y ordenando por fecha de procesamiento.
    """
    try:
        df_jerarquia_dialer = df_jerarquia_dialer.sort('process_date', ascending=False)

        # 1. Report Groups to Super Groups
        jerarquia_dialer_hist_rg_sg = df_jerarquia_dialer.filter(col('tab') == 'Report Groups to Super Groups')
        jerarquia_dialer_hist_rg_sg = jerarquia_dialer_hist_rg_sg.select(
            col('reportnamemasterid'), col('reportname'), col('supergroupmasterid'), col('supergroupname'),
            to_date(col('startdate'), "dd/MM/yyyy").alias('startdate_sg'),
            to_date(col('stopdate'), "dd/MM/yyyy").alias('stopdate_sg'),
            to_date(col('process_date'), "yyyy-MM-dd").alias('process_date')
        ).dropDuplicates(subset=['reportnamemasterid', 'reportname', 'supergroupmasterid', 'supergroupname', 'startdate_sg'])
        
        # 2. LOB Group - ReportGrp (Inbound)
        jerarquia_dialer_hist_lg_rg_ib = df_jerarquia_dialer.filter(
            (col('tab') == 'LOB Group - ReportGrp') & (col('classification') == 'IB Service')
        )
        jerarquia_dialer_hist_lg_rg_ib = jerarquia_dialer_hist_lg_rg_ib.select(
            col('lobmasterid'), col('groupname'),
            to_date(col('lobtorgstartdate'), "dd/MM/yyyy").alias('lobtorgstartdate'),
            to_date(col('lobtorgstopdate'), "dd/MM/yyyy").alias('lobtorgstopdate'),
            col('reportnamemasterid'), col('reportname'),
            to_date(col('rgstartdate'), "dd/MM/yyyy").alias('rgstartdate'),
            to_date(col('process_date'), "yyyy-MM-dd").alias('process_date')
        ).dropDuplicates(subset=['lobmasterid', 'groupname', 'lobtorgstartdate', 'reportnamemasterid', 'reportname'])

        # 3. LOBs to Service Ids (Inbound)
        jerarquia_dialer_hist_lob_sid = df_jerarquia_dialer.filter(
            (col('tab') == 'LOBs to Service Ids') & (col('classification') == 'IB Service')
        )
        jerarquia_dialer_hist_lob_sid = jerarquia_dialer_hist_lob_sid.select(
            col('lobmasterid'), col('groupname'),
            to_date(col('startdate'), "dd/MM/yyyy").alias('startdate_serviceid'),
            to_date(col('stopdate'), "dd/MM/yyyy").alias('stopdate_serviceid'),
            col('uip_inst'), col('uip_inst_serviceid'), col('serviceid'),
            to_date(col('process_date'), "yyyy-MM-dd").alias('process_date')
        ).dropDuplicates(subset=['lobmasterid', 'groupname', 'startdate', 'serviceid'])

        # 4. LOB Group - ReportGrp (Outbound)
        jerarquia_dialer_hist_lg_rg_ob = df_jerarquia_dialer.filter(
            (col('tab') == 'LOB Group - ReportGrp') & (col('classification') == 'List')
        )
        jerarquia_dialer_hist_lg_rg_ob = jerarquia_dialer_hist_lg_rg_ob.select(
            col('lobmasterid'), col('groupname'),
            to_date(col('lobtorgstartdate'), "dd/MM/yyyy").alias('lobtorgstartdate'),
            to_date(col('lobtorgstopdate'), "dd/MM/yyyy").alias('lobtorgstopdate'),
            col('reportnamemasterid'), col('reportname'),
            to_date(col('rgstartdate'), "dd/MM/yyyy").alias('rgstartdate'),
            to_date(col('process_date'), "yyyy-MM-dd").alias('process_date')
        ).dropDuplicates(subset=['lobmasterid', 'groupname', 'lobtorgstartdate', 'reportnamemasterid', 'reportname'])

        # 5. LOBs to ALMLists (Outbound)
        jerarquia_dialer_hist_lob_alm = df_jerarquia_dialer.filter(col('tab') == 'LOBs to ALMLists')
        jerarquia_dialer_hist_lob_alm = jerarquia_dialer_hist_lob_alm.select(
            col('lobmasterid'), col('groupname'), col('uip_inst'), col('listname'),
            to_date(col('listname_startdate'), "dd/MM/yyyy").alias('listname_startdate'),
            to_date(col('listname_stopdate'), "dd/MM/yyyy").alias('listname_stopdate'),
            to_date(col('process_date'), "yyyy-MM-dd").alias('process_date')
        ).dropDuplicates(subset=['lobmasterid', 'groupname', 'uip_inst', 'listname', 'listname_startdate'])

        # 6. ALMList Active Goals (Outbound)
        jerarquia_dialer_hist_alm_active = df_jerarquia_dialer.filter(col('tab') == 'ALMList Active Goals')
        jerarquia_dialer_hist_alm_active = jerarquia_dialer_hist_alm_active.select(
            col('listname').alias('listname_active'),
            to_date(col('updatedate'), "dd/MM/yyyy").alias('updatedate_active'),
            col('goallow').alias('goallow_active'), col('goalhigh').alias('goalhigh_active'),
            to_date(col('startdate'), "dd/MM/yyyy").alias('startdate_active'),
            to_date(col('stopdate'), "dd/MM/yyyy").alias('stopdate_active'),
            to_date(col('process_date'), "yyyy-MM-dd").alias('process_date')
        ).dropDuplicates(subset=['listname_active', 'updatedate_active', 'startdate_active'])
        
        # 7. LOB Group - ReportGrp (Staff)
        jerarquia_dialer_hist_lg_rg_staff = df_jerarquia_dialer.filter(col('tab') == 'LOB Group - ReportGrp')
        jerarquia_dialer_hist_lg_rg_staff = jerarquia_dialer_hist_lg_rg_staff.select(
            col('lobmasterid'), col('groupname'),
            to_date(col('lobtorgstartdate'), "dd/MM/yyyy").alias('lobtorgstartdate'),
            to_date(col('lobtorgstopdate'), "dd/MM/yyyy").alias('lobtorgstopdate'),
            col('reportnamemasterid'), col('reportname'),
            to_date(col('rgstartdate'), "dd/MM/yyyy").alias('rgstartdate'),
            to_date(col('process_date'), "yyyy-MM-dd").alias('process_date')
        ).dropDuplicates(subset=['lobmasterid', 'groupname', 'lobtorgstartdate', 'reportnamemasterid', 'reportname'])
        
        # 8. NICEMU-WorkSeg-ReportGrp Config (Staff)
        jerarquia_dialer_hist_mu_rg = df_jerarquia_dialer.filter(col('tab') == 'NICEMU-WorkSeg-ReportGrp Config')
        jerarquia_dialer_hist_mu_rg = jerarquia_dialer_hist_mu_rg.select(
            col('reportnamemasterid'), col('reportname'), col('mu_id'), col('nicemu'),
            to_date(col('nicemu_startdate'), "dd/MM/yyyy").alias('nicemu_startdate'),
            to_date(col('nicemu_stopdate'), "dd/MM/yyyy").alias('nicemu_stopdate'),
            to_date(col('process_date'), "yyyy-MM-dd").alias('process_date')
        ).dropDuplicates(subset=['reportnamemasterid', 'reportname', 'mu_id', 'nicemu', 'nicemu_startdate'])

        # 9. Forecast Group to Report Group (Staff)
        jerarquia_dialer_hist_fg_rg_staff = df_jerarquia_dialer.filter(col('tab') == 'Forecast Group to Report Group')
        jerarquia_dialer_hist_fg_rg_staff = jerarquia_dialer_hist_fg_rg_staff.select(
            col('reportnamemasterid'), col('reportname'), col('fcstgrpid'), col('forecast_group_code'),
            to_date(col('fcst_group_code_startdate'), "dd/MM/yyyy").alias('fcst_group_code_startdate'),
            to_date(col('fcst_group_code_stopdate'), "dd/MM/yyyy").alias('fcst_group_code_stopdate'),
            to_date(col('process_date'), "yyyy-MM-dd").alias('process_date')
        ).dropDuplicates(subset=['reportnamemasterid', 'reportname', 'fcstgrpid', 'forecast_group_code', 'fcst_group_code_startdate'])

        return (jerarquia_dialer_hist_rg_sg, jerarquia_dialer_hist_lg_rg_ib, jerarquia_dialer_hist_lob_sid,
                jerarquia_dialer_hist_lg_rg_ob, jerarquia_dialer_hist_lob_alm, jerarquia_dialer_hist_alm_active,
                jerarquia_dialer_hist_lg_rg_staff, jerarquia_dialer_hist_mu_rg, jerarquia_dialer_hist_fg_rg_staff)
                
    except Exception as e:
        print(f"Error en la función jerarquia_dialer: {e}")
        return None

def data_inbound(
    df_contact_event: DataFrame,
    df_int_agent_det: DataFrame,
    jerarquia_dialer_hist_fg_rg_staff: DataFrame,
    jerarquia_dialer_hist_rg_sg: DataFrame
) -> DataFrame:
    """
    Procesa los datos de inbound combinando eventos de contacto, interacciones de agentes
    y las jerarquías del dialer.
    """
    try:
        df_contact_event_proc = _process_datetime_columns(
            df_contact_event,
            'time_of_contact',
            {'contact_list_name': 'contact_list_name', 'response_status': 'response_status',
             'agent_login_name': 'agent_login_name', 'total_number_of_records': 'total_number_of_records',
             'seqnum': 'seqnum'}
        )

        df_int_agent_det_proc = _process_datetime_columns(
            df_int_agent_det.filter(col('agenttime') > 0),
            'interactionstartdt',
            {'user_id': 'user_id', 'seqnum': 'seqnum', 'agenttime': 'agenttime',
             'previewtime': 'previewtime', 'activetime': 'activetime',
             'wraptime': 'wraptime', 'holdtime': 'holdtime'}
        )
        
        join_key_contact = ['record_date', 'interval_hour', 'contact_list_name']
        contact_event_grouped = df_contact_event_proc.groupBy(*join_key_contact).agg(
            sum('total_number_of_records').alias('total_number_of_records'),
            sum(when(col('response_status') != '', 1).otherwise(0)).alias('records_with_response_status')
        )

        join_key_int_agent = ['record_date', 'interval_hour', 'user_id', 'seqnum']
        int_agent_grouped = df_int_agent_det_proc.groupBy(*join_key_int_agent).agg(
            *[sum(c).alias(c) for c in ['agenttime', 'previewtime', 'activetime', 'wraptime', 'holdtime']]
        )
        
        uip_inbound_d = contact_event_grouped.join(
            int_agent_grouped,
            on=[
                contact_event_grouped['record_date'] == int_agent_grouped['record_date'],
                contact_event_grouped['interval_hour'] == int_agent_grouped['interval_hour']
            ],
            how='full_outer'
        ).drop(int_agent_grouped['record_date']).drop(int_agent_grouped['interval_hour'])

    except Exception as e:
        print(f"Error en el procesamiento inicial de data_inbound: {e}")
        return None

    try:
        uip_inbound_d = uip_inbound_d.join(
            jerarquia_dialer_hist_fg_rg_staff,
            on=(
                (uip_inbound_d['contact_list_name'] == jerarquia_dialer_hist_fg_rg_staff['fcstgrpid']) &
                (uip_inbound_d['record_date'] >= jerarquia_dialer_hist_fg_rg_staff['fcst_group_code_startdate']) &
                (uip_inbound_d['record_date'] <= when(
                    jerarquia_dialer_hist_fg_rg_staff['fcst_group_code_stopdate'].isNull(),
                    uip_inbound_d['record_date']
                ).otherwise(jerarquia_dialer_hist_fg_rg_staff['fcst_group_code_stopdate']))
            ),
            how='inner'
        ).withColumn('reportnamemasterid', jerarquia_dialer_hist_fg_rg_staff['reportnamemasterid'])\
         .withColumn('reportname', jerarquia_dialer_hist_fg_rg_staff['reportname'])

        uip_inbound_d = uip_inbound_d.join(
            jerarquia_dialer_hist_rg_sg,
            on=(
                (uip_inbound_d['reportnamemasterid'] == jerarquia_dialer_hist_rg_sg['reportnamemasterid']) &
                (uip_inbound_d['record_date'] >= jerarquia_dialer_hist_rg_sg['startdate_sg']) &
                (uip_inbound_d['record_date'] <= when(
                    jerarquia_dialer_hist_rg_sg['stopdate_sg'].isNull(),
                    uip_inbound_d['record_date']
                ).otherwise(jerarquia_dialer_hist_rg_sg['stopdate_sg']))
            ),
            how='inner'
        ).withColumn('supergroupname', jerarquia_dialer_hist_rg_sg['supergroupname'])

    except Exception as e:
        print(f"Error en las uniones con las jerarquías: {e}")
        return None

    uip_inbound_d_final = uip_inbound_d.withColumn(
        'month', month(col('record_date'))
    ).drop('reportnamemasterid', 'contact_list_name')

    columnas_finales = [
        col('interval_hour'), col('record_date').cast(DateType()), col('supergroupname'),
        col('reportname'), col('total_number_of_records').cast(IntegerType()),
        col('records_with_response_status').cast(IntegerType()), col('agenttime').cast(IntegerType()),
        col('previewtime').cast(IntegerType()), col('activetime').cast(IntegerType()),
        col('wraptime').cast(IntegerType()), col('holdtime').cast(IntegerType()),
        col('month').cast(IntegerType())
    ]
    
    uip_inbound_d_final = uip_inbound_d_final.select(*columnas_finales).sort('record_date')
    
    return _fill_nulls(uip_inbound_d_final)

def data_outbound(
    df_contact_event: DataFrame,
    df_int_agent_det: DataFrame,
    df_cat_dialer: DataFrame,
    jerarquia_dialer_hist_lob_alm: DataFrame,
    jerarquia_dialer_hist_alm_active: DataFrame,
    jerarquia_dialer_hist_lg_rg_ob: DataFrame,
    jerarquia_dialer_hist_rg_sg: DataFrame
) -> DataFrame:
    """
    Procesa los datos de outbound combinando información de eventos de contacto,
    detalle de agentes y jerarquías del dialer.
    """
    try:
        df_contact_event_proc = df_contact_event.select(
            col('contact_list_name'), col('response_status'), lower(col('agent_login_name')).alias('agent_login_name'),
            col('total_number_of_records'), col('seqnum'),
            to_timestamp(col('time_of_contact') - F.expr('INTERVAL 6 HOURS')).alias('time_of_contact')
        ).withColumn(
            'record_date', to_date(col('time_of_contact'))
        ).withColumn(
            'interval_hour', concat_ws(
                ':',
                hour(col('time_of_contact')),
                when(minute(col('time_of_contact')) < 30, lit('00')).otherwise(lit('30'))
            )
        )
        
        max_tot_num_rec = df_contact_event_proc.withColumn(
            'join_key', concat_ws('-', col('record_date'), col('contact_list_name'))
        ).groupBy('join_key').agg(
            max('total_number_of_records').alias('max_total_number_of_records')
        )

        not_emp_res_sta = df_contact_event_proc.filter(col('response_status') != '').withColumn(
            'join_key', concat_ws('-', col('record_date'), col('contact_list_name'))
        ).groupBy('join_key').agg(
            max('total_number_of_records').alias('max_total_number_of_records_not_emp_res_sta')
        )
        
        df_contact_event_final = df_contact_event_proc.withColumn(
            'join_key', concat_ws('-', col('record_date'), col('interval_hour'), col('seqnum'), col('agent_login_name'))
        ).join(
            df_contact_event_proc.withColumn('join_key', concat_ws('-', col('record_date'), col('contact_list_name')))\
                                  .join(max_tot_num_rec, on='join_key', how='left')\
                                  .join(not_emp_res_sta, on='join_key', how='left'),
            on=df_contact_event_proc['join_key'] == df_int_agent_det_proc['join_key'], # Corregir este join
            how='left'
        )

    except Exception as e:
        print(f"Error en el procesamiento de ContactEvent: {e}")
        return None

    try:
        df_int_agent_det_proc = df_int_agent_det.filter(
            col('agenttime') > 0
        ).withColumn(
            'interactionstartdt', to_timestamp(col('interactionstartdt') - F.expr('INTERVAL 6 HOURS'))
        ).withColumn(
            'record_date', to_date(col('interactionstartdt'))
        ).withColumn(
            'interval_hour', concat_ws(
                ':',
                hour(col('interactionstartdt')),
                when(minute(col('interactionstartdt')) < 30, lit('00')).otherwise(lit('30'))
            )
        ).withColumn(
            'join_key', concat_ws('-', col('record_date'), col('interval_hour'), col('seqnum'), lower(col('user_id')))
        )

        df_int_agent_det_grouped = df_int_agent_det_proc.groupBy('join_key').agg(
            sum('agenttime').alias('agenttime'),
            sum('previewtime').alias('previewtime'),
            sum('activetime').alias('activetime'),
            sum('wraptime').alias('wraptime'),
            sum('holdtime').alias('holdtime')
        )

        df_contact_event_final = df_contact_event_final.withColumn(
            'join_key', concat_ws('-', col('record_date'), col('interval_hour'), col('seqnum'), col('agent_login_name'))
        ).join(
            df_int_agent_det_grouped, on='join_key', how='left'
        ).drop('join_key')

    except Exception as e:
        print(f"Error en el procesamiento de InteractionAgentDetail o su unión: {e}")
        return None

    try:
        df_cat_dialer_list, df_cat_dialer_disp = _process_catalogo_dialer(df_cat_dialer)
        
        uip_outbound_d = df_contact_event_final.join(
            df_cat_dialer_list,
            on=df_contact_event_final['contact_list_name'] == df_cat_dialer_list['aspect_list_name'],
            how='left'
        ).join(
            df_cat_dialer_disp,
            on=df_contact_event_final['response_status'] == df_cat_dialer_disp['disposition'],
            how='left'
        )
    except Exception as e:
        print(f"Error en la unión con el catálogo del dialer: {e}")
        return None
        
    try:
        uip_outbound_d = uip_outbound_d.join(
            jerarquia_dialer_hist_lob_alm.distinct(),
            on=uip_outbound_d['contact_list_name'] == jerarquia_dialer_hist_lob_alm['listname'],
            how='left'
        ).withColumn('lobmasterid', jerarquia_dialer_hist_lob_alm['lobmasterid'])\
         .withColumn('groupname', jerarquia_dialer_hist_lob_alm['groupname'])

        uip_outbound_d = uip_outbound_d.join(
            jerarquia_dialer_hist_alm_active.select('listname_active', 'goalhigh_active').distinct(),
            on=uip_outbound_d['contact_list_name'] == jerarquia_dialer_hist_alm_active['listname_active'],
            how='left'
        ).withColumn('goalhigh_active', jerarquia_dialer_hist_alm_active['goalhigh_active'])
        
        uip_outbound_d = uip_outbound_d.join(
            jerarquia_dialer_hist_lg_rg_ob,
            on=(
                (uip_outbound_d['lobmasterid'] == jerarquia_dialer_hist_lg_rg_ob['lobmasterid']) &
                (uip_outbound_d['record_date'] >= jerarquia_dialer_hist_lg_rg_ob['lobtorgstartdate']) &
                (uip_outbound_d['record_date'] <= when(
                    jerarquia_dialer_hist_lg_rg_ob['lobtorgstopdate'].isNull(),
                    uip_outbound_d['record_date']
                ).otherwise(jerarquia_dialer_hist_lg_rg_ob['lobtorgstopdate']))
            ),
            how='left'
        ).withColumn('reportnamemasterid', jerarquia_dialer_hist_lg_rg_ob['reportnamemasterid'])\
         .withColumn('reportname', jerarquia_dialer_hist_lg_rg_ob['reportname'])
        
        uip_outbound_d = uip_outbound_d.join(
            jerarquia_dialer_hist_rg_sg,
            on=(
                (uip_outbound_d['reportnamemasterid'] == jerarquia_dialer_hist_rg_sg['reportnamemasterid']) &
                (uip_outbound_d['record_date'] >= jerarquia_dialer_hist_rg_sg['startdate_sg']) &
                (uip_outbound_d['record_date'] <= when(
                    jerarquia_dialer_hist_rg_sg['stopdate_sg'].isNull(),
                    uip_outbound_d['record_date']
                ).otherwise(jerarquia_dialer_hist_rg_sg['stopdate_sg']))
            ),
            how='left'
        ).withColumn('supergroupname', jerarquia_dialer_hist_rg_sg['supergroupname'])
        
    except Exception as e:
        print(f"Error en las uniones con la jerarquía del dialer: {e}")
        return None
        
    uip_outbound_d = uip_outbound_d.withColumn(
        'supergroupname', when(col('supergroupname').isNull(), 'Sin Asignar').otherwise(col('supergroupname'))
    ).withColumn(
        'reportname', when(col('reportname').isNull(), 'Sin Asignar').otherwise(col('reportname'))
    ).withColumn(
        'month', month(col('record_date'))
    ).drop(
        'lobmasterid', 'reportnamemasterid'
    )
    
    columnas_finales = [
        col('interval_hour'), col('record_date').cast(DateType()), col('supergroupname'), col('reportname'),
        col('contact_list_name'), col('agent_login_name'), col('max_total_number_of_records').cast(IntegerType()),
        col('max_total_number_of_records_not_emp_res_sta').cast(IntegerType()), col('agenttime').cast(IntegerType()),
        col('previewtime').cast(IntegerType()), col('activetime').cast(IntegerType()), col('wraptime').cast(IntegerType()),
        col('holdtime').cast(IntegerType()), col('saturacion').cast(IntegerType()), col('attempt').cast(IntegerType()),
        col('penetration').cast(IntegerType()), col('connection').cast(IntegerType()), col('direct_contact').cast(IntegerType()),
        col('promise').cast(IntegerType()), col('abandon').cast(IntegerType()),
        col('group_disposition'), col('goalhigh_active').cast(FloatType()), col('month').cast(IntegerType())
    ]
    
    uip_outbound_d = uip_outbound_d.select(*columnas_finales).sort('record_date')
    
    return _fill_nulls(uip_outbound_d)

def data_staff(
    df_agent_act_sum: DataFrame,
    df_int_agent_det: DataFrame,
    jerarquia_dialer_hist_rg_sg: DataFrame,
    jerarquia_dialer_hist_mu_rg: DataFrame,
    df_nice_agent_info: DataFrame
) -> DataFrame:
    """
    Procesa los datos de staff combinando información de actividad del agente,
    interacciones y jerarquías.
    """
    try:
        df_agent_act_sum_proc = df_agent_act_sum.filter(col('service_id') == 0).select(
            lower(col('user_id')).alias('user_id'), col('service_id'),
            col('totallogintime'), col('totalidletime'), col('totalnotreadytime'),
            col('totalgaptime'), col('totalparkidletime'), col('totalparktime'),
            to_timestamp(col('begintimeperioddt') - F.expr('INTERVAL 6 HOURS')).alias('begintimeperioddt')
        ).withColumn(
            'record_date', to_date(col('begintimeperioddt'))
        ).withColumn(
            'interval_hour', concat_ws(':', hour(col('begintimeperioddt')), when(minute(col('begintimeperioddt')) < 30, lit('00')).otherwise(lit('30')))
        )
        
        df_agent_act_sum_grouped = df_agent_act_sum_proc.groupBy(
            'interval_hour', 'record_date', 'user_id', 'service_id'
        ).agg(
            *[sum(c).alias(c) for c in [
                'totallogintime', 'totalidletime', 'totalnotreadytime',
                'totalgaptime', 'totalparkidletime', 'totalparktime'
            ]]
        )

        df_int_agent_det_proc = df_int_agent_det.filter(col('agenttime') > 0).select(
            lower(col('user_id')).alias('user_id'), col('seqnum'),
            col('agenttime'), col('previewtime'), col('activetime'),
            col('wraptime'), col('holdtime'), 
            to_timestamp(col('interactionstartdt') - F.expr('INTERVAL 6 HOURS')).alias('interactionstartdt')
        ).withColumn(
            'record_date', to_date(col('interactionstartdt'))
        ).withColumn(
            'interval_hour', concat_ws(':', hour(col('interactionstartdt')), when(minute(col('interactionstartdt')) < 30, lit('00')).otherwise(lit('30')))
        )
        
        df_int_agent_det_grouped = df_int_agent_det_proc.groupBy(
            'interval_hour', 'record_date', 'user_id'
        ).agg(
            *[sum(c).alias(c) for c in [
                'agenttime', 'activetime', 'wraptime', 'previewtime', 'holdtime'
            ]]
        )
        
        join_key_cols = ['record_date', 'interval_hour', 'user_id']
        uip_staff_d = df_agent_act_sum_grouped.join(
            df_int_agent_det_grouped, on=join_key_cols, how='left'
        )
    except Exception as e:
        print(f"Error en el procesamiento de AgentActivity o InteractionDetail: {e}")
        return None
    
    try:
        window_spec = Window.partitionBy('date', 'muid', 'externalid').orderBy(col('process_date').desc())
        df_nice_agent_info_proc = df_nice_agent_info.withColumn(
            'date_nice_agent_info', date_format(to_date(col('date'), 'yyyyMMdd'), 'yyyy-MM-dd')
        ).withColumn(
            'agent_info_id', lower(col('externalid'))
        ).withColumn(
            'process_date', to_date(col('process_date'))
        ).withColumn(
            'row_num', row_number().over(window_spec)
        ).filter(
            col('row_num') == 1
        ).drop('row_num', 'date', 'externalid')
        
        uip_staff_d = uip_staff_d.join(
            df_nice_agent_info_proc,
            on=[
                uip_staff_d['record_date'] == df_nice_agent_info_proc['date_nice_agent_info'],
                uip_staff_d['user_id'] == df_nice_agent_info_proc['agent_info_id']
            ],
            how='left'
        ).select(
            uip_staff_d['*'],
            df_nice_agent_info_proc['muid'].alias('nice_muid'),
            df_nice_agent_info_proc['agent_info_id'].alias('nice_externalid')
        ).drop('date_nice_agent_info', 'agent_info_id', 'process_date')

    except Exception as e:
        print(f"Error en la unión con df_nice_agent_info: {e}")
        return None

    try:
        uip_staff_d = uip_staff_d.join(
            jerarquia_dialer_hist_mu_rg,
            on=(
                (uip_staff_d['nice_muid'] == jerarquia_dialer_hist_mu_rg['mu_id']) &
                (uip_staff_d['record_date'] >= jerarquia_dialer_hist_mu_rg['nicemu_startdate']) &
                (uip_staff_d['record_date'] <= when(
                    jerarquia_dialer_hist_mu_rg['nicemu_stopdate'].isNull(),
                    uip_staff_d['record_date']
                ).otherwise(jerarquia_dialer_hist_mu_rg['nicemu_stopdate']))
            ),
            how='left'
        ).withColumn('reportnamemasterid', jerarquia_dialer_hist_mu_rg['reportnamemasterid'])\
         .withColumn('reportname', jerarquia_dialer_hist_mu_rg['reportname'])
        
        uip_staff_d = uip_staff_d.join(
            jerarquia_dialer_hist_rg_sg,
            on=(
                (uip_staff_d['reportnamemasterid'] == jerarquia_dialer_hist_rg_sg['reportnamemasterid']) &
                (uip_staff_d['record_date'] >= jerarquia_dialer_hist_rg_sg['startdate_sg']) &
                (uip_staff_d['record_date'] <= when(
                    jerarquia_dialer_hist_rg_sg['stopdate_sg'].isNull(),
                    uip_staff_d['record_date']
                ).otherwise(jerarquia_dialer_hist_rg_sg['stopdate_sg']))
            ),
            how='left'
        ).withColumn('supergroupname', jerarquia_dialer_hist_rg_sg['supergroupname'])
        
    except Exception as e:
        print(f"Error en las uniones con las jerarquías: {e}")
        return None

    uip_staff_d_final = uip_staff_d.withColumn(
        'supergroupname', when(col('supergroupname').isNull(), 'Sin Asignar').otherwise(col('supergroupname'))
    ).withColumn(
        'reportname', when(col('reportname').isNull(), 'Sin Asignar').otherwise(col('reportname'))
    ).withColumn(
        'month', month(col('record_date'))
    ).drop(
        'nice_muid', 'nice_externalid', 'reportnamemasterid', 'service_id'
    )
    
    columnas_finales = [
        col('interval_hour'), col('record_date').cast(DateType()), col('supergroupname'), col('reportname'),
        col('user_id'), col('totallogintime').cast(IntegerType()),
        col('totalidletime').cast(IntegerType()), col('totalnotreadytime').cast(IntegerType()),
        col('totalgaptime').cast(IntegerType()), col('totalparkidletime').cast(IntegerType()),
        col('totalparktime').cast(IntegerType()), col('agenttime').cast(IntegerType()),
        col('previewtime').cast(IntegerType()), col('activetime').cast(IntegerType()),
        col('wraptime').cast(IntegerType()), col('holdtime').cast(IntegerType()), col('month').cast(IntegerType())
    ]
    
    uip_staff_d_final = uip_staff_d_final.select(*columnas_finales).sort('record_date')
    
    return _fill_nulls(uip_staff_d_final)

def data_forecast(
    jerarquia_dialer_hist_rg_sg: DataFrame,
    jerarquia_dialer_hist_fg_rg_staff: DataFrame,
    df_nice_active_forecast: DataFrame
) -> DataFrame:
    """
    Procesa los datos de previsión combinando la previsión activa de Nice
    con las jerarquías del dialer.
    """
    try:
        df_nice_active_forecast_proc = df_nice_active_forecast.withColumn(
            'date_nice_active_fcst', date_format(to_date(col('date'), 'yyyyMMdd'), 'yyyy-MM-dd')
        ).withColumn(
            'period_hour', when(hour(to_timestamp(col('period'), 'H:mm')) < 10, substring(col('period'), 1, 1)).otherwise(substring(col('period'), 1, 2))
        ).withColumn(
            'period_minute', substring(col('period'), -2, 2)
        ).withColumn(
            'period_nice_active_fcst', concat_ws(':', col('period_hour'), col('period_minute'))
        ).withColumn(
            'process_date', to_date(col('process_date'))
        )
        
        window_spec = Window.partitionBy('date_nice_active_fcst', 'period_nice_active_fcst', 'ctid').orderBy(col('process_date').desc())
        df_nice_active_forecast_final = df_nice_active_forecast_proc.withColumn('row_num', row_number().over(window_spec))\
            .filter(col('row_num') == 1)\
            .drop('row_num', 'period_hour', 'period_minute', 'process_date', 'date', 'period')

    except Exception as e:
        print(f"Error en el procesamiento inicial de df_nice_active_forecast: {e}")
        return None

    try:
        uip_nice_active_forecast_d = df_nice_active_forecast_final.join(
            jerarquia_dialer_hist_fg_rg_staff,
            on=(
                (df_nice_active_forecast_final['ctid'] == jerarquia_dialer_hist_fg_rg_staff['fcstgrpid']) &
                (df_nice_active_forecast_final['date_nice_active_fcst'] >= jerarquia_dialer_hist_fg_rg_staff['fcst_group_code_startdate']) &
                (df_nice_active_forecast_final['date_nice_active_fcst'] <= when(
                    jerarquia_dialer_hist_fg_rg_staff['fcst_group_code_stopdate'].isNull(),
                    df_nice_active_forecast_final['date_nice_active_fcst']
                ).otherwise(jerarquia_dialer_hist_fg_rg_staff['fcst_group_code_stopdate']))
            ),
            how='inner'
        ).withColumn('reportnamemasterid', jerarquia_dialer_hist_fg_rg_staff['reportnamemasterid'])\
         .withColumn('reportname', jerarquia_dialer_hist_fg_rg_staff['reportname'])

        uip_nice_active_forecast_d = uip_nice_active_forecast_d.join(
            jerarquia_dialer_hist_rg_sg,
            on=(
                (uip_nice_active_forecast_d['reportnamemasterid'] == jerarquia_dialer_hist_rg_sg['reportnamemasterid']) &
                (uip_nice_active_forecast_d['date_nice_active_fcst'] >= jerarquia_dialer_hist_rg_sg['startdate_sg']) &
                (uip_nice_active_forecast_d['date_nice_active_fcst'] <= when(
                    jerarquia_dialer_hist_rg_sg['stopdate_sg'].isNull(),
                    uip_nice_active_forecast_d['date_nice_active_fcst']
                ).otherwise(jerarquia_dialer_hist_rg_sg['stopdate_sg']))
            ),
            how='inner'
        ).withColumn('supergroupname', jerarquia_dialer_hist_rg_sg['supergroupname'])
        
    except Exception as e:
        print(f"Error en las uniones con las jerarquías: {e}")
        return None

    uip_nice_active_forecast_d_final = uip_nice_active_forecast_d.withColumn(
        'month', month(col('date_nice_active_fcst'))
    ).drop('ctid', 'reportnamemasterid')
    
    columnas_finales = [
        col('period_nice_active_fcst'), col('date_nice_active_fcst').cast(DateType()),
        col('supergroupname'), col('reportname'), col('fcstcontactsreceived').cast(FloatType()),
        col('fcstaht').cast(FloatType()), col('fcstreq').cast(FloatType()),
        col('schedopen').cast(FloatType()), col('month').cast(IntegerType())
    ]
    
    uip_nice_active_forecast_d_final = uip_nice_active_forecast_d_final.select(*columnas_finales).sort('date_nice_active_fcst')

    return _fill_nulls(uip_nice_active_forecast_d_final)

def data_nice_agent_adherence_summary(
    df_nice_agent_adherence_summary: DataFrame,
    jerarquia_dialer_hist_mu_rg: DataFrame,
    jerarquia_dialer_hist_rg_sg: DataFrame
) -> DataFrame:
    """
    Procesa los datos de adherencia del agente combinando el resumen de Nice
    con las jerarquías del dialer.
    """
    try:
        window_spec = Window.partitionBy(
            'date_nice_agent_adh_summary', 'muid', 'attribute', 'externalid'
        ).orderBy(col('process_date').desc())

        df_nice_agent_adh_proc = df_nice_agent_adherence_summary.withColumn(
            'date_nice_agent_adh_summary', date_format(to_date(col('date'), 'yyyyMMdd'), 'yyyy-MM-dd')
        ).withColumn(
            'attribute', trim(col('attribute'))
        ).withColumn(
            'total_active', (col('totalact') / 3600).cast(FloatType())
        ).withColumn(
            'total_scheduled', (col('totalsched') / 3600).cast(FloatType())
        ).withColumn(
            'unitmanagerid', lower(substring(col('unitmanager'), -7, 7))
        ).withColumn(
            'logonid', lower(col('logonid'))
        ).withColumn(
            'process_date', to_date(col('process_date'))
        ).withColumn(
            'row_num', row_number().over(window_spec)
        ).filter(
            col('row_num') == 1
        ).drop(
            'row_num', 'process_date', 'date'
        )

    except Exception as e:
        print(f"Error en el procesamiento inicial de df_nice_agent_adherence_summary: {e}")
        return None
    
    try:
        uip_nice_agent_adh_summary_d = df_nice_agent_adh_proc.join(
            jerarquia_dialer_hist_mu_rg,
            on=(
                (df_nice_agent_adh_proc['muid'] == jerarquia_dialer_hist_mu_rg['mu_id']) &
                (df_nice_agent_adh_proc['date_nice_agent_adh_summary'] >= jerarquia_dialer_hist_mu_rg['nicemu_startdate']) &
                (df_nice_agent_adh_proc['date_nice_agent_adh_summary'] <= when(
                    jerarquia_dialer_hist_mu_rg['nicemu_stopdate'].isNull(),
                    df_nice_agent_adh_proc['date_nice_agent_adh_summary']
                ).otherwise(jerarquia_dialer_hist_mu_rg['nicemu_stopdate']))
            ),
            how='inner'
        ).withColumn('reportnamemasterid', jerarquia_dialer_hist_mu_rg['reportnamemasterid'])\
         .withColumn('reportname', jerarquia_dialer_hist_mu_rg['reportname'])

        uip_nice_agent_adh_summary_d = uip_nice_agent_adh_summary_d.join(
            jerarquia_dialer_hist_rg_sg,
            on=(
                (uip_nice_agent_adh_summary_d['reportnamemasterid'] == jerarquia_dialer_hist_rg_sg['reportnamemasterid']) &
                (uip_nice_agent_adh_summary_d['date_nice_agent_adh_summary'] >= jerarquia_dialer_hist_rg_sg['startdate_sg']) &
                (uip_nice_agent_adh_summary_d['date_nice_agent_adh_summary'] <= when(
                    jerarquia_dialer_hist_rg_sg['stopdate_sg'].isNull(),
                    uip_nice_agent_adh_summary_d['date_nice_agent_adh_summary']
                ).otherwise(jerarquia_dialer_hist_rg_sg['stopdate_sg']))
            ),
            how='inner'
        ).withColumn('supergroupname', jerarquia_dialer_hist_rg_sg['supergroupname'])

    except Exception as e:
        print(f"Error en las uniones con las jerarquías: {e}")
        return None

    uip_nice_agent_adh_summary_d_final = uip_nice_agent_adh_summary_d.withColumn(
        'month', month(col('date_nice_agent_adh_summary'))
    ).drop('muid', 'reportnamemasterid')

    columnas_finales = [
        col('date_nice_agent_adh_summary').cast(DateType()), col('supergroupname'),
        col('reportname'), col('unitmanager'), col('unitmanagerid'),
        col('logonid'), col('attribute'), col('total_active').cast(FloatType()),
        col('total_scheduled').cast(FloatType()), col('month').cast(IntegerType())
    ]

    uip_nice_agent_adh_summary_d_final = uip_nice_agent_adh_summary_d_final.select(*columnas_finales).sort('date_nice_agent_adh_summary')
    
    return _fill_nulls(uip_nice_agent_adh_summary_d_final)

def nice_dialer(
    df_nice_agent_info: DataFrame,
    df_nice_agent_dialer: DataFrame,
    df_nice_agent_hier_master: DataFrame
) -> DataFrame:
    """
    Procesa las jerarquías de Nice para generar una tabla maestra de agentes con sus IDs
    y jerarquías asociadas, extrayendo el registro más reciente.
    """
    try:
        window_spec_info = Window.partitionBy('date', 'muid', 'externalid').orderBy(col('process_date').desc())
        df_nice_agent_info_proc = df_nice_agent_info.withColumn(
            'row_num', row_number().over(window_spec_info)
        ).filter(
            col('row_num') == 1
        ).select(
            col('muid').alias('muid_info'),
            lower(col('externalid')).alias('externalid_info'),
            to_date(col('date'), 'yyyyMMdd').alias('date_info')
        ).drop('row_num')

        window_spec_dialer = Window.partitionBy('date', 'loginid', 'listname').orderBy(col('process_date').desc())
        df_nice_agent_dialer_proc = df_nice_agent_dialer.withColumn(
            'row_num', row_number().over(window_spec_dialer)
        ).filter(
            col('row_num') == 1
        ).select(
            to_date(col('date'), 'yyyyMMdd').alias('date_dialer'),
            lower(col('loginid')).alias('loginid_dialer'),
            trim(col('listname')).alias('listname_dialer')
        ).drop('row_num')

        window_spec_hier = Window.partitionBy('externalid', 'subgroupname').orderBy(col('process_date').desc())
        df_nice_agent_hier_proc = df_nice_agent_hier_master.withColumn(
            'row_num', row_number().over(window_spec_hier)
        ).filter(
            col('row_num') == 1
        ).select(
            lower(col('externalid')).alias('externalid_hier'),
            trim(col('subgroupname')).alias('subgroupname_hier'),
            trim(col('supervisorname')).alias('supervisorname_hier'),
            trim(col('reportname')).alias('reportname_hier'),
            trim(col('lobname')).alias('lobname_hier'),
            trim(col('campaignname')).alias('campaignname_hier')
        ).drop('row_num')

        df_unificado = df_nice_agent_info_proc.join(
            df_nice_agent_dialer_proc,
            on=[
                df_nice_agent_info_proc['date_info'] == df_nice_agent_dialer_proc['date_dialer'],
                df_nice_agent_info_proc['externalid_info'] == df_nice_agent_dialer_proc['loginid_dialer']
            ],
            how='left'
        )

        df_unificado = df_unificado.join(
            df_nice_agent_hier_proc,
            on=df_unificado['externalid_info'] == df_nice_agent_hier_proc['externalid_hier'],
            how='left'
        )

        columnas_finales = [
            col('date_info').alias('date'),
            col('muid_info').alias('muid'),
            col('externalid_info').alias('externalid'),
            col('listname_dialer').alias('listname'),
            col('subgroupname_hier').alias('subgroupname'),
            col('supervisorname_hier').alias('supervisorname'),
            col('reportname_hier').alias('reportname'),
            col('lobname_hier').alias('lobname'),
            col('campaignname_hier').alias('campaignname')
        ]

        df_final = df_unificado.select(*columnas_finales)

        return _fill_nulls(df_final)

    except Exception as e:
        print(f"Error en el procesamiento de nice_dialer: {e}")
        return None