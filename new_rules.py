 # 4. Aplicar la lógica de negocio del Factor Multiplicativo
        df_with_factor = df_joined.withColumn('Rbucket', sf.col('dpd_bucket') - sf.col('dpd_bucket_sug'))
        df_with_factor = df_with_factor.withColumn('Rbucket', sf.when((sf.col('Rbucket') == 6) | (sf.col('Rbucket').isNull()), 0.0).otherwise(df_with_factor.Rbucket))
        df_with_factor = df_with_factor.withColumn('dpd_bucket_sug', sf.when((sf.col('dpd_bucket_sug') == 0) | (sf.col('dpd_bucket_sug').isNull()), 0).otherwise(df_with_factor.dpd_bucket_sug))
        
        # Lógica compleja del when/otherwise para el factor
        df_final = df_with_factor.withColumn('Factor_Multiplicativo',
            sf.when((sf.col('dpd_bucket') == 0) & (sf.col('promesa_cumplida') == 'cumplida') & (sf.col('dpd_bucket_sug') == 0), 1.0)
            .when((sf.col('dpd_bucket') == 1) & (sf.col('promesa_cumplida') == 'cumplida') & (sf.col('dpd_bucket_sug') == 0), 2.0)
            .when((sf.col('dpd_bucket') == 1) & (sf.col('promesa_cumplida') == 'cumplida') & (sf.col('dpd_bucket_sug') == 1), 1.0)
            .when((sf.col('dpd_bucket') == 2) & (sf.col('promesa_cumplida') == 'cumplida') & (sf.col('dpd_bucket_sug') == 0), 2.0)
            .when((sf.col('dpd_bucket') == 2) & (sf.col('promesa_cumplida') == 'cumplida') & (sf.col('dpd_bucket_sug') == 1), 1.5)
            .when((sf.col('dpd_bucket') == 2) & (sf.col('promesa_cumplida') == 'cumplida') & (sf.col('dpd_bucket_sug') == 2), 1.0)
            .when((sf.col('dpd_bucket') == 3) & (sf.col('promesa_cumplida') == 'cumplida') & (sf.col('dpd_bucket_sug') == 0), 2.0)
            .when((sf.col('dpd_bucket') == 3) & (sf.col('promesa_cumplida') == 'cumplida') & (sf.col('dpd_bucket_sug') == 1), 1.5)
            .when((sf.col('dpd_bucket') == 3) & (sf.col('promesa_cumplida') == 'cumplida') & (sf.col('dpd_bucket_sug') == 2), 1.5)
            .when((sf.col('dpd_bucket') == 3) & (sf.col('promesa_cumplida') == 'cumplida') & (sf.col('dpd_bucket_sug') == 3), 1.0)
            .when((sf.col('dpd_bucket') == 4) & (sf.col('promesa_cumplida') == 'cumplida') & (sf.col('dpd_bucket_sug') == 0), 2.0)
            .when((sf.col('dpd_bucket') == 4) & (sf.col('promesa_cumplida') == 'cumplida') & (sf.col('dpd_bucket_sug') == 1), 1.5)
            .when((sf.col('dpd_bucket') == 4) & (sf.col('promesa_cumplida') == 'cumplida') & (sf.col('dpd_bucket_sug') == 2), 1.5)
            .when((sf.col('dpd_bucket') == 4) & (sf.col('promesa_cumplida') == 'cumplida') & (sf.col('dpd_bucket_sug') == 3), 1.5)
            .when((sf.col('dpd_bucket') == 4) & (sf.col('promesa_cumplida') == 'cumplida') & (sf.col('dpd_bucket_sug') == 4), 1.0)
            .when((sf.col('dpd_bucket') == 5) & (sf.col('promesa_cumplida') == 'cumplida') & (sf.col('dpd_bucket_sug') == 0), 2.0)
            .when((sf.col('dpd_bucket') == 5) & (sf.col('promesa_cumplida') == 'cumplida') & (sf.col('dpd_bucket_sug') == 1), 1.5)
            .when((sf.col('dpd_bucket') == 5) & (sf.col('promesa_cumplida') == 'cumplida') & (sf.col('dpd_bucket_sug') == 2), 1.5)
            .when((sf.col('dpd_bucket') == 5) & (sf.col('promesa_cumplida') == 'cumplida') & (sf.col('dpd_bucket_sug') == 3), 1.5)
            .when((sf.col('dpd_bucket') == 5) & (sf.col('promesa_cumplida') == 'cumplida') & (sf.col('dpd_bucket_sug') == 4), 1.5)
            .when((sf.col('dpd_bucket') == 5) & (sf.col('promesa_cumplida') == 'cumplida') & (sf.col('dpd_bucket_sug') == 5), 1.0)
            .when((sf.col('dpd_bucket') == 6) & (sf.col('promesa_cumplida') == 'cumplida') & (sf.col('dpd_bucket_sug') == 0), 2.0)
            .when((sf.col('dpd_bucket') == 6) & (sf.col('promesa_cumplida') == 'cumplida') & (sf.col('dpd_bucket_sug') == 1), 1.5)
            .when((sf.col('dpd_bucket') == 6) & (sf.col('promesa_cumplida') == 'cumplida') & (sf.col('dpd_bucket_sug') == 2), 1.5)
            .when((sf.col('dpd_bucket') == 6) & (sf.col('promesa_cumplida') == 'cumplida') & (sf.col('dpd_bucket_sug') == 3), 1.5)
            .when((sf.col('dpd_bucket') == 6) & (sf.col('promesa_cumplida') == 'cumplida') & (sf.col('dpd_bucket_sug') == 4), 1.5)
            .when((sf.col('dpd_bucket') == 6) & (sf.col('promesa_cumplida') == 'cumplida') & (sf.col('dpd_bucket_sug') == 5), 1.5)
            .when((sf.col('dpd_bucket') == 6) & (sf.col('promesa_cumplida') == 'cumplida') & (sf.col('dpd_bucket_sug') == 6), 1.0)
            .otherwise(0.0)
        )
        
        # 5. Guardar el DataFrame final como un atributo de la clase para ser usado después
        self.df_final_con_factor = df_final.cache()
        print("Módulo Factor: Cálculo del Factor Multiplicativo completado.")


"""
        Constructor de la clase.
        :param fecha_inicio: La fecha de inicio para las consultas.
        :param fecha_fin: La fecha de fin para las consultas.
        :param df_input: El DataFrame que resulta del módulo de Reglas (nueva_union).
        :param data_ops_bpa: El objeto de conexión a Spark (tu clase bnmxspark).
        """


  df_final = df_final.withColumn('Clasificacion_Bucket',
        sf.when(sf.col('dpd_bucket') > sf.col('dpd_bucket_sug'), 'RB')
          .when(sf.col('dpd_bucket') < sf.col('dpd_bucket_sug'), 'LE')
          .otherwise('ST') # Si son iguales


    DataOpsBPA.write_log("Start calculating Multiplicative Factor")
    nombre_factor = "factor_" + str(z)
    
    # Creamos la instancia con el orden de parámetros correcto
    valor_factor = CalculoFactor.Factor(fechas_iniciales[i], fechas_finales[j], valor_regla.nueva_union, DataOpsBPA)
    
    # Ejecutamos el método que hace todos los cálculos (incluida la nueva regla RB/ST/LE)
    valor_factor.calcular_factor()
    
    # Guardamos el objeto con los resultados en su diccionario
    dicc_factores[nombre_factor] = valor_factor
    
    DataOpsBPA.write_log("Finished calculating Multiplicative Factor")
    step_number = step_number + 1