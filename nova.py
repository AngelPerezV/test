import pandas as pd
import numpy as np
import statsmodels.api as sm
from statsmodels.formula.api import ols
from scipy import stats
import seaborn as sns
import matplotlib.pyplot as plt

class AnalizadorEstadistico:
    """
    Clase definitiva para análisis estadísticos con limpieza de datos robusta.
    """
    def __init__(self, df):
        if not isinstance(df, pd.DataFrame):
            raise TypeError("La entrada debe ser un DataFrame de Pandas.")
        self.df = df
        print("Analizador Estadístico listo.")

    def realizar_prueba_t(self, var_dependiente, var_grupo, alpha=0.05):
        """
        Realiza una Prueba t de Student y genera un boxplot para comparar
        las medias de DOS grupos.
        """
        print(f"\n--- Ejecutando Prueba t de Student para '{var_dependiente}' por '{var_grupo}' ---")
        
        # Validación y limpieza de datos
        df_clean = self.df[[var_dependiente, var_grupo]].dropna()
        grupos = df_clean[var_grupo].unique()
        if len(grupos) != 2:
            print(f"❌ Error: La columna '{var_grupo}' debe tener exactamente 2 grupos. Encontrados: {len(grupos)}.")
            return

        grupo1 = df_clean[df_clean[var_grupo] == grupos[0]][var_dependiente]
        grupo2 = df_clean[df_clean[var_grupo] == grupos[1]][var_dependiente]

        # Prueba estadística
        t_stat, p_value = stats.ttest_ind(grupo1, grupo2)
        
        print(f"Estadístico t: {t_stat:.4f}, Valor p: {p_value:.4f}")
        if p_value < alpha:
            print("✅ Conclusión: La diferencia entre los promedios de los dos grupos es estadísticamente significativa.")
        else:
            print("❌ Conclusión: No hay evidencia de una diferencia significativa entre los dos grupos.")

        # Boxplot
        print("\n--- Generando Visualización (Boxplot) ---")
        plt.figure(figsize=(8, 6))
        sns.boxplot(data=df_clean, x=var_grupo, y=var_dependiente)
        plt.title(f'Distribución de {var_dependiente} por {var_grupo}')
        plt.xlabel(var_grupo)
        plt.ylabel(var_dependiente)
        plt.grid(True, linestyle='--', alpha=0.6)
        plt.show()

    def realizar_anova_un_factor(self, var_dependiente, var_grupo, alpha=0.05):
        """
        Realiza un ANOVA de un factor para comparar las medias de TRES O MÁS grupos.
        """
        print(f"\n--- Ejecutando ANOVA de Un Factor para '{var_dependiente}' por '{var_grupo}' ---")
        df_clean = self.df[[var_dependiente, var_grupo]].dropna()
        grupos_unicos = df_clean[var_grupo].unique()
        if len(grupos_unicos) < 3:
            print(f"❌ Error: Se recomienda usar ANOVA con 3 o más grupos. Encontrados: {len(grupos_unicos)}. Considera usar una Prueba t.")
            return

        samples = [df_clean[df_clean[var_grupo] == grupo][var_dependiente] for grupo in grupos_unicos]
        
        f_stat, p_value = stats.f_oneway(*samples)
        
        print(f"Estadístico F: {f_stat:.4f}, Valor p: {p_value:.4f}")
        if p_value < alpha:
            print("✅ Conclusión: Existe una diferencia significativa entre los promedios de al menos dos de los grupos.")
            print("   >> Se recomienda realizar una prueba post-hoc (ej. Tukey HSD) para ver qué pares son diferentes.")
        else:
            print("❌ Conclusión: No hay evidencia de una diferencia significativa entre los promedios de los grupos.")

    def realizar_anova_dos_factores(self, var_dependiente, factor1, factor2, alpha=0.05):
        """
        Realiza un ANOVA de dos factores completo, incluyendo limpieza robusta,
        tabla de resultados, interpretación y visualizaciones.
        """
        print(f"\n--- Ejecutando ANOVA de Dos Factores para '{var_dependiente}' por '{factor1}' y '{factor2}' ---")
        
        # 1. Validación de existencia de columnas
        columnas_necesarias = [var_dependiente, factor1, factor2]
        if not all(col in self.df.columns for col in columnas_necesarias):
            raise ValueError(f"Una o más columnas no se encuentran en el DataFrame. Se necesitan: {columnas_necesarias}")
        
        # 2. LIMPIEZA ROBUSTA Y DIAGNÓSTICO
        print("\nPaso de limpieza y validación de datos:")
        df_clean = self.df[columnas_necesarias].copy()
        
        # Forzar la conversión de la variable dependiente a numérica.
        df_clean[var_dependiente] = pd.to_numeric(df_clean[var_dependiente], errors='coerce')

        # Reemplazar infinitos (positivos y negativos) con NaN
        df_clean.replace([np.inf, -np.inf], np.nan, inplace=True)
        
        nans_antes = df_clean.isnull().sum().sum()
        if nans_antes > 0:
            print(f"  - Se encontraron y eliminarán {nans_antes} filas con valores NaN o infinitos.")
        
        df_clean.dropna(inplace=True)
        
        if df_clean.empty:
            print("❌ Error Crítico: No quedaron datos válidos después de la limpieza. Revisa tu DataFrame original.")
            return
        
        print(f"  - El análisis se realizará con {len(df_clean)} filas limpias.")
        
        # 3. Ajustar el modelo y continuar
        try:
            formula = f"{var_dependiente} ~ C({factor1}) + C({factor2}) + C({factor1}):C({factor2})"
            model = ols(formula, data=df_clean).fit()
            anova_table = sm.stats.anova_lm(model, typ=2)
            
            # Interpretación y gráficos
            print("\n--- Tabla de Resultados del ANOVA ---")
            display(anova_table)
            
            print("\n--- Interpretación de los Resultados (p-valores) ---")
            p_value_factor1 = anova_table.loc[f"C({factor1})", "PR(>F)"]
            if p_value_factor1 < alpha: print(f"✅ El factor '{factor1}' tiene un efecto significativo (p={p_value_factor1:.4f}).")
            else: print(f"❌ No hay evidencia de un efecto significativo del factor '{factor1}' (p={p_value_factor1:.4f}).")

            p_value_factor2 = anova_table.loc[f"C({factor2})", "PR(>F)"]
            if p_value_factor2 < alpha: print(f"✅ El factor '{factor2}' tiene un efecto significativo (p={p_value_factor2:.4f}).")
            else: print(f"❌ No hay evidencia de un efecto significativo del factor '{factor2}' (p={p_value_factor2:.4f}).")

            interaction_term = f"C({factor1}):C({factor2})"
            p_value_interaction = anova_table.loc[interaction_term, "PR(>F)"]
            if p_value_interaction < alpha:
                print(f"✅ Hay un efecto de interacción significativo (p={p_value_interaction:.4f}).")
                print("   >> El efecto de un factor depende del nivel del otro.")
            else: print(f"❌ No hay evidencia de un efecto de interacción significativo (p={p_value_interaction:.4f}).")

            print("\n--- Generando Visualizaciones ---")
            plt.figure(figsize=(10, 7)); sns.boxplot(data=df_clean, x=factor1, y=var_dependiente, hue=factor2); plt.title(f'Boxplot de {var_dependiente} por {factor1} y {factor2}'); plt.xlabel(factor1); plt.ylabel(var_dependiente); plt.grid(True, linestyle='--', alpha=0.6); plt.show()
            plt.figure(figsize=(8, 6)); sns.pointplot(data=df_clean, x=factor1, y=var_dependiente, hue=factor2, dodge=True, errorbar=None); plt.title(f'Gráfico de Interacción entre {factor1} y {factor2}'); plt.xlabel(factor1); plt.ylabel(f'Media de {var_dependiente}'); plt.grid(True, linestyle='--', alpha=0.6); plt.show()

        except Exception as e:
            print(f"\n❌ Ocurrió un error inesperado durante el análisis ANOVA: {e}")
            print("   >> Esto puede pasar si las columnas de factores tienen un solo valor después de limpiar los datos.")
            print("   >> Revisa las características de tus datos limpios a continuación:")
            display(df_clean.describe(include='all'))


            import pandas as pd
import numpy as np

# --- 1. Crear un DataFrame de ejemplo con el problema ---
#    - 'ventas_ok' es numérica y limpia.
#    - 'unidades_mal' debería ser numérica, pero tiene texto.
#    - 'region' es de texto y está bien que lo sea.
fechas = pd.to_datetime(pd.date_range(start='2024-01-15', periods=4, freq='D'))
df = pd.DataFrame({
    'ventas_ok': [100.5, 150.2, 120.0, 200.8],
    'unidades_mal': ['50', '45', 'No Registrado', '30,0'],
    'region': ['Norte', 'Sur', 'Norte', 'Este']
}, index=fechas)

print("--- DataFrame Original ---")
display(df)
print("\n--- Tipos de Datos Originales ---")
df.info()
# Nota como 'unidades_mal' es de tipo 'object'


# --- 2. Diagnóstico: Encontrar la columna que falla ---
print("\n--- Buscando columnas problemáticas ---")
for columna in df.columns:
    # Solo intentamos convertir columnas que no sean ya numéricas
    if df[columna].dtype == 'object':
        # Intentamos la conversión forzando errores a NaN
        conversion_intento = pd.to_numeric(df[columna], errors='coerce')
        
        # Si la conversión generó algún NaN, encontramos una columna problemática
        if conversion_intento.isnull().any():
            print(f"🚨 ¡Problema encontrado en la columna '{columna}'!")


# --- 3. Limpiar la columna identificada ---
# Basado en el diagnóstico, sabemos que 'unidades_mal' es el problema.
# La limpiamos y convertimos.
print("\n--- Limpiando la columna 'unidades_mal' ---")
df['unidades_mal'] = df['unidades_mal'].str.replace(',', '.', regex=False) # Reemplazar coma por punto
df['unidades_mal'] = pd.to_numeric(df['unidades_mal'], errors='coerce')
df.fillna(0, inplace=True) # Rellenamos el 'No Registrado' que se convirtió en NaN


# --- 4. Ejecutar resample() de nuevo ---
# Ahora que TODAS las columnas numéricas están limpias, el comando funcionará
print("\n--- Resample Exitoso ---")
resumen_semanal = df.resample('W').mean()
display(resumen_semanal)


import pandas as pd
import numpy as np

def limpiar_y_convertir_dataframe(df):
    """
    Itera sobre un DataFrame y convierte automáticamente las columnas
    de tipo 'object' que parezcan numéricas a un formato numérico.
    
    :param df: El DataFrame de Pandas a limpiar.
    :return: Un nuevo DataFrame con las columnas limpias y convertidas.
    """
    print("--- Iniciando limpieza automática del DataFrame ---")
    df_limpio = df.copy() # Trabajar sobre una copia para no modificar el original
    
    for columna in df_limpio.columns:
        # Solo nos interesan las columnas de tipo 'object'
        if df_limpio[columna].dtype == 'object':
            print(f"Analizando columna '{columna}' (tipo object)...")
            
            # Intentar la conversión, forzando errores a NaN
            conversion_intento = pd.to_numeric(df_limpio[columna], errors='coerce')
            
            # Calcular cuántos valores no eran numéricos
            num_nulos = conversion_intento.isnull().sum()
            total_valores = len(df_limpio[columna])
            
            # Si casi todos los valores se pudieron convertir, asumimos que es una columna numérica "sucia"
            # (El umbral del 90% es ajustable)
            if num_nulos < total_valores * 0.1: # Si menos del 10% de los valores fallaron
                print(f"  >> ¡Conversión exitosa! La columna '{columna}' ahora es numérica.")
                df_limpio[columna] = conversion_intento
            else:
                print(f"  >> Se mantuvo como texto. No parece ser una columna numérica.")
                
    print("\n--- Limpieza automática finalizada ---")
    return df_limpio

# --- Ejemplo de Uso ---
# 1. DataFrame sucio con varios tipos de problemas
df_sucio = pd.DataFrame({
    'id_producto': ['SKU-001', 'SKU-002', 'SKU-003'],
    'precio': ['$1,500.50', '€2,100.20', '850'],
    'stock': ['50', '45', 'No Registrado'],
    'valoracion': ['4.5', '3.8', '5.0']
})
print("--- DataFrame Original ---")
df_sucio.info()

# 2. Aplicar la función de limpieza automática
df_limpio = limpiar_y_convertir_dataframe(df_sucio)

# 3. Revisar los resultados
print("\n--- DataFrame Limpio ---")
df_limpio.info()


def limpiar_y_convertir_dataframe_robusto(df):
    """
    Versión mejorada que primero limpia las columnas de texto y luego
    intenta convertirlas a un formato numérico.

    :param df: El DataFrame de Pandas a limpiar.
    :return: Un nuevo DataFrame con las columnas limpias y convertidas.
    """
    print("--- Iniciando limpieza automática robusta ---")
    df_limpio = df.copy()
    
    for columna in df_limpio.columns:
        # Solo nos interesan las columnas de tipo 'object' (texto)
        if pd.api.types.is_object_dtype(df_limpio[columna]):
            print(f"Analizando la columna de texto '{columna}'...")
            
            # 1. Limpieza primero: creamos una versión temporal limpia de la columna
            #    Quitamos espacios, comas, y símbolos de moneda comunes
            try:
                columna_temporal_limpia = df_limpio[columna].str.strip().str.replace('[$,€]', '', regex=True)
            except AttributeError:
                # Esto puede pasar si la columna 'object' contiene datos no-string (ej. números mezclados)
                print(f"  >> No se pudo aplicar limpieza de texto a '{columna}'. Se omite.")
                continue

            # 2. Intentamos la conversión en la versión limpia
            conversion_intento = pd.to_numeric(columna_temporal_limpia, errors='coerce')
            
            # 3. Verificamos si la conversión fue exitosa en la mayoría de los casos
            #    (Si al menos un valor se pudo convertir, procedemos)
            if conversion_intento.notna().any():
                print(f"  >> ¡Conversión exitosa! La columna '{columna}' ahora es numérica.")
                # Reemplazamos la columna original con la versión numérica
                df_limpio[columna] = conversion_intento
            else:
                print(f"  >> Se mantuvo como texto. No parece ser una columna numérica.")
                
    print("\n--- Limpieza automática finalizada ---")
    return df_limpio



import pandas as pd
import numpy as np
import statsmodels.api as sm
from statsmodels.formula.api import ols
from scipy import stats
import seaborn as sns
import matplotlib.pyplot as plt

class AnalizadorEstadistico:
    """
    Kit de herramientas definitivo para análisis estadísticos, incluyendo
    limpieza automática y pruebas de supuestos.
    """
    def __init__(self, df):
        if not isinstance(df, pd.DataFrame):
            raise TypeError("La entrada debe ser un DataFrame de Pandas.")
        self.df = self._limpiar_automaticamente(df)
        print("\nAnalizador Estadístico listo con datos limpios.")

    def _limpiar_automaticamente(self, df):
        print("--- Iniciando limpieza automática del DataFrame ---")
        df_limpio = df.copy()
        for col in df_limpio.columns:
            if pd.api.types.is_object_dtype(df_limpio[col]):
                try:
                    # Limpia caracteres comunes y espacios
                    temp_col = df_limpio[col].str.replace('[$,€,]', '', regex=True).str.strip()
                    conversion = pd.to_numeric(temp_col, errors='coerce')
                    # Si la mayoría (>80%) se convierte, reemplaza la columna
                    if conversion.notna().sum() / len(df_limpio) > 0.8:
                        print(f"  >> Columna '{col}' convertida a tipo numérico.")
                        df_limpio[col] = conversion
                except Exception:
                    # Si falla (ej. la columna object no contiene solo strings), se omite
                    pass
        print("--- Limpieza finalizada ---")
        return df_limpio

    # --- MÉTODOS DE DIAGNÓSTICO (SUPUESTOS) ---
    def probar_normalidad_shapiro(self, columna_valor, alpha=0.05):
        """
        Realiza la prueba de Shapiro-Wilk sobre una columna para verificar la normalidad.
        """
        print(f"\n--- Prueba de Normalidad (Shapiro-Wilk) para '{columna_valor}' ---")
        print("H0: La muestra proviene de una distribución normal.")
        
        datos = self.df[columna_valor].dropna()
        if len(datos) < 3:
            print("❌ Error: Se necesitan al menos 3 puntos de datos para la prueba de Shapiro-Wilk.")
            return

        stat, p_value = stats.shapiro(datos)
        print(f"Estadístico W: {stat:.4f}, Valor p: {p_value:.4f}")
        if p_value < alpha:
            print("Conclusión: -> ❌ (No normal). Se rechaza la hipótesis nula.")
        else:
            print("Conclusión: -> ✅ (Normal). No se puede rechazar la hipótesis nula.")

    def probar_homogeneidad_levene(self, var_dependiente, var_grupo, alpha=0.05):
        """
        Realiza la prueba de Levene para la homogeneidad de varianzas entre grupos.
        """
        print(f"\n--- Prueba de Homogeneidad de Varianzas (Levene) para '{var_dependiente}' por '{var_grupo}' ---")
        print("H0: Las varianzas de todos los grupos son iguales.")
        
        df_clean = self.df[[var_dependiente, var_grupo]].dropna()
        samples = [group[var_dependiente].values for name, group in df_clean.groupby(var_grupo)]
        
        stat, p_value = stats.levene(*samples)
        print(f"Estadístico de Levene: {stat:.4f}, Valor p: {p_value:.4f}")
        if p_value < alpha:
            print("Conclusión: -> ❌ (Las varianzas NO son homogéneas). La suposición no se cumple.")
        else:
            print("Conclusión: -> ✅ (Las varianzas son homogéneas). La suposición se cumple.")

    # --- MÉTODOS DE ANÁLISIS ---
    def realizar_prueba_t(self, var_dependiente, var_grupo, alpha=0.05):
        """
        Realiza una Prueba t de Student y genera un boxplot para comparar las medias de DOS grupos.
        """
        print(f"\n--- Ejecutando Prueba t de Student para '{var_dependiente}' por '{var_grupo}' ---")
        
        df_clean = self.df[[var_dependiente, var_grupo]].dropna()
        grupos = df_clean[var_grupo].unique()
        if len(grupos) != 2:
            print(f"❌ Error: La columna '{var_grupo}' debe tener exactamente 2 grupos. Encontrados: {len(grupos)}.")
            return

        grupo1 = df_clean[df_clean[var_grupo] == grupos[0]][var_dependiente]
        grupo2 = df_clean[df_clean[var_grupo] == grupos[1]][var_dependiente]

        t_stat, p_value = stats.ttest_ind(grupo1, grupo2)
        
        print(f"Estadístico t: {t_stat:.4f}, Valor p: {p_value:.4f}")
        if p_value < alpha:
            print("✅ Conclusión: La diferencia entre los promedios es estadísticamente significativa.")
        else:
            print("❌ Conclusión: No hay evidencia de una diferencia significativa.")

        print("\n--- Generando Visualización (Boxplot) ---")
        plt.figure(figsize=(8, 6)); sns.boxplot(data=df_clean, x=var_grupo, y=var_dependiente); plt.title(f'Distribución de {var_dependiente} por {var_grupo}'); plt.xlabel(var_grupo); plt.ylabel(var_dependiente); plt.grid(True, linestyle='--', alpha=0.6); plt.show()

    def realizar_anova_un_factor(self, var_dependiente, var_grupo, alpha=0.05):
        """
        Realiza un ANOVA de un factor para comparar las medias de TRES O MÁS grupos.
        """
        print(f"\n--- Ejecutando ANOVA de Un Factor para '{var_dependiente}' por '{var_grupo}' ---")
        
        df_clean = self.df[[var_dependiente, var_grupo]].dropna()
        grupos_unicos = df_clean[var_grupo].unique()
        if len(grupos_unicos) < 3:
            print(f"❌ Error: Se recomienda usar ANOVA con 3 o más grupos. Encontrados: {len(grupos_unicos)}. Considera usar una Prueba t.")
            return

        samples = [df_clean[df_clean[var_grupo] == grupo][var_dependiente] for grupo in grupos_unicos]
        f_stat, p_value = stats.f_oneway(*samples)
        
        print(f"Estadístico F: {f_stat:.4f}, Valor p: {p_value:.4f}")
        if p_value < alpha:
            print("✅ Conclusión: Existe una diferencia significativa entre los promedios de al menos dos grupos.")
            print("   >> Se recomienda realizar una prueba post-hoc (ej. Tukey HSD) para ver qué pares son diferentes.")
        else:
            print("❌ Conclusión: No hay evidencia de una diferencia significativa entre los promedios.")
            
    def realizar_anova_dos_factores(self, var_dependiente, factor1, factor2, alpha=0.05):
        """
        Realiza un ANOVA de dos factores completo con interpretación y visualizaciones.
        """
        print(f"\n--- Ejecutando ANOVA de Dos Factores para '{var_dependiente}' por '{factor1}' y '{factor2}' ---")
        
        df_clean = self.df[[var_dependiente, factor1, factor2]].dropna()
        if df_clean.empty:
            print("❌ Error: No hay datos válidos para el análisis tras eliminar NaNs.")
            return
        
        formula = f"{var_dependiente} ~ C({factor1}) + C({factor2}) + C({factor1}):C({factor2})"
        model = ols(formula, data=df_clean).fit()
        anova_table = sm.stats.anova_lm(model, typ=2)
        
        print("\n--- Tabla de Resultados del ANOVA ---"); display(anova_table)
        
        print("\n--- Interpretación de los Resultados (p-valores) ---")
        p_value_factor1 = anova_table.loc[f"C({factor1})", "PR(>F)"]
        if p_value_factor1 < alpha: print(f"✅ El factor '{factor1}' tiene un efecto significativo (p={p_value_factor1:.4f}).")
        else: print(f"❌ No hay evidencia de un efecto significativo del factor '{factor1}' (p={p_value_factor1:.4f}).")
        
        p_value_factor2 = anova_table.loc[f"C({factor2})", "PR(>F)"]
        if p_value_factor2 < alpha: print(f"✅ El factor '{factor2}' tiene un efecto significativo (p={p_value_factor2:.4f}).")
        else: print(f"❌ No hay evidencia de un efecto significativo del factor '{factor2}' (p={p_value_factor2:.4f}).")
            
        interaction_term = f"C({factor1}):C({factor2})"
        p_value_interaction = anova_table.loc[interaction_term, "PR(>F)"]
        if p_value_interaction < alpha:
            print(f"✅ Hay un efecto de interacción significativo (p={p_value_interaction:.4f})."); print("   >> El efecto de un factor depende del nivel del otro.")
        else: print(f"❌ No hay evidencia de un efecto de interacción significativo (p={p_value_interaction:.4f}).")

        print("\n--- Generando Visualizaciones ---")
        plt.figure(figsize=(10, 7)); sns.boxplot(data=df_clean, x=factor1, y=var_dependiente, hue=factor2); plt.title(f'Boxplot de {var_dependiente} por {factor1} y {factor2}'); plt.xlabel(factor1); plt.ylabel(var_dependiente); plt.grid(True, linestyle='--', alpha=0.6); plt.show()
        plt.figure(figsize=(8, 6)); sns.pointplot(data=df_clean, x=factor1, y=var_dependiente, hue=factor2, dodge=True, errorbar=None); plt.title(f'Gráfico de Interacción entre {factor1} y {factor2}'); plt.xlabel(factor1); plt.ylabel(f'Media de {var_dependiente}'); plt.grid(True, linestyle='--', alpha=0.6); plt.show()