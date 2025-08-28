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