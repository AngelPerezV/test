import pandas as pd
import numpy as np
import statsmodels.api as sm
from statsmodels.formula.api import ols
import seaborn as sns
import matplotlib.pyplot as plt

class AnovaDosFactores:
    """
    Una clase para realizar y visualizar un ANOVA de dos factores.
    """
    def __init__(self, df, var_dependiente, factor1, factor2):
        """
        Inicializa la clase con los datos y los nombres de las columnas.
        
        :param df: DataFrame de Pandas con los datos.
        :param var_dependiente: Nombre de la columna de la variable dependiente (numérica).
        :param factor1: Nombre de la columna del primer factor (categórica).
        :param factor2: Nombre de la columna del segundo factor (categórica).
        """
        if not all(col in df.columns for col in [var_dependiente, factor1, factor2]):
            raise ValueError("Uno o más nombres de columna no se encuentran en el DataFrame.")
            
        self.df = df
        self.var_dependiente = var_dependiente
        self.factor1 = factor1
        self.factor2 = factor2
        self.formula = f"{self.var_dependiente} ~ C({self.factor1}) + C({self.factor2}) + C({self.factor1}):C({self.factor2})"
        self.model = None
        self.anova_table = None

    def ajustar_modelo(self):
        """
        Ajusta el modelo ANOVA usando la fórmula definida.
        """
        print("Ajustando el modelo ANOVA...")
        self.model = ols(self.formula, data=self.df).fit()
        self.anova_table = sm.stats.anova_lm(self.model, typ=2)
        print("Modelo ajustado exitosamente.")
        return self.anova_table

    def obtener_tabla_anova(self):
        """
        Devuelve la tabla de resultados del ANOVA.
        """
        if self.anova_table is None:
            print("El modelo aún no ha sido ajustado. Llama a 'ajustar_modelo()' primero.")
            return None
        return self.anova_table

    def interpretar_resultados(self, alpha=0.05):
        """
        Imprime una interpretación de los resultados del ANOVA.
        """
        if self.anova_table is None:
            print("El modelo aún no ha sido ajustado.")
            return

        print("\n--- Interpretación de los Resultados (p-valores) ---")
        
        # Efecto del Factor 1
        p_value_factor1 = self.anova_table.loc[f"C({self.factor1})", "PR(>F)"]
        if p_value_factor1 < alpha:
            print(f"✅ El factor '{self.factor1}' tiene un efecto estadísticamente significativo sobre '{self.var_dependiente}' (p={p_value_factor1:.4f}).")
        else:
            print(f"❌ No hay evidencia de un efecto significativo del factor '{self.factor1}' (p={p_value_factor1:.4f}).")

        # Efecto del Factor 2
        p_value_factor2 = self.anova_table.loc[f"C({self.factor2})", "PR(>F)"]
        if p_value_factor2 < alpha:
            print(f"✅ El factor '{self.factor2}' tiene un efecto estadísticamente significativo sobre '{self.var_dependiente}' (p={p_value_factor2:.4f}).")
        else:
            print(f"❌ No hay evidencia de un efecto significativo del factor '{self.factor2}' (p={p_value_factor2:.4f}).")
            
        # Efecto de la Interacción
        interaction_term = f"C({self.factor1}):C({self.factor2})"
        p_value_interaction = self.anova_table.loc[interaction_term, "PR(>F)"]
        if p_value_interaction < alpha:
            print(f"✅ Hay un efecto de interacción estadísticamente significativo entre '{self.factor1}' y '{self.factor2}' (p={p_value_interaction:.4f}).")
            print("   >> Esto significa que el efecto de un factor depende del nivel del otro factor.")
        else:
            print(f"❌ No hay evidencia de un efecto de interacción significativo (p={p_value_interaction:.4f}).")

    def graficar_interaccion(self):
        """
        Crea un gráfico de interacción para visualizar los efectos.
        """
        print("\nGenerando gráfico de interacción...")
        plt.figure(figsize=(8, 6))
        sns.pointplot(data=self.df, x=self.factor1, y=self.var_dependiente, hue=self.factor2, dodge=True, errorbar=None)
        plt.title(f'Gráfico de Interacción entre {self.factor1} y {self.factor2}')
        plt.grid(True, linestyle='--', alpha=0.6)
        plt.show()

# --- Cómo Usar la Clase ---

# 1. Crear un conjunto de datos de ejemplo
np.random.seed(42)
data = {
    'fertilizante': ['A']*20 + ['B']*20,
    'sol': ['Bajo', 'Alto']*20,
    'altura': list(np.random.normal(20, 5, 10)) +  # A, Bajo
              list(np.random.normal(35, 5, 10)) +  # A, Alto
              list(np.random.normal(22, 5, 10)) +  # B, Bajo
              list(np.random.normal(25, 5, 10))   # B, Alto
}
df_ejemplo = pd.DataFrame(data)

# 2. Crear una instancia de la clase
analisis = AnovaDosFactores(df=df_ejemplo, 
                            var_dependiente='altura', 
                            factor1='fertilizante', 
                            factor2='sol')

# 3. Ajustar el modelo y obtener la tabla
tabla_resultados = analisis.ajustar_modelo()
print("\n--- Tabla ANOVA ---")
print(tabla_resultados)

# 4. Interpretar los resultados automáticamente
analisis.interpretar_resultados()

# 5. Visualizar la interacción
analisis.graficar_interaccion()



# (Esto iría dentro de la definición de la clase AnovaDosFactores)

    def graficar_boxplot(self):
        """
        Crea un boxplot para visualizar las distribuciones de los grupos.
        """
        print("\nGenerando boxplot...")
        plt.figure(figsize=(10, 7))
        sns.boxplot(data=self.df, x=self.factor1, y=self.var_dependiente, hue=self.factor2)
        plt.title(f'Boxplot de {self.var_dependiente} por {self.factor1} y {self.factor2}')
        plt.grid(True, linestyle='--', alpha=0.6)
        plt.show()

# Y lo llamarías al final
# analisis_real.graficar_boxplot()




import pandas as pd
import numpy as np
import statsmodels.api as sm
from statsmodels.formula.api import ols
import seaborn as sns
import matplotlib.pyplot as plt
from scipy import stats

class AnovaDosFactores:
    """
    Una clase completa para realizar y visualizar un ANOVA de dos factores.
    """
    def __init__(self, df, var_dependiente, factor1, factor2):
        """
        Inicializa la clase con los datos y los nombres de las columnas.
        """
        if not all(col in df.columns for col in [var_dependiente, factor1, factor2]):
            raise ValueError("Uno o más nombres de columna no se encuentran en el DataFrame.")
            
        self.df = df
        self.var_dependiente = var_dependiente
        self.factor1 = factor1
        self.factor2 = factor2
        self.formula = f"{self.var_dependiente} ~ C({self.factor1}) + C({self.factor2}) + C({self.factor1}):C({self.factor2})"
        self.summary_table = None
        self.model = None
        self.anova_table = None

    def preparar_datos_con_ic(self, alpha=0.05):
        """
        Calcula estadísticas descriptivas y el intervalo de confianza para la media de cada grupo.
        """
        print("--- Preparando Resumen de Datos con Intervalos de Confianza ---")
        grouped = self.df.groupby([self.factor1, self.factor2])[self.var_dependiente]
        self.summary_table = grouped.agg(['mean', 'std', 'count']).reset_index()
        self.summary_table['sem'] = self.summary_table['std'] / np.sqrt(self.summary_table['count'])
        self.summary_table['ic_lower'], self.summary_table['ic_upper'] = stats.t.interval(
            1 - alpha,
            df=self.summary_table['count'] - 1,
            loc=self.summary_table['mean'],
            scale=self.summary_table['sem']
        )
        self.summary_table = self.summary_table.round(2)
        return self.summary_table

    def ajustar_modelo(self):
        """
        Ajusta el modelo ANOVA usando la fórmula definida.
        """
        print("\nAjustando el modelo ANOVA...")
        self.model = ols(self.formula, data=self.df).fit()
        self.anova_table = sm.stats.anova_lm(self.model, typ=2)
        print("Modelo ajustado exitosamente.")
        return self.anova_table

    def interpretar_resultados(self, alpha=0.05):
        """
        Imprime una interpretación de los resultados del ANOVA.
        """
        if self.anova_table is None:
            print("El modelo aún no ha sido ajustado.")
            return

        print("\n--- Interpretación de los Resultados (p-valores) ---")
        p_value_factor1 = self.anova_table.loc[f"C({self.factor1})", "PR(>F)"]
        if p_value_factor1 < alpha:
            print(f"✅ El factor '{self.factor1}' tiene un efecto estadísticamente significativo sobre '{self.var_dependiente}' (p={p_value_factor1:.4f}).")
        else:
            print(f"❌ No hay evidencia de un efecto significativo del factor '{self.factor1}' (p={p_value_factor1:.4f}).")

        p_value_factor2 = self.anova_table.loc[f"C({self.factor2})", "PR(>F)"]
        if p_value_factor2 < alpha:
            print(f"✅ El factor '{self.factor2}' tiene un efecto estadísticamente significativo sobre '{self.var_dependiente}' (p={p_value_factor2:.4f}).")
        else:
            print(f"❌ No hay evidencia de un efecto significativo del factor '{self.factor2}' (p={p_value_factor2:.4f}).")
            
        interaction_term = f"C({self.factor1}):C({self.factor2})"
        p_value_interaction = self.anova_table.loc[interaction_term, "PR(>F)"]
        if p_value_interaction < alpha:
            print(f"✅ Hay un efecto de interacción estadísticamente significativo entre '{self.factor1}' y '{self.factor2}' (p={p_value_interaction:.4f}).")
            print("   >> Esto significa que el efecto de un factor depende del nivel del otro factor.")
        else:
            print(f"❌ No hay evidencia de un efecto de interacción significativo (p={p_value_interaction:.4f}).")

    def graficar_interaccion(self):
        """
        Crea un gráfico de interacción para visualizar los efectos.
        """
        print("\nGenerando gráfico de interacción...")
        plt.figure(figsize=(8, 6))
        sns.pointplot(data=self.df, x=self.factor1, y=self.var_dependiente, hue=self.factor2, dodge=True, errorbar=None)
        plt.title(f'Gráfico de Interacción entre {self.factor1} y {self.factor2}')
        plt.grid(True, linestyle='--', alpha=0.6)
        plt.show()

    def graficar_boxplot(self):
        """
        Crea un boxplot para visualizar las distribuciones de los grupos.
        """
        print("\nGenerando boxplot...")
        plt.figure(figsize=(10, 7))
        sns.boxplot(data=self.df, x=self.factor1, y=self.var_dependiente, hue=self.factor2)
        plt.title(f'Boxplot de {self.var_dependiente} por {self.factor1} y {self.factor2}')
        plt.grid(True, linestyle='--', alpha=0.6)
        plt.show()


import pandas as pd
import numpy as np
import statsmodels.api as sm
from statsmodels.formula.api import ols
import seaborn as sns
import matplotlib.pyplot as plt
from scipy import stats

class AnovaDosFactores:
    """
    Clase completa para ANOVA de dos factores, incluyendo pruebas de normalidad.
    """
    def __init__(self, df, var_dependiente, factor1, factor2):
        # ... (código del __init__ sin cambios) ...
        if not all(col in df.columns for col in [var_dependiente, factor1, factor2]):
            raise ValueError("Uno o más nombres de columna no se encuentran en el DataFrame.")
        self.df = df
        self.var_dependiente = var_dependiente
        self.factor1 = factor1
        self.factor2 = factor2
        self.formula = f"{self.var_dependiente} ~ C({self.factor1}) + C({self.factor2}) + C({self.factor1}):C({self.factor2})"
        self.summary_table = None
        self.model = None
        self.anova_table = None

    def preparar_datos_con_ic(self, alpha=0.05):
        # ... (código de preparar_datos_con_ic sin cambios) ...
        print("--- Preparando Resumen de Datos con Intervalos de Confianza ---")
        grouped = self.df.groupby([self.factor1, self.factor2])[self.var_dependiente]
        self.summary_table = grouped.agg(['mean', 'std', 'count']).reset_index()
        self.summary_table['sem'] = self.summary_table['std'] / np.sqrt(self.summary_table['count'])
        self.summary_table['ic_lower'], self.summary_table['ic_upper'] = stats.t.interval(
            1 - alpha, df=self.summary_table['count'] - 1, loc=self.summary_table['mean'], scale=self.summary_table['sem']
        )
        self.summary_table = self.summary_table.round(2)
        return self.summary_table

    # --- NUEVO MÉTODO 1: NORMALIDAD POR GRUPO ---
    def probar_normalidad_por_grupo(self, alpha=0.05):
        """
        Realiza la prueba de Shapiro-Wilk para cada grupo individualmente.
        """
        print("\n--- Prueba de Normalidad de Shapiro-Wilk por Grupo ---")
        print("H0: La muestra proviene de una distribución normal.")
        
        grupos = self.df.groupby([self.factor1, self.factor2])
        for (nombre_grupo, data_grupo) in grupos:
            stat, p_value = stats.shapiro(data_grupo[self.var_dependiente])
            print(f"Grupo {nombre_grupo}: p-valor={p_value:.4f}", end=' ')
            if p_value < alpha:
                print("-> ❌ (No normal)")
            else:
                print("-> ✅ (Normal)")

    def ajustar_modelo(self):
        # ... (código de ajustar_modelo sin cambios) ...
        print("\nAjustando el modelo ANOVA...")
        self.model = ols(self.formula, data=self.df).fit()
        self.anova_table = sm.stats.anova_lm(self.model, typ=2)
        print("Modelo ajustado exitosamente.")
        return self.anova_table

    # --- NUEVO MÉTODO 2: NORMALIDAD DE RESIDUOS (EL MÁS IMPORTANTE) ---
    def probar_normalidad_residuos(self, alpha=0.05):
        """
        Realiza la prueba de Shapiro-Wilk sobre los residuos del modelo.
        Esta es la prueba de normalidad formal para la suposición del ANOVA.
        """
        if self.model is None:
            print("El modelo aún no ha sido ajustado. Llama a 'ajustar_modelo()' primero.")
            return

        print("\n--- Prueba de Normalidad de Shapiro-Wilk para los Residuos del Modelo ---")
        print("H0: Los residuos se distribuyen normalmente.")
        
        residuos = self.model.resid
        stat, p_value = stats.shapiro(residuos)
        
        print(f"Estadístico de Shapiro-Wilk: {stat:.4f}")
        print(f"Valor p: {p_value:.4f}")
        
        if p_value < alpha:
            print("Conclusión: -> ❌ (Los residuos NO siguen una distribución normal). La suposición de normalidad no se cumple.")
        else:
            print("Conclusión: -> ✅ (Los residuos siguen una distribución normal). La suposición de normalidad se cumple.")

    def interpretar_resultados(self, alpha=0.05):
        # ... (código de interpretar_resultados sin cambios) ...
        if self.anova_table is None:
            print("El modelo aún no ha sido ajustado.")
            return
        print("\n--- Interpretación de los Resultados (p-valores) ---")
        p_value_factor1 = self.anova_table.loc[f"C({self.factor1})", "PR(>F)"]
        if p_value_factor1 < alpha:
            print(f"✅ El factor '{self.factor1}' tiene un efecto estadísticamente significativo sobre '{self.var_dependiente}' (p={p_value_factor1:.4f}).")
        else:
            print(f"❌ No hay evidencia de un efecto significativo del factor '{self.factor1}' (p={p_value_factor1:.4f}).")
        p_value_factor2 = self.anova_table.loc[f"C({self.factor2})", "PR(>F)"]
        if p_value_factor2 < alpha:
            print(f"✅ El factor '{self.factor2}' tiene un efecto estadísticamente significativo sobre '{self.var_dependiente}' (p={p_value_factor2:.4f}).")
        else:
            print(f"❌ No hay evidencia de un efecto significativo del factor '{self.factor2}' (p={p_value_factor2:.4f}).")
        interaction_term = f"C({self.factor1}):C({self.factor2})"
        p_value_interaction = self.anova_table.loc[interaction_term, "PR(>F)"]
        if p_value_interaction < alpha:
            print(f"✅ Hay un efecto de interacción estadísticamente significativo entre '{self.factor1}' y '{self.factor2}' (p={p_value_interaction:.4f}).")
            print("   >> Esto significa que el efecto de un factor depende del nivel del otro factor.")
        else:
            print(f"❌ No hay evidencia de un efecto de interacción significativo (p={p_value_interaction:.4f}).")

    def graficar_interaccion(self):
        # ... (código de graficar_interaccion sin cambios) ...
        print("\nGenerando gráfico de interacción...")
        plt.figure(figsize=(8, 6))
        sns.pointplot(data=self.df, x=self.factor1, y=self.var_dependiente, hue=self.factor2, dodge=True, errorbar=None)
        plt.title(f'Gráfico de Interacción entre {self.factor1} y {self.factor2}')
        plt.grid(True, linestyle='--', alpha=0.6)
        plt.show()

    def graficar_boxplot(self):
        # ... (código de graficar_boxplot sin cambios) ...
        print("\nGenerando boxplot...")
        plt.figure(figsize=(10, 7))
        sns.boxplot(data=self.df, x=self.factor1, y=self.var_dependiente, hue=self.factor2)
        plt.title(f'Boxplot de {self.var_dependiente} por {self.factor1} y {self.factor2}')
        plt.grid(True, linestyle='--', alpha=0.6)
        plt.show()



import pandas as pd
import numpy as np
import statsmodels.api as sm
from statsmodels.formula.api import ols
from scipy import stats

class AnalizadorEstadistico:
    """
    Un kit de herramientas para realizar diferentes análisis estadísticos
    sobre un DataFrame de Pandas.
    """
    def __init__(self, df):
        """
        Inicializa el analizador con un DataFrame.
        """
        if not isinstance(df, pd.DataFrame):
            raise TypeError("La entrada debe ser un DataFrame de Pandas.")
        self.df = df
        print("Analizador Estadístico listo.")

    def realizar_prueba_t(self, var_dependiente, var_grupo, alpha=0.05):
        """
        Realiza una Prueba t de Student para comparar las medias de DOS grupos.
        
        :param var_dependiente: Columna numérica a medir.
        :param var_grupo: Columna categórica que define los dos grupos.
        """
        print(f"\n--- Ejecutando Prueba t de Student para '{var_dependiente}' por '{var_grupo}' ---")
        
        grupos = self.df[var_grupo].unique()
        if len(grupos) != 2:
            print(f"❌ Error: La columna '{var_grupo}' debe tener exactamente 2 grupos para una Prueba t. Encontrados: {len(grupos)}.")
            return

        grupo1 = self.df[self.df[var_grupo] == grupos[0]][var_dependiente]
        grupo2 = self.df[self.df[var_grupo] == grupos[1]][var_dependiente]

        t_stat, p_value = stats.ttest_ind(grupo1, grupo2, nan_policy='omit')
        
        print(f"Estadístico t: {t_stat:.4f}, Valor p: {p_value:.4f}")
        if p_value < alpha:
            print("✅ Conclusión: La diferencia entre los promedios de los dos grupos es estadísticamente significativa.")
        else:
            print("❌ Conclusión: No hay evidencia de una diferencia significativa entre los dos grupos.")

    def realizar_anova_un_factor(self, var_dependiente, var_grupo, alpha=0.05):
        """
        Realiza un ANOVA de un factor para comparar las medias de TRES O MÁS grupos.
        
        :param var_dependiente: Columna numérica a medir.
        :param var_grupo: Columna categórica que define los grupos.
        """
        print(f"\n--- Ejecutando ANOVA de Un Factor para '{var_dependiente}' por '{var_grupo}' ---")
        
        grupos_unicos = self.df[var_grupo].unique()
        if len(grupos_unicos) < 3:
            print(f"❌ Error: Se recomienda usar ANOVA con 3 o más grupos. Encontrados: {len(grupos_unicos)}. Considera usar una Prueba t.")
            return

        samples = [self.df[self.df[var_grupo] == grupo][var_dependiente].dropna() for grupo in grupos_unicos]
        
        f_stat, p_value = stats.f_oneway(*samples)
        
        print(f"Estadístico F: {f_stat:.4f}, Valor p: {p_value:.4f}")
        if p_value < alpha:
            print("✅ Conclusión: Existe una diferencia significativa entre los promedios de al menos dos de los grupos.")
            print("   >> Se recomienda realizar una prueba post-hoc (ej. Tukey HSD) para ver qué pares son diferentes.")
        else:
            print("❌ Conclusión: No hay evidencia de una diferencia significativa entre los promedios de los grupos.")
            
    def realizar_anova_dos_factores(self, var_dependiente, factor1, factor2):
        """
        Realiza un ANOVA de dos factores completo, incluyendo la interacción.
        """
        print(f"\n--- Ejecutando ANOVA de Dos Factores para '{var_dependiente}' por '{factor1}' y '{factor2}' ---")
        
        formula = f"{var_dependiente} ~ C({factor1}) + C({factor2}) + C({factor1}):C({factor2})"
        model = ols(formula, data=self.df).fit()
        anova_table = sm.stats.anova_lm(model, typ=2)
        
        print("Tabla de Resultados del ANOVA:")
        display(anova_table)
        # Aquí podrías añadir la interpretación automática y las visualizaciones si lo deseas.