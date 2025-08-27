import pandas as pd
import numpy as np
import statsmodels.api as sm
from statsmodels.formula.api import ols
from scipy import stats
import seaborn as sns
import matplotlib.pyplot as plt

class AnalizadorEstadistico:
    """
    Un kit de herramientas para realizar diferentes análisis estadísticos
    sobre un DataFrame de Pandas.
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
        
        # Validación de datos
        df_clean = self.df[[var_dependiente, var_grupo]].dropna()
        grupos = df_clean[var_grupo].unique()
        if len(grupos) != 2:
            print(f"❌ Error: La columna '{var_grupo}' debe tener exactamente 2 grupos. Encontrados: {len(grupos)}.")
            return

        # Separar los grupos
        grupo1 = df_clean[df_clean[var_grupo] == grupos[0]][var_dependiente]
        grupo2 = df_clean[df_clean[var_grupo] == grupos[1]][var_dependiente]

        # Realizar la prueba estadística
        t_stat, p_value = stats.ttest_ind(grupo1, grupo2)
        
        print(f"Estadístico t: {t_stat:.4f}, Valor p: {p_value:.4f}")
        if p_value < alpha:
            print("✅ Conclusión: La diferencia entre los promedios de los dos grupos es estadísticamente significativa.")
        else:
            print("❌ Conclusión: No hay evidencia de una diferencia significativa entre los dos grupos.")

        # Generar el Boxplot
        print("\n--- Generando Visualización (Boxplot) ---")
        plt.figure(figsize=(8, 6))
        sns.boxplot(data=df_clean, x=var_grupo, y=var_dependiente)
        plt.title(f'Distribución de {var_dependiente} por {var_grupo}')
        plt.xlabel(var_grupo)
        plt.ylabel(var_dependiente)
        plt.grid(True, linestyle='--', alpha=0.6)
        plt.show()

    def realizar_anova_un_factor(self, var_dependiente, var_grupo, alpha=0.05):
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
        # ... (El método completo que ya teníamos) ...
        print(f"\n--- Ejecutando ANOVA de Dos Factores para '{var_dependiente}' por '{factor1}' y '{factor2}' ---")
        df_clean = self.df[[var_dependiente, factor1, factor2]].dropna()
        if len(df_clean) < len(self.df):
            print(f"Advertencia: Se eliminaron {len(self.df) - len(df_clean)} filas con valores faltantes.")
        formula = f"{var_dependiente} ~ C({factor1}) + C({factor2}) + C({factor1}):C({factor2})"
        model = ols(formula, data=df_clean).fit()
        anova_table = sm.stats.anova_lm(model, typ=2)
        print("\n--- Tabla de Resultados del ANOVA ---")
        display(anova_table)
        print("\n--- Interpretación de los Resultados (p-valores) ---")
        p_value_factor1 = anova_table.loc[f"C({factor1})", "PR(>F)"]
        if p_value_factor1 < alpha: print(f"✅ El factor '{factor1}' tiene un efecto estadísticamente significativo (p={p_value_factor1:.4f}).")
        else: print(f"❌ No hay evidencia de un efecto significativo del factor '{factor1}' (p={p_value_factor1:.4f}).")
        p_value_factor2 = anova_table.loc[f"C({factor2})", "PR(>F)"]
        if p_value_factor2 < alpha: print(f"✅ El factor '{factor2}' tiene un efecto estadísticamente significativo (p={p_value_factor2:.4f}).")
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