"""
Modelo de Machine Learning para Previsão de Situação Cadastral
Baseado em dados temporais (cnpj + ano_mes)
"""

import pandas as pd
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Modelos e métricas
from sklearn.model_selection import train_test_split, TimeSeriesSplit
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.metrics import (
    classification_report, 
    confusion_matrix, 
    accuracy_score,
    f1_score,
    precision_score,
    recall_score
)
from sklearn.ensemble import RandomForestClassifier
try:
    import xgboost as xgb
    XGBOOST_AVAILABLE = True
except ImportError:
    XGBOOST_AVAILABLE = False
    print("XGBoost não disponível. Usando RandomForest como padrão.")
import joblib

# Visualizações
import matplotlib.pyplot as plt
import seaborn as sns

# Configuração de visualização
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")


class SituacaoCadastralPredictor:
    """
    Classe para prever situação cadastral baseado em dados temporais
    """
    
    def __init__(self, model_type='xgboost'):
        """
        Inicializa o predictor
        
        Args:
            model_type: 'xgboost' ou 'random_forest'
        """
        self.model_type = model_type
        self.model = None
        self.label_encoder = LabelEncoder()
        self.scaler = StandardScaler()
        self.feature_columns = None
        self.categorical_encoders = {}  # Armazena encoders por coluna categórica
        self.is_trained = False
        
    def load_data(self, file_path):
        """
        Carrega e prepara os dados
        
        Args:
            file_path: caminho para o arquivo CSV
        """
        print("Carregando dados...")
        df = pd.read_csv(file_path)
        
        # Remove coluna de índice se existir
        if 'Unnamed: 0' in df.columns:
            df = df.drop('Unnamed: 0', axis=1)
        
        # Converte ano_mes para datetime
        df['data_ref'] = pd.to_datetime(df['data_ref'])
        df['ano_mes'] = pd.to_datetime(df['ano_mes'], format='%Y-%m')
        
        # Ordena por cnpj e data
        df = df.sort_values(['cnpj', 'ano_mes']).reset_index(drop=True)
        
        print(f"Dados carregados: {len(df)} registros")
        print(f"CNPJs únicos: {df['cnpj'].nunique()}")
        print(f"Período: {df['ano_mes'].min()} a {df['ano_mes'].max()}")
        
        return df
    
    def create_temporal_features(self, df):
        """
        Cria features temporais a partir de ano_mes
        """
        print("Criando features temporais...")
        
        # Extrai componentes de data
        df['ano'] = df['ano_mes'].dt.year
        df['mes'] = df['ano_mes'].dt.month
        df['trimestre'] = df['ano_mes'].dt.quarter
        df['semestre'] = (df['mes'] <= 6).astype(int) + 1
        df['mes_sin'] = np.sin(2 * np.pi * df['mes'] / 12)
        df['mes_cos'] = np.cos(2 * np.pi * df['mes'] / 12)
        
        # Número de meses desde o início do dataset
        min_date = df['ano_mes'].min()
        df['meses_desde_inicio'] = (
            (df['ano_mes'].dt.year - min_date.year) * 12 + 
            (df['ano_mes'].dt.month - min_date.month)
        )
        
        return df
    
    def create_lag_features(self, df, lag_periods=[1, 2, 3, 6, 12]):
        """
        Cria features de lag para cada empresa
        """
        print("Criando features de lag...")
        
        # Agrupa por CNPJ e ordena por data
        df = df.sort_values(['cnpj', 'ano_mes']).reset_index(drop=True)
        
        # Features de lag para situação cadastral
        for lag in lag_periods:
            df[f'situacao_cadastral_lag_{lag}'] = df.groupby('cnpj')['situacao_cadastral'].shift(lag)
        
        # Features de lag para valores PGFN
        pgfn_cols = [
            'pgfn_fgts_valor_acumulado_t_minus_1',
            'pgfn_naoprev_valor_acumulado_t_minus_1',
            'pgfn_prev_valor_acumulado_t_minus_1',
            'pgfn_fgts_ajuizados_t_minus_1'
        ]
        
        for col in pgfn_cols:
            if col in df.columns:
                for lag in [1, 2, 3]:
                    df[f'{col}_lag_{lag}'] = df.groupby('cnpj')[col].shift(lag)
        
        return df
    
    def create_rolling_features(self, df, windows=[3, 6, 12]):
        """
        Cria features de rolling statistics por empresa
        """
        print("Criando features de rolling statistics...")
        
        # Rolling mean e std da situação cadastral
        for window in windows:
            df[f'situacao_cadastral_rolling_mean_{window}'] = (
                df.groupby('cnpj')['situacao_cadastral']
                .rolling(window=window, min_periods=1)
                .mean()
                .reset_index(0, drop=True)
            )
            
            df[f'situacao_cadastral_rolling_std_{window}'] = (
                df.groupby('cnpj')['situacao_cadastral']
                .rolling(window=window, min_periods=1)
                .std()
                .fillna(0)
                .reset_index(0, drop=True)
            )
        
        # Rolling sum para valores PGFN
        pgfn_cols = [
            'pgfn_fgts_valor_acumulado_t_minus_1',
            'pgfn_naoprev_valor_acumulado_t_minus_1',
            'pgfn_prev_valor_acumulado_t_minus_1'
        ]
        
        for col in pgfn_cols:
            if col in df.columns:
                for window in [3, 6]:
                    df[f'{col}_rolling_sum_{window}'] = (
                        df.groupby('cnpj')[col]
                        .rolling(window=window, min_periods=1)
                        .sum()
                        .reset_index(0, drop=True)
                    )
        
        return df
    
    def create_aggregated_features(self, df):
        """
        Cria features agregadas por empresa
        """
        print("Criando features agregadas...")
        
        # Features históricas por empresa
        empresa_stats = df.groupby('cnpj').agg({
            'situacao_cadastral': ['mean', 'std', 'min', 'max', 'count'],
            'pgfn_fgts_valor_acumulado_t_minus_1': ['mean', 'max', 'sum'],
            'pgfn_naoprev_valor_acumulado_t_minus_1': ['mean', 'max', 'sum'],
            'pgfn_prev_valor_acumulado_t_minus_1': ['mean', 'max', 'sum'],
            'tempo_atividade_anos': 'first'
        }).reset_index()
        
        # Flatten column names
        empresa_stats.columns = ['cnpj'] + [
            '_'.join(col).strip() if col[1] else col[0] 
            for col in empresa_stats.columns[1:]
        ]
        
        # Merge de volta
        df = df.merge(empresa_stats, on='cnpj', suffixes=('', '_empresa'))
        
        # Posição temporal relativa (primeiro, último, meio)
        df['posicao_temporal'] = df.groupby('cnpj').cumcount()
        df['total_registros_empresa'] = df.groupby('cnpj')['cnpj'].transform('count')
        df['posicao_relativa'] = df['posicao_temporal'] / df['total_registros_empresa']
        
        return df
    
    def prepare_features(self, df):
        """
        Prepara todas as features para o modelo
        """
        print("\n=== Preparando Features ===")
        
        # Cria todas as features
        df = self.create_temporal_features(df)
        df = self.create_lag_features(df)
        df = self.create_rolling_features(df)
        df = self.create_aggregated_features(df)
        
        # Seleciona features para o modelo
        feature_cols = [
            # Temporais
            'ano', 'mes', 'trimestre', 'semestre', 
            'mes_sin', 'mes_cos', 'meses_desde_inicio',
            
            # Categóricas
            'cnae_fiscal_principal', 'uf',
            
            # Numéricas base
            'tempo_atividade_anos',
            'pgfn_fgts_valor_acumulado_t_minus_1',
            'pgfn_naoprev_valor_acumulado_t_minus_1',
            'pgfn_prev_valor_acumulado_t_minus_1',
            'pgfn_fgts_ajuizados_t_minus_1',
            'situacao_cadastral_t_minus_1',
            
            # Lag features
            'situacao_cadastral_lag_1', 'situacao_cadastral_lag_2', 
            'situacao_cadastral_lag_3', 'situacao_cadastral_lag_6',
            
            # Rolling features
            'situacao_cadastral_rolling_mean_3',
            'situacao_cadastral_rolling_mean_6',
            'situacao_cadastral_rolling_mean_12',
            'situacao_cadastral_rolling_std_3',
            'situacao_cadastral_rolling_std_6',
            
            # Agregadas
            'posicao_relativa',
            'situacao_cadastral_mean_empresa',
            'situacao_cadastral_std_empresa',
        ]
        
        # Adiciona features de lag de PGFN se existirem
        for col in df.columns:
            if 'pgfn' in col.lower() and 'lag' in col.lower():
                feature_cols.append(col)
            if 'pgfn' in col.lower() and 'rolling' in col.lower():
                feature_cols.append(col)
        
        # Filtra apenas colunas que existem
        feature_cols = [col for col in feature_cols if col in df.columns]
        
        # Remove linhas com NaN (primeiras linhas de cada empresa devido aos lags)
        # Mas só se tiver a coluna target (para previsão, não precisa)
        if 'situacao_cadastral' in df.columns:
            df_clean = df.dropna(subset=feature_cols + ['situacao_cadastral']).copy()
        else:
            df_clean = df.dropna(subset=feature_cols).copy()
        
        print(f"\nFeatures selecionadas: {len(feature_cols)}")
        print(f"Registros após limpeza: {len(df_clean)}")
        
        # Só atualiza self.feature_columns se o modelo não foi treinado ainda
        # (durante o treino, será atualizado depois da codificação)
        if not self.is_trained:
            self.feature_columns = feature_cols
        
        return df_clean
    
    def encode_categorical_features(self, df, fit=True):
        """
        Codifica features categóricas
        """
        # Converte UF (pode estar como lista)
        if 'uf' in df.columns:
            df['uf'] = df['uf'].astype(str).str.replace(r"\[|\]|'", '', regex=True)
            df['uf'] = df['uf'].str.strip()
        
        # CNAE como string
        if 'cnae_fiscal_principal' in df.columns:
            df['cnae_fiscal_principal'] = df['cnae_fiscal_principal'].astype(str)
        
        return df
    
    def train(self, df, test_size=0.2, validation_split_date=None):
        """
        Treina o modelo
        
        Args:
            df: DataFrame com features preparadas
            test_size: proporção para teste (se validation_split_date não for fornecido)
            validation_split_date: data para separar treino/teste (formato 'YYYY-MM')
        """
        print("\n=== Treinando Modelo ===")
        
        # Prepara features
        df = self.prepare_features(df)
        df = self.encode_categorical_features(df, fit=True)
        
        # Separação temporal
        if validation_split_date:
            split_date = pd.to_datetime(validation_split_date, format='%Y-%m')
            train_mask = df['ano_mes'] < split_date
            test_mask = df['ano_mes'] >= split_date
            
            X_train = df[train_mask][self.feature_columns]
            X_test = df[test_mask][self.feature_columns]
            y_train = df[train_mask]['situacao_cadastral']
            y_test = df[test_mask]['situacao_cadastral']
        else:
            # Usa TimeSeriesSplit para validação temporal
            split_idx = int(len(df) * (1 - test_size))
            X_train = df.iloc[:split_idx][self.feature_columns]
            X_test = df.iloc[split_idx:][self.feature_columns]
            y_train = df.iloc[:split_idx]['situacao_cadastral']
            y_test = df.iloc[split_idx:]['situacao_cadastral']
        
        print(f"Treino: {len(X_train)} registros")
        print(f"Teste: {len(X_test)} registros")
        
        # Codifica features categóricas
        categorical_cols = ['cnae_fiscal_principal', 'uf']
        categorical_cols = [col for col in categorical_cols if col in X_train.columns]
        
        # Label encoding para categóricas (usa todos os valores únicos de treino+teste)
        for col in categorical_cols:
            # Pega todos os valores únicos de treino e teste
            all_values = pd.concat([X_train[col], X_test[col]]).astype(str).unique()
            le = LabelEncoder()
            le.fit(all_values)
            
            # Salva encoder para uso futuro
            self.categorical_encoders[col] = le
            
            X_train[col + '_encoded'] = le.transform(X_train[col].astype(str))
            # Para valores não vistos, usa 0 (ou pode usar o valor mais frequente)
            X_test[col + '_encoded'] = X_test[col].astype(str).apply(
                lambda x: le.transform([x])[0] if x in le.classes_ else 0
            )
            
            # Remove coluna original
            X_train = X_train.drop(col, axis=1)
            X_test = X_test.drop(col, axis=1)
            self.feature_columns = [c for c in self.feature_columns if c != col] + [col + '_encoded']
        
        # Atualiza feature columns
        X_train = X_train[self.feature_columns]
        X_test = X_test[self.feature_columns]
        
        # Preenche NaN com 0
        X_train = X_train.fillna(0)
        X_test = X_test.fillna(0)
        
        # Treina modelo
        if self.model_type == 'xgboost' and XGBOOST_AVAILABLE:
            self.model = xgb.XGBClassifier(
                n_estimators=200,
                max_depth=6,
                learning_rate=0.1,
                subsample=0.8,
                colsample_bytree=0.8,
                random_state=42,
                eval_metric='mlogloss',
                use_label_encoder=False
            )
        else:
            if self.model_type == 'xgboost' and not XGBOOST_AVAILABLE:
                print("XGBoost não disponível. Usando RandomForest.")
            self.model = RandomForestClassifier(
                n_estimators=200,
                max_depth=10,
                random_state=42,
                n_jobs=-1
            )
        
        print(f"\nTreinando modelo {self.model_type}...")
        self.model.fit(X_train, y_train)
        
        # Avalia
        y_pred = self.model.predict(X_test)
        
        print("\n=== Métricas de Avaliação ===")
        print(f"Acurácia: {accuracy_score(y_test, y_pred):.4f}")
        print(f"F1-Score (macro): {f1_score(y_test, y_pred, average='macro'):.4f}")
        print(f"Precisão (macro): {precision_score(y_test, y_pred, average='macro'):.4f}")
        print(f"Recall (macro): {recall_score(y_test, y_pred, average='macro'):.4f}")
        
        print("\n=== Classification Report ===")
        print(classification_report(y_test, y_pred))
        
        print("\n=== Confusion Matrix ===")
        cm = confusion_matrix(y_test, y_pred)
        print(cm)
        
        self.is_trained = True
        
        return {
            'accuracy': accuracy_score(y_test, y_pred),
            'f1_score': f1_score(y_test, y_pred, average='macro'),
            'precision': precision_score(y_test, y_pred, average='macro'),
            'recall': recall_score(y_test, y_pred, average='macro'),
            'confusion_matrix': cm,
            'y_test': y_test,
            'y_pred': y_pred
        }
    
    def predict(self, df, ano_mes):
        """
        Faz previsão para um ano_mes específico
        
        Args:
            df: DataFrame completo com histórico
            ano_mes: string no formato 'YYYY-MM'
        """
        if not self.is_trained:
            raise ValueError("Modelo não foi treinado ainda. Chame train() primeiro.")
        
        print(f"\n=== Fazendo Previsão para {ano_mes} ===")
        
        # Prepara features (mas não codifica categóricas ainda - isso será feito depois)
        df = self.prepare_features(df)
        # NÃO chama encode_categorical_features aqui - será feito manualmente depois
        
        # Filtra dados até o ano_mes
        target_date = pd.to_datetime(ano_mes, format='%Y-%m')
        df_filtered = df[df['ano_mes'] <= target_date].copy()
        
        if len(df_filtered) == 0:
            raise ValueError(f"Não há dados disponíveis até {ano_mes}")
        
        # Pega o último registro de cada empresa
        df_last = df_filtered.sort_values(['cnpj', 'ano_mes']).groupby('cnpj').last().reset_index()
        
        # Prepara features
        # Primeiro, precisa codificar as categóricas antes de selecionar as colunas
        categorical_cols = ['cnae_fiscal_principal', 'uf']
        
        # Cria cópia para trabalhar
        df_work = df_last.copy()
        
        # Codifica categóricas usando os encoders salvos
        for col in categorical_cols:
            if col in df_work.columns and col in self.categorical_encoders:
                le = self.categorical_encoders[col]
                # Para valores não vistos, usa 0
                df_work[col + '_encoded'] = df_work[col].astype(str).apply(
                    lambda x: le.transform([x])[0] if x in le.classes_ else 0
                )
        
        # Remove colunas categóricas originais se existirem (IMPORTANTE: antes de selecionar)
        for col in categorical_cols:
            if col in df_work.columns:
                df_work = df_work.drop(col, axis=1)
        
        # Agora seleciona apenas as colunas que o modelo espera (apenas as codificadas)
        # Filtra para garantir que não inclua colunas categóricas originais
        feature_cols_to_use = [
            c for c in self.feature_columns 
            if c in df_work.columns and c not in categorical_cols
        ]
        
        # Verifica se todas as colunas esperadas estão disponíveis
        missing_in_data = set(self.feature_columns) - set(feature_cols_to_use) - set(categorical_cols)
        if missing_in_data:
            print(f"Aviso: Colunas faltando nos dados: {missing_in_data}")
        
        # Remove colunas categóricas originais se ainda estiverem presentes
        problematic_cols = [c for c in categorical_cols if c in df_work.columns]
        if problematic_cols:
            df_work = df_work.drop(columns=problematic_cols)
        
        X = df_work[feature_cols_to_use].copy()
        
        # Preenche NaN
        X = X.fillna(0)
        
        # Garante que todas as colunas esperadas estão presentes
        missing_cols = set(self.feature_columns) - set(X.columns)
        for col in missing_cols:
            X[col] = 0
        
        # Ordena colunas na ordem esperada pelo modelo
        # Garante que só usa colunas que existem e que são esperadas pelo modelo
        final_cols = [c for c in self.feature_columns if c in X.columns]
        if len(final_cols) != len(self.feature_columns):
            missing = set(self.feature_columns) - set(final_cols)
            print(f"Aviso: Adicionando colunas faltantes com zeros: {missing}")
            for col in missing:
                X[col] = 0
            final_cols = self.feature_columns
        
        X = X[final_cols]
        
        # Verificação final: garante que não há colunas inesperadas
        unexpected = [c for c in X.columns if c not in self.feature_columns]
        if unexpected:
            X = X.drop(columns=unexpected)
        
        # Faz previsão
        predictions = self.model.predict(X)
        probabilities = self.model.predict_proba(X)
        
        # Adiciona ao DataFrame
        df_last['situacao_cadastral_predita'] = predictions
        df_last['probabilidade_max'] = probabilities.max(axis=1)
        
        # Adiciona probabilidades por classe
        classes = self.model.classes_
        for i, class_val in enumerate(classes):
            df_last[f'prob_classe_{class_val}'] = probabilities[:, i]
        
        return df_last[['cnpj', 'ano_mes', 'situacao_cadastral', 
                        'situacao_cadastral_predita', 'probabilidade_max'] + 
                       [f'prob_classe_{c}' for c in classes]]
    
    def save_model(self, filepath):
        """Salva o modelo treinado"""
        if not self.is_trained:
            raise ValueError("Modelo não foi treinado ainda.")
        
        joblib.dump({
            'model': self.model,
            'feature_columns': self.feature_columns,
            'model_type': self.model_type,
            'categorical_encoders': self.categorical_encoders
        }, filepath)
        print(f"Modelo salvo em {filepath}")
    
    def load_model(self, filepath):
        """Carrega um modelo salvo"""
        data = joblib.load(filepath)
        self.model = data['model']
        self.feature_columns = data['feature_columns']
        self.model_type = data['model_type']
        self.categorical_encoders = data.get('categorical_encoders', {})
        self.is_trained = True
        print(f"Modelo carregado de {filepath}")


def main():
    """Função principal para treinar e avaliar o modelo"""
    
    # Inicializa predictor
    predictor = SituacaoCadastralPredictor(model_type='xgboost')
    
    # Carrega dados
    df = predictor.load_data('dataset_silver.csv')
    
    # Treina modelo (usa 80% para treino, 20% para teste)
    # Ou pode usar uma data específica: validation_split_date='2024-06'
    results = predictor.train(df, test_size=0.2)
    
    # Salva modelo
    predictor.save_model('modelo_situacao_cadastral.pkl')
    
    # Exemplo de previsão para um ano_mes específico
    print("\n" + "="*50)
    predictions = predictor.predict(df, '2025-06')
    print(f"\nPrevisões para 2025-06:")
    print(predictions.head(10))
    
    # Estatísticas das previsões
    print(f"\nDistribuição de previsões:")
    print(predictions['situacao_cadastral_predita'].value_counts())
    
    return predictor, results


if __name__ == '__main__':
    predictor, results = main()

