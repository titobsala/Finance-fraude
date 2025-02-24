import os
import numpy as np
import pandas as pd
import yaml
import joblib
from typing import Dict, List, Any, Tuple, Optional
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score
from sklearn.pipeline import Pipeline
from loguru import logger


class FraudModel:
    """Modelo de Machine Learning para detecção de fraudes."""
    
    def __init__(self, config_path: str = "config/spark.yml"):
        """
        Inicializa o modelo de fraude.
        
        Args:
            config_path: Caminho para arquivo de configuração
        """
        # Carregar configuração
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)['spark']['ml']
        
        # Definir feature columns
        self.feature_columns = self.config['feature_columns']
        
        # Pipeline de preprocessamento
        self.preprocessor = None
        
        # Modelo treinado
        self.model = None
        
        logger.info("Modelo de fraude inicializado")
    
    def preprocess_data(self, df: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray]:
        """
        Preprocessa os dados para treinamento/inferência.
        
        Args:
            df: DataFrame com dados de transações
            
        Returns:
            Features e labels (X, y)
        """
        # Selecionar features relevantes
        X = df[self.feature_columns].copy()
        
        # Target
        y = df['is_fraud'].astype(int).values if 'is_fraud' in df.columns else None
        
        # One-hot encoding para categorias
        X = pd.get_dummies(X, columns=['transaction_type', 'merchant_category'])
        
        # Transformações específicas
        if 'hour_of_day' in X.columns:
            # Transformação cíclica para hora do dia
            X['hour_sin'] = np.sin(2 * np.pi * X['hour_of_day'] / 24)
            X['hour_cos'] = np.cos(2 * np.pi * X['hour_of_day'] / 24)
            X.drop('hour_of_day', axis=1, inplace=True)
        
        if 'day_of_week' in X.columns:
            # Transformação cíclica para dia da semana
            X['day_sin'] = np.sin(2 * np.pi * X['day_of_week'] / 7)
            X['day_cos'] = np.cos(2 * np.pi * X['day_of_week'] / 7)
            X.drop('day_of_week', axis=1, inplace=True)
        
        # Transformação logarítmica para valores monetários
        if 'amount' in X.columns:
            X['amount'] = np.log1p(X['amount'])
        
        # Aplicar scaling se já temos um preprocessador
        if self.preprocessor is not None:
            X_processed = self.preprocessor.transform(X.values)
        else:
            # Criar novo preprocessador durante o treinamento
            self.preprocessor = StandardScaler()
            X_processed = self.preprocessor.fit_transform(X.values)
        
        return X_processed, y
    
    def train(self, df: pd.DataFrame, model_type: str = 'random_forest') -> Dict[str, Any]:
        """
        Treina o modelo de detecção de fraudes.
        
        Args:
            df: DataFrame com dados de treinamento
            model_type: Tipo de modelo ('random_forest', 'gradient_boosting', 'logistic')
            
        Returns:
            Métricas de performance do modelo
        """
        logger.info(f"Treinando modelo {model_type} com {len(df)} registros")
        
        # Preprocessar dados
        X, y = self.preprocess_data(df)
        
        # Split train/test
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, 
            test_size=1 - self.config['train_test_split'],
            random_state=42, 
            stratify=y
        )
        
        # Selecionar modelo
        if model_type == 'random_forest':
            model = RandomForestClassifier(
                n_estimators=100,
                max_depth=10,
                class_weight='balanced',
                random_state=42
            )
        elif model_type == 'gradient_boosting':
            model = GradientBoostingClassifier(
                n_estimators=100,
                max_depth=5,
                random_state=42
            )
        elif model_type == 'logistic':
            model = LogisticRegression(
                max_iter=1000,
                class_weight='balanced',
                random_state=42
            )
        else:
            raise ValueError(f"Tipo de modelo desconhecido: {model_type}")
        
        # Treinar modelo
        model.fit(X_train, y_train)
        
        # Avaliar performance
        y_pred = model.predict(X_test)
        y_prob = model.predict_proba(X_test)[:, 1]
        
        # Calcular métricas
        metrics = {
            'accuracy': model.score(X_test, y_test),
            'roc_auc': roc_auc_score(y_test, y_prob),
            'confusion_matrix': confusion_matrix(y_test, y_pred).tolist(),
            'classification_report': classification_report(y_test, y_pred, output_dict=True)
        }
        
        # Salvar modelo
        self.model = model
        
        logger.info(f"Modelo treinado. Acurácia: {metrics['accuracy']:.4f}, ROC AUC: {metrics['roc_auc']:.4f}")
        
        return metrics
    
    def hyperparameter_tuning(self, df: pd.DataFrame, model_type: str = 'random_forest') -> Dict[str, Any]:
        """
        Realiza otimização de hiperparâmetros.
        
        Args:
            df: DataFrame com dados de treinamento
            model_type: Tipo de modelo
            
        Returns:
            Melhores hiperparâmetros e métricas
        """
        logger.info(f"Iniciando otimização de hiperparâmetros para {model_type}")
        
        # Preprocessar dados
        X, y = self.preprocess_data(df)
        
        # Split train/test
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, 
            test_size=0.2,
            random_state=42, 
            stratify=y
        )
        
        # Configurar grid de hiperparâmetros
        if model_type == 'random_forest':
            model = RandomForestClassifier(random_state=42)
            param_grid = {
                'n_estimators': [50, 100, 200],
                'max_depth': [5, 10, 15, None],
                'min_samples_split': [2, 5, 10],
                'class_weight': [None, 'balanced']
            }
        elif model_type == 'gradient_boosting':
            model = GradientBoostingClassifier(random_state=42)
            param_grid = {
                'n_estimators': [50, 100, 200],
                'max_depth': [3, 5, 7],
                'learning_rate': [0.01, 0.1, 0.2]
            }
        elif model_type == 'logistic':
            model = LogisticRegression(random_state=42)
            param_grid = {
                'C': [0.1, 1, 10],
                'penalty': ['l1', 'l2'],
                'class_weight': [None, 'balanced']
            }
        else:
            raise ValueError(f"Tipo de modelo desconhecido: {model_type}")
        
        # Cross-validation com Grid Search
        grid_search = GridSearchCV(
            model, 
            param_grid,
            cv=self.config['cross_validation_folds'],
            scoring='roc_auc',
            n_jobs=-1
        )
        
        grid_search.fit(X_train, y_train)
        
        # Melhor modelo
        best_model = grid_search.best_estimator_
        
        # Avaliar no conjunto de teste
        y_pred = best_model.predict(X_test)
        y_prob = best_model.predict_proba(X_test)[:, 1]
        
        # Calcular métricas
        results = {
            'best_params': grid_search.best_params_,
            'best_score': grid_search.best_score_,
            'test_accuracy': best_model.score(X_test, y_test),
            'test_roc_auc': roc_auc_score(y_test, y_prob),
            'confusion_matrix': confusion_matrix(y_test, y_pred).tolist(),
            'classification_report': classification_report(y_test, y_pred, output_dict=True)
        }
        
        # Salvar melhor modelo
        self.model = best_model
        
        logger.info(f"Otimização concluída. Melhor score: {results['best_score']:.4f}")
        logger.info(f"Melhores parâmetros: {results['best_params']}")
        
        return results
    
    def predict(self, df: pd.DataFrame) -> np.ndarray:
        """
        Faz predições com o modelo treinado.
        
        Args:
            df: DataFrame com dados para predição
            
        Returns:
            Array com probabilidades de fraude
        """
        if self.model is None:
            raise ValueError("Modelo não treinado. Execute train() primeiro.")
        
        # Preprocessar dados
        X, _ = self.preprocess_data(df)
        
        # Predição
        return self.model.predict_proba(X)[:, 1]
    
    def save_model(self, path: str) -> None:
        """
        Salva o modelo treinado.
        
        Args:
            path: Caminho para salvar o modelo
        """
        if self.model is None:
            raise ValueError("Modelo não treinado. Execute train() primeiro.")
        
        # Criar diretório se não existir
        os.makedirs(os.path.dirname(path), exist_ok=True)
        
        # Salvar modelo e preprocessador
        joblib.dump({
            'model': self.model,
            'preprocessor': self.preprocessor,
            'feature_columns': self.feature_columns
        }, path)
        
        logger.info(f"Modelo salvo em {path}")
    
    def load_model(self, path: str) -> None:
        """
        Carrega um modelo treinado.
        
        Args:
            path: Caminho do modelo salvo
        """
        if not os.path.exists(path):
            raise FileNotFoundError(f"Modelo não encontrado: {path}")
        
        # Carregar modelo
        loaded_data = joblib.load(path)
        
        self.model = loaded_data['model']
        self.preprocessor = loaded_data['preprocessor']
        self.feature_columns = loaded_data['feature_columns']
        
        logger.info(f"Modelo carregado de {path}")


def load_transactions_from_csv(file_path: str) -> pd.DataFrame:
    """
    Carrega transações de um arquivo CSV.
    
    Args:
        file_path: Caminho do arquivo CSV
        
    Returns:
        DataFrame com transações
    """
    return pd.read_csv(file_path)


def main():
    """Função principal para treinar e testar modelo."""
    # Caminho para dados de treinamento
    data_path = os.environ.get("TRAINING_DATA", "data/transactions.csv")
    
    # Carregar dados
    df = load_transactions_from_csv(data_path)
    
    # Inicializar modelo
    model = FraudModel()
    
    # Treinar modelo
    metrics = model.train(df, model_type='random_forest')
    
    # Imprimir métricas
    print(f"Acurácia: {metrics['accuracy']:.4f}")
    print(f"ROC AUC: {metrics['roc_auc']:.4f}")
    
    # Salvar modelo
    model.save_model("models/fraud_model.joblib")


if __name__ == "__main__":
    main()