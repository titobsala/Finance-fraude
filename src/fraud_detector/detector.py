import random
import numpy as np
import yaml
import joblib
import os
from typing import Dict, Any, Tuple, Optional, List
from loguru import logger


class FraudDetector:
    """Detector de fraudes para transações financeiras."""
    
    def __init__(self, model_path: Optional[str] = None):
        """
        Inicializa o detector de fraudes.
        
        Args:
            model_path: Caminho para o modelo treinado (se None, usa modelo base)
        """
        # Inicialmente usaremos regras básicas até o modelo ML ser treinado
        self.model = None
        self.model_path = model_path
        
        # Carregar modelo se existir
        if model_path and os.path.exists(model_path):
            try:
                self.model = joblib.load(model_path)
                logger.info(f"Modelo carregado de {model_path}")
            except Exception as e:
                logger.error(f"Erro ao carregar modelo: {e}")
        
        # Regras para detecção inicial
        self.rules = {
            # Valores extremos para diferentes tipos de transação
            "amount_thresholds": {
                "credit_card": 2000.0,
                "debit_card": 1000.0,
                "transfer": 5000.0,
                "digital_wallet": 500.0
            },
            # Categorias de alto risco
            "high_risk_categories": [
                "entertainment", 
                "travel"
            ],
            # Países de alto risco
            "high_risk_countries": [
                # Lista de países com alto índice de fraude
            ]
        }
        
        logger.info("Detector de fraudes inicializado")
    
    def _rule_based_score(self, amount: float, tx_type: str, 
                        merchant_category: str, hour: int, 
                        day: int, country: str, device_type: str) -> float:
        """
        Calcula uma pontuação de risco baseada em regras simples.
        
        Args:
            amount: Valor da transação
            tx_type: Tipo de transação
            merchant_category: Categoria do comerciante
            hour: Hora do dia (0-23)
            day: Dia da semana (0-6, sendo 0=segunda)
            country: País da transação
            device_type: Tipo de dispositivo
            
        Returns:
            Pontuação de risco entre 0 e 1
        """
        score = 0.0
        
        # Verificar valor da transação
        if tx_type in self.rules["amount_thresholds"]:
            threshold = self.rules["amount_thresholds"][tx_type]
            if amount > threshold:
                ratio = amount / threshold
                score += min(0.5, ratio / 10)
        
        # Verificar categoria do comerciante
        if merchant_category in self.rules["high_risk_categories"]:
            score += 0.2
        
        # Verificar país
        if country in self.rules["high_risk_countries"]:
            score += 0.3
        
        # Verificar horário (madrugada = mais risco)
        if 0 <= hour <= 5:
            score += 0.2
        
        # Final score capped at 0.95 (never 100% certain without ML)
        return min(0.95, score)
    
    def predict_fraud_probability(self, amount: float, tx_type: str, 
                                merchant_category: str, hour: int, 
                                day: int, country: str, 
                                device_type: str) -> float:
        """
        Prediz a probabilidade de fraude para uma transação.
        
        Args:
            amount: Valor da transação
            tx_type: Tipo de transação
            merchant_category: Categoria do comerciante
            hour: Hora do dia (0-23)
            day: Dia da semana (0-6)
            country: País da transação
            device_type: Tipo de dispositivo
            
        Returns:
            Probabilidade de fraude entre 0 e 1
        """
        # Se temos um modelo treinado, usá-lo
        if self.model:
            try:
                # Preprocessar features
                features = self._preprocess_features(
                    amount, tx_type, merchant_category, 
                    hour, day, country, device_type
                )
                
                # Predição do modelo
                probability = self.model.predict_proba([features])[0, 1]
                return float(probability)
            
            except Exception as e:
                logger.error(f"Erro na predição do modelo: {e}")
                # Fallback para regras simples
        
        # Usar sistema baseado em regras
        return self._rule_based_score(
            amount, tx_type, merchant_category, 
            hour, day, country, device_type
        )
    
    def _preprocess_features(self, amount: float, tx_type: str,
                           merchant_category: str, hour: int,
                           day: int, country: str,
                           device_type: str) -> List[float]:
        """
        Preprocessa as features para entrada no modelo.
        
        Esta é uma versão simplificada, um sistema real teria
        uma pipeline de preprocessamento mais robusta.
        
        Args:
            amount: Valor da transação
            tx_type: Tipo de transação
            merchant_category: Categoria do comerciante
            hour: Hora do dia (0-23)
            day: Dia da semana (0-6)
            country: País da transação
            device_type: Tipo de dispositivo
            
        Returns:
            Lista de features preprocessadas
        """
        # Normalizar valor
        norm_amount = np.log1p(amount)  # log transform para lidar com skew
        
        # One-hot encoding para tipo de transação
        tx_type_features = [0, 0, 0, 0]
        tx_types = ['credit_card', 'debit_card', 'transfer', 'digital_wallet']
        if tx_type in tx_types:
            tx_type_features[tx_types.index(tx_type)] = 1
        
        # One-hot encoding para categoria
        category_features = [0, 0, 0, 0, 0, 0, 0, 0]
        categories = ['retail', 'grocery', 'restaurant', 'travel', 
                     'entertainment', 'utilities', 'healthcare', 'other']
        if merchant_category in categories:
            category_features[categories.index(merchant_category)] = 1
        
        # Hora e dia (ciclicamente)
        hour_sin = np.sin(2 * np.pi * hour / 24)
        hour_cos = np.cos(2 * np.pi * hour / 24)
        day_sin = np.sin(2 * np.pi * day / 7)
        day_cos = np.cos(2 * np.pi * day / 7)
        
        # Feature para país de risco
        country_risk = 1 if country in self.rules["high_risk_countries"] else 0
        
        # One-hot para tipo de dispositivo
        device_features = [0, 0, 0, 0, 0]
        devices = ['mobile', 'desktop', 'tablet', 'pos', 'atm']
        if device_type in devices:
            device_features[devices.index(device_type)] = 1
        
        # Combinar todas as features
        features = [norm_amount, hour_sin, hour_cos, day_sin, day_cos, country_risk]
        features.extend(tx_type_features)
        features.extend(category_features)
        features.extend(device_features)
        
        return features

    def train_model(self, transactions: List[Dict[str, Any]], output_path: Optional[str] = None) -> None:
        """
        Treina o modelo de detecção de fraudes.
        
        Args:
            transactions: Lista de transações para treinamento
            output_path: Caminho para salvar o modelo treinado
        """
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.model_selection import train_test_split
        
        logger.info(f"Treinando modelo com {len(transactions)} transações")
        
        # Preparar dados
        X = []
        y = []
        
        for tx in transactions:
            features = self._preprocess_features(
                tx['amount'], tx['transaction_type'], tx['merchant_category'],
                tx['hour_of_day'], tx['day_of_week'], tx['country'], tx['device_type']
            )
            X.append(features)
            y.append(1 if tx['is_fraud'] else 0)
        
        # Split dados
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        # Treinar modelo
        model = RandomForestClassifier(
            n_estimators=100, 
            max_depth=10,
            random_state=42
        )
        
        model.fit(X_train, y_train)
        
        # Avaliar modelo
        train_accuracy = model.score(X_train, y_train)
        test_accuracy = model.score(X_test, y_test)
        
        logger.info(f"Modelo treinado. Acurácia treino: {train_accuracy:.4f}, teste: {test_accuracy:.4f}")
        
        # Salvar modelo
        if output_path:
            joblib.dump(model, output_path)
            logger.info(f"Modelo salvo em {output_path}")
        
        # Atualizar modelo em uso
        self.model = model