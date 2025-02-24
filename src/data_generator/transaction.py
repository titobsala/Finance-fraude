import uuid
import json
import random
import datetime
from enum import Enum
from typing import Dict, Any, Optional, List, Tuple
from pydantic import BaseModel, Field


class TransactionType(str, Enum):
    CREDIT_CARD = "credit_card"
    DEBIT_CARD = "debit_card"
    TRANSFER = "transfer"
    DIGITAL_WALLET = "digital_wallet"


class MerchantCategory(str, Enum):
    RETAIL = "retail"
    GROCERY = "grocery"
    RESTAURANT = "restaurant"
    TRAVEL = "travel"
    ENTERTAINMENT = "entertainment"
    UTILITIES = "utilities"
    HEALTHCARE = "healthcare"
    OTHER = "other"


class DeviceType(str, Enum):
    MOBILE = "mobile"
    DESKTOP = "desktop"
    TABLET = "tablet"
    POS = "pos"
    ATM = "atm"


class FraudType(str, Enum):
    LEGITIMATE = "legitimate"
    VELOCITY = "velocity"
    AMOUNT_OUTLIER = "amount_outlier"
    LOCATION_ANOMALY = "location_anomaly"
    BEHAVIOR_ANOMALY = "behavior_anomaly"


class Transaction(BaseModel):
    """Model representing a financial transaction."""
    
    # Identificadores
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str
    merchant_id: str
    
    # Detalhes da transação
    timestamp: datetime.datetime
    amount: float
    transaction_type: TransactionType
    merchant_category: MerchantCategory
    status: str = "pending"
    
    # Detalhes do dispositivo
    device_id: str
    device_type: DeviceType
    ip_address: str
    
    # Detalhes de localização
    country: str
    city: str
    latitude: float
    longitude: float
    
    # Metadados de fraude (para dados simulados)
    is_fraud: bool = False
    fraud_type: FraudType = FraudType.LEGITIMATE
    fraud_probability: float = 0.0
    
    # Campos derivados para análise
    hour_of_day: int = 0
    day_of_week: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert transaction to dictionary."""
        result = self.dict()
        # Converter valores enum para string
        result["transaction_type"] = self.transaction_type.value
        result["merchant_category"] = self.merchant_category.value
        result["device_type"] = self.device_type.value
        result["fraud_type"] = self.fraud_type.value
        # Converter timestamp para string ISO
        result["timestamp"] = self.timestamp.isoformat()
        return result
    
    def to_json(self) -> str:
        """Convert transaction to JSON string."""
        return json.dumps(self.to_dict())
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Transaction":
        """Create transaction from dictionary."""
        # Se timestamp é string, converter para datetime
        if isinstance(data.get("timestamp"), str):
            data["timestamp"] = datetime.datetime.fromisoformat(data["timestamp"])
        
        # Converter valores de string para enum
        if "transaction_type" in data:
            data["transaction_type"] = TransactionType(data["transaction_type"])
        if "merchant_category" in data:
            data["merchant_category"] = MerchantCategory(data["merchant_category"])
        if "device_type" in data:
            data["device_type"] = DeviceType(data["device_type"])
        if "fraud_type" in data:
            data["fraud_type"] = FraudType(data["fraud_type"])
            
        return cls(**data)
    
    @classmethod
    def from_json(cls, json_str: str) -> "Transaction":
        """Create transaction from JSON string."""
        return cls.from_dict(json.loads(json_str))
    
    def set_derived_fields(self) -> None:
        """Set derived fields based on transaction data."""
        self.hour_of_day = self.timestamp.hour
        self.day_of_week = self.timestamp.weekday()