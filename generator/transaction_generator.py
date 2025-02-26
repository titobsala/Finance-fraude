import random
import uuid
import json
import time
import datetime
from dataclasses import dataclass, asdict
from enum import Enum
from typing import Dict, Any, List, Optional


class PaymentMethod(str, Enum):
    CREDIT_CARD = "credit_card"
    DEBIT_CARD = "debit_card"
    BANK_TRANSFER = "bank_transfer"
    DIGITAL_WALLET = "digital_wallet"
    PIX = "pix"


class TransactionStatus(str, Enum):
    APPROVED = "approved"
    REJECTED = "rejected"
    PENDING = "pending"


class DeviceType(str, Enum):
    MOBILE = "mobile"
    WEB = "web"
    POS = "pos"
    ATM = "atm"


@dataclass
class GeoLocation:
    latitude: float
    longitude: float
    country: str
    city: str


@dataclass
class TransactionData:
    transaction_id: str
    user_id: str
    amount: float
    currency: str
    payment_method: PaymentMethod
    status: TransactionStatus
    device_type: DeviceType
    timestamp: str
    geo_location: Dict[str, Any]
    is_fraud: bool = False
    fraud_score: float = 0.0
    merchant_id: str = ""
    merchant_category: str = ""
    description: str = ""


class TransactionGenerator:
    def __init__(self, fraud_probability: float = 0.02):
        self.fraud_probability = fraud_probability
        self.users = self._generate_users(1000)
        self.merchants = self._generate_merchants(500)
        self.locations = self._generate_locations()
        
    def _generate_users(self, count: int) -> List[Dict[str, Any]]:
        """Generate a pool of users with IDs and some profile data."""
        users = []
        for _ in range(count):
            user_id = f"user_{uuid.uuid4().hex[:8]}"
            users.append({
                "id": user_id,
                "typical_amounts": random.choice([
                    (10, 100),  # Small transactions
                    (50, 500),  # Medium transactions
                    (200, 2000)  # Large transactions
                ]),
                "preferred_payment_methods": random.sample(
                    list(PaymentMethod), 
                    k=random.randint(1, len(PaymentMethod))
                ),
                "typical_locations": random.sample(
                    range(len(self._generate_locations())),
                    k=random.randint(1, 3)
                )
            })
        return users
    
    def _generate_merchants(self, count: int) -> List[Dict[str, Any]]:
        """Generate a pool of merchants."""
        categories = [
            "food", "retail", "entertainment", "travel", 
            "services", "education", "healthcare", "tech"
        ]
        
        merchants = []
        for _ in range(count):
            merchant_id = f"merchant_{uuid.uuid4().hex[:8]}"
            merchants.append({
                "id": merchant_id,
                "category": random.choice(categories),
                "name": f"Business {merchant_id[-4:]}"
            })
        return merchants
    
    def _generate_locations(self) -> List[Dict[str, Any]]:
        """Generate a list of possible locations."""
        return [
            {"latitude": -23.5505, "longitude": -46.6333, "country": "Brazil", "city": "São Paulo"},
            {"latitude": -22.9068, "longitude": -43.1729, "country": "Brazil", "city": "Rio de Janeiro"},
            {"latitude": -19.9167, "longitude": -43.9345, "country": "Brazil", "city": "Belo Horizonte"},
            {"latitude": -30.0346, "longitude": -51.2177, "country": "Brazil", "city": "Porto Alegre"},
            {"latitude": -15.7797, "longitude": -47.9297, "country": "Brazil", "city": "Brasília"},
            {"latitude": -3.7172, "longitude": -38.5433, "country": "Brazil", "city": "Fortaleza"},
            {"latitude": -8.0476, "longitude": -34.8770, "country": "Brazil", "city": "Recife"},
            {"latitude": -12.9714, "longitude": -38.5014, "country": "Brazil", "city": "Salvador"},
            {"latitude": -25.4290, "longitude": -49.2671, "country": "Brazil", "city": "Curitiba"},
            {"latitude": -2.5297, "longitude": -44.3027, "country": "Brazil", "city": "São Luís"}
        ]
    
    def _generate_amount(self, is_fraud: bool, typical_range: tuple) -> float:
        """Generate transaction amount, potentially anomalous for fraud cases."""
        if is_fraud:
            # For fraud, sometimes generate unusually large amounts
            if random.random() < 0.7:
                return round(random.uniform(typical_range[1], typical_range[1] * 10), 2)
            else:
                return round(random.uniform(typical_range[0], typical_range[1]), 2)
        else:
            return round(random.uniform(typical_range[0], typical_range[1]), 2)
    
    def _select_payment_method(self, user: Dict[str, Any], is_fraud: bool) -> PaymentMethod:
        """Select payment method, potentially anomalous for fraud cases."""
        if is_fraud and random.random() < 0.8:
            # For fraud, often use an unusual payment method for this user
            all_methods = list(PaymentMethod)
            unusual_methods = [m for m in all_methods if m not in user["preferred_payment_methods"]]
            if unusual_methods:
                return random.choice(unusual_methods)
        
        # Normal case or fallback
        return random.choice(user["preferred_payment_methods"])
    
    def _select_location(self, user: Dict[str, Any], is_fraud: bool) -> Dict[str, Any]:
        """Select location, potentially anomalous for fraud cases."""
        all_locations = self._generate_locations()
        
        if is_fraud and random.random() < 0.8:
            # For fraud, often use an unusual location for this user
            typical_indices = user["typical_locations"]
            unusual_indices = [i for i in range(len(all_locations)) if i not in typical_indices]
            if unusual_indices:
                return all_locations[random.choice(unusual_indices)]
        
        # Normal case or fallback
        return all_locations[random.choice(user["typical_locations"])]
    
    def _calculate_fraud_score(self, transaction: TransactionData) -> float:
        """Calculate a fraud score based on various factors."""
        score = 0.0
        
        # Check for unusual amount
        user = next((u for u in self.users if u["id"] == transaction.user_id), None)
        if user:
            typical_low, typical_high = user["typical_amounts"]
            if transaction.amount > typical_high * 2:
                score += 0.3
            elif transaction.amount > typical_high:
                score += 0.1
                
            # Check for unusual payment method
            if transaction.payment_method not in [str(m) for m in user["preferred_payment_methods"]]:
                score += 0.2
                
            # Check for unusual location
            user_locations = [self._generate_locations()[i] for i in user["typical_locations"]]
            if not any(
                loc["city"] == transaction.geo_location["city"] 
                for loc in user_locations
            ):
                score += 0.3
                
        # Add some randomness
        score += random.uniform(0, 0.2)
        
        # Cap at 1.0
        return min(score, 1.0)
    
    def generate_transaction(self) -> TransactionData:
        """Generate a single transaction, some of which may be fraudulent."""
        # Determine if this will be a fraudulent transaction
        is_fraud = random.random() < self.fraud_probability
        
        # Select a random user
        user = random.choice(self.users)
        
        # Generate transaction data, with anomaly patterns for fraudulent ones
        amount = self._generate_amount(is_fraud, user["typical_amounts"])
        payment_method = self._select_payment_method(user, is_fraud)
        location = self._select_location(user, is_fraud)
        merchant = random.choice(self.merchants)
        
        # Create the transaction
        transaction = TransactionData(
            transaction_id=f"tx_{uuid.uuid4().hex}",
            user_id=user["id"],
            amount=amount,
            currency="BRL",
            payment_method=payment_method,
            status=TransactionStatus.APPROVED,
            device_type=random.choice(list(DeviceType)),
            timestamp=datetime.datetime.now().isoformat(),
            geo_location=location,
            merchant_id=merchant["id"],
            merchant_category=merchant["category"],
            description=f"Payment to {merchant['name']}"
        )
        
        # Calculate and assign fraud score
        fraud_score = self._calculate_fraud_score(transaction)
        transaction.fraud_score = fraud_score
        
        # Set as fraud if intended to be fraudulent or if score is very high
        transaction.is_fraud = is_fraud or fraud_score > 0.8
        
        return transaction
    
    def to_json(self, transaction: TransactionData) -> str:
        """Convert transaction to JSON string."""
        return json.dumps(asdict(transaction))


if __name__ == "__main__":
    # Simple test: generate and print 5 transactions
    generator = TransactionGenerator(fraud_probability=0.2)
    for _ in range(5):
        tx = generator.generate_transaction()
        print(generator.to_json(tx))
        print(f"Fraud: {tx.is_fraud}, Score: {tx.fraud_score:.2f}")
        print("-" * 50)