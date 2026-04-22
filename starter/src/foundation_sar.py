from pydantic import BaseModel
from typing import List

class CustomerData(BaseModel):
    customer_id: str
    risk_rating: str

class TransactionData(BaseModel):
    transaction_id: str
    amount: float
``
