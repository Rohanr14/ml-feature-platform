"""Pydantic schemas for transaction events."""

from pydantic import BaseModel


class Transaction(BaseModel):
    transaction_id: str
    user_id: str
    amount: float
    currency: str
    category: str
    merchant_id: str
    timestamp: str
    is_anomaly: bool
    session_id: str
    device_type: str
    location_country: str
