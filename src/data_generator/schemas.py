# src/data_generator/schemas.py
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List
import json

@dataclass
class Customer:
    customer_id: str
    email: str
    first_name: str
    last_name: str
    age: int
    gender: str
    city: str
    country: str
    registration_date: datetime
    
    def to_dict(self):
        return {
            'customer_id': self.customer_id,
            'email': self.email,
            'first_name': self.first_name,
            'last_name': self.last_name,
            'age': self.age,
            'gender': self.gender,
            'city': self.city,
            'country': self.country,
            'registration_date': self.registration_date.isoformat()
        }

@dataclass
class Product:
    product_id: str
    name: str
    category: str
    subcategory: str
    brand: str
    price: float
    cost: float
    stock_quantity: int
    
    def to_dict(self):
        return {
            'product_id': self.product_id,
            'name': self.name,
            'category': self.category,
            'subcategory': self.subcategory,
            'brand': self.brand,
            'price': self.price,
            'cost': self.cost,
            'stock_quantity': self.stock_quantity
        }

@dataclass
class Transaction:
    transaction_id: str
    customer_id: str
    product_id: str
    quantity: int
    unit_price: float
    total_amount: float
    discount: float
    transaction_date: datetime
    payment_method: str
    shipping_address: str
    
    def to_dict(self):
        return {
            'transaction_id': self.transaction_id,
            'customer_id': self.customer_id,
            'product_id': self.product_id,
            'quantity': self.quantity,
            'unit_price': self.unit_price,
            'total_amount': self.total_amount,
            'discount': self.discount,
            'transaction_date': self.transaction_date.isoformat(),
            'payment_method': self.payment_method,
            'shipping_address': self.shipping_address
        }

@dataclass
class UserEvent:
    event_id: str
    customer_id: str
    product_id: Optional[str]
    event_type: str  # 'page_view', 'add_to_cart', 'purchase', 'search'
    event_timestamp: datetime
    session_id: str
    page_url: str
    user_agent: str
    
    def to_dict(self):
        return {
            'event_id': self.event_id,
            'customer_id': self.customer_id,
            'product_id': self.product_id,
            'event_type': self.event_type,
            'event_timestamp': self.event_timestamp.isoformat(),
            'session_id': self.session_id,
            'page_url': self.page_url,
            'user_agent': self.user_agent
        }
