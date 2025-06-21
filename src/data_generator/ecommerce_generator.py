# src/data_generator/ecommerce_generator.py
import random
import uuid
from datetime import datetime, timedelta
from faker import Faker
from typing import List, Dict
import pandas as pd
import json
from .schemas import Customer, Product, Transaction, UserEvent
import os
import yaml
class EcommerceDataGenerator:
    def __init__(self, seed: int = 42, config_path: str = None):
        self.fake = Faker()
        Faker.seed(seed)
        random.seed(seed)
        # Cargar configuración si existe
        self.config = {}
        if config_path and os.path.exists(config_path):
            with open(config_path, 'r') as f:
                self.config = yaml.safe_load(f)
        # Categorías de productos
        self.categories = {
            'Electronics': ['Smartphones', 'Laptops', 'Headphones', 'Cameras'],
            'Clothing': ['Shirts', 'Pants', 'Dresses', 'Shoes'],
            'Home': ['Furniture', 'Kitchenware', 'Bedding', 'Decor'],
            'Books': ['Fiction', 'Non-fiction', 'Educational', 'Comics'],
            'Sports': ['Equipment', 'Clothing', 'Accessories', 'Supplements']
        }
        
        self.brands = ['Apple', 'Samsung', 'Nike', 'Adidas', 'IKEA', 'Sony', 'Dell', 'HP']
        self.payment_methods = ['credit_card', 'debit_card', 'paypal', 'bank_transfer']
        self.event_types = ['page_view', 'add_to_cart', 'remove_from_cart', 'purchase', 'search']
        
    def generate_customers(self, count: int) -> List[Customer]:
        """Genera una lista de clientes falsos"""
        customers = []
        
        for _ in range(count):
            customer = Customer(
                customer_id=str(uuid.uuid4()),
                email=self.fake.email(),
                first_name=self.fake.first_name(),
                last_name=self.fake.last_name(),
                age=random.randint(18, 80),
                gender=random.choice(['M', 'F', 'Other']),
                city=self.fake.city(),
                country=self.fake.country(),
                registration_date=self.fake.date_time_between(
                    start_date='-2y', end_date='now'
                )
            )
            customers.append(customer)
            
        return customers
    
    def generate_products(self, count: int) -> List[Product]:
        """Genera una lista de productos falsos"""
        products = []
        
        for _ in range(count):
            category = random.choice(list(self.categories.keys()))
            subcategory = random.choice(self.categories[category])
            cost = round(random.uniform(5, 500), 2)
            price = round(cost * random.uniform(1.5, 3.0), 2)  # Margen de ganancia
            
            product = Product(
                product_id=str(uuid.uuid4()),
                name=f"{random.choice(self.brands)} {subcategory} {self.fake.word()}",
                category=category,
                subcategory=subcategory,
                brand=random.choice(self.brands),
                price=price,
                cost=cost,
                stock_quantity=random.randint(0, 1000)
            )
            products.append(product)
            
        return products
    
    def generate_transactions(self, customers: List[Customer], products: List[Product], 
                            count: int, start_date: datetime = None) -> List[Transaction]:
        """Genera transacciones basadas en clientes y productos existentes"""
        if start_date is None:
            start_date = datetime.now() - timedelta(days=30)
            
        transactions = []
        
        for _ in range(count):
            customer = random.choice(customers)
            product = random.choice(products)
            quantity = random.randint(1, 5)
            discount = round(random.uniform(0, 0.3), 2)  # Hasta 30% descuento
            
            unit_price = product.price
            subtotal = unit_price * quantity
            discount_amount = subtotal * discount
            total_amount = round(subtotal - discount_amount, 2)
            
            transaction = Transaction(
                transaction_id=str(uuid.uuid4()),
                customer_id=customer.customer_id,
                product_id=product.product_id,
                quantity=quantity,
                unit_price=unit_price,
                total_amount=total_amount,
                discount=discount,
                transaction_date=self.fake.date_time_between(
                    start_date=start_date, end_date='now'
                ),
                payment_method=random.choice(self.payment_methods),
                shipping_address=f"{self.fake.street_address()}, {self.fake.city()}"
            )
            transactions.append(transaction)
            
        return transactions
    
    def generate_user_events(self, customers: List[Customer], products: List[Product], 
                           count: int) -> List[UserEvent]:
        """Genera eventos de usuario en tiempo real"""
        events = []
        
        for _ in range(count):
            customer = random.choice(customers)
            product = random.choice(products) if random.random() > 0.3 else None
            
            event = UserEvent(
                event_id=str(uuid.uuid4()),
                customer_id=customer.customer_id,
                product_id=product.product_id if product else None,
                event_type=random.choice(self.event_types),
                event_timestamp=datetime.now(),
                session_id=str(uuid.uuid4())[:8],
                page_url=f"https://ecommerce.com/{random.choice(['home', 'products', 'cart', 'checkout'])}",
                user_agent=self.fake.user_agent()
            )
            events.append(event)
            
        return events
    
    def save_to_json(self, data: List, filename: str):
        with open(filename, 'w') as f:
            for item in data:
                json_line = json.dumps(item.to_dict(), default=str)
                f.write(json_line + '\n')

    
    def save_to_csv(self, data: List, filename: str):
        """Guarda datos en formato CSV"""
        df = pd.DataFrame([item.to_dict() for item in data])
        df.to_csv(filename, index=False)

    def save_data(self, data: List, path: str, fmt: str = "json"):
        if fmt == "json":
            self.save_to_json(data, path)
        elif fmt == "csv":
            self.save_to_csv(data, path)
        else:
            raise ValueError(f"Formato '{fmt}' no soportado")


