import os
from src.data_generator.ecommerce_generator import EcommerceDataGenerator

os.makedirs('data', exist_ok=True)

if __name__ == "__main__":
    generator = EcommerceDataGenerator()
    
    print("🔄 Generando clientes...")
    customers = generator.generate_customers(1000)

    print("🔄 Generando productos...")
    products = generator.generate_products(500)

    print("🔄 Generando transacciones...")
    transactions = generator.generate_transactions(customers, products, 10000)

    print("💾 Guardando archivos...")
    generator.save_data(customers, 'data/raw/customers.json', 'json')
    generator.save_data(products, 'data/raw/products.json', 'json')
    generator.save_data(transactions, 'data/raw/transactions.json', 'json')

    generator.save_data(customers, 'data/raw/customers.csv', 'csv')
    generator.save_data(products, 'data/raw/products.csv', 'csv')
    generator.save_data(transactions, 'data/raw/transactions.csv', 'csv')

    print("✅ ¡Datos generados y guardados exitosamente!")
