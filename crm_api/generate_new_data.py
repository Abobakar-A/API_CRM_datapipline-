import json
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()

# توليد بيانات المنتجات
products = []
for i in range(1, 101):
    products.append({
        "product_id": i,
        "product_name": fake.word(),
        "category": random.choice(["Electronics", "Books", "Home Appliances", "Clothing"]),
        "price": round(random.uniform(10, 500), 2)
    })

# حفظ الملف
with open("data/products.json", "w") as f:
    json.dump(products, f, indent=2)

print("✅ تم توليد بيانات المنتجات بنجاح.")