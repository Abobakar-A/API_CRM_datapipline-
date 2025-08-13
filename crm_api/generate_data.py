import json
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

# توليد العملاء
customers = []
for i in range(1, 101):
    customers.append({
        "customer_id": i,
        "name": fake.name(),
        "email": fake.email(),
        "region": random.choice(["Dubai", "Sharjah", "Abu Dhabi", "Ajman", "Fujairah"])
    })

# توليد المشتريات
purchases = []
for i in range(1, 501):
    customer_id = random.randint(1, 100)
    purchase_date = datetime.now() - timedelta(days=random.randint(0, 365))
    purchases.append({
        "purchase_id": i,
        "customer_id": customer_id,
        "amount": round(random.uniform(50, 1000), 2),
        "date": purchase_date.strftime("%Y-%m-%d")
    })

# حفظ الملفات
with open("data/customers.json", "w") as f:
    json.dump(customers, f, indent=2)

with open("data/purchases.json", "w") as f:
    json.dump(purchases, f, indent=2)

print("✅ تم توليد البيانات بنجاح")
