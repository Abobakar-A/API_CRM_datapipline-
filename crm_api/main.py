from fastapi import FastAPI
import json

# قم بإنشاء تطبيق FastAPI مع عنوان (title)
app = FastAPI(title="CRM Mock API")

# دالة مساعدة لتحميل ملفات JSON
def load_json(file_path):
    with open(file_path, "r") as f:
        return json.load(f)

# نقطة النهاية (Endpoint) لعرض قائمة العملاء
@app.get("/customers")
def get_customers():
    return load_json("data/customers.json")

# نقطة النهاية (Endpoint) لعرض قائمة المشتريات
@app.get("/purchases")
def get_purchases():
    return load_json("data/purchases.json")

# نقطة النهاية (Endpoint) لعرض عدد العملاء
@app.get("/customers/count")
def get_customer_count():
    customers = load_json("data/customers.json")
    return {"count": len(customers)}

# نقطة النهاية (Endpoint) لعرض عدد المشتريات
@app.get("/purchases/count")
def get_purchase_count():
    purchases = load_json("data/purchases.json")
    return {"count": len(purchases)}
# أضف هذا السطر مع بقية الـ Endpoints
@app.get("/products")
def get_products():
    return load_json("data/products.json")