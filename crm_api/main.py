from fastapi import FastAPI
import json

# قم بإنشاء تطبيق FastAPI مع عنوان (title)
app = FastAPI(title="CRM Mock API")

# دالة مساعدة لتحميل ملفات JSON
# هذا أفضل من تحميلها مباشرة في الكود الرئيسي لأنه يجعل الكود أكثر تنظيمًا وقابلية لإعادة الاستخدام.
def load_json(file_path):
    with open(file_path, "r") as f:
        return json.load(f)

# نقطة النهاية (Endpoint) لعرض قائمة العملاء
# هذا المسار (path) `/customers` سيقوم بإرجاع كل بيانات العملاء
@app.get("/customers")
def get_customers():
    return load_json("data/customers.json")

# نقطة النهاية (Endpoint) لعرض قائمة المشتريات
# هذا المسار (path) `/purchases` سيقوم بإرجاع كل بيانات المشتريات
@app.get("/purchases")
def get_purchases():
    return load_json("data/purchases.json")

# نقطة النهاية (Endpoint) لعرض عدد العملاء
# هذا المسار (path) `/customers/count` سيقوم بحساب عدد العملاء وإرجاعه
@app.get("/customers/count")
def get_customer_count():
    customers = load_json("data/customers.json")
    return {"count": len(customers)}

# نقطة النهاية (Endpoint) لعرض عدد المشتريات
# هذا المسار (path) `/purchases/count` سيقوم بحساب عدد المشتريات وإرجاعها
@app.get("/purchases/count")
def get_purchase_count():
    purchases = load_json("data/purchases.json")
    return {"count": len(purchases)}