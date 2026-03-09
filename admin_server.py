from flask import Flask, request, jsonify, send_from_directory, render_template_string
import os
from db import (
    add_product, append_product_credentials_with_stock, deactivate_product, create_category, list_all_products_admin
)

app = Flask(__name__, static_folder=".")

@app.route("/")
def index():
    with open("index.html", "r", encoding="utf-8") as f:
        return f.read()

@app.route("/api/admin_products", methods=["GET"])
def admin_products():
    products = list_all_products_admin()
    return jsonify({
        "ok": True,
        "products": [
            {"id": p[0], "title": p[1], "category": p[2], "price": p[3], "stock": p[4]} for p in products
        ]
    })

@app.route("/api/admin_add_product", methods=["POST"])
def admin_add_product():
    data = request.json
    title = data.get("title", "").strip()
    category = data.get("category", "").strip()
    price = float(data.get("price", 0))
    stock = int(data.get("stock", 0))
    credentials = data.get("credentials", "").strip()
    description = data.get("description", "").strip()
    product_id = add_product(title, price, credentials, category, description, stock)
    return jsonify({"ok": True, "product_id": product_id})

@app.route("/api/admin_add_creds", methods=["POST"])
def admin_add_creds():
    data = request.json
    product_id = int(data.get("product_id", 0))
    credentials = data.get("credentials", "").strip()
    result = append_product_credentials_with_stock(product_id, credentials)
    return jsonify({"ok": bool(result)})

@app.route("/api/admin_deactivate_product", methods=["POST"])
def admin_deactivate_product():
    data = request.json
    product_id = int(data.get("product_id", 0))
    ok = deactivate_product(product_id)
    return jsonify({"ok": ok})

@app.route("/api/admin_add_category", methods=["POST"])
def admin_add_category():
    data = request.json
    slug = data.get("slug", "").strip()
    title = data.get("title", "").strip()
    ok, msg = create_category(slug, title)
    return jsonify({"ok": ok, "msg": msg})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
