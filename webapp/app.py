from flask import Flask, render_template, request, jsonify

app = Flask(__name__)

# Пример каталога
PRODUCTS = [
    {"id": 1, "name": "Плагин A", "price": 100},
    {"id": 2, "name": "Плагин B", "price": 150},
    {"id": 3, "name": "Плагин C", "price": 200},
]

@app.route("/")
def index():
    return render_template("index.html", products=PRODUCTS)

@app.route("/buy", methods=["POST"])
def buy():
    data = request.json
    product_id = data.get("product_id")
    # Здесь можно добавить обработку покупки и интеграцию с ботом
    return jsonify({"status": "ok", "product_id": product_id})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
