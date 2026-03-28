from flask import Flask, render_template, request, redirect, url_for

app = Flask(__name__)

# Пример товаров (Telegram аккаунты)
products = [
    {'id': 1, 'name': 'TG Аккаунт #1', 'desc': 'Верифицированный, без спама', 'price': 100},
    {'id': 2, 'name': 'TG Аккаунт #2', 'desc': 'Старый, с историей', 'price': 150},
]

ADMIN_PASSWORD = 'admin123'  # Задай свой пароль!

@app.route('/')
def index():
    return render_template('index.html', products=products)

@app.route('/product/<int:pid>')
def product(pid):
    prod = next((p for p in products if p['id'] == pid), None)
    if not prod:
        return 'Товар не найден', 404
    return render_template('product.html', product=prod)

@app.route('/buy/<int:pid>')
def buy(pid):
    prod = next((p for p in products if p['id'] == pid), None)
    if not prod:
        return 'Товар не найден', 404
    return f'Спасибо за покупку: {prod["name"]}! (оплата не реализована)'

@app.route('/admin', methods=['GET', 'POST'])
def admin():
    if request.method == 'POST':
        if request.form.get('password') != ADMIN_PASSWORD:
            return 'Неверный пароль!'
        name = request.form.get('name')
        desc = request.form.get('desc')
        price = request.form.get('price')
        if name and price:
            products.append({'id': len(products)+1, 'name': name, 'desc': desc, 'price': int(price)})
    return render_template('admin.html', products=products)

@app.route('/admin/delete/<int:pid>', methods=['POST'])
def delete_product(pid):
    global products
    products = [p for p in products if p['id'] != pid]
    return redirect(url_for('admin'))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000)
