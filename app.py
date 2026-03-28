from flask import Flask, render_template_string, request, redirect, url_for

app = Flask(__name__)

products = [
    {'id': 1, 'name': 'TG Аккаунт #1', 'desc': 'Верифицированный, без спама', 'price': 100},
    {'id': 2, 'name': 'TG Аккаунт #2', 'desc': 'Старый, с историей', 'price': 150},
]

ADMIN_PASSWORD = 'admin123'

index_html = '''<!DOCTYPE html><html lang="ru"><head><meta charset="UTF-8"><title>Магазин TG Аккаунтов</title></head><body><h1>Магазин Telegram аккаунтов</h1><ul>{% for p in products %}<li><a href="/product/{{p.id}}">{{p.name}}</a> — {{p.price}} руб.</li>{% endfor %}</ul><a href="/admin">Админка</a></body></html>'''
product_html = '''<!DOCTYPE html><html lang="ru"><head><meta charset="UTF-8"><title>{{product.name}}</title></head><body><h1>{{product.name}}</h1><p>{{product.desc}}</p><p>Цена: {{product.price}} руб.</p><a href="/buy/{{product.id}}">Купить</a><br><a href="/">Назад</a></body></html>'''
admin_html = '''<!DOCTYPE html><html lang="ru"><head><meta charset="UTF-8"><title>Админка</title></head><body><h1>Админка</h1><form method="post"><input type="password" name="password" placeholder="Пароль" required><input type="text" name="name" placeholder="Название" required><input type="text" name="desc" placeholder="Описание"><input type="number" name="price" placeholder="Цена" required><button type="submit">Добавить</button></form><h2>Товары</h2><ul>{% for p in products %}<li>{{p.name}} — {{p.price}} руб.<form method="post" action="/admin/delete/{{p.id}}" style="display:inline;"><button type="submit">Удалить</button></form></li>{% endfor %}</ul><a href="/">На главную</a></body></html>'''

@app.route('/')
def index():
    return render_template_string(index_html, products=products)

@app.route('/product/<int:pid>')
def product(pid):
    prod = next((p for p in products if p['id'] == pid), None)
    if not prod:
        return 'Товар не найден', 404
    return render_template_string(product_html, product=prod)

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
    return render_template_string(admin_html, products=products)

@app.route('/admin/delete/<int:pid>', methods=['POST'])
def delete_product(pid):
    global products
    products = [p for p in products if p['id'] != pid]
    return redirect(url_for('admin'))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000)
