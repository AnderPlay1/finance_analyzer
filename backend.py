import os
from dotenv import load_dotenv
from flask import Flask, flash, redirect, render_template, request, send_from_directory, session, url_for
from flask_mail import Mail, Message
from itsdangerous import BadTimeSignature, SignatureExpired, URLSafeTimedSerializer
from datetime import datetime
from sqlalchemy import extract, func, select

from scripts.db_functions import (
    add_transaction,
    add_user,
    count_percentile,
    count_percentile_by_category,
    get_all_categories,
    get_monthly_spending_summary,
    get_user_by_email,
    get_user_group,
    import_transactions_csv,
    is_confirmed,
    is_existing,
    is_password_correct,
    set_confirmed,
    sync_category_catalog,
    update_user_info,
)
from scripts.init_db import Base, engine, SessionLocal, Transaction

MONTH_NAMES = [
    "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
    "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь",
]

REGION_MAP_FILE = 'data/regions_mapping.txt'


def load_region_map():
    """Загружает отображение код-region -> человекочитаемое название."""
    mapping = []
    with open(REGION_MAP_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            parts = line.strip().split('|', 1)
            if len(parts) == 2:
                code, label = parts
                mapping.append((code.strip(), label.strip()))
    return mapping


REGION_MAP = load_region_map()
REGION_LABEL_BY_CODE = {code: label for code, label in REGION_MAP}
REGION_CODE_BY_LABEL = {label: code for code, label in REGION_MAP}


def load_regions():
    """Возвращает список пар (code, label) для шаблонов."""
    return REGION_MAP


def normalize_region(region_value: str) -> str:
    """
    Нормализует значение региона, пытаясь сопоставить его с известными кодами и названиями.
    :param region_value: str - Входное значение региона, которое может быть кодом
    или человекочитаемым названием
    :return: str - Нормализованное значение региона (код или человекочитаемое название) 
    или исходное значение, если сопоставление не удалось
    """
    region_value = (region_value or '').strip()
    if not region_value:
        return ''
    if region_value in REGION_CODE_BY_LABEL:
        return region_value
    if region_value in REGION_LABEL_BY_CODE:
        return REGION_LABEL_BY_CODE[region_value]
    upper_value = region_value.upper()
    if upper_value in REGION_CODE_BY_LABEL:
        return upper_value
    if upper_value in REGION_LABEL_BY_CODE:
        return REGION_LABEL_BY_CODE[upper_value]
    for label, code in REGION_LABEL_BY_CODE.items():
        if label.upper() == upper_value:
            return code
    return upper_value


def get_region_label(region_code: str) -> str:
    """
    Получает человекочитаемое название региона по его коду.
    :param region_code: str - Код региона
    :return: str - Человекочитаемое название региона или код, если название не найдено
    """
    return REGION_LABEL_BY_CODE.get((region_code or '').strip(), region_code or '')


load_dotenv()

TEMPLATE_DIR = "graphics/templates"
STATIC_DIR = "graphics/static"
app = Flask(
    __name__,
    template_folder=TEMPLATE_DIR,
    static_folder=STATIC_DIR,
    static_url_path="/static",
)


@app.route("/static/fonts/<path:filename>")
def serve_font(filename):
    """
    Отдаёт шрифты из новой папки graphics/fonts.
    :param filename: str - Имя файла шрифта
    :return: Ответ с файлом шрифта или 404, если файл не найден
    """
    return send_from_directory("graphics/fonts", filename)


app.config['SECRET_KEY'] = os.getenv('SECRET_KEY')
if not app.config['SECRET_KEY']:
    raise RuntimeError('SECRET_KEY environment variable is required')
app.secret_key = app.config['SECRET_KEY']
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv(
    'SQLALCHEMY_DATABASE_URI', 'sqlite:///site.db')
app.config['MAIL_SERVER'] = os.getenv('MAIL_SERVER', 'smtp.gmail.com')
app.config['MAIL_PORT'] = int(os.getenv('MAIL_PORT', '587'))
app.config['MAIL_USE_TLS'] = os.getenv(
    'MAIL_USE_TLS', 'True'
).lower() in ('1', 'true', 'yes')
app.config['MAIL_USERNAME'] = os.getenv('MAIL_USERNAME')
app.config['MAIL_PASSWORD'] = os.getenv('MAIL_PASSWORD')
app.config['MAIL_DEFAULT_SENDER'] = os.getenv(
    'MAIL_DEFAULT_SENDER', 'noreply@example.com')


mail = Mail(app)

Base.metadata.create_all(engine)
sync_category_catalog()

# Инициализация сериализатора для токенов
serializer = URLSafeTimedSerializer(app.config['SECRET_KEY'])


@app.template_filter('rub')
def format_rub(value):
    """Форматирует число в строку с разделителями тысяч и запятой как десятичным знаком."""
    try:
        num = float(value)
    except (TypeError, ValueError):
        return value
    formatted = f"{num:,.2f}".replace(
        ",", "X").replace(".", ",").replace("X", " ")
    return formatted


@app.before_request
def require_auth():
    """
    Проверка авторизации перед каждым запросом.
    Если пользователь не авторизован —
    редирект на /sign-up
    """

    allowed_routes = [
        'sign_up',
        'sign_in',
        'confirm_email',
        'static',
        'serve_font'
    ]

    if request.endpoint in allowed_routes:
        return

    if "email" not in session:
        return redirect(url_for('sign_in'))


@app.errorhandler(404)
def not_found_404(_error):
    """
    Обработчик ошибки 404 - Страница не найдена.
    :param _error: Исключение, вызвавшее ошибку 404
    :return: Рендеринг шаблона 404.html
    """
    return render_template("404.html", user=1)


@app.errorhandler(500)
def not_found_500(_error):
    """
    Обработчик ошибки 500 - Внутренняя ошибка сервера.
    :param _error: Исключение, вызвавшее ошибку 500
    :return: Рендеринг шаблона 500.html
    """
    return render_template("500.html", user=1)


def auth(route):
    """Декоратор для проверки аутентификации пользователя перед доступом к маршруту."""

    def inner(*args, **kwargs):
        """
        Внутренняя функция, которая выполняет проверку аутентификации.
        :param args: Позиционные аргументы для маршрута
        :param kwargs: Именованные аргументы для маршрута
        """
        if "email" in session:
            return route(*args, **kwargs)
        return redirect("/sign-in/")

    inner.__name__ = route.__name__
    return inner


@app.route('/')
def index():
    """
    Главная страница сайта.
    :return: Рендеринг шаблона main.html
    """
    if "email" not in session:
        return redirect(url_for('sign_in'))

    user = get_user_by_email(session.get("email"))
    if not user:
        session.pop("email", None)
        return redirect(url_for('sign_in'))

    now = datetime.now()
    month = request.args.get('month', type=int, default=now.month)
    year = request.args.get('year', type=int, default=now.year)

    if not month or month < 1 or month > 12:
        month = now.month
    if not year or year < 2000 or year > 2100:
        year = now.year

    summary = get_monthly_spending_summary(user.user_id, month, year)
    year_options = list(range(now.year - 2, now.year + 1))

    return render_template(
        'main.html',
        user=True,
        summary=summary,
        selected_month=month,
        selected_year=year,
        month_names=MONTH_NAMES,
        year_options=year_options,
    )


@app.route('/add', methods=['GET', 'POST'])
@auth
def add_transactions():
    """
    Страница добавления трат (ручной ввод и загрузка CSV).
    :return: Рендеринг шаблона add.html
    """
    user = get_user_by_email(session.get("email"))
    if not user:
        session.pop("email", None)
        return redirect(url_for('sign_in'))

    categories = get_all_categories()

    if request.method == 'POST':
        mode = request.form.get('mode', '')

        if mode == 'manual':
            amount_raw = request.form.get(
                'amount', '').strip().replace(',', '.')
            category = request.form.get('category', '').strip()
            custom_category = request.form.get('custom_category', '').strip()
            date_raw = request.form.get('transaction_date', '').strip()

            if custom_category:
                category = custom_category

            try:
                amount = float(amount_raw)
            except ValueError:
                flash('Укажите корректную сумму.')
                return redirect(url_for('add_transactions'))

            if amount <= 0:
                flash('Сумма должна быть больше нуля.')
                return redirect(url_for('add_transactions'))

            if not category:
                flash('Выберите категорию или введите свою.')
                return redirect(url_for('add_transactions'))

            try:
                transaction_date = datetime.strptime(
                    date_raw, '%Y-%m-%d').date()
            except ValueError:
                flash('Укажите корректную дату.')
                return redirect(url_for('add_transactions'))

            if add_transaction(user.user_id, amount, category, transaction_date):
                flash(f'Трата «{category}» на сумму {amount:.2f} ₽ добавлена.')
                return redirect(url_for('index'))
            flash('Не удалось сохранить трату. Попробуйте ещё раз.')
            return redirect(url_for('add_transactions'))

        if mode == 'csv':
            uploaded = request.files.get('csv_file')
            if not uploaded or not uploaded.filename:
                flash('Выберите CSV-файл для загрузки.')
                return redirect(url_for('add_transactions'))

            if not uploaded.filename.lower().endswith('.csv'):
                flash('Файл должен иметь расширение .csv')
                return redirect(url_for('add_transactions'))

            result = import_transactions_csv(user.user_id, uploaded.read())
            added = result.get('added', 0)
            errors = result.get('errors', [])

            if added > 0:
                flash(f'Импортировано транзакций: {added}.')
                if errors:
                    flash(f'Пропущено строк с ошибками: {len(errors)}.')
                return redirect(url_for('index'))

            if errors:
                flash(errors[0])
            else:
                flash('Не удалось импортировать транзакции.')
            return redirect(url_for('add_transactions'))

    return render_template('add.html', user=True, categories=categories)


@app.route('/dashboard', methods=['GET', 'POST'])
@auth
def dashboard():
    """
    Страница профиля пользователя.
    :return: Рендеринг шаблона profile.html с данными пользователя
    """
    user = get_user_by_email(session.get("email"))
    if not user:
        session.pop("email", None)
        return redirect(url_for('sign_in'))

    if request.method == 'POST':
        form_email = request.form.get('email', '').strip()
        full_name = request.form.get('full_name', '').strip()
        if full_name:
            parts = full_name.split()
            last_name = parts[0] if len(parts) > 0 else ''
            first_name = parts[1] if len(parts) > 1 else ''
            middle_name = ' '.join(parts[2:]) if len(parts) > 2 else ''
        else:
            last_name = request.form.get('last_name', '')
            first_name = request.form.get('first_name', '')
            middle_name = request.form.get('middle_name', '')
        updated = update_user_info(session["email"], {
            'first_name': first_name,
            'last_name': last_name,
            'middle_name': middle_name,
            'email': form_email,
            'region': normalize_region(request.form.get('region', '')),
            'income': request.form.get('income', ''),
        })

        if not updated:
            flash(
                '''Не удалось сохранить данные. Возможно, email уже занят 
                или введены некорректные значения.'''
            )
            return redirect(url_for('dashboard'))

        session["email"] = form_email
        flash('Данные профиля успешно сохранены.')
        return redirect(url_for('dashboard'))

    setattr(user, 'avatar', None)
    region_label = get_region_label(user.region)
    setattr(user, 'region_label', region_label)
    return render_template('profile.html', user=user, regions=load_regions())


@app.route('/sign-in', methods=['GET', 'POST'])
def sign_in():
    """
    Страница входа для пользователей.
    :return: Рендеринг шаблона sign-in.html или перенаправление на главную страницу входа
    """
    if request.method == 'POST':
        email = request.form['email']
        password = request.form['password']
        if is_existing(email):
            if is_password_correct(email, password):
                session["email"] = email
                return redirect(url_for('index'))
            else:
                flash('Неправильный пароль')
                return redirect(url_for('sign_in'))
        else:
            flash('Неправильный логин или пароль')
            return redirect(url_for('sign_in'))

    return render_template('sign-in.html')


@app.route('/sign-up', methods=['GET', 'POST'])
def sign_up():
    """
    Страница регистрации для новых пользователей.
    :return: Рендеринг шаблона sign-up.html или перенаправление
    """
    if request.method == 'POST':
        email = request.form['email']
        if is_existing(email):
            flash('Электронная почта уже зарегистрирована')
            return redirect(url_for('sign_up'))
        # Сделать проверку, вдруг пользователь зарегистрирован
        add_user(request.form)
        # Генерация токена подтверждения
        token = serializer.dumps(email, salt='email-confirm')

        # Отправка письма
        msg = Message('Подтверждение регистрации на сайте', recipients=[email])
        msg.sender = app.config['MAIL_USERNAME']
        confirm_url = url_for('confirm_email', token=token, _external=True)
        msg.html = render_template(
            'email_confirmation.html', confirm_url=confirm_url)
        try:
            mail.send(msg)
            flash('Письмо с подтверждением отправлено на вашу почту')
        except Exception as e:
            print(f"Error sending email: {e}")
            flash('Регистрация успешна, но письмо подтверждения не удалось отправить.')

    return render_template('sign-up.html', user=1, regions=load_regions())


@app.route('/confirm/<token>')
def confirm_email(token):
    """
    Маршрут для подтверждения электронной почты пользователя.
    :param token: str - Токен подтверждения, полученный из ссылки в пись
    """
    try:
        # Проверка токена с сроком действия 24 часа
        email = serializer.loads(token, salt='email-confirm', max_age=86400)
    except (SignatureExpired, BadTimeSignature):
        flash('Ссылка подтверждения истекла или неверна')
        return redirect(url_for('sign_in'))

    if is_existing(email):
        if is_confirmed(email):
            flash('Аккаунт уже подтвержден')
        else:
            set_confirmed(email)
            flash('Аккаунт успешно подтвержден!')
    else:
        flash('Пользователь не найден')

    return redirect(url_for('sign_in'))


@app.route("/logout/")
def logout():
    """
    Маршрут для выхода пользователя из системы.
    :return: Перенаправление на главную страницу после выхода
    """
    session.pop("email", None)
    return redirect("/")


@app.route('/analytics')
def analytics():
    email = session.get("email")
    if not email:
        return redirect(url_for('sign_in'))

    user = get_user_by_email(email)
    if not user:
        session.pop("email", None)
        return redirect(url_for('sign_in'))

    user_id = user.user_id
    selected_month = request.args.get(
        'month', type=int, default=datetime.now().month)
    if not selected_month or selected_month < 1 or selected_month > 12:
        selected_month = datetime.now().month

    db_session = SessionLocal()
    try:
        user_total = db_session.execute(
            select(func.sum(Transaction.amount)).where(
                Transaction.user_id == user_id,
                extract("month", Transaction.transaction_date) == selected_month,
            )
        ).scalar_one_or_none() or 0.0

        total_percentile = count_percentile(user_id, selected_month)
        group_users = get_user_group(user_id)

        group_avg_total = 0.0
        if group_users:
            group_total_sum = db_session.execute(
                select(func.sum(Transaction.amount)).where(
                    Transaction.user_id.in_(group_users),
                    extract(
                        "month", Transaction.transaction_date) == selected_month,
                )
            ).scalar_one_or_none() or 0.0
            group_avg_total = group_total_sum / len(group_users)

        user_categories_query = db_session.execute(
            select(
                Transaction.category,
                func.sum(Transaction.amount).label('cat_total'),
            )
            .where(
                Transaction.user_id == user_id,
                extract("month", Transaction.transaction_date) == selected_month,
            )
            .group_by(Transaction.category)
        ).all()

        categories_data = []
        for cat_row in user_categories_query:
            cat_name = cat_row.category
            cat_user_total = cat_row.cat_total or 0.0

            cat_group_avg = 0.0
            if group_users:
                cat_group_sum = db_session.execute(
                    select(func.sum(Transaction.amount)).where(
                        Transaction.user_id.in_(group_users),
                        extract(
                            "month", Transaction.transaction_date) == selected_month,
                        Transaction.category == cat_name,
                    )
                ).scalar_one_or_none() or 0.0
                cat_group_avg = cat_group_sum / len(group_users)

            categories_data.append({
                'name': cat_name,
                'user_total': float(cat_user_total),
                'group_avg': float(cat_group_avg),
                'percentile': float(count_percentile_by_category(
                    user_id, selected_month, cat_name)),
            })
    finally:
        db_session.close()

    categories_data.sort(key=lambda x: x['user_total'], reverse=True)

    tips = []
    if not group_users:
        tips.append({
            "type": "warning",
            "text": "Для сравнения с похожей группой заполните в профиле возраст, доход и регион. Пока показаны только ваши расходы за месяц.",
        })
    elif total_percentile > 75:
        tips.append({
            "type": "danger",
            "text": "Общие расходы заметно выше уровня похожей группы. Начните с двух самых крупных категорий и задайте для них месячный лимит.",
        })
    elif total_percentile < 25 and float(user_total) > 0:
        tips.append({
            "type": "success",
            "text": "Общие расходы ниже большинства похожих пользователей. Это хороший уровень контроля, его стоит поддерживать.",
        })

    if group_users:
        for cat in categories_data:
            if cat['percentile'] >= 80:
                diff = max(cat['user_total'] - cat['group_avg'], 0)
                tips.append({
                    "type": "warning",
                    "text": (
                        f"Категория «{cat['name']}» находится в высоком персентиле: "
                        f"выше или равна расходам {cat['percentile']}% группы. "
                        f"Потенциал экономии относительно среднего: {diff:,.2f} ₽."
                    ),
                })
            elif cat['percentile'] <= 20:
                tips.append({
                    "type": "success",
                    "text": (
                        f"В категории «{cat['name']}» расходы ниже большинства группы. "
                        "Это удачная зона контроля, ее можно не трогать в первую очередь."
                    ),
                })

    return render_template(
        'analytics.html',
        user=user,
        user_total=float(user_total),
        total_percentile=float(total_percentile),
        group_avg_total=float(group_avg_total),
        categories_data=categories_data,
        tips=tips,
        selected_month=selected_month,
        month_names=MONTH_NAMES,
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
