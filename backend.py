import os
import json
from dotenv import load_dotenv
from flask import Flask, flash, redirect, render_template, request, send_from_directory, session, url_for
from flask_mail import Mail, Message
from itsdangerous import BadTimeSignature, SignatureExpired, URLSafeTimedSerializer
from datetime import datetime

from scripts.db_functions import (
    add_transaction,
    add_user,
    count_user_transactions,
    delete_user_category,
    delete_user_transaction,
    get_all_categories,
    get_big_spending_advice,
    get_monthly_spending_summary,
    get_spending_analytics,
    get_spending_regression_analysis,
    get_user_custom_categories,
    get_user_by_email,
    get_user_transactions,
    get_yearly_spending_dynamics,
    import_transactions_csv,
    is_confirmed,
    is_existing,
    is_password_correct,
    set_confirmed,
    sync_category_catalog,
    update_user_info,
)
from scripts.init_db import Base, engine, ensure_performance_indexes, ensure_schema_columns

MONTH_NAMES = [
    "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
    "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь",
]

REGION_MAP_FILE = 'data/regions_mapping.txt'
INTERFACE_SETTINGS_FILE = 'data/interface_settings.json'

DEFAULT_INTERFACE_SETTINGS = {
    "theme": "teal",
    "font": "system",
    "font_size": "normal",
}

THEME_PRESETS = {
    "teal": {
        "label": "Бирюзовая",
        "accent": "#0097a8",
        "accent_dark": "#007d8a",
        "background": "#fffaf4",
        "surface": "#ffffff",
        "text": "#2c3e50",
        "muted": "#7f8c8d",
        "navbar": "#292c2f",
    },
    "graphite": {
        "label": "Графитовая",
        "accent": "#607d8b",
        "accent_dark": "#455a64",
        "background": "#f4f6f7",
        "surface": "#ffffff",
        "text": "#263238",
        "muted": "#6f7f86",
        "navbar": "#1f2528",
    },
    "berry": {
        "label": "Ягодная",
        "accent": "#b23a6f",
        "accent_dark": "#8f2f59",
        "background": "#fff7fb",
        "surface": "#ffffff",
        "text": "#332832",
        "muted": "#7f6a78",
        "navbar": "#312832",
    },
}

FONT_PRESETS = {
    "system": {
        "label": "Системный",
        "family": "Arial, sans-serif",
    },
    "mono": {
        "label": "Моноширинный",
        "family": "'Roboto Mono', monospace",
    },
    "serif": {
        "label": "Классический",
        "family": "Georgia, 'Times New Roman', serif",
    },
}

FONT_SIZE_PRESETS = {
    "compact": {
        "label": "Компактный",
        "size": "13px",
    },
    "normal": {
        "label": "Обычный",
        "size": "14px",
    },
    "large": {
        "label": "Крупный",
        "size": "16px",
    },
}


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
        return REGION_CODE_BY_LABEL[region_value]
    if region_value in REGION_LABEL_BY_CODE:
        return region_value
    upper_value = region_value.upper()
    if upper_value in REGION_CODE_BY_LABEL:
        return REGION_CODE_BY_LABEL[upper_value]
    if upper_value in REGION_LABEL_BY_CODE:
        return upper_value
    for code, label in REGION_LABEL_BY_CODE.items():
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


def _load_interface_settings_store() -> dict:
    """
    Загружает пользовательские настройки интерфейса из JSON-конфига.
    :return: dict с настройками по email
    """
    try:
        with open(INTERFACE_SETTINGS_FILE, 'r', encoding='utf-8') as file:
            data = json.load(file)
            return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _save_interface_settings_store(settings_store: dict) -> None:
    """
    Сохраняет настройки интерфейса в конфигурационный файл.
    :param settings_store: dict с настройками по email
    :return: None
    """
    os.makedirs(os.path.dirname(INTERFACE_SETTINGS_FILE), exist_ok=True)
    with open(INTERFACE_SETTINGS_FILE, 'w', encoding='utf-8') as file:
        json.dump(settings_store, file, ensure_ascii=False, indent=2)


def _normalize_interface_settings(values: dict | None) -> dict:
    """
    Проверяет значения настроек и подставляет безопасные значения по умолчанию.
    :param values: dict или None
    :return: dict с нормализованными настройками
    """
    values = values or {}
    theme = values.get("theme", DEFAULT_INTERFACE_SETTINGS["theme"])
    font = values.get("font", DEFAULT_INTERFACE_SETTINGS["font"])
    font_size = values.get(
        "font_size", DEFAULT_INTERFACE_SETTINGS["font_size"])

    return {
        "theme": theme if theme in THEME_PRESETS else DEFAULT_INTERFACE_SETTINGS["theme"],
        "font": font if font in FONT_PRESETS else DEFAULT_INTERFACE_SETTINGS["font"],
        "font_size": (
            font_size
            if font_size in FONT_SIZE_PRESETS
            else DEFAULT_INTERFACE_SETTINGS["font_size"]
        ),
    }


def get_interface_settings(email: str | None) -> dict:
    """
    Возвращает настройки интерфейса для пользователя.
    :param email: str или None
    :return: dict
    """
    settings_store = _load_interface_settings_store()
    return _normalize_interface_settings(settings_store.get(email or ""))


def save_interface_settings(email: str, values: dict) -> dict:
    """
    Сохраняет настройки интерфейса для пользователя.
    :param email: str
    :param values: dict из формы
    :return: dict с сохранёнными настройками
    """
    normalized = _normalize_interface_settings(values)
    settings_store = _load_interface_settings_store()
    settings_store[email] = normalized
    _save_interface_settings_store(settings_store)
    return normalized


def build_interface_css(settings: dict) -> str:
    """
    Собирает CSS-переменные для выбранных настроек интерфейса.
    :param settings: dict
    :return: str
    """
    theme = THEME_PRESETS[settings["theme"]]
    font = FONT_PRESETS[settings["font"]]
    font_size = FONT_SIZE_PRESETS[settings["font_size"]]
    return (
        f"--ui-accent: {theme['accent']}; "
        f"--ui-accent-dark: {theme['accent_dark']}; "
        f"--ui-bg: {theme['background']}; "
        f"--ui-surface: {theme['surface']}; "
        f"--ui-text: {theme['text']}; "
        f"--ui-muted: {theme['muted']}; "
        f"--ui-navbar: {theme['navbar']}; "
        f"--ui-font-family: {font['family']}; "
        f"--ui-font-size: {font_size['size']};"
    )


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
app.config['SQLALCHEMY_DATABASE_URI'] = (
    os.getenv('DATABASE_URL') or os.getenv('SQLALCHEMY_DATABASE_URI')
)
if not app.config['SQLALCHEMY_DATABASE_URI']:
    raise RuntimeError(
        'DATABASE_URL или SQLALCHEMY_DATABASE_URI должны указывать на MySQL.'
    )
if not app.config['SQLALCHEMY_DATABASE_URI'].startswith('mysql'):
    raise RuntimeError('Проект по ТЗ работает только с MySQL.')
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
ensure_schema_columns()
ensure_performance_indexes()
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


@app.context_processor
def inject_interface_settings():
    """
    Передаёт в шаблоны текущие настройки интерфейса и CSS-переменные.
    :return: dict для Jinja
    """
    settings = get_interface_settings(session.get("email"))
    return {
        "interface_settings": settings,
        "interface_css": build_interface_css(settings),
    }


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
    yearly_dynamics = get_yearly_spending_dynamics(user.user_id, year)
    has_yearly_dynamics = any(item["amount"] > 0 for item in yearly_dynamics)
    regression_analysis = get_spending_regression_analysis(user.user_id)
    if regression_analysis.get("ready"):
        forecast_month = regression_analysis["forecast_month"]
        regression_analysis["forecast_period"] = (
            f"{MONTH_NAMES[forecast_month - 1]} "
            f"{regression_analysis['forecast_year']}"
        )
    year_options = list(range(now.year - 2, now.year + 1))

    return render_template(
        'main.html',
        user=True,
        summary=summary,
        yearly_dynamics=yearly_dynamics,
        has_yearly_dynamics=has_yearly_dynamics,
        regression_analysis=regression_analysis,
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
    transaction_page = request.args.get('page', type=int, default=1)
    if not transaction_page or transaction_page < 1:
        transaction_page = 1
    transactions_per_page = 30

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

    transactions_count = count_user_transactions(user.user_id)
    total_transaction_pages = max(
        1,
        (transactions_count + transactions_per_page - 1) // transactions_per_page,
    )
    if transaction_page > total_transaction_pages:
        transaction_page = total_transaction_pages
    transaction_offset = (transaction_page - 1) * transactions_per_page

    return render_template(
        'add.html',
        user=True,
        categories=categories,
        transactions=get_user_transactions(
            user.user_id,
            limit=transactions_per_page,
            offset=transaction_offset,
        ),
        transaction_page=transaction_page,
        total_transaction_pages=total_transaction_pages,
        user_categories=get_user_custom_categories(user.user_id),
    )


@app.route('/transactions/<int:transaction_id>/delete', methods=['POST'])
@auth
def delete_transaction(transaction_id):
    """
    Удаляет трату текущего пользователя.
    :param transaction_id: int
    :return: redirect на страницу добавления трат
    """
    user = get_user_by_email(session.get("email"))
    if not user:
        session.pop("email", None)
        return redirect(url_for('sign_in'))

    if delete_user_transaction(user.user_id, transaction_id):
        flash('Трата удалена.')
    else:
        flash('Не удалось удалить трату.')
    return redirect(url_for('add_transactions', tab='manage'))


@app.route('/categories/delete', methods=['POST'])
@auth
def delete_category():
    """
    Удаляет пользовательскую категорию, если она не используется в тратах.
    :return: redirect на страницу добавления трат
    """
    user = get_user_by_email(session.get("email"))
    if not user:
        session.pop("email", None)
        return redirect(url_for('sign_in'))

    category_name = request.form.get('category_name', '')
    result = delete_user_category(user.user_id, category_name)
    if result == "deleted":
        flash('Категория удалена.')
    elif result == "has_user_transactions":
        flash('Сначала удалите траты в этой категории, потом удалите категорию.')
    elif result == "used_by_others":
        flash('Категория используется другими пользователями и не может быть удалена.')
    elif result == "protected":
        flash('Системную категорию удалить нельзя.')
    else:
        flash('Не удалось удалить категорию.')
    return redirect(url_for('add_transactions', tab='manage'))


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
            'gender': request.form.get('gender', ''),
            'region': normalize_region(request.form.get('region', '')),
            'income': request.form.get('income', ''),
            'age': request.form.get('age', ''),
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


@app.route('/settings', methods=['GET', 'POST'])
@auth
def interface_settings():
    """
    Страница настройки интерфейса: тема, шрифт и размер текста.
    :return: Рендеринг settings.html
    """
    email = session.get("email")
    user_obj = get_user_by_email(email)
    if not user_obj:
        session.pop("email", None)
        return redirect(url_for('sign_in'))

    if request.method == 'POST':
        save_interface_settings(email, {
            "theme": request.form.get("theme", ""),
            "font": request.form.get("font", ""),
            "font_size": request.form.get("font_size", ""),
        })
        flash('Настройки интерфейса сохранены.')
        return redirect(url_for('interface_settings'))

    return render_template(
        'settings.html',
        user=True,
        settings=get_interface_settings(email),
        theme_presets=THEME_PRESETS,
        font_presets=FONT_PRESETS,
        font_size_presets=FONT_SIZE_PRESETS,
    )


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
@auth
def analytics():
    '''
    Страница аналитики расходов пользователя.
    :return: Рендеринг шаблона analytics.html с данными аналитики
    '''
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

    analytics_data = get_spending_analytics(user_id, selected_month)
    user_total = analytics_data['user_total']
    total_percentile = analytics_data['total_percentile']
    group_users = analytics_data['group_users']
    group_avg_total = analytics_data['group_avg_total']
    categories_data = analytics_data['categories_data']

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


@app.route('/saving-goal', methods=['GET', 'POST'])
@auth
def saving_goal():
    user = get_user_by_email(session.get("email"))
    if not user:
        session.pop("email", None)
        return redirect(url_for('sign_in'))

    now = datetime.now()
    if request.method == 'POST':
        goal_amount = request.form.get('goal_amount', '').strip()
        month = request.form.get('month', type=int, default=now.month)
        return redirect(url_for(
            'saving_goal',
            goal_amount=goal_amount,
            month=month,
        ))

    selected_month = request.args.get('month', type=int, default=now.month)
    if not selected_month or selected_month < 1 or selected_month > 12:
        selected_month = now.month

    raw_goal_amount = request.args.get('goal_amount', '').strip()
    goal_amount = 0.0
    goal_error = ''
    if raw_goal_amount:
        try:
            goal_amount = float(raw_goal_amount.replace(
                ' ', '').replace(',', '.'))
            if goal_amount <= 0:
                goal_error = 'Введите сумму цели больше нуля.'
                goal_amount = 0.0
        except ValueError:
            goal_error = 'Введите корректную сумму цели.'

    advice = None
    if goal_amount > 0:
        advice = get_big_spending_advice(
            user.user_id,
            selected_month,
            goal_amount,
        )

    return render_template(
        'saving-goal.html',
        user=user,
        selected_month=selected_month,
        month_names=MONTH_NAMES,
        goal_amount=goal_amount,
        raw_goal_amount=raw_goal_amount,
        goal_error=goal_error,
        advice=advice,
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
