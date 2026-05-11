import os
from dotenv import load_dotenv
from flask import Flask, flash, redirect, render_template, request, send_from_directory, session, url_for
from flask_mail import Mail, Message
from itsdangerous import BadTimeSignature, SignatureExpired, URLSafeTimedSerializer
from scripts.db_functions import (
    add_user,
    is_confirmed,
    is_existing,
    is_password_correct,
    set_confirmed,
)

load_dotenv()

template_dir = "graphics/templates"
static_dir = "graphics/static"
app = Flask(
    __name__,
    template_folder=template_dir,
    static_folder=static_dir,
    static_url_path="/static",
)


@app.route("/static/fonts/<path:filename>")
def serve_font(filename):
    """Отдаёт шрифты из новой папки graphics/fonts."""
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

# Инициализация сериализатора для токенов
serializer = URLSafeTimedSerializer(app.config['SECRET_KEY'])


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
    if "email" in session:
        user = 1
    else:
        user = 0
    return render_template('main.html', user=user)


@app.route('/dashboard')
@auth
def dashboard():
    """
    Страница профиля пользователя.
    :return: Рендеринг шаблона profile.html с данными пользователя
    """
    user = {
        "last_name": "Иванов",
        "first_name": "Иван",
        "middle_name": "Иванович",
        "email": session.get("email"),
        "id": "1",
        "avatar": None
    }
    return render_template('profile.html', user=user)


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

    return render_template('sign-up.html', user=1)


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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
