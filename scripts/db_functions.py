from decimal import Decimal

from sqlalchemy import and_, case, extract, func, select

from scripts.init_db import SessionLocal, Transaction, User
from typing import Dict


def get_db():
    """
    Получение сессии базы данных для выполнения операций.
    :return: Генератор, который предоставляет сессию базы данных
    :rtype: Generator[Session, None, None]
    """
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


def count_percentile(user_id, month) -> float:
    """
    Вычисляет процент пользователей, у которых траты меньше или 
    равны тратам данного пользователя, среди пользователей из той же возрастной группы, 
    с доходом в пределах ±10% от дохода данного пользователя 
    и из того же региона.
    :param user_id: int
    :return: float
    """
    session = next(get_db())
    try:
        group = get_user_group(user_id)

        if not group:
            return 0.0

        user_spendings = (
            session.scalar(
                select(func.sum(Transaction.amount)).where(
                    Transaction.user_id == user_id,
                    extract("month", Transaction.transaction_date) == month
                )
            )
            or 0
        )

        group_query = select(
            Transaction.user_id,
            func.sum(Transaction.amount).label('total')
        ).where(
                Transaction.user_id.in_(group),
                extract("month", Transaction.transaction_date) == month
        ).group_by(Transaction.user_id)

        spendings_map = {
            row.user_id: (row.total or 0)
            for row in session.execute(group_query).all()
        }

        below = sum(
            1 for uid in group
            if spendings_map.get(uid, 0) <= user_spendings
        )

        return round(below / len(group) * 100, 2)
    finally:
        session.close()


def count_percentile_by_category(user_id, month, category):
    session = next(get_db())
    try:

        group = get_user_group(user_id)

        if not group:
            return 0.0

        user_spendings = (
            session.scalar(
                select(func.sum(Transaction.amount)).where(
                    Transaction.user_id == user_id,
                    extract("month", Transaction.transaction_date) == month,
                    Transaction.category == category
                )
            )
            or 0
        )

        group_query = select(
            Transaction.user_id,
            func.sum(Transaction.amount).label('total')
        ).where(
                Transaction.user_id.in_(group),
                extract("month", Transaction.transaction_date) == month,
                Transaction.category == category
        ).group_by(Transaction.user_id)

        spendings_map = {
            row.user_id: (row.total or 0)
            for row in session.execute(group_query).all()
        }

        below = sum(
            1 for uid in group
            if spendings_map.get(uid, 0) <= user_spendings
        )

        return round(below / len(group) * 100, 2)
    finally:
        session.close()


def define_age_group(age) -> int:
    """
    Определяет возрастную группу по возрасту.
    :param age: int
    :return: int
    """
    if age <= 26:
        return 0
    if age <= 45:
        return 1
    if age <= 60:
        return 2
    return 3



def get_user_group(user_id):
    '''
    user_id пользоавтелей в том же регионе, возрастной группе 
    и в диапазоне ±10% от зарплаты
    '''   
    session = next(get_db())
    try:
        user = session.execute(select(User).where(User.user_id == user_id)).scalar_one()

        age_group = define_age_group(user.age)

        age_group_expr = case(
            (User.age <= 26, 0),
            (User.age <= 45, 1),
            (User.age <= 60, 2),
            else_=3
        ).label('age_group')
   
        return session.execute(select(User.user_id).where(and_(
            age_group_expr == age_group,
            User.income.between(
            user.income * Decimal("0.9"),   # type: ignore[operator]
            user.income * Decimal("1.1")),  # type: ignore[operator]
        User.region == user.region))
        ).scalars().all()
    finally:
        session.close()


def get_all_users() -> list[User]:
    """
    :return: list[User]
    """
    session = next(get_db())
    try:
        return session.query(User).all()
    finally:
        session.close()


def get_user_by_email(email: str) -> User | None:
    """
    Получает пользователя по email.
    :param email: str
    :return: User или None
    """
    session = next(get_db())
    try:
        return session.query(User).filter_by(email=email).first()
    finally:
        session.close()


def update_user_info(old_email: str, values: dict) -> bool:
    """
    Обновляет поля пользователя и сохраняет изменения в базе.
    :param old_email: str
    :param values: dict
    :return: bool
    """
    session = next(get_db())
    try:
        user = session.query(User).filter_by(email=old_email).first()
        if not user:
            return False

        new_email = values.get('email', '').strip()
        if new_email and new_email != old_email:
            existing = session.query(User).filter_by(email=new_email).first()
            if existing:
                return False
            user.email = new_email

        user.first_name = values.get('first_name', '').strip()
        user.last_name = values.get('last_name', '').strip()
        user.middle_name = values.get('middle_name', '').strip()
        user.region = values.get('region', '').strip()

        income_value = values.get('income', '').strip()
        if income_value:
            try:
                user.income = float(income_value)
            except Exception:
                return False
        else:
            user.income = 0.0

        session.commit()
        return True
    finally:
        session.close()


def add_user(response: dict) -> None:
    """
    Добавляет нового пользователя в базу данных.
    :param response: dict{name: str, surname: str, patronymic: str, email: str, password: str}
    :return: None
    """
    session = next(get_db())
    try:
        max_user_id = session.scalar(select(func.max(User.user_id))) or 0
        new_user = User(
            user_id=max_user_id + 1,
            first_name=response["name"],
            last_name=response["surname"],
            middle_name=response["patronymic"],
            email=response["email"],
            is_admin=False,
            confirmed=False,
            age=response.get("age"),
            income=response.get("income"),
            region=response.get("region"),
        )
        new_user.set_password(response["password"])
        session.add(new_user)
        session.commit()
    finally:
        session.close()


def is_confirmed(email: str) -> bool:
    """
    :param email: str
    :return: bool
    """
    session = next(get_db())
    try:
        user = session.query(User).filter_by(email=email).first()
        return bool(user and user.confirmed)
    finally:
        session.close()


def is_existing(email: str) -> bool:
    """
    Проверяет, существует ли пользователь с указанным email.
    :param email: str
    :return: bool
    """
    session = next(get_db())
    try:
        user = session.query(User).filter_by(email=email).first()
        return bool(user)
    finally:
        session.close()


def set_confirmed(email: str) -> None:
    """
    Устанавливает статус подтверждения для пользователя с указанным email.
    :param email: str
    :return: None
    """
    session = next(get_db())
    try:
        user = session.query(User).filter_by(email=email).first()
        if user:
            user.confirmed = True
            session.commit()
    finally:
        session.close()


def is_password_correct(email: str, password: str) -> bool:
    """
    Проверяет, правильный ли пароль для пользователя с указанным email.
    :param email: str
    :param password: str
    :return: bool
    """
    session = next(get_db())
    try:
        user = session.query(User).filter_by(email=email).first()
        return bool(user and user.check_password(password))
    finally:
        session.close()


def big_spending(user_id, month) -> list:
    categories = ['Транспорт', 'Ж/д билеты', 'Косметика', 'Рестораны', 'Различные товары', 'Образование', 'Авиабилеты', 'Металлы', 'Фото и видео', 'Местный транспорт', 'Турагентства', 'Животные', 'Азартные игры и лотереи', 'Одежда и обувь', 'Каршеринг', 'Автоуслуги', 'Маркетплейсы', 'Проценты', 'Госуслуги', 'Социальные сети', 'Наличные', 'Благотворительность', 'Эл. кошельки и переводы', 'Такси', 'Кино', 'Сетевой маркетинг', 'Зарядка электромобилей', 'Фастфуд', 'Развлечения', 'Duty Free', 'Красота', 'Детские товары', 'Онлайн-кинотеатры', 'Книги', 'Сервис', 'Отели', 'Другое', 'Частные услуги', 'Телевидение', 'Бонусы', 'Аренда авто', 'Экосистема Сбер', 'Музыка', 'НКО', 'Электроника и техника', 'Искусство', 'Телефония', 'Канцтовары', 'Платные дороги', 'Сувениры', 'Дом и ремонт', 'Интернет', 'Цифровые товары', 'Кредиты', 'Цветы', 'Соцвыплаты и пенсии', 'Финансы', 'Спорттовары', 'Дивиденды', 'ЖКХ', 'Экосистема Яндекс', 'Услуги банка']
    overspending = []
    for i in categories:
        if count_percentile_by_category(user_id, month, i) >= 50:
            overspending.append(i)
    return overspending


def categories_share(user_id, month) -> Dict:
    '''
    Функция для подсчёта доли каждой категории среди всех расходов в рамках одного месяца
    '''
    session = next(get_db())

    try:
        result = session.execute(
            select(
                Transaction.category,
                func.coalesce(func.sum(Transaction.amount), 0).label("cat_sum")
            )
            .where(
                Transaction.user_id == user_id,
                extract("month", Transaction.transaction_date) == month,
            )
            .group_by(Transaction.category)
        ).all()

        total = sum(r.cat_sum for r in result)

        shares: Dict[str, float] = {}
        if total == 0:
            return shares
        for r in result:
            shares[r.category] = round(r.cat_sum / total, 4)

        return shares

    finally:
        session.close()
