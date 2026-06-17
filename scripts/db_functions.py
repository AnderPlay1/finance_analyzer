from decimal import Decimal
from datetime import date as py_date
import io

import numpy as np
import pandas as pd
from sqlalchemy import and_, case, extract, func, select
from sklearn.linear_model import LinearRegression

from scripts.init_db import SessionLocal, Category, Transaction, User
from typing import Dict

REGION_MAP_FILE = 'data/regions_mapping.txt'


def _load_region_maps() -> tuple[dict[str, str], dict[str, str]]:
    code_by_label = {}
    label_by_code = {}
    try:
        with open(REGION_MAP_FILE, 'r', encoding='utf-8') as file:
            for line in file:
                parts = line.strip().split('|', 1)
                if len(parts) != 2:
                    continue
                code, label = parts
                code = code.strip()
                label = label.strip()
                code_by_label[label] = code
                label_by_code[code] = label
    except OSError:
        pass
    return code_by_label, label_by_code


REGION_CODE_BY_LABEL, REGION_LABEL_BY_CODE = _load_region_maps()


def normalize_region_code(region: str | None) -> str:
    region = (region or '').strip()
    if not region:
        return ''
    if region in REGION_CODE_BY_LABEL:
        return REGION_CODE_BY_LABEL[region]
    if region in REGION_LABEL_BY_CODE:
        return region

    upper_region = region.upper()
    if upper_region in REGION_CODE_BY_LABEL:
        return REGION_CODE_BY_LABEL[upper_region]
    if upper_region in REGION_LABEL_BY_CODE:
        return upper_region

    for label, code in REGION_CODE_BY_LABEL.items():
        if label.upper() == upper_region:
            return code
    return upper_region


def normalize_age(age) -> int | None:
    try:
        age_value = int(age)
    except (TypeError, ValueError):
        return None
    if age_value < 0:
        return None
    return age_value


def normalize_gender(gender: str | None) -> str:
    gender_value = (gender or '').strip().lower()
    gender_map = {
        'm': 'male',
        'male': 'male',
        'м': 'male',
        'мужской': 'male',
        'f': 'female',
        'female': 'female',
        'ж': 'female',
        'женский': 'female',
    }
    return gender_map.get(gender_value, '')


def normalize_income(income) -> float | None:
    try:
        income_value = float(str(income).strip().replace(',', '.'))
    except (TypeError, ValueError):
        return None

    if income_value < 0:
        return None

    # The imported comparison dataset stores monthly income in hundreds of rubles.
    if income_value > 10000:
        income_value = income_value / 100
    return round(income_value, 2)

DEFAULT_CATEGORIES = [
    'Транспорт', 'Ж/д билеты', 'Косметика', 'Рестораны', 'Различные товары',
    'Образование', 'Авиабилеты', 'Металлы', 'Фото и видео', 'Местный транспорт',
    'Турагентства', 'Животные', 'Азартные игры и лотереи', 'Одежда и обувь',
    'Каршеринг', 'Автоуслуги', 'Маркетплейсы', 'Проценты', 'Госуслуги',
    'Социальные сети', 'Наличные', 'Благотворительность', 'Эл. кошельки и переводы',
    'Такси', 'Кино', 'Сетевой маркетинг', 'Зарядка электромобилей', 'Фастфуд',
    'Развлечения', 'Duty Free', 'Красота', 'Детские товары', 'Онлайн-кинотеатры',
    'Книги', 'Сервис', 'Отели', 'Другое', 'Частные услуги', 'Телевидение',
    'Бонусы', 'Аренда авто', 'Экосистема Сбер', 'Музыка', 'НКО',
    'Электроника и техника', 'Искусство', 'Телефония', 'Канцтовары', 'Платные дороги',
    'Сувениры', 'Дом и ремонт', 'Интернет', 'Цифровые товары', 'Кредиты', 'Цветы',
    'Соцвыплаты и пенсии', 'Финансы', 'Спорттовары', 'Дивиденды', 'ЖКХ',
    'Экосистема Яндекс', 'Услуги банка',
]


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
    '''
    Вычисляет процент пользователей, у которых траты в категории меньше или 
    равны тратам данного пользователя, среди пользователей из той же возрастной группы, 
    с доходом в пределах ±10% от дохода данного пользователя 
    и из того же региона.
    :param user_id: int
    :param month: int
    :param category: str
    :return: float
    '''
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


def define_age_group(age) -> int | None:
    """
    Определяет возрастную группу по возрасту.
    :param age: int
    :return: int
    """
    age = normalize_age(age)
    if age is None:
        return None

    if age <= 26:
        return 0
    if age <= 45:
        return 1
    if age <= 60:
        return 2
    return 3


def _get_user_group(session, user_id):
    user = session.execute(select(User).where(
        User.user_id == user_id)).scalar_one()

    age_group = define_age_group(user.age)
    income = normalize_income(user.income)
    region = normalize_region_code(user.region)
    if age_group is None or income is None or not region:
        return []

    age_group_expr = case(
        (User.age <= 26, 0),
        (User.age <= 45, 1),
        (User.age <= 60, 2),
        else_=3
    ).label('age_group')

    return session.execute(select(User.user_id).where(and_(
        age_group_expr == age_group,
        User.income.between(
            Decimal(str(income)) * Decimal("0.9"),
            Decimal(str(income)) * Decimal("1.1")),
        User.region == region))
    ).scalars().all()


def get_user_group(user_id):
    '''
    user_id пользоавтелей в том же регионе, возрастной группе 
    и в диапазоне ±10% от зарплаты
    '''
    session = next(get_db())
    try:
        return _get_user_group(session, user_id)
    finally:
        session.close()


def get_spending_analytics(user_id: int, month: int) -> dict:
    """
    Быстрая сводка для страницы аналитики: считает группу, суммы,
    средние значения и персентили пачкой, без запроса на каждую категорию.
    """
    session = next(get_db())
    try:
        group_users = _get_user_group(session, user_id)

        user_total = (
            session.scalar(
                select(func.sum(Transaction.amount)).where(
                    Transaction.user_id == user_id,
                    extract("month", Transaction.transaction_date) == month,
                )
            )
            or 0
        )

        user_category_rows = session.execute(
            select(
                Transaction.category,
                func.sum(Transaction.amount).label('cat_total'),
            )
            .where(
                Transaction.user_id == user_id,
                extract("month", Transaction.transaction_date) == month,
            )
            .group_by(Transaction.category)
        ).all()

        user_categories = {
            row.category: (row.cat_total or 0)
            for row in user_category_rows
        }

        if not group_users:
            return {
                'group_users': [],
                'user_total': float(user_total),
                'group_avg_total': 0.0,
                'total_percentile': 0.0,
                'categories_data': [
                    {
                        'name': category,
                        'user_total': float(total),
                        'group_avg': 0.0,
                        'percentile': 0.0,
                    }
                    for category, total in user_categories.items()
                ],
            }

        group_total_rows = session.execute(
            select(
                Transaction.user_id,
                func.sum(Transaction.amount).label('total'),
            )
            .where(
                Transaction.user_id.in_(group_users),
                extract("month", Transaction.transaction_date) == month,
            )
            .group_by(Transaction.user_id)
        ).all()

        group_totals = {
            row.user_id: (row.total or 0)
            for row in group_total_rows
        }
        group_total_sum = sum(group_totals.get(uid, 0) for uid in group_users)
        group_avg_total = group_total_sum / len(group_users)
        total_below = sum(
            1 for uid in group_users
            if group_totals.get(uid, 0) <= user_total
        )
        total_percentile = round(total_below / len(group_users) * 100, 2)

        group_category_totals: dict[str, dict[int, float]] = {}
        if user_categories:
            category_names = list(user_categories.keys())
            group_category_rows = session.execute(
                select(
                    Transaction.category,
                    Transaction.user_id,
                    func.sum(Transaction.amount).label('cat_total'),
                )
                .where(
                    Transaction.user_id.in_(group_users),
                    extract("month", Transaction.transaction_date) == month,
                    Transaction.category.in_(category_names),
                )
                .group_by(Transaction.category, Transaction.user_id)
            ).all()

            for row in group_category_rows:
                group_category_totals.setdefault(row.category, {})[row.user_id] = (
                    row.cat_total or 0
                )

        categories_data = []
        for category, user_category_total in user_categories.items():
            category_totals = group_category_totals.get(category, {})
            group_category_sum = sum(
                category_totals.get(uid, 0) for uid in group_users
            )
            group_category_avg = group_category_sum / len(group_users)
            category_below = sum(
                1 for uid in group_users
                if category_totals.get(uid, 0) <= user_category_total
            )
            category_percentile = round(
                category_below / len(group_users) * 100, 2
            )

            categories_data.append({
                'name': category,
                'user_total': float(user_category_total),
                'group_avg': float(group_category_avg),
                'percentile': float(category_percentile),
            })

        return {
            'group_users': group_users,
            'user_total': float(user_total),
            'group_avg_total': float(group_avg_total),
            'total_percentile': float(total_percentile),
            'categories_data': categories_data,
        }
    finally:
        session.close()


def get_all_users() -> list[User]:
    """
    Получает всех пользователей из базы данных.
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
        user.gender = normalize_gender(values.get('gender', ''))
        user.region = normalize_region_code(values.get('region', ''))
        user.age = normalize_age(values.get('age'))
        income_value = normalize_income(values.get('income'))

        user.income = income_value if income_value is not None else 0.0

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
            gender=normalize_gender(response.get("gender")),
            age=normalize_age(response.get("age")),
            income=normalize_income(response.get("income")) or 0.0,
            region=normalize_region_code(response.get("region")),
        )
        new_user.set_password(response["password"])
        session.add(new_user)
        session.commit()
    finally:
        session.close()


def is_confirmed(email: str) -> bool:
    """
    Проверяет, подтверждён ли пользователь с указанным email.
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
    '''
    Функция для определения категорий, в которых пользователь тратит больше 50% своего дохода
    :param user_id: int
    :param month: int
    :return: list
    '''
    advice = get_big_spending_advice(user_id, month)
    return [
        {
            "category": item["category"],
            "reduction": item["recommended_cut"],
        }
        for item in advice["items"]
    ]


def get_big_spending_advice(user_id: int, month: int, goal_amount: float | None = None) -> dict:
    """
    Подбирает категории, где пользователю разумнее всего экономить для крупной цели.
    Использует персентили по группе и сохраняет старую формулу big_spending:
    если персентиль категории выше 50, рекомендуемое сокращение равно
    тратам в категории * (percentile - 50) / 100.
    """
    analytics = get_spending_analytics(user_id, month)
    items = []

    for category in analytics["categories_data"]:
        percentile = float(category["percentile"])
        user_total = float(category["user_total"])
        if percentile <= 50 or user_total <= 0:
            continue

        recommended_cut = round(user_total * (percentile - 50) / 100, 2)
        if recommended_cut <= 0:
            continue

        items.append({
            "category": category["name"],
            "current_spending": user_total,
            "group_avg": float(category["group_avg"]),
            "percentile": percentile,
            "recommended_cut": recommended_cut,
        })

    items.sort(key=lambda item: item["recommended_cut"], reverse=True)

    monthly_potential = round(
        sum(item["recommended_cut"] for item in items), 2
    )
    goal = float(goal_amount or 0)
    months_to_goal = None
    if goal > 0 and monthly_potential > 0:
        months_to_goal = int((goal + monthly_potential - 0.01) // monthly_potential)

    return {
        "items": items,
        "monthly_potential": monthly_potential,
        "annual_potential": round(monthly_potential * 12, 2),
        "months_to_goal": months_to_goal,
        "goal_amount": goal,
        "group_users": analytics["group_users"],
        "user_total": analytics["user_total"],
    }


def category_spendings(user_id, month, category) -> float:
    '''
    Подсчёт трат в категории за месяц
    :param user_id: int
    :param month: int
    :param category: str
    :return: float
    '''
    session = next(get_db())
    try:
        return session.scalar(
            select(func.sum(Transaction.amount)).
            where(Transaction.user_id == user_id,
                  extract("month", Transaction.transaction_date) == month,
                  Transaction.category == category)) or 0
    finally:
        session.close()


def get_monthly_spending_summary(user_id: int, month: int, year: int) -> dict:
    """
    Сводка трат пользователя за выбранный месяц: сумма и разбивка по категориям.
    :param user_id: int
    :param month: int
    :param year: int
    :return: dict с полями total и categories
    """
    session = next(get_db())
    try:
        result = session.execute(
            select(
                Transaction.category,
                func.coalesce(func.sum(Transaction.amount),
                              0).label("cat_sum"),
            )
            .where(
                Transaction.user_id == user_id,
                extract("month", Transaction.transaction_date) == month,
                extract("year", Transaction.transaction_date) == year,
            )
            .group_by(Transaction.category)
            .order_by(func.sum(Transaction.amount).desc())
        ).all()

        total = sum(r.cat_sum for r in result)
        categories = []
        for row in result:
            amount = float(row.cat_sum)
            share = round(amount / float(total) * 100, 1) if total > 0 else 0.0
            categories.append({
                "name": row.category,
                "amount": round(amount, 2),
                "share": share,
            })

        return {
            "total": round(float(total), 2),
            "categories": categories,
        }
    finally:
        session.close()


def get_yearly_spending_dynamics(user_id: int, year: int) -> list[dict]:
    """
    Возвращает динамику расходов пользователя по месяцам выбранного года.
    :param user_id: int
    :param year: int
    :return: list[dict] с номером месяца, суммой и количеством покупок
    """
    session = next(get_db())
    try:
        rows = session.execute(
            select(
                extract("month", Transaction.transaction_date).label("month"),
                func.coalesce(func.sum(Transaction.amount), 0).label("total"),
                func.count(Transaction.id).label("count"),
            )
            .where(
                Transaction.user_id == user_id,
                extract("year", Transaction.transaction_date) == year,
            )
            .group_by(extract("month", Transaction.transaction_date))
        ).all()

        totals_by_month = {
            int(row.month): {
                "amount": round(float(row.total), 2),
                "count": int(row.count),
            }
            for row in rows
        }

        return [
            {
                "month": month,
                "amount": totals_by_month.get(month, {}).get("amount", 0.0),
                "count": totals_by_month.get(month, {}).get("count", 0),
            }
            for month in range(1, 13)
        ]
    finally:
        session.close()


def get_spending_regression_analysis(user_id: int) -> dict:
    """
    Строит линейную регрессию по месячным расходам пользователя.
    Используются только месяцы, где были покупки: так модель не принимает
    отсутствие загруженной выписки за нулевые расходы.
    :param user_id: int
    :return: dict с прогнозом, наклоном и исходными точками
    """
    session = next(get_db())
    try:
        rows = session.execute(
            select(
                extract("year", Transaction.transaction_date).label("year"),
                extract("month", Transaction.transaction_date).label("month"),
                func.coalesce(func.sum(Transaction.amount), 0).label("total"),
            )
            .where(Transaction.user_id == user_id)
            .group_by(
                extract("year", Transaction.transaction_date),
                extract("month", Transaction.transaction_date),
            )
            .order_by(
                extract("year", Transaction.transaction_date),
                extract("month", Transaction.transaction_date),
            )
        ).all()
    finally:
        session.close()

    points = [
        {
            "year": int(row.year),
            "month": int(row.month),
            "amount": round(float(row.total), 2),
        }
        for row in rows
        if row.total and float(row.total) > 0
    ]

    if len(points) < 2:
        return {
            "ready": False,
            "points_count": len(points),
            "message": "Для линейной регрессии нужно минимум два месяца с тратами.",
            "history": points,
        }

    first_index = points[0]["year"] * 12 + points[0]["month"]
    x_values = np.array([
        [point["year"] * 12 + point["month"] - first_index]
        for point in points
    ])
    y_values = np.array([point["amount"] for point in points])

    model = LinearRegression()
    model.fit(x_values, y_values)

    fitted_values = model.predict(x_values)
    last_point = points[-1]
    next_month_index = last_point["year"] * 12 + last_point["month"] + 1
    next_offset = next_month_index - first_index
    forecast_amount = max(0.0, float(model.predict([[next_offset]])[0]))
    next_year = (next_month_index - 1) // 12
    next_month = (next_month_index - 1) % 12 + 1
    monthly_change = float(model.coef_[0])
    if monthly_change > 100:
        direction = "growing"
    elif monthly_change < -100:
        direction = "decreasing"
    else:
        direction = "stable"

    r2_score = None
    if len(points) > 2:
        r2_score = round(float(model.score(x_values, y_values)), 3)

    return {
        "ready": True,
        "points_count": len(points),
        "forecast_year": next_year,
        "forecast_month": next_month,
        "forecast_amount": round(forecast_amount, 2),
        "monthly_change": round(monthly_change, 2),
        "direction": direction,
        "r2_score": r2_score,
        "history": points,
        "fitted": [
            {
                "year": point["year"],
                "month": point["month"],
                "amount": round(max(0.0, float(value)), 2),
            }
            for point, value in zip(points, fitted_values)
        ],
    }


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


def sync_category_catalog() -> None:
    """
    Синхронизирует каталог categories со всеми уникальными категориями из transactions.
    """
    session = next(get_db())
    try:
        distinct_names = session.execute(
            select(Transaction.category)
            .where(
                Transaction.category.isnot(None),
                Transaction.category != "",
                Transaction.category != "0",
            )
            .group_by(Transaction.category)
            .order_by(Transaction.category)
        ).scalars().all()

        existing = set(
            session.execute(select(Category.name)).scalars().all()
        )
        for raw_name in distinct_names:
            name = str(raw_name).strip()[:50]
            if not name or name in existing:
                continue
            session.add(Category(name=name))
            existing.add(name)

        for name in DEFAULT_CATEGORIES:
            if name not in existing:
                session.add(Category(name=name))
                existing.add(name)

        session.commit()
    except Exception:
        session.rollback()
    finally:
        session.close()


def ensure_category_exists(category: str, owner_user_id: int | None = None) -> None:
    """
    Добавляет категорию в каталог, если её ещё нет.
    :param category: str
    :param owner_user_id: int | None
    """
    name = (category or "").strip()[:50]
    if not name or name == "0":
        return

    session = next(get_db())
    try:
        category_obj = session.execute(
            select(Category).where(Category.name == name)
        ).scalar_one_or_none()
        if category_obj:
            if (
                owner_user_id is not None
                and category_obj.owner_user_id is None
                and name not in DEFAULT_CATEGORIES
            ):
                category_obj.owner_user_id = owner_user_id
                session.commit()
            return
        session.add(
            Category(
                name=name,
                owner_user_id=(
                    owner_user_id if name not in DEFAULT_CATEGORIES else None
                ),
            )
        )
        session.commit()
    except Exception:
        session.rollback()
    finally:
        session.close()


def get_all_categories() -> list[str]:
    """
    Возвращает список категорий из таблицы categories.
    Перед чтением синхронизирует каталог с transactions.
    :return: list[str]
    """
    sync_category_catalog()

    session = next(get_db())
    try:
        return list(
            session.execute(
                select(Category.name).order_by(Category.name)
            ).scalars().all()
        )
    finally:
        session.close()


def get_user_transactions(
    user_id: int,
    limit: int = 30,
    offset: int = 0,
) -> list[dict]:
    """
    Возвращает последние транзакции пользователя для управления списком трат.
    :param user_id: int
    :param limit: int
    :param offset: int
    :return: list[dict]
    """
    session = next(get_db())
    try:
        rows = session.execute(
            select(Transaction)
            .where(Transaction.user_id == user_id)
            .order_by(Transaction.transaction_date.desc(), Transaction.id.desc())
            .limit(limit)
            .offset(offset)
        ).scalars().all()
        return [
            {
                "id": tx.id,
                "amount": round(float(tx.amount), 2),
                "category": tx.category,
                "transaction_date": tx.transaction_date,
            }
            for tx in rows
        ]
    finally:
        session.close()


def count_user_transactions(user_id: int) -> int:
    """
    Считает количество транзакций пользователя.
    :param user_id: int
    :return: int
    """
    session = next(get_db())
    try:
        return int(
            session.scalar(
                select(func.count(Transaction.id)).where(
                    Transaction.user_id == user_id,
                )
            )
            or 0
        )
    finally:
        session.close()


def delete_user_transaction(user_id: int, transaction_id: int) -> bool:
    """
    Удаляет транзакцию, только если она принадлежит пользователю.
    :param user_id: int
    :param transaction_id: int
    :return: bool
    """
    session = next(get_db())
    try:
        tx = session.execute(
            select(Transaction).where(
                Transaction.id == transaction_id,
                Transaction.user_id == user_id,
            )
        ).scalar_one_or_none()
        if not tx:
            return False
        session.delete(tx)
        session.commit()
        return True
    except Exception:
        session.rollback()
        return False
    finally:
        session.close()


def get_user_custom_categories(user_id: int) -> list[dict]:
    """
    Возвращает пользовательские категории и информацию, можно ли их удалить.
    Системные категории из DEFAULT_CATEGORIES не показываются.
    :param user_id: int
    :return: list[dict]
    """
    sync_category_catalog()
    session = next(get_db())
    try:
        categories = session.execute(
            select(Category)
            .where(~Category.name.in_(DEFAULT_CATEGORIES))
            .order_by(Category.name)
        ).scalars().all()
        usage_rows = session.execute(
            select(
                Transaction.category,
                func.count(Transaction.id).label("total_count"),
                func.coalesce(
                    func.sum(
                        case((Transaction.user_id == user_id, 1), else_=0)
                    ),
                    0,
                ).label("user_count"),
            )
            .where(Transaction.category.isnot(None))
            .group_by(Transaction.category)
        ).all()
        usage_by_category = {
            row.category: {
                "total": int(row.total_count or 0),
                "user": int(row.user_count or 0),
            }
            for row in usage_rows
        }

        result = []
        for category in categories:
            usage = usage_by_category.get(
                category.name,
                {"total": 0, "user": 0},
            )
            user_count = usage["user"]
            other_count = usage["total"] - user_count

            belongs_to_user = (
                category.owner_user_id == user_id
                or user_count > 0
                or (
                    category.owner_user_id is None
                    and user_count == 0
                    and other_count == 0
                )
            )
            if not belongs_to_user:
                continue

            result.append({
                "name": category.name,
                "transactions_count": int(user_count),
                "can_delete": user_count == 0 and other_count == 0,
            })
        return result
    finally:
        session.close()


def delete_user_category(user_id: int, category_name: str) -> str:
    """
    Удаляет пользовательскую категорию из каталога, если она не системная
    и больше не используется в транзакциях.
    :param user_id: int
    :param category_name: str
    :return: статус операции
    """
    name = (category_name or "").strip()[:50]
    if not name or name in DEFAULT_CATEGORIES:
        return "protected"

    session = next(get_db())
    try:
        category = session.execute(
            select(Category).where(Category.name == name)
        ).scalar_one_or_none()
        if not category:
            return "missing"

        user_count = session.scalar(
            select(func.count(Transaction.id)).where(
                Transaction.user_id == user_id,
                Transaction.category == name,
            )
        ) or 0
        total_count = session.scalar(
            select(func.count(Transaction.id)).where(Transaction.category == name)
        ) or 0

        if user_count > 0:
            return "has_user_transactions"
        if total_count > 0:
            return "used_by_others"
        if category.owner_user_id not in (None, user_id):
            return "forbidden"

        session.delete(category)
        session.commit()
        return "deleted"
    except Exception:
        session.rollback()
        return "error"
    finally:
        session.close()


def add_transaction(
    user_id: int,
    amount: float,
    category: str,
    transaction_date: py_date,
) -> bool:
    """
    Добавляет одну транзакцию пользователя.
    :param user_id: int
    :param amount: float
    :param category: str
    :param transaction_date: date
    :return: bool
    """
    category = (category or "").strip()
    if not category or amount <= 0:
        return False

    session = next(get_db())
    try:
        next_id = session.scalar(select(func.max(Transaction.id))) or 0
        session.add(
            Transaction(
                id=next_id + 1,
                user_id=user_id,
                amount=round(float(amount), 2),
                category=category[:50],
                transaction_date=transaction_date,
            )
        )
        session.commit()
        ensure_category_exists(category, user_id)
        return True
    except Exception:
        session.rollback()
        return False
    finally:
        session.close()


def _normalize_amount(value: str) -> float | None:
    """Преобразует строковое значение суммы в число."""
    cleaned = (
        str(value)
        .strip()
        .replace(" ", "")
        .replace(",", ".")
        .replace("—", "")
        .replace("None", "")
        .replace("nan", "")
    )
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _read_csv_dataframe(raw: bytes) -> pd.DataFrame | None:
    """Читает CSV из байтов, пробуя разные кодировки."""
    for encoding in ("utf-8-sig", "utf-8", "cp1251"):
        try:
            return pd.read_csv(io.BytesIO(raw), sep=";", encoding=encoding, dtype=str)
        except UnicodeDecodeError:
            continue
    return None


def _resolve_csv_columns(df: pd.DataFrame) -> tuple[str, str, str] | None:
    """Определяет колонки даты, категории и суммы в загруженном CSV."""
    columns = {c.strip().lower(): c for c in df.columns}

    tbank_date = columns.get("real_transaction_dttm")
    tbank_cat = columns.get("loyalty_cashback_category_nm")
    tbank_amt = columns.get("transaction_amt_rur")
    if tbank_date and tbank_cat and tbank_amt:
        return tbank_date, tbank_cat, tbank_amt

    date_aliases = ("date", "дата", "transaction_date")
    cat_aliases = ("category", "категория")
    amt_aliases = ("amount", "сумма", "transaction_amt_rur")

    date_col = next((columns[a] for a in date_aliases if a in columns), None)
    cat_col = next((columns[a] for a in cat_aliases if a in columns), None)
    amt_col = next((columns[a] for a in amt_aliases if a in columns), None)

    if date_col and cat_col and amt_col:
        return date_col, cat_col, amt_col

    if len(df.columns) >= 3:
        cols = list(df.columns)
        return cols[0], cols[1], cols[2]

    return None


def import_transactions_csv(user_id: int, raw: bytes) -> dict:
    """
    Импортирует транзакции пользователя из CSV-файла.
    :param user_id: int
    :param raw: bytes
    :return: dict с полями added и errors
    """
    df = _read_csv_dataframe(raw)
    if df is None:
        return {
            "added": 0,
            "errors": ["Не удалось прочитать файл. Используйте кодировку UTF-8 или CP1251."],
        }
    df = df.dropna(how='all')
    if df.empty:
        return {"added": 0, "errors": ["Файл не содержит данных."]}

    resolved = _resolve_csv_columns(df)
    if not resolved:
        return {
            "added": 0,
            "errors": [
                "Неизвестный формат CSV. Ожидаются колонки date;category;amount "
                "или формат выписки Т-банка."
            ],
        }

    date_col, cat_col, amt_col = resolved
    errors: list[str] = []
    to_add: list[Transaction] = []

    for idx, row in df.iterrows():
        row_num = int(idx) + 2
        amount = _normalize_amount(row[amt_col])
        if amount is None or amount <= 0:
            errors.append(f"Строка {row_num}: некорректная сумма")
            continue

        category = str(row[cat_col]).strip()
        if not category or category == "0":
            errors.append(f"Строка {row_num}: категория не указана")
            continue

        parsed_date = pd.to_datetime(row[date_col], errors="coerce")
        if pd.isna(parsed_date):
            errors.append(f"Строка {row_num}: некорректная дата")
            continue

        to_add.append(
            Transaction(
                user_id=user_id,
                amount=round(amount, 2),
                category=category[:50],
                transaction_date=parsed_date.date(),
            )
        )

    if not to_add:
        return {"added": 0, "errors": errors or ["Нет строк для импорта."]}

    session = next(get_db())
    try:
        next_id = session.scalar(select(func.max(Transaction.id))) or 0
        for tx in to_add:
            next_id += 1
            tx.id = next_id
            session.add(tx)
        session.commit()
        for category in {tx.category for tx in to_add}:
            ensure_category_exists(category, user_id)
        sync_category_catalog()
        return {"added": len(to_add), "errors": errors}
    except Exception:
        session.rollback()
        return {"added": 0, "errors": errors + ["Ошибка при сохранении в базу данных."]}
    finally:
        session.close()
