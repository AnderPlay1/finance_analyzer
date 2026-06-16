from decimal import Decimal
from datetime import date as py_date
import io

import pandas as pd
from sqlalchemy import and_, case, extract, func, select

from scripts.init_db import SessionLocal, Category, Transaction, User
from typing import Dict

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
    if age is None:
        return None

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
        if age_group is None or user.income is None or not user.region:
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
            user.income * Decimal("0.9"),   # type: ignore[operator]
            user.income * Decimal("1.1")),  # type: ignore[operator]
        User.region == user.region))
        ).scalars().all()
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
    categories = DEFAULT_CATEGORIES
    overspending = []
    for cat in categories:
        percentile = count_percentile_by_category(user_id, month, cat)

        if percentile > 50:
            reduction = float(category_spendings(user_id, month, cat)) * float(percentile-50) / 100
            if reduction > 0:
                overspending.append({
                    "category": cat,
                    "reduction": round(reduction, 2)
                })
    return overspending


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
                func.coalesce(func.sum(Transaction.amount), 0).label("cat_sum"),
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


def ensure_category_exists(category: str) -> None:
    """
    Добавляет категорию в каталог, если её ещё нет.
    :param category: str
    """
    name = (category or "").strip()[:50]
    if not name or name == "0":
        return

    session = next(get_db())
    try:
        exists = session.scalar(
            select(Category.id).where(Category.name == name)
        )
        if exists:
            return
        session.add(Category(name=name))
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
        ensure_category_exists(category)
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
        sync_category_catalog()
        return {"added": len(to_add), "errors": errors}
    except Exception:
        session.rollback()
        return {"added": 0, "errors": errors + ["Ошибка при сохранении в базу данных."]}
    finally:
        session.close()
