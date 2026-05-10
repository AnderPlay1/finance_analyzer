from sqlalchemy import create_engine, select, func, and_, case
from sqlalchemy.orm import sessionmaker
from os import getenv
from dotenv import find_dotenv, load_dotenv
from init_db import User, Transaction
from decimal import Decimal

load_dotenv(dotenv_path=find_dotenv(".env"), override=False)

DATABASE_URL = str(getenv("DATABASE_URL"))

engine = create_engine(DATABASE_URL, echo=False)

SessionLocal = sessionmaker(bind=engine)

db = SessionLocal()

# функция подсчёта перцентиля пользователя
def count_percentile(user_id) -> float:
    user = db.execute(select(User).where(User.user_id == user_id)).scalar_one()
    age_group = define_age_group(user.age)

    age_group_expr = case(
        (User.age <= 26, 0),
        (User.age <= 35, 1),
        (User.age <= 45, 2),
        else_=3
    ).label('age_group')

    # пользоавтели в том же регионе, возрастной группе и в диапазоне ±10% от зарплаты
    group = db.execute(select(User).where(and_(
        age_group_expr == age_group,
        User.income.between(
            user.income * Decimal("0.9"),   # type: ignore[operator]
            user.income * Decimal("1.1")),  # type: ignore[operator]
        User.region == user.region))
    ).scalars().all()
    
    user_spendings = db.scalar(
            select(func.sum(Transaction.amount))
            .where(Transaction.user_id == user_id)) or 0

    user_ids = [u.user_id for u in group]

    spendings_map = {
        row.user_id: (row.total or 0)
        for row in db.execute(
            select(
                Transaction.user_id,
                func.sum(Transaction.amount).label('total')
            )
            .where(Transaction.user_id.in_(user_ids))
            .group_by(Transaction.user_id)
        ).all()
    }

    below = 0
    for member in group:
        member_spendings = spendings_map.get(member.user_id, 0)
        if member_spendings <= user_spendings: below += 1

    return round(below / len(group) * 100, 2)

# разбиение по возрастным группам
def define_age_group(age) -> int:
    if age <= 26: return 0
    if age <= 35: return 1
    if age <= 45: return 2
    return 3
