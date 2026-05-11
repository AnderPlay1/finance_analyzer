from decimal import Decimal

from sqlalchemy import and_, case, func, select

from scripts.init_db import SessionLocal, Transaction, User


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


#
def count_percentile(user_id) -> float:
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
        user = session.execute(select(User).where(
            User.user_id == user_id)).scalar_one()
        age_group = define_age_group(user.age)

        age_group_expr = case(
            (User.age <= 26, 0),
            (User.age <= 35, 1),
            (User.age <= 45, 2),
            else_=3,
        ).label("age_group")

        group = (
            session.execute(
                select(User).where(
                    and_(
                        age_group_expr == age_group,
                        User.income.between(
                            # type: ignore[operator]
                            user.income * Decimal("0.9"),
                            # type: ignore[operator]
                            user.income * Decimal("1.1"),
                        ),
                        User.region == user.region,
                    )
                )
            )
            .scalars()
            .all()
        )

        if not group:
            return 0.0

        user_spendings = (
            session.scalar(
                select(func.sum(Transaction.amount)).where(
                    Transaction.user_id == user_id
                )
            )
            or 0
        )

        user_ids = [u.user_id for u in group]
        spendings_map = {
            row.user_id: (row.total or 0)
            for row in session.execute(
                select(
                    Transaction.user_id,
                    func.sum(Transaction.amount).label("total"),
                )
                .where(Transaction.user_id.in_(user_ids))
                .group_by(Transaction.user_id)
            ).all()
        }

        below = 0
        for member in group:
            member_spendings = spendings_map.get(member.user_id, 0)
            if member_spendings <= user_spendings:
                below += 1

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
    if age <= 35:
        return 1
    if age <= 45:
        return 2
    return 3


def get_all_users() -> list[User]:
    """
    :return: list[User]
    """
    session = next(get_db())
    try:
        return session.query(User).all()
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
