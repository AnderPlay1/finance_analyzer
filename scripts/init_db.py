from datetime import date as py_date
from os import getenv

from dotenv import find_dotenv, load_dotenv
from sqlalchemy import (
    Boolean,
    Date,
    Integer,
    Numeric,
    String,
    create_engine,
    literal_column,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker
from werkzeug.security import check_password_hash, generate_password_hash

load_dotenv(dotenv_path=find_dotenv(".env"), override=False)

DATABASE_URL = getenv("DATABASE_URL") or getenv(
    "SQLALCHEMY_DATABASE_URI") or "sqlite:///site.db"

if DATABASE_URL.startswith("mysql+pymysql://"):
    try:
        import pymysql  # noqa: F401
    except ImportError:
        print("WARNING: pymysql не установлен, используется sqlite:///site.db")
        DATABASE_URL = "sqlite:///site.db"

engine = create_engine(
    DATABASE_URL,
    echo=False,
    pool_pre_ping=True,
)

SessionLocal = sessionmaker(bind=engine)


class Base(DeclarativeBase):
    """
    Базовый класс для всех моделей базы данных.
    """


class User(Base):
    """
    Модель для хранения информации о пользователях.
    """
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(Integer, unique=True, index=True)
    first_name: Mapped[str] = mapped_column(String(120), nullable=True)
    last_name: Mapped[str] = mapped_column(String(120), nullable=True)
    middle_name: Mapped[str] = mapped_column(String(120), nullable=True)
    email: Mapped[str] = mapped_column(
        String(120), unique=True, index=True, nullable=True)
    password_hash: Mapped[str] = mapped_column(String(256), nullable=True)
    is_admin: Mapped[bool] = mapped_column(
        Boolean, default=False, server_default=literal_column("0"), nullable=False)
    confirmed: Mapped[bool] = mapped_column(
        Boolean, default=False, server_default=literal_column("0"), nullable=False)
    age: Mapped[int] = mapped_column(Integer, nullable=True)
    income: Mapped[float] = mapped_column(Numeric(10, 2), nullable=True)
    region: Mapped[str] = mapped_column(String(50), nullable=True)

    def set_password(self, password: str) -> None:
        '''
        Устанавливает хэш пароля для пользователя.
        :param password: str
        :return: None
        '''
        self.password_hash = generate_password_hash(password)

    def check_password(self, password: str) -> bool:
        """
        Проверяет, правильный ли пароль для пользователя.
        :param password: str
        :return: bool
        """
        if not self.password_hash:
            return False
        return check_password_hash(self.password_hash, password)


class Transaction(Base):
    """
    Модель для хранения информации о транзакциях пользователей.
    """

    __tablename__ = "transactions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(Integer)
    amount: Mapped[float] = mapped_column(Numeric(10, 2))
    transaction_date: Mapped[py_date] = mapped_column(Date)
    category: Mapped[str] = mapped_column(String(50))


class Category(Base):
    """
    Каталог категорий расходов (синхронизируется из транзакций).
    """

    __tablename__ = "categories"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(50), unique=True, index=True)


def init_db():
    """ 
    Инициализирует базу данных, создавая необходимые таблицы.
    Если таблицы уже существуют, они будут удалены и созданы заново.
    Это может привести к потере данных, поэтому используйте с осторожностью.
    :return: None
    """
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
