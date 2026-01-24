from datetime import date as py_date

from sqlalchemy import (
    create_engine,
    Integer,
    String,
    Date,
    Numeric
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column
)

DATABASE_URL = "mysql+pymysql://user:password@localhost:3306/mydb"

engine = create_engine(
    DATABASE_URL,
    echo=True,
    pool_pre_ping=True
)


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(Integer, unique=True, index=True)
    age: Mapped[int] = mapped_column(Integer)
    income: Mapped[float] = mapped_column(Numeric(10, 2))
    region: Mapped[str] = mapped_column(String(50))


class Transaction(Base):
    __tablename__ = "transactions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(Integer)

    amount: Mapped[float] = mapped_column(Numeric(10, 2))
    transaction_date: Mapped[py_date] = mapped_column(Date)
    category: Mapped[str] = mapped_column(String(50))


def init_db():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
