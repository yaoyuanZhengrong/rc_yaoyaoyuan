from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session, scoped_session
from contextlib import contextmanager
from config import settings
from models import Base

engine = create_engine(settings.database_url, pool_pre_ping=True)
SessionFactory = sessionmaker(autocommit=False, autoflush=False, bind=engine)
# 使用 scoped_session 确保多线程环境下每个线程获得唯一的 Session 对象
SessionLocal = scoped_session(SessionFactory)


def init_db():
    Base.metadata.create_all(bind=engine)


def get_db() -> Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        SessionLocal.remove()


@contextmanager
def get_db_context():
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        SessionLocal.remove()
