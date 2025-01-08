from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import sessionmaker, DeclarativeBase
from sqlalchemy import String, create_engine
from config import settings
from typing import Annotated

# sync engine
sync_engine = create_engine(
    settings.DATABASE_URL_psycopg, 
    echo=True,
    pool_size=5,
    max_overflow=10
    )


# async engine
async_engine = create_async_engine(
    settings.DATABASE_URL_asyncpg, 
    echo=True
    ) 


session_factory = sessionmaker(sync_engine)
async_session_factory = async_sessionmaker(async_engine)


str_256 = Annotated[str, 256]

class Base(DeclarativeBase):
    type_annotation_map = {
        str_256: String(256)
    }
    repr_cols_num = 3
    repr_cols = tuple()

    def __repr__(self):
        cols = []
        for col in self.__table__.columns.keys():
            cols.append(f"{col}={getattr(self, col)}")
        return f"{self.__class__.__name__} {','.join(cols)}"