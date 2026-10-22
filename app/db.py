from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.config import get_settings

engine = create_async_engine(get_settings().database_url, pool_pre_ping=True)
sessions = async_sessionmaker(engine, expire_on_commit=False)


async def get_session():
    async with sessions() as session:
        yield session
