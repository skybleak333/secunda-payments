from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.schema import CreateSchema, DropSchema

from app.api import app
from app.config import get_settings
from app.db import get_session
from app.models import Base


@pytest_asyncio.fixture
async def ctx():
    """Real PostgreSQL, isolated schema per test; never drop an application's tables."""
    url = get_settings().database_url
    schema = "test_" + uuid4().hex
    admin = create_async_engine(url)
    async with admin.begin() as connection:
        await connection.execute(CreateSchema(schema))
    engine = create_async_engine(url, connect_args={"server_settings": {"search_path": schema}})
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)

    async def session_override():
        async with sessions() as session:
            yield session

    app.dependency_overrides[get_session] = session_override
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
            headers={"X-API-Key": get_settings().api_key.get_secret_value()},
        ) as client:
            yield SimpleNamespace(client=client, sessions=sessions, engine=engine)
    finally:
        app.dependency_overrides.clear()
        await engine.dispose()
        async with admin.begin() as connection:
            await connection.execute(DropSchema(schema, cascade=True))
        await admin.dispose()
