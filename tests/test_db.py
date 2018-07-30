import asyncio
from aioapp.app import Application
from aioapp_pg import Postgres, PostgresTracerConfig
from aioapp.error import PrepareError
import pytest
import string
from aioapp.misc import rndstr
from aioapp.tracer import Span
from aioapp.misc import async_call


async def _start_postgres(app: Application, url: str,
                          connect_max_attempts=10,
                          connect_retry_delay=1.0) -> Postgres:
    db = Postgres(url, connect_max_attempts=connect_max_attempts,
                  connect_retry_delay=connect_retry_delay)
    app.add('db', db)
    await app.run_prepare()
    await db.start()
    return db


def _create_span(app) -> Span:
    if app.tracer:
        return app.tracer.new_trace(sampled=False, debug=False)


async def test_postgres(app, postgres):
    table_name = 'tbl_' + rndstr(20, string.ascii_lowercase + string.digits)
    db = await _start_postgres(app, postgres)
    span = _create_span(app)

    res = await db.execute(span, 'test',
                           'SELECT $1::int as a', 1, timeout=10,
                           tracer_config=PostgresTracerConfig())
    assert res is not None

    res = await db.query_one(span, 'test',
                             'SELECT $1::int as a, $2::json, $3::jsonb', 1, {},
                             {}, timeout=10)
    assert res is not None
    assert len(res) == 3
    assert res[0] == 1
    assert res[1] == {}
    assert res[2] == {}
    assert res['a'] == 1

    res = await db.query_one(span, 'test',
                             'SELECT $1::int as a WHERE FALSE', 1, timeout=10)
    assert res is None

    res = await db.query_all(span, 'test',
                             'SELECT UNNEST(ARRAY[$1::int, $2::int]) as a',
                             1, 2, timeout=10)
    assert res is not None
    assert len(res) == 2
    assert res[0][0] == 1
    assert res[1][0] == 2
    assert res[0]['a'] == 1
    assert res[1]['a'] == 2

    res = await db.query_all(span, 'test',
                             'SELECT $1::int as a WHERE FALSE', 1, timeout=10)
    assert res is not None
    assert len(res) == 0

    async with db.connection(span) as conn:
        async with conn.xact(span):
            await conn.execute(span, 'test',
                               'CREATE TABLE %s(id int);' % table_name)
            await conn.execute(span, 'test',
                               'INSERT INTO %s(id) VALUES(1)' % table_name)

    res = await db.query_one(span, 'test',
                             'SELECT COUNT(*) FROM %s' % table_name,
                             timeout=10)
    assert res[0] == 1

    try:
        async with db.connection(span) as conn:
            async with conn.xact(span, isolation_level='SERIALIZABLE'):
                await conn.execute(span, 'test',
                                   'INSERT INTO %s(id) VALUES(2)' % table_name)
                raise UserWarning()
    except UserWarning:
        pass

    res = await db.query_one(span, 'test',
                             'SELECT COUNT(*) FROM %s' % table_name,
                             timeout=10)
    assert res[0] == 1


async def test_postgres_prepare_failure(app, unused_tcp_port):
    with pytest.raises(PrepareError):
        await _start_postgres(app, 'postgres://postgres@%s:%s/postgres'
                                   '' % ('127.0.0.1', unused_tcp_port),
                              connect_max_attempts=2,
                              connect_retry_delay=0.001)


async def test_postgres_health_bad(app: Application, unused_tcp_port: int,
                                   loop: asyncio.AbstractEventLoop) -> None:
    url = 'postgres://postgres@%s:%s/postgres' % ('127.0.0.1', unused_tcp_port)

    db = Postgres(url)
    app.add('postgres', db)

    async def start():
        await app.run_prepare()
        await db.start()

    res = async_call(loop, start)
    await asyncio.sleep(1)

    result = await app.health()
    assert 'postgres' in result
    assert result['postgres'] is not None
    assert isinstance(result['postgres'], BaseException)

    if res['fut'] is not None:
        res['fut'].cancel()


async def test_postgres_health_ok(app: Application, postgres: str,
                                  loop: asyncio.AbstractEventLoop) -> None:
    db = Postgres(postgres)
    app.add('postgres', db)

    async def start():
        await app.run_prepare()
        await db.start()

    res = async_call(loop, start)
    await asyncio.sleep(1)

    result = await app.health()
    assert 'postgres' in result
    assert result['postgres'] is None

    if res['fut'] is not None:
        res['fut'].cancel()
