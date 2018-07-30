import os
import time
import logging
import asyncio
from functools import partial
from aioapp.app import Application
from aioapp import config
from aioapp.tracer import Span
from aioapp_pg import Postgres


class Config(config.Config):
    db_url: str
    _vars = {
        'db_url': {
            'type': str,
            'name': 'DB_URL',
            'descr': 'Database connection string in following format '
                     'postgresql://user:passwd@host:port/dbname'
        }
    }


async def do_something(app: Application, ctx: Span) -> None:
    """
    do not run this task infinitely!!!
    there is no graceful shutdown!
    """
    res = await app.db.query_one(
        ctx, 'example_query_id',
        'SELECT $1::float as now, pg_sleep(1)',
        time.time())
    print('query result', res['now'])


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    loop = asyncio.get_event_loop()

    cfg = Config(os.environ)

    app = Application(loop=loop)
    app.add(
        'db',
        Postgres(
            url=cfg.db_url,
            pool_min_size=2,
            pool_max_size=19,
            pool_max_queries=50000,
            pool_max_inactive_connection_lifetime=300.,
            connect_max_attempts=10,
            connect_retry_delay=1.0),
        stop_after=[]
    )
    app.on_start = partial(do_something, app)
    app.run()
