from loguru import logger
from sqlalchemy import create_engine, URL, Engine, text

def get_db_conn(drivername: str, username: str, password: str, host: str, port: str, database: str) -> Engine:
    connection_string = URL.create(
        drivername=drivername,
        username=username,
        password=password,
        host=host,
        port=port,
        database=database,
    )
    db_conn = create_engine(connection_string)
    return db_conn

def get_sql_from_file(file_name: str) -> str:
    with open(f"sql/{file_name}") as f:
        sql = f.read()

    return sql

def update_url_scrape_status(db_engine: Engine, pkey: int, status: str, timestamp: str):
    sql = get_sql_from_file("update_url_scrape_status.sql")
    sql = sql.format(status=status, timestamp=timestamp, pkey=pkey)
    execute_query(db_engine, sql)

def execute_query(engine: Engine, sql: str) -> None:
    logger.info(f"Running query {sql}")
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()

    logger.info("Query successfully executed.")