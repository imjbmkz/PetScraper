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

def execute_query(engine: Engine, query: str) -> None:
    with engine.connect() as conn:
        conn.execute(text(query))
        conn.commit()