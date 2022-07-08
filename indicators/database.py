from psycopg2 import extras
import psycopg2
import logging

logger = logging.getLogger()


def get_connection(connect: dict) -> psycopg2.extensions.connection:
    with psycopg2.connect(
            database=connect['database'],
            user=connect['user'],
            password=connect['password'],
            host=connect['host'],
            port=connect['port'],
            options=f"-c search_path={connect['schema']}"
    ) as conn:
        conn.autocommit = True
        return conn


def insert_data(**kwargs):
    # SQL query and values
    query = f"INSERT INTO {kwargs['table']}({', '.join(kwargs['columns'])}) VALUES %s"
    logger.info(f'Execute query columns ::: {query}')
    tuples = [tuple(x) for x in kwargs['values']]
    # Connection
    conn = get_connection(kwargs['connect'])
    cursor = conn.cursor()
    # Set timezone
    cursor.execute("SET TIME ZONE 'Asia/Seoul'")
    try:
        # Insert
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'Error: {error}')
        conn.rollback()
        cursor.close()
        return False
    logger.info('The dataframe is inserted')
    cursor.close()
    return True
