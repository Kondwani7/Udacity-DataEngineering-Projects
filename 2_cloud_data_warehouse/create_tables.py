import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    drop tables our sqltables based on based on the scripts in sql_queries.py
    it allows us to reset our tables every time we run the "python create_tables.py" script
    Args:
        - cur is used to to execute our sql query
        - conn is used to commit the changes to our sql tables:
        - in this case dropping the tables
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    this creates tables our sqltables based on the scripts in sql_queries.py 
    it creates our tables and inserts data every time we run our "python create_tables.py" script
    Args:
        - cur is used to to execute our sql query
        - conn is used to commit the changes to our sql tables:
        - in this case creating and inserting in our the tables
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    it autheticates access to redshift  best on the credentials in our "dwh.cfg" file
    it allows us to create postgres tables in redshift
    Args:
        - cur is used to to execute our sql query
        - conn is used to commit the changes to our sql tables:
        - in this case  create, insert and drop (when needed) tables in redshift
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()