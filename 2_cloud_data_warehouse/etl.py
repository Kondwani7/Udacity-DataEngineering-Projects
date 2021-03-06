import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    this copies our in redshift log_data to our staging events and staging songs tables
    its subject to authetication from redshift based on our IAM Role
    Args:
        - cur is used to to execute our sql query
        - conn is used to commit the changes to our sql tables:
        - in this case  copy log data in redshift to staging events and staging song tables
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    this inserts data into our fact and dimensions tables
    Args:
        - cur is used to to execute our sql query
        - conn is used to commit the changes to our sql tables:
        - in this case insert data into our fact and dimensions tables
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    it autheticates access to redshift  best on the credentials in our "dwh.cfg" file
    it allows us to complete our etl pipeline by committing the fact dimension tables to redshift
    Args:
        - cur is used to to execute our sql query
        - conn is used to commit the changes to our sql tables:
        - in this case copy log data to our staging tables and insert data to our fact and dimension tables
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()