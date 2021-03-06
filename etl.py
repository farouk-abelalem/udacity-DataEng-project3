"""
import needed packages and modules
"""
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    load the data from s3 into staging tables
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    insert the data from staging tables into dwh tables
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    get the connection parameters from config file and connect to database then run load_staging_tables and insert_tables functions
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