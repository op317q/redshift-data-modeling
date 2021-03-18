import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    a) drop all the staging and dimension table in redshift DB
    --------
    Param:
        input_data: connection to redshift DB
        output_data: drop all the  table in redshiftDB
    Return:
        None.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    a) create all the staging and dimension table in redshift DB
    --------
    Param:
        input_data: connection to redshift DB
        output_data: create all the  table in redshiftDB
    Return:
        None.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
 
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
 
    drop_tables(cur, conn)
    create_tables(cur, conn)
 
    conn.close()
 
 
if __name__ == "__main__":
    main()