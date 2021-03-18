import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    a) Process the event log and song file and  populate  staging table data.
    b) Process the data from the staging table and extract the data for table time, users and songplays
       populate it
    
    --------
    Param:
        input_data: connection to redshift DB
        input_data: s3 location
        output_data: populate the redshiftDB
    Return:
        None.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    
    """
    a) create the staging and dimension table in redshift DB
    --------
    Param:
        input_data: connection to redshift DB
        output_data: creation of table in  redshiftDB
    Return:
        None.
    """
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
 
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
     
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
 
    conn.close()
 
 
if __name__ == "__main__":
    main()