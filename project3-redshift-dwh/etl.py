
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, create_table_queries, drop_table_queries

# drop and create
def load_staging_tables(cur, conn):
    '''
    Populate staging tables with S3 sources
    '''
    for query in copy_table_queries:
        print("Query Staging Table", query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    Populate DWH star schema
    '''
    print("Insert")
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
