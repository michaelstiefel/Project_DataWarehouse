import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    This function executes sql queries in order to
    delete tables that might exist from an earlier call to create tables.
    
    Input:
    - cursor object which interacts with redshift
    - conn object that connects and commits to redshift
    
    Output: 
    - NULL (tables are dropped on the fly)
    
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This function deletes executes sql queries in order to create tables
    which are then used to stage and fill with data from an s3 bucket. 
    
    Input:
    - cursor object which interacts with redshift
    - conn object that connects and commits to redshift
    
    Output: 
    - NULL (tables are created on the fly)    
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This is the main function that this script executes. It connects to the redshift database
    and then calls the functions drop_tables and create_tables specified earlier. 
    
    Input: 
    NULL
    Output:
    NULL
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