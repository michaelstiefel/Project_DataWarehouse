import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This function executes copy_table_queries which connect to an S3 bucket
    and copy the data from those buckets into two staging tables - 
    (one for events, one for songs).
    
        
    Input:
    - cursor object which interacts with redshift
    - conn object that connects and commits to redshift
    
    Output: 
    - NULL (tables are created on the fly)  
    
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This function executes insert_table_queries which insert the rows from the 
    staging table into fact and dimension tables defined by a star schema.
    
        
        
    Input:
    - cursor object which interacts with redshift
    - conn object that connects and commits to redshift
    
    Output: 
    - NULL (tables are created on the fly)  
    
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This is the main file which is executed when calling etl.py .
    It connects to the Redshift database and creates a curser and connection object.
    
    With those two objects, it then calls the load_staging_tables and insert_tables defined above.  
        
        
    Input:
    - NULL
    
    Output: 
    - NULL
    
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