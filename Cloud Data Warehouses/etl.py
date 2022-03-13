import configparser
import psycopg2
from sqlalchemy import create_engine
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """ Staging song data and log data json files from AWS S3 to Redshift
    
    Arguments:
            cur {object}:    Allows Python code to execute PostgreSQL command in a database session
            conn {object}:   Handles the connection to a PostgreSQL database instance. It encapsulates a database session.
    Returns:
            No return values
    """
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        print('-- Staging --> {}'.format(query))

    print('----')
    
    
def insert_tables(cur, conn):
    """ Transforming data from staging tables into dimensional tables
    
    Arguments:
            cur {object}:    Allows Python code to execute PostgreSQL command in a database session
            conn {object}:   Handles the connection to a PostgreSQL database instance. It encapsulates a database session.
    Returns:
            No return values
    """
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        print('-- Insert in Dimensional --> {}'.format(query))

    print('----')
    
    
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connecting to a database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DWH'].values()))
    print('-- Connected in AWS RedShift [Host --> {} / DbName --> {}]'.format(*config['DWH'].values()))
    print('----')
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()

    print('--- Processed Successfully !!! ---')

    
if __name__ == "__main__":
    main()