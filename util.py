# Jessica Embury, SDSU, GEOG683 final project, Spring 2022
# Functions related to Python package utility

# IMPORTS
from sqlalchemy import create_engine
import psycopg2
import sys


# FUNCTIONS
def sqlalchemy_engine(dbparams):
    """
    Connect to a PostgreSQL database using SQLAlchemy
    :param dbparams: dictionary containing database log in information (user, password, host, port, dbname)
    :return: SQLAlchemy engine
    """
    # create enging for database connection
    engine = create_engine(
        "postgresql+psycopg2://{}:{}@{}:{}/{}".format(
            dbparams['user'], dbparams['password'], dbparams['host'], dbparams['port'], dbparams['dbname']))

    return engine


def psycopg2_connect(dbparams):
    """
    Create a Postgres database connection using psycopg2
    reference: https://github.com/NaysanSaran/pandas2postgresql/blob/master/notebooks/CompleteExample.ipynb
    :param dbparams: database connection parameters
    :return: conn
    """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**dbparams)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    return conn
