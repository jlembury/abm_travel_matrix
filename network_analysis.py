# Jessica Embury, SDSU, GEOG683 final project, Spring 2022
# Functions related to network analysis for the O-D cost-distance matrix

# IMPORTS
import pandas as pd
import numpy
from psycopg2.extensions import register_adapter, AsIs
import networkx as nx
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, update, text, bindparam
from util import sqlalchemy_engine


# FUNCTIONS
def create_networkx_object(dbparams, edge_schema, edge_table, walk=False):
    """
    Create a directional NetworkX graph object using a Postgres table with the following columns:
        edge, fnode, tnode, dist_meters, travel_time_sec
    :param dbparams: Dictionary containing database log in information (user, password, host, port, dbname)
    :param edge_schema: String containing database schema name
    :param edge_table: String containing database table name
    :param walk: Boolean, True if walking routes and False for driving routes
    :return: NetworkX DiGraph
    """
    # SQL to return network info from database
    if walk:
        sql_str = "SELECT * from {}.{} WHERE walk = 1;".format(edge_schema, edge_table)
    else:
        sql_str = "SELECT * from {}.{};".format(edge_schema, edge_table)

    # connect to database
    engine = sqlalchemy_engine(dbparams)
    with engine.connect() as conn:
        # create pandas dataframe with network info
        df = pd.read_sql(sql_str, conn)

    # create NetworkX Graph
    # graph = nx.from_pandas_edgelist(df=df, source='fnode', target='tnode', edge_attr=True, create_using=nx.DiGraph)  # directed
    graph = nx.from_pandas_edgelist(df=df, source='fnode', target='tnode', edge_attr=True)  # undirected

    return graph


def get_od_routes(dbparams, routes_schema, routes_table, row_limit=None, first_row=None, walk=False):
    """
    Create a Pandas dataframe with O-D route node IDs from a Postgres table with the following columns:
        routeid, node_orig, node_dest
    :param dbparams: Dictionary containing database log in information (user, password, host, port, dbname)
    :param routes_schema: String containing database schema name
    :param routes_table: String containing database table name
    :param row_limit: (optional) Number of rows to return
    :param first_row: (optional) First row's 'routeid'
    :param walk: Boolean, True if walking routes and False for driving routes
    :return: Pandas dataframe containing routeid, node_orig, and node_dest columns
    """
    # walking routes
    if walk:
        # SQL to return all table rows if row_limit not given
        if row_limit is None:
            sql_str = "SELECT * from {}.{} WHERE walk = 1;".format(routes_schema, routes_table)
        # SQL to return specified number of rows, starting with provided ID
        else:
            sql_str = "SELECT * from {}.{} WHERE walk = 1 AND routeid > {} ORDER by routeid LIMIT {};".format(routes_schema, routes_table, first_row, row_limit)
    # driving routes
    else:
        # SQL to return all table rows if row_limit not given
        if row_limit is None:
            sql_str = "SELECT * from {}.{};".format(routes_schema, routes_table)
        # SQL to return specified number of rows, starting with provided ID
        else:
            sql_str = "SELECT * from {}.{} WHERE routeid >= {} ORDER by routeid LIMIT {};".format(routes_schema, routes_table, first_row, row_limit)

    # connect to data base
    engine = sqlalchemy_engine(dbparams)
    with engine.connect() as conn:
        # create a Pandas dataframe with O-D info
        df = pd.read_sql(sql_str, conn)

    return df


def find_shortest_route(graph, routes_df, col_id, col_orig, col_dest, col_cost, walk=False):
    """
    Find the shortest routes for the O-D nodes provided in a Pandas dataframe (get_od_routes()).
    Uses a NetworkX graph object (create_networkx_object()) and Dijkstra's algorithm
    :param graph: NetworkX DiGraph object
    :param routes_df: Pandas dataframe with routeid, node_orig, and node_dest columns
    :param col_id: String containing name of O-D route ID column ('routeid')
    :param col_orig: String containing name of source node column ('node_orig')
    :param col_dest: String containing name of target node column ('node_dest')
    :param col_cost: String containing name of weight column ('time_drive_sec' or 'time_walk_sec')
    :param walk: Boolean, True if walking routes and False for driving routes
    :return: 2 list objects: p (path nodes and travel time for each route) and e (error messages for failed routes)
    """
    p = []  # store paths
    e = []  # store errors

    if walk:
        # for each O-D pair
        for i, row in routes_df.iterrows():
            try:
                # find the shortest path
                path = nx.shortest_path(G=graph, source=routes_df[col_orig][i], target=routes_df[col_dest][i], weight=col_cost, method='dijkstra')

                # calculate the travel time
                time = 0
                for j in range(len(path) - 1):
                    temp_time = graph.get_edge_data(path[j], path[j + 1])
                    time += temp_time['time_walk_sec']

                # append path and time info to list
                p.append({'b_routeid': routes_df[col_id][i], 'walk_path': str(path), 'walk_time_sec': int(round(time, 0))})
            except Exception as e_message:
                # append error info to list
                e.append({'routeid':routes_df[col_id][i], 'od_pair': [routes_df[col_orig][i], routes_df[col_dest][i]], 'exception': e_message})
    else:
        # for each O-D pair
        for i, row in routes_df.iterrows():
            try:
                # find the shortest path
                path = nx.shortest_path(G=graph, source=routes_df[col_orig][i], target=routes_df[col_dest][i], weight=col_cost, method='dijkstra')

                # calculate the travel time
                time = 0
                for j in range(len(path) - 1):
                    temp_time = graph.get_edge_data(path[j], path[j + 1])
                    time += temp_time['time_drive_sec']

                # append path and time info to list
                p.append({'b_routeid': routes_df[col_id][i], 'drive_path': str(path), 'drive_time_sec': int(round(time, 0))})
            except Exception as e_message:
                # append error info to list
                e.append({'routeid':routes_df[col_id][i], 'od_pair': [routes_df[col_orig][i], routes_df[col_dest][i]], 'exception': e_message})

    return p, e


def routes2dbtable(dbparams, routes_schema, routes_table, col_path, col_cost, paths_list, walk=False):
    """
    Update the Postgres O-D routes table with path and travel time columns.
    reference: https://docs.sqlalchemy.org/en/14/tutorial/data_update.html#updating-and-deleting-rows-with-core
    :param dbparams: dictionary containing database log in information (user, password, host, port, dbname)
    :param routes_schema: String containing database schema name
    :param routes_table: String containing database table name
    :param col_path: Name of new column to store route path (drive_path, walk_path)
    :param col_cost: Name of new column to store route cost (drive_time_sec, walk_time_sec)
    :param paths_list: List with routeids, paths, and travel times (find_shortest_route())
    :param walk: Boolean, True if walking routes and False for driving routes
    :return: None
    """
    # SQL to add path and time columns
    sql_add_col1 = "ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS {} TEXT DEFAULT NULL;".format(routes_schema, routes_table, col_path)
    sql_add_col2 = "ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS {} INT DEFAULT NULL;".format(routes_schema, routes_table, col_cost)
    # SQL to set schema search path
    sql_set_schema = "SET SEARCH_PATH = public, {};".format(routes_schema)

    # fixes numpy int64 values so they work with psycopg2
    def addapt_numpy_float64(numpy_float64):
        return AsIs(numpy_float64)
    def addapt_numpy_int64(numpy_int64):
        return AsIs(numpy_int64)
    register_adapter(numpy.float64, addapt_numpy_float64)
    register_adapter(numpy.int64, addapt_numpy_int64)

    # connect to the database
    engine = sqlalchemy_engine(dbparams)

    with engine.connect() as conn:
        # add path and time columns to db table
        conn.execute(sql_add_col1)
        conn.execute(sql_add_col2)

        # set schema search path
        conn.execute(sql_set_schema)

        # create a SQLAlchemy Table object
        meta = MetaData(bind=engine)
        # walking routes
        if walk:
            routes = Table('{}'.format(routes_table), meta,
                           Column('routeid', Integer, primary_key=True),
                           Column('node_orig', String),
                           Column('node_dest', String),
                           Column('walk_path', String),
                           Column('walk_time_sec', Integer))
            # update columns
            stmt = (update(routes).
                    where(routes.c.routeid == bindparam('b_routeid')).
                    values(walk_path=bindparam('walk_path'), walk_time_sec=bindparam('walk_time_sec')))
        # driving routes
        else:
            routes = Table('{}'.format(routes_table), meta,
                           Column('routeid', Integer, primary_key=True),
                           Column('node_orig', String),
                           Column('node_dest', String),
                           Column('drive_path', String),
                           Column('drive_time_sec', Integer))
            # update columns
            stmt = (update(routes).
                    where(routes.c.routeid == bindparam('b_routeid')).
                    values(drive_path=bindparam('drive_path'), drive_time_sec=bindparam('drive_time_sec')))
        conn.execute(stmt, paths_list)

    return
