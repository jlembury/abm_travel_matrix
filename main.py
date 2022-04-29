# Jessica Embury, SDSU, GEOG683 final project, Spring 2022

# Notes: Python 3.8 environment (abmsd_env), use EPSG 2230 projection for all (SG originally in 4326)

import time
from db_implementation import shp2dbtable, csv2dbtable, create_project_tables, set_primary_key, set_foreign_key, create_spatial_index
from network_analysis import create_networkx_object, get_od_routes, find_shortest_route, routes2dbtable
from passwords import get_db_pass

DB_PASS = get_db_pass()
DB_CONN = {
    'host': '127.0.0.1',
    'dbname': 'abm_sandiego',
    'port': 5432,
    'user': 'jembury',
    'password': DB_PASS
}

ROADS_SHP = './data/RoadsAll_2017.shp'
PARCELS_SHP = './data/ParcelsOct2017.shp'
SRA_SHP = './data/SRA2010tiger.shp'
CORE_PLACES_CSV = './data/sg_core_places_24feb2022.csv'
CORE_PLACES_SHP = './data/sg_core_places_24feb2022_2230.shp'

ROW_LIMIT = 10000
STARTING_ROUTEID = 3120001

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    ##############
    # RAW SCHEMA #
    ##############
    # create raw data tables with SANDAG SRAs, SANDAG parcels, SANDAG roads, and SafeGraph core places
    # shp2dbtable(SRA_SHP, DB_CONN, 'raw', 'sandag_sra_2010', ['sra_pk', ['"SRA"']])
    # shp2dbtable(ROADS_SHP, DB_CONN, 'raw', 'sandag_roadsall_2017', ['roadsegid_pk', ['"ROADSEGID"']])
    # shp2dbtable(PARCELS_SHP, DB_CONN, 'raw', 'sandag_parcels_2017', ['parcelid_apn_pk', ['"PARCELID"', '"APN"']])
    # shp2dbtable(CORE_PLACES_SHP, DB_CONN, 'raw', 'sg_core_places_srid2230', ['placekey_srid2230_pk', ['placekey']])
    # csv2dbtable(CORE_PLACES_CSV, DB_CONN, 'raw', 'sg_core_places', ['placekey_pk', ['placekey']])

    ####################
    # ESCONDIDO SCHEMA #
    ####################
    # create sra table
    # create_project_tables(DB_CONN, 'raw', 'sandag_sra_2010', 'esco', 'sra_2010', {'"SRA"':'sra', '"NAME"':'name', 'geometry':'geometry'}, False, '"NAME" ILIKE \'%ESCONDIDO%\'', ['sra_pk', ['sra']], None, ['idx_sra_2010_geometry', 'geometry', False])

    # set keys for parcel, roads, and poi tables (created directly with SQL syntax)
    # create_spatial_index(DB_CONN, 'esco', 'parcels_2017', 'idx_parcels_2017_geometry', 'geometry', False)
    # set_primary_key(DB_CONN, 'esco', 'roads_2017', 'roadsegid_pk', ['roadsegid'])
    # create_spatial_index(DB_CONN, 'esco', 'roads_2017', 'idx_roads_2017_geometry', 'geometry', False)
    # set_primary_key(DB_CONN, 'esco', 'sg_poi', 'placekey_pk', ['placekey'])
    # create_spatial_index(DB_CONN, 'esco', 'sg_poi', 'idx_sg_poi_geometry', 'geometry', False)

    # keys/indexes for node, edge, od tables
    # create_spatial_index(DB_CONN, 'esco', 'nodes', 'idx_nodes_geometry', 'geometry', False)
    # set_foreign_key(DB_CONN, 'esco', 'roads_2017', 'roads_2017_fnode_fk', ['fnode'], 'esco', 'nodes', ['nodeid'])
    # set_foreign_key(DB_CONN, 'esco', 'roads_2017', 'roads_2017_tnode_fk', ['tnode'], 'esco', 'nodes', ['nodeid'])
    # set_foreign_key(DB_CONN, 'esco', 'edges', 'edges_fnode_fk', ['fnode'], 'esco', 'nodes', ['nodeid'])
    # set_foreign_key(DB_CONN, 'esco', 'edges', 'edges_tnode_fk', ['tnode'], 'esco', 'nodes', ['nodeid'])
    # set_foreign_key(DB_CONN, 'esco', 'edges', 'edges_roadsegid_fk', ['roadsegid'], 'esco', 'roads_2017', ['roadsegid'])
    # set_foreign_key(DB_CONN, 'esco', 'parcels_2017', 'res_node_fk', ['node_closest'], 'esco', 'nodes', ['nodeid'])
    # set_foreign_key(DB_CONN, 'esco', 'sg_poi', 'poi_node_fk', ['node_closest'], 'esco', 'nodes', ['nodeid'])
    # set_foreign_key(DB_CONN, 'esco', 'od_routes', 'node_orig_fk', ['node_orig'], 'esco', 'nodes', ['nodeid'])
    # set_foreign_key(DB_CONN, 'esco', 'od_routes', 'node_dest_fk', ['node_dest'], 'esco', 'nodes', ['nodeid'])

    #########################
    # ESCO NETWORK ANALYSIS #
    #########################
    # create networkx graph object from edges table in database
    G = create_networkx_object(DB_CONN, 'esco', 'edges')
    print('NetworkX graph object has {} nodes and {} edges.'.format(G.number_of_nodes(), G.number_of_edges()))

    od_len = ROW_LIMIT
    first_routeid = STARTING_ROUTEID
    print(od_len, first_routeid)
    while od_len == ROW_LIMIT:
        st = time.time()
        # get routes from routes table in database
        od = get_od_routes(DB_CONN, 'esco', 'od_routes', od_len, first_routeid)

        # find shortest routes using Dijkstra's algorithm and calculate travel times
        paths, errors = find_shortest_route(G, od, 'routeid', 'node_orig', 'node_dest', 'time_drive_sec')
        # print('Shortest path routes: ', paths)
        if len(errors) > 0:
            print('Shortest path errors: ', errors)

        # commit paths and travel times to the routes table in database
        routes2dbtable(DB_CONN, 'esco', 'od_routes', 'time_drive_sec', paths)
        et = time.time()

        processing_time = et - st
        od_len = len(od)
        first_routeid += od_len
        print("Last loop took {} seconds. Next loop has {} rows, starting with 'routeid' {}.".format(processing_time, od_len, first_routeid))

    print("Complete.")
