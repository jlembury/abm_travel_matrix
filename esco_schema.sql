-- Jessica Embury, SDSU, GEOG683 final project, Spring 2022

-- create database
CREATE DATABASE abm_sandiego ENCODING 'UTF8';
ALTER DATABASE abm_sandiego OWNER TO jembury;
-- postgis extension
CREATE EXTENSION postgis;
-- create schemas
CREATE SCHEMA raw;  -- raw tables, no changes to data
CREATE SCHEMA esco;  -- schema for test using Escondido only
CREATE SCHEMA sd;

-- create esco schema tables for parcels, roads, and pois (set keys and spatial indexes through main.py)
-- parcels within escondido SRA border
DROP TABLE IF EXISTS esco.parcels_2017;
CREATE TABLE esco.parcels_2017 AS (
SELECT a."APN" as apn, a."PARCELID" as parcelid, a."UNITQTY" as unitqty, a.geometry
FROM (SELECT * FROM raw.sandag_parcels_2017 WHERE "NUCLEUS_US" = '90' or "NUCLEUS_US" = '91' or "NUCLEUS_US" = '92' or "NUCLEUS_US" = '93' or "NUCLEUS_US" = '94' or "NUCLEUS_US" = '95' or "NUCLEUS_US" = '96' or "NUCLEUS_US" = '97' or "NUCLEUS_US" = '98' or "NUCLEUS_US" = '99' or "NUCLEUS_US" = '100' or "NUCLEUS_US" = '110' or "NUCLEUS_US" = '111' or "NUCLEUS_US" = '112' or "NUCLEUS_US" = '113' or "NUCLEUS_US" = '114' or "NUCLEUS_US" = '115' or "NUCLEUS_US" = '116' or "NUCLEUS_US" = '117' or "NUCLEUS_US" = '120' or "NUCLEUS_US" = '130' or "NUCLEUS_US" = '140' or "NUCLEUS_US" = '150' or "NUCLEUS_US" = '152' or "NUCLEUS_US" = '153' or "NUCLEUS_US" = '160' or "NUCLEUS_US" = '162' or "NUCLEUS_US" = '163' or "NUCLEUS_US" = '170' or "NUCLEUS_US" = '171' or "NUCLEUS_US" = '172' or "NUCLEUS_US" = '173' or "NUCLEUS_US" = '174') AS a, (SELECT geometry FROM esco.sra_2010) AS b WHERE ST_Within(a.geometry, b.geometry)
ORDER BY random());
-- replace parcel polygon geometry with centroid point geometry
ALTER TABLE esco.parcels_2017 ADD COLUMN geometry_temp Geometry(Point, 2230);
UPDATE esco.parcels_2017 SET geometry_temp = ST_Centroid(geometry) WHERE geometry_temp is NULL;
ALTER TABLE esco.parcels_2017 DROP COLUMN geometry;
ALTER TABLE esco.parcels_2017 RENAME COLUMN geometry_temp TO geometry;
-- add unique identifier id (vs. apn + parcelid)
ALTER TABLE esco.parcels_2017 ADD COLUMN resid SERIAL PRIMARY KEY;

-- pois within escondido SRA border
DROP TABLE IF EXISTS esco.sg_poi;
CREATE TABLE esco.sg_poi AS (
SELECT a.placekey, a.naics_code as naics, a.open_hours, a.geometry
FROM (SELECT * FROM raw.sg_core_places_srid2230 WHERE (opened_on NOT ILIKE '%2020%' AND opened_on NOT ILIKE '%2021%' AND opened_on NOT ILIKE '%2022%') OR (opened_on IS NULL)) AS a, (SELECT geometry FROM esco.sra_2010) AS b WHERE ST_Within(a.geometry, b.geometry)
ORDER BY random());

-- roads within 1 mile buffer of escondido SRA border
-- create buffer
CREATE TABLE esco.esco_1mi_buff (geometry Geometry(POLYGON, 2230));
INSERT INTO esco.esco_1mi_buff (geometry) (SELECT ST_SetSRID(ST_Buffer(geometry, 5280), 2230) as geometry FROM esco.sra_2010);
-- create roads table
CREATE TABLE esco.roads_2017 AS (
SELECT a."ROADSEGID" as roadsegid, a."FNODE" as fnode, a."TNODE" as tnode, a."ONEWAY" as oneway, a."SPEED" as speed, a.geometry
FROM (SELECT * FROM raw.sandag_roadsall_2017 WHERE "CARTO" NOT LIKE 'P') AS a, (SELECT geometry from esco.esco_1mi_buff) AS b
WHERE ST_Intersects(a.geometry, b.geometry)
ORDER BY random());
-- calculate distances
ALTER TABLE esco.roads_2017 ADD COLUMN dist_meters NUMERIC;
UPDATE esco.roads_2017 SET dist_meters = ROUND(ST_Length(geometry)::NUMERIC/3.28084, 5) WHERE dist_meters IS NULL;
-- update speeds = 0 to pedestrian speed (=3)
UPDATE esco.roads_2017 SET speed = 3 WHERE speed = 0;
-- drop buffer
DROP TABLE esco.esco_1mi_buff;

-- create nodes table (roads)
DROP TABLE IF EXISTS esco.nodes;
CREATE TABLE esco.nodes (nodeid NUMERIC PRIMARY KEY, res INT DEFAULT 0, poi INT DEFAULT 0, geometry Geometry(POINT, 2230));
INSERT INTO esco.nodes (nodeid, geometry) (SELECT DISTINCT fnode AS nodeid, ST_SetSRID(ST_PointN(geometry, 1), 2230) AS geometry FROM esco.roads_2017);
INSERT INTO esco.nodes (nodeid, geometry) (SELECT DISTINCT tnode AS nodeid, ST_SetSRID(ST_PointN(geometry, -1), 2230) AS geometry FROM esco.roads_2017 t1 WHERE NOT EXISTS (SELECT nodeid, geometry FROM esco.nodes t2 WHERE t2.nodeid = t1.tnode));

--create an UNDIRECTED edge table (roads)
DROP TABLE IF EXISTS esco.edges;
CREATE TABLE esco.edges (edgeid SERIAL PRIMARY KEY, roadsegid BIGINT, fnode NUMERIC, tnode NUMERIC, dist_meters NUMERIC, time_drive_sec NUMERIC, time_walk_sec NUMERIC);
INSERT INTO  esco.edges (roadsegid, fnode, tnode, dist_meters, time_drive_sec, time_walk_sec) (
    SELECT roadsegid, fnode, tnode, dist_meters, (dist_meters/(speed * 0.44704)) AS time_drive_sec, (dist_meters/(1.34112)) AS time_walk_sec FROM esco.roads_2017);

--create DIRECTIONAL edges table (roads)
DROP TABLE IF EXISTS esco.edges;
CREATE TABLE esco.edges (edgeid SERIAL PRIMARY KEY, roadsegid BIGINT, fnode NUMERIC, tnode NUMERIC, dist_meters NUMERIC, time_drive_sec NUMERIC, time_walk_sec NUMERIC);
INSERT INTO  esco.edges (roadsegid, fnode, tnode, dist_meters, time_drive_sec, time_walk_sec) (
    SELECT roadsegid, fnode, tnode, dist_meters, (dist_meters/(speed * 0.44704)) AS time_drive_sec, (dist_meters/(1.34112)) AS time_walk_sec FROM esco.roads_2017 WHERE oneway = 'F' OR oneway = 'B');
INSERT INTO  esco.edges (roadsegid, fnode, tnode, dist_meters, time_drive_sec, time_walk_sec) (
    SELECT roadsegid, tnode AS fnode, fnode AS tnode, dist_meters, (dist_meters/(speed * 0.44704)) AS time_drive_sec, (dist_meters/(1.34112)) AS time_walk_sec FROM esco.roads_2017 WHERE oneway = 'T' OR oneway = 'B');
ALTER TABLE esco.edges RENAME TO edges_directed;

-- find nodes closest to res parcels and pois (use nearest neighbor)
ALTER TABLE esco.parcels_2017 ADD COLUMN node_closest NUMERIC, ADD COLUMN node_dist_meters NUMERIC;
UPDATE esco.parcels_2017 AS t1 SET node_closest = t2.node_closest, node_dist_meters = t2.node_dist_meters FROM (
WITH parcels AS (SELECT resid, geometry AS resgeom FROM esco.parcels_2017),
    nodes AS (SELECT nodeid, geometry AS nodegeom FROM esco.nodes),
    dist AS (SELECT nodes.resid, nodes.nodeid, dist_ft FROM parcels CROSS JOIN LATERAL (SELECT resid, nodeid, resgeom::Geometry(POINT, 2230), nodegeom::Geometry(POINT, 2230), nodegeom <-> resgeom AS dist_ft FROM nodes ORDER BY dist_ft LIMIT 1) AS nodes)
SELECT dist.resid, dist.nodeid AS node_closest, ROUND(dist.dist_ft::NUMERIC/3.28084, 5) AS node_dist_meters FROM dist) AS t2
WHERE t1.resid = t2.resid;

ALTER TABLE esco.sg_poi ADD COLUMN node_closest NUMERIC, ADD COLUMN node_dist_meters NUMERIC;
UPDATE esco.sg_poi AS t1 SET node_closest = t2.node_closest, node_dist_meters = t2.node_dist_meters FROM (
WITH poi AS (SELECT placekey, geometry AS poigeom FROM esco.sg_poi),
    nodes AS (SELECT nodeid, geometry AS nodegeom FROM esco.nodes),
    dist AS (SELECT nodes.placekey, nodes.nodeid, dist_ft FROM poi CROSS JOIN LATERAL (SELECT placekey, nodeid, poigeom::Geometry(POINT, 2230), nodegeom::Geometry(POINT, 2230), nodegeom <-> poigeom AS dist_ft FROM nodes ORDER BY dist_ft LIMIT 1) AS nodes)
SELECT dist.placekey, dist.nodeid AS node_closest, ROUND(dist.dist_ft::NUMERIC/3.28084, 5) AS node_dist_meters FROM dist) AS t2
WHERE t1.placekey = t2.placekey;

-- update res and poi columns in nodes table (=1 if node is the closest node to a res parcel or poi)
UPDATE esco.nodes SET res = t.res FROM (SELECT nodeid, 1 AS res FROM esco.nodes t1 WHERE EXISTS (SELECT node_closest FROM esco.parcels_2017 t2 WHERE t2.node_closest = t1.nodeid)) AS t WHERE esco.nodes.nodeid = t.nodeid;
UPDATE esco.nodes SET poi = t.poi FROM (SELECT nodeid, 1 AS poi FROM esco.nodes t1 WHERE EXISTS (SELECT node_closest FROM esco.sg_poi t2 WHERE t2.node_closest = t1.nodeid)) AS t WHERE esco.nodes.nodeid = t.nodeid;

-- create od routes table with routes for all res-poi nodes and poi-poi nodes
CREATE TABLE esco.od_routes (routeid SERIAL PRIMARY KEY, node_orig NUMERIC, node_dest NUMERIC);
INSERT INTO esco.od_routes (node_orig, node_dest) (
    WITH res AS (SELECT nodeid AS resnode FROM esco.nodes WHERE res = 1),
         poi AS (SELECT nodeid AS poinode FROM esco.nodes WHERE poi = 1)
    SELECT t1.resnode AS node_orig, t2.poinode AS node_dest from res t1 CROSS JOIN poi t2 WHERE t1.resnode != t2.poinode);
INSERT INTO esco.od_routes (node_orig, node_dest) (
    WITH poi AS (SELECT nodeid AS poinode FROM esco.nodes WHERE poi = 1),
         routes AS (SELECT t1.poinode AS node_orig, t2.poinode AS node_dest from poi t1 CROSS JOIN poi t2 WHERE t1.poinode != t2.poinode)
    SELECT routes.node_orig AS node_orig, routes.node_dest AS node_dest FROM routes WHERE NOT EXISTS (SELECT node_orig, node_dest FROM esco.od_routes t3 WHERE t3.node_orig = routes.node_orig AND t3.node_dest = routes.node_dest));

-- Rename path and time columns to specify driving mode
ALTER TABLE esco.od_routes RENAME COLUMN shortest_path to drive_path;
ALTER TABLE esco.od_routes RENAME COLUMN time_drive_sec to drive_time_sec;

-- Pedestrian routes - same as road network, only check for O-D <= 5 miles apart --> ~8050 meters (reference Bradley et al., 2010)
--add binary walk column (1=TRUE, route can use pedestrian mode.)
ALTER TABLE esco.od_routes ADD COLUMN walk INT DEFAULT 0;
WITH t AS (SELECT routes.node_orig, routes.node_dest, orig.geometry AS orig_geom, dest.geometry AS dest_geom
            FROM esco.od_routes routes LEFT JOIN esco.nodes orig ON routes.node_orig = orig.nodeid
            LEFT JOIN esco.nodes dest ON routes.node_dest = dest.nodeid),
     t1 AS (SELECT t.node_orig, t.node_dest, ST_Distance(t.orig_geom, t.dest_geom) AS distance FROM t)
UPDATE esco.od_routes SET walk = t2.walk FROM (
    SELECT t1.node_orig, t1.node_dest, t1.distance,
           CASE WHEN t1.distance <= 8050 THEN 1 WHEN t1.distance > 8050 THEN 0 END walk FROM t1) AS t2
WHERE od_routes.node_orig = t2.node_orig AND od_routes.node_dest = t2.node_dest;

SELECT COUNT(*) FROM esco.od_routes WHERE walk = 1; --1,334,135 pedestrian routes

--add binary column to edges table for pedestrian routes: 1=walkable, 0=not walkable (freeway, highway, freeway ramps)
ALTER TABLE esco.roads_2017 ADD COLUMN carto TEXT;
WITH t AS (SELECT roadsegid, "CARTO" AS carto FROM esco.roads_2017 e LEFT JOIN raw.sandag_roadsall_2017 r ON e.roadsegid = r."ROADSEGID")
UPDATE esco.roads_2017 SET carto=t.carto FROM t WHERE esco.roads_2017.roadsegid = t.roadsegid;

ALTER TABLE esco.edges ADD COLUMN walk INT DEFAULT 0;
WITH t AS (SELECT roadsegid, carto, 1 AS walk FROM esco.roads_2017 WHERE carto NOT IN ('1', '2', '8', '9'))
UPDATE esco.edges SET walk = t.walk FROM t WHERE esco.edges.roadsegid = t.roadsegid;

SELECT COUNT(*) FROM esco.edges WHERE walk = 1; --11,499 walkable edges (not freeways, highways, or ramps)

-- change binary walk column in od_routes to 1 where travel time <6000 sec (5 miles at 3mph), 0 otherwise
WITH t as (SELECT routeid, CASE WHEN walk_time_sec > 6000 THEN 0 WHEN walk_time_sec IS NULL THEN 0 WHEN walk_time_sec <= 6000 THEN 1 END walk FROM esco.od_routes)
UPDATE esco.od_routes SET walk = t.walk FROM t WHERE esco.od_routes.routeid = t.routeid;
SELECT SUM(walk) FROM esco.od_routes; --1,232,988 walkable od pairs with time < 6000 sec (5 miles at 3mph)
