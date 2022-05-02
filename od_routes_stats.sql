-- Jessica Embury, SDSU, GEOG683 final project, Spring 2022

-----------------------------
-- ESCO SCHEMA DRIVE TIMES --
-----------------------------
select count(*) from esco.od_routes where time_drive_sec is not null;

-- travel time summary statistics
select min(time_drive_sec), max(time_drive_sec), avg(time_drive_sec), stddev(time_drive_sec) from esco.od_routes where time_drive_sec is not null limit 1000000;

-- number of routes under 1 min or over 1 hour
select count(*) from esco.od_routes where time_drive_sec < 60;
select count(*) from esco.od_routes where time_drive_sec > 3600;

-- frequency table of route travel times by 5 minute increment
select '0-5 min' as TotalRange,count(time_drive_sec) as Count from esco.od_routes
        where time_drive_sec between 0 and 300
     union (
     select '5-10 min' as TotalRange,count(time_drive_sec) as Count from esco.od_routes
        where time_drive_sec between 301 and 600 )
     union (
     select '10-15 min' as TotalRange,count(time_drive_sec) as Count from esco.od_routes
        where time_drive_sec between 601 and 900 )
     union (
     select '15-20 min' as TotalRange,count(time_drive_sec) as Count from esco.od_routes
        where time_drive_sec between 901 and 1200 )
     union (
     select '20-25 min' as TotalRange,count(time_drive_sec) as Count from esco.od_routes
        where time_drive_sec between 1201 and 1500 )
     union (
     select '25-30 min' as TotalRange,count(time_drive_sec) as Count from esco.od_routes
        where time_drive_sec between 1501 and 1800 )
     union (
     select '30-35 min' as TotalRange,count(time_drive_sec) as Count from esco.od_routes
        where time_drive_sec between 1801 and 2100 )
     union (
     select '35-40 min' as TotalRange,count(time_drive_sec) as Count from esco.od_routes
        where time_drive_sec between 2101 and 2400 )
     union (
     select '40-45 min' as TotalRange,count(time_drive_sec) as Count from esco.od_routes
        where time_drive_sec between 2401 and 2700 )
     union (
     select '45-50 min' as TotalRange,count(time_drive_sec) as Count from esco.od_routes
        where time_drive_sec between 2701 and 3000 )
     union (
     select '50-55 min' as TotalRange,count(time_drive_sec) as Count from esco.od_routes
        where time_drive_sec between 3001 and 3300 )
     union (
     select '55-60 min' as TotalRange,count(time_drive_sec) as Count from esco.od_routes
        where time_drive_sec between 3301 and 3600 )
     union (
     select '60+ min' as TotalRange,count(time_drive_sec) as Count from esco.od_routes
        where time_drive_sec > 3601 );
