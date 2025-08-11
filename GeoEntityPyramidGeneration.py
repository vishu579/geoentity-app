import psycopg2
#import mercantile
import zlib
import base64
from shapely.geometry import shape, mapping
from shapely.wkt import loads
from shapely.ops import transform
from functools import partial


# Connect to the PostGIS database move to config on a per layer basis
conn = psycopg2.connect(database="geoentity_stats", user="postgres", password="Vedas@123", host="192.168.2.149", port="5433")
cur = conn.cursor()



filter_query = ""

#Create the vedas_vector_tiles table if it does not exist
#cur.execute("DROP TABLE IF EXISTS vedas_vector_server_geom_cache")
# cur.execute("CREATE TABLE IF NOT EXISTS vedas_vector_server_geom_cache (layer_name VARCHAR,geo_id VARCHAR, geom GEOMETRY,tolerance VARCHAR)")
# cur.execute("Create index vedas_vector_server_geom_cache_gist on vedas_vector_server_geom_cache using GIST(geom)")
cur = conn.cursor()


#************************Parameter required to change********************************
# geoentity_source_id for which pyramid has to be generated.
geoentity_source_id = "31"
#if geometry type  is not polygon or multipolygon then False::
isPolygon = True


delete_query = "DELETE FROM geoentity_pyramid_levels where geoentity_source_id = "+geoentity_source_id
cur.execute(delete_query)
conn.commit()

tolerances = ["0.08192","0.04096","0.02048","0.01024","0.00512", "0.00256", "0.00128", "0.00064","0.00032","0.00016","0.00008", "0.00004", "0.00002", "0.00001","original"]
tolerances.reverse()
prev_tol = ""



for level in range(len(tolerances)):

    
    insert_prefix = "INSERT INTO geoentity_pyramid_levels (geoentity_source_id,geoentity_id,level,geom) "

    print("Looping ",level)
    if level == 0:
        print("Entered if",level)
        print("Ingesting ",level,tolerances[level])
        querystr = insert_prefix +" SELECT geoentity_source_id, geoentity_id, "+str(level)+",  geom FROM geoentity where geoentity_source_id="+geoentity_source_id+" " + ("and ST_IsValid(ST_Buffer(geom,0))" if isPolygon else " ")
        print("Query",querystr)
        cur.execute(querystr)
        conn.commit()
    else:
        print("Entered else",level)
        gridsize = str(0.000001)
        if(float(tolerances[level])>0.00001):
            gridsize = str(0.00001)
        if(float(tolerances[level])>0.0001):
            gridsize = str(0.0001)
        if(float(tolerances[level])>0.001):
            gridsize = str(0.001)
        print("Ingesting ",level,tolerances[level],gridsize)
        querystr = insert_prefix +" SELECT geoentity_source_id, geoentity_id,"+str(level)+", "+ ("ST_MakeValid(ST_Buffer(ST_SnapToGrid(ST_SimplifyPreserveTopology(geom,"+tolerances[level]+"),0,0,"+gridsize+","+gridsize+"),0))" if isPolygon else "geom") +" FROM geoentity_pyramid_levels where geoentity_source_id="+geoentity_source_id+" and level="+str(level-1)+" AND geom IS NOT NULL AND NOT ST_IsEmpty(geom) AND ST_IsValid(geom)"
        print("Query",querystr)
        cur.execute(querystr)
        conn.commit()
