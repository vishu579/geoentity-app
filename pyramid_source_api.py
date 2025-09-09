import time
from flask import Flask
import psycopg2
import requests


# SELECT
#     gs.id, gs.name
# FROM
#     public.geoentity_source AS gs
# LEFT JOIN
#     public.geoentity_pyramid_levels AS gpl
# ON
#     gs.id = gpl.geoentity_source_id
# WHERE
#     gpl.geoentity_source_id IS NULL;

app = Flask(__name__)

# Database configuration
DATABASE_CONFIG = {
    "dbname": "geoentity_stats",
    "user": "postgres",
    "password": "Vedas@123",
    "host": "192.168.2.149",
    "port": "5433"
}

# External API endpoint
SOURCE_API_URL = "https://vedas.sac.gov.in/geoentity-services/api/geoentity-sources/"


@app.route("/check-pyramid-levels", methods=["GET"])
def check_pyramid_levels():
    print("Received request to /check-pyramid-levels")
    
    print("Starting cache update...")
    try:
        print("Fetching from API...")
        api_start = time.time()
        # Step 1: Fetch source data from external API
        response = requests.get(SOURCE_API_URL)
        response.raise_for_status()
        api_data = response.json().get("data", [])
        
        print(f"Fetched API data in {time.time() - api_start:.2f} seconds")

        # Step 2: Get a list of IDs to check
        source_ids = [source.get("id") for source in api_data if source.get("id") is not None]

        if not source_ids:
            print("No source IDs found from API.")
            return

        print(f"Number of source IDs: {len(source_ids)}")
        print("First few IDs:", source_ids[:5])

        print("Querying the database...")
        db_start = time.time()


        # Step 3: Query the database
        conn = psycopg2.connect(**DATABASE_CONFIG)
        print("Connected")
        cur = conn.cursor()
        sql_query = """
            SELECT
                gs.id, gs.name
            FROM
                public.geoentity_source AS gs
            LEFT JOIN
                public.geoentity_pyramid_levels AS gpl
            ON
                gs.id = gpl.geoentity_source_id
            WHERE
                gpl.geoentity_source_id IS NULL;
        """
        
        print(sql_query)
        cur.execute(sql_query, (source_ids,))
        print("Query executed")
        db_results = cur.fetchall()
        cur.close()
        conn.close()

        print(f"Queried DB in {time.time() - db_start:.2f} seconds", db_results)


        print("Merging and saving data...")
        # merge_start = time.time()



        # Step 4: Combine data
        db_map = {row[0]: row[1] for row in db_results}
        final_results = []
        for source in api_data:
            source_id = source.get("id")
            source_name = source.get("name")
            if source_id is not None and source_name is not None:
                pyramid_status = 'no' if source_id in db_map else 'yes'
                final_results.append({
                    "id": source_id,
                    "name": source_name,
                    "pyramid_levels_available": pyramid_status
                })
        return final_results
    except Exception as e:
        print("error is",e)
                
        

if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=False)
