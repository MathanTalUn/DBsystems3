import mysql.connector
import requests
import time
import os
import sys
from datetime import datetime

# Configuration
API_KEY_FILE = 'api_key.txt'
DEFAULT_DB_USER = 'root'
DEFAULT_DB_PASS = 'password'
DEFAULT_DB_HOST = 'mysqlsrv1.cs.tau.ac.il'
DB_NAME = 'movie_db_assign3'
MIN_RECORDS = 5000 # Minimum target

def get_api_key():
    api_key = os.getenv('TMDB_API_KEY')
    if api_key:
        return api_key
    
    if os.path.exists(API_KEY_FILE):
        try:
            with open(API_KEY_FILE, 'r') as f:
                return f.read().strip()
        except:
            pass
    
    print(f"Error: TMDB API Key not found. Please set TMDB_API_KEY env var or create {API_KEY_FILE}.")
    sys.exit(1)

def get_db_connection():
    user = os.getenv('DB_USER', DEFAULT_DB_USER)
    password = os.getenv('DB_PASSWORD', DEFAULT_DB_PASS)
    host = os.getenv('DB_HOST', DEFAULT_DB_HOST)
    
    creds_file = 'mysql_and_user_password.txt'
    if os.path.exists(creds_file):
        try:
            with open(creds_file, 'r') as f:
                content = f.read().strip().split()
                if len(content) >= 2:
                    user = content[0]
                    password = content[1]
        except Exception as e:
            print(f"Warning: Could not read {creds_file}: {e}")

    try:
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=DB_NAME
        )
        return conn
    except mysql.connector.Error as err:
        print(f"Error connecting to MySQL: {err}")
        sys.exit(1)

def fetch_and_populate():
    api_key = get_api_key()
    conn = get_db_connection()
    cursor = conn.cursor()

    base_url = "https://api.themoviedb.org/3"
    
    # Auth headers logic
    params = {}
    if len(api_key) > 40: # Read Access Token
        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {api_key}"
        }
    else: # API API Key
        params['api_key'] = api_key
        headers = {"accept": "application/json"}

    print("Starting data retrieval...")

    # 1. Fetch and Insert Genres FIRST
    print("Fetching Genres list...")
    genres_url = f"{base_url}/genre/movie/list"
    try:
        g_resp = requests.get(genres_url, headers=headers, params=params)
        if g_resp.status_code == 200:
            g_data = g_resp.json()
            for g in g_data.get('genres', []):
                gid = g['id']
                name = g['name']
                cursor.execute("INSERT IGNORE INTO Genres (genre_id, name) VALUES (%s, %s)", (gid, name))
            conn.commit()
            print("Genres populated.")
        else:
            print(f"Failed to fetch genres: {g_resp.status_code}")
    except Exception as e:
        print(f"Error fetching genres: {e}")
        # Proceeding might fail if FKs are strict, but we try.

    # 2. Fetch Movies and details
    movies_count = 0
    page = 1
    seen_movies = set()

    while movies_count < MIN_RECORDS:
        print(f"Fetching page {page} (Total Movies: {movies_count})...")
        discover_url = f"{base_url}/discover/movie"
        p = params.copy()
        p['page'] = page
        p['sort_by'] = 'popularity.desc'
        
        try:
            response = requests.get(discover_url, headers=headers, params=p)
            if response.status_code == 429:
                print("Rate limit reached. Sleeping 10s...")
                time.sleep(10)
                continue
            elif response.status_code != 200:
                print(f"Error fetching page {page}: {response.status_code} {response.text}")
                break

            data = response.json()
            results = data.get('results', [])
            
            if not results:
                print("No more results.")
                break

            for movie in results:
                movie_id = movie['id']
                if movie_id in seen_movies:
                    continue
                seen_movies.add(movie_id)

                # Insert Movie
                title = movie.get('title', '')
                release_date = movie.get('release_date')
                if not release_date: 
                    release_date = None
                popularity = movie.get('popularity', 0)
                vote_average = movie.get('vote_average', 0)
                vote_count = movie.get('vote_count', 0)
                overview = movie.get('overview', '')
                original_language = movie.get('original_language', '')

                insert_movie_sql = """
                    INSERT IGNORE INTO Movies (movie_id, title, release_date, popularity, vote_average, vote_count, overview, original_language)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(insert_movie_sql, (movie_id, title, release_date, popularity, vote_average, vote_count, overview, original_language))

                # Insert Movie_Genres
                # Now that Genres table is populated (hopefully), this should work.
                for genre_id in movie.get('genre_ids', []):
                    try:
                        cursor.execute("INSERT IGNORE INTO Movie_Genres (movie_id, genre_id) VALUES (%s, %s)", (movie_id, genre_id))
                    except mysql.connector.Error as err:
                        # Could fail if genre_id missing in Genres (if API adds new genres not in list endpoint)
                        pass

                # Fetch Credits
                credits_url = f"{base_url}/movie/{movie_id}/credits"
                c_resp = requests.get(credits_url, headers=headers, params=params)
                # Simple retry logic
                if c_resp.status_code == 429:
                    time.sleep(5)
                    c_resp = requests.get(credits_url, headers=headers, params=params)
                
                if c_resp.status_code == 200:
                    c_data = c_resp.json()
                    cast = c_data.get('cast', [])[:10] 
                    
                    for actor in cast:
                        actor_id = actor['id']
                        name = actor['name']
                        gender = actor.get('gender', 0)
                        
                        cursor.execute("INSERT IGNORE INTO Actors (actor_id, name, gender) VALUES (%s, %s, %s)", (actor_id, name, gender))
                        
                        try:
                            cursor.execute("""
                                INSERT IGNORE INTO Movie_Actors (movie_id, actor_id, character_name, cast_order)
                                VALUES (%s, %s, %s, %s)
                            """, (movie_id, actor_id, actor.get('character', ''), actor.get('order', 0)))
                        except:
                            pass

                    crew = c_data.get('crew', [])
                    producers = [m for m in crew if m['job'] == 'Producer']
                    for prod in producers:
                        prod_id = prod['id']
                        name = prod['name']
                        
                        cursor.execute("INSERT IGNORE INTO Producers (producer_id, name) VALUES (%s, %s)", (prod_id, name))
                        
                        try:
                            cursor.execute("INSERT IGNORE INTO Movie_Producers (movie_id, producer_id) VALUES (%s, %s)", (movie_id, prod_id))
                        except:
                            pass

                movies_count += 1
                if movies_count % 50 == 0:
                    conn.commit()
                    print(f"Processed {movies_count} movies...")

            page += 1
            if page > 300: # Cap pages to avoid infinite loop
                break
                
        except Exception as e:
            print(f"Error processing page {page}: {e}")
            break

    conn.commit()
    cursor.close()
    conn.close()
    print("Data Retrieval and Insertion Complete.")

if __name__ == "__main__":
    fetch_and_populate()
