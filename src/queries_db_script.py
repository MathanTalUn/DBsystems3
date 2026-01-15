import mysql.connector
import os
import sys

db_name = 'matant2'

def get_db_connection():
    # Similar connection logic as other scripts
    user = os.getenv('DB_USER', 'matant2')
    password = os.getenv('DB_PASSWORD', 'matant2')
    host = os.getenv('DB_HOST', '127.0.0.1')
    
    creds_file = 'mysql_and_user_password.txt'
    if os.path.exists(creds_file):
        try:
            with open(creds_file, 'r') as f:
                content = f.read().strip().split()
                if len(content) >= 2:
                    user = content[0]
                    password = content[1]
        except:
            pass

    port = int(os.getenv('DB_PORT', 3305))
    return mysql.connector.connect(host=host, port=port, user=user, password=password, database=db_name)

def query_1(keyword):
    """
    Full-text search on Movies.title.
    Returns top 10 (title, release_date) rows, ordered by best match of `keyword` to the movie title.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    query = """
    SELECT
        M.title,
        M.release_date
    FROM Movies M
    WHERE MATCH(M.title) AGAINST (%s IN NATURAL LANGUAGE MODE)
    ORDER BY MATCH(M.title) AGAINST (%s IN NATURAL LANGUAGE MODE) DESC
    LIMIT 10;
    """

    cursor.execute(query, (keyword, keyword))
    results = cursor.fetchall()

    if not results:
        print(f"No movies found with title matching '{keyword}'")
    else:
        print(f"-- Top matches for '{keyword}' in movie titles --")
        for title, release_date in results:
            print(f"Title: {title} | Released: {release_date}")

    cursor.close()
    conn.close()

def query_2(keyword):
    """
    Full-text search on Actors.name.
    Returns the top 3 actors whose names best match `keyword`, along with the number of distinct movies
    they appear in our DB. Results are ordered by relevance (DESC), with ties broken by movies_in_db (DESC).
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    query = """
    SELECT
        A.name,
        COUNT(DISTINCT MA.movie_id) AS movies_in_db
    FROM Actors A
    JOIN Movie_Actors MA ON A.actor_id = MA.actor_id
    WHERE MATCH(A.name) AGAINST (%s IN NATURAL LANGUAGE MODE)
    GROUP BY A.actor_id, A.name
    ORDER BY MATCH(A.name) AGAINST (%s IN NATURAL LANGUAGE MODE) DESC, movies_in_db DESC
    LIMIT 3;
    """

    cursor.execute(query, (keyword, keyword))
    results = cursor.fetchall()

    if not results:
        print(f"No actors found matching '{keyword}'")
    else:
        print(f"-- Top matches for '{keyword}' in actor names --")
        for name, movies_in_db in results:
            print(f"Actor: {name} | Movies in DB: {movies_in_db}")

    cursor.close()
    conn.close()


def query_3(min_movies_count):
    """
    Complex Query 1: Aggregation, Group By, and Join.
    Find "High Quality" Genres. 
    Selects genres that have at least 'min_movies_count' movies in the DB,
    ordered by the *average* rating (vote_average) of their movies.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = """
    SELECT G.name, AVG(M.vote_average) as avg_rating, COUNT(M.movie_id) as movie_count
    FROM Genres G
    JOIN Movie_Genres MG ON G.genre_id = MG.genre_id
    JOIN Movies M ON MG.movie_id = M.movie_id
    GROUP BY G.name
    HAVING movie_count >= %s
    ORDER BY avg_rating DESC
    LIMIT 5;
    """
    
    cursor.execute(query, (min_movies_count,))
    results = cursor.fetchall()
    
    print(f"-- Top Rated Genres (min {min_movies_count} movies) --")
    for r in results:
        # r[0]=Name, r[1]=AvgRating, r[2]=Count
        print(f"Genre: {r[0]} | Avg Rating: {r[1]:.2f} | Movies in DB: {r[2]}")
    
    cursor.close()
    conn.close()

def query_4(actor_name):
    """
    Complex Query 2: Nested Query / EXISTS.
    Find Producers who have produced a movie that features 'actor_name'.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Using EXISTS
    query = """
    SELECT P.name
    FROM Producers P
    WHERE EXISTS (
        SELECT 1 
        FROM Movie_Producers MP
        JOIN Movies M ON MP.movie_id = M.movie_id
        JOIN Movie_Actors MA ON M.movie_id = MA.movie_id
        JOIN Actors A ON MA.actor_id = A.actor_id
        WHERE MP.producer_id = P.producer_id
        AND A.name = %s
    )
    LIMIT 20;
    """
    
    cursor.execute(query, (actor_name,))
    results = cursor.fetchall()
    
    print(f"-- Producers who worked with {actor_name} --")
    if not results:
        print("None found (or actor not found).")
    for r in results:
        print(r[0])
        
    cursor.close()
    conn.close()

def query_5(min_genres):
    """
    Complex Query 3: Join 4 tables.
    Find "Versatile Actors".
    Selects actors who have appeared in movies belonging to at least 'min_genres' distinct genres.
    Ordered by the number of unique genres they have played in.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = """
    SELECT A.name, COUNT(DISTINCT MG.genre_id) as distinct_genres
    FROM Actors A
    JOIN Movie_Actors MA ON A.actor_id = MA.actor_id
    JOIN Movies M ON MA.movie_id = M.movie_id
    JOIN Movie_Genres MG ON M.movie_id = MG.movie_id
    GROUP BY A.actor_id, A.name
    HAVING distinct_genres >= %s
    ORDER BY distinct_genres DESC
    LIMIT 5;
    """
    
    cursor.execute(query, (min_genres,))
    results = cursor.fetchall()
    
    print(f"-- Versatile Actors (appearing in >= {min_genres} distinct genres) --")
    for r in results:
        print(f"{r[0]} (Genres: {r[1]})")
        
    cursor.close()
    conn.close()
