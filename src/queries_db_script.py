import mysql.connector
import os
import sys

db_name = 'movie_db_assign3'

def get_db_connection():
    # Similar connection logic as other scripts
    user = os.getenv('DB_USER', 'root')
    password = os.getenv('DB_PASSWORD', 'password')
    host = os.getenv('DB_HOST', 'mysqlsrv1.cs.tau.ac.il')
    
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
    Full-text search on Movie overview.
    Returns title and overview.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = """
    SELECT title, overview 
    FROM Movies 
    WHERE MATCH(overview) AGAINST (%s IN NATURAL LANGUAGE MODE)
    LIMIT 10;
    """
    
    cursor.execute(query, (keyword,))
    results = cursor.fetchall()
    
    if not results:
        print(f"No movies found with overview matching '{keyword}'")
    else:
        print(f"-- Movies with '{keyword}' in overview --")
        for r in results:
            print(f"Title: {r[0]}")
            print(f"Overview: {r[1][:100]}...") # truncate for display
            print("-" * 20)
            
    cursor.close()
    conn.close()

def query_2(keyword):
    """
    Full-text search on Movie title.
    Returns title, release_date.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = """
    SELECT title, release_date
    FROM Movies 
    WHERE MATCH(title) AGAINST (%s IN NATURAL LANGUAGE MODE)
    LIMIT 10;
    """
    
    cursor.execute(query, (keyword,))
    results = cursor.fetchall()
    
    if not results:
        print(f"No movies found with title matching '{keyword}'")
    else:
        print(f"-- Movies with '{keyword}' in title --")
        for r in results:
            print(f"Title: {r[0]}, Released: {r[1]}")
            
    cursor.close()
    conn.close()

def query_3(min_vote_count):
    """
    Complex Query 1: Aggregation and Group By.
    Find Genres that have more than 'min_vote_count' total votes across all their movies,
    ordered by total votes descending.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = """
    SELECT G.name, SUM(M.vote_count) as total_votes
    FROM Genres G
    JOIN Movie_Genres MG ON G.genre_id = MG.genre_id
    JOIN Movies M ON MG.movie_id = M.movie_id
    GROUP BY G.name
    HAVING total_votes > %s
    ORDER BY total_votes DESC;
    """
    
    cursor.execute(query, (min_vote_count,))
    results = cursor.fetchall()
    
    print(f"-- Genres with total votes > {min_vote_count} --")
    for r in results:
        print(f"Genre: {r[0]}, Total Votes: {r[1]}")
    
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

def query_5():
    """
    Complex Query 3: Join 3 tables to find Top 5 Actors by number of movies they appeared in,
    but only consider movies with popularity > 10.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = """
    SELECT A.name, COUNT(MA.movie_id) as movie_count
    FROM Actors A
    JOIN Movie_Actors MA ON A.actor_id = MA.actor_id
    JOIN Movies M ON MA.movie_id = M.movie_id
    WHERE M.popularity > 10
    GROUP BY A.actor_id, A.name
    ORDER BY movie_count DESC
    LIMIT 5;
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    print("-- Top 5 Actors in popular movies (>10 popularity) --")
    for r in results:
        print(f"{r[0]} ({r[1]} movies)")
        
    cursor.close()
    conn.close()
