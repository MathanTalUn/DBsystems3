import mysql.connector
import os
import sys

def get_db_connection():
    # Attempt to read credentials from file, otherwise use defaults/env vars
    user = 'royzemah'
    password = 'royzemah'
    host = '127.0.0.1' # As per instructions, but usually this is for deployment. 
                                     # For local dev, maybe localhost. 
                                     # I will stick to a sensible default or env var.
    
    # Check for credentials file
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

    # Allow env var override
    user = os.getenv('DB_USER', user)
    password = os.getenv('DB_PASSWORD', password)
    host = os.getenv('DB_HOST', host) # Use existing host (from assignment) if env var not set

    host = os.getenv('DB_HOST', '127.0.0.1')
    port = int(os.getenv('DB_PORT', 3305))

    try:
        conn = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password
        )
        return conn
    except mysql.connector.Error as err:
        print(f"Error connecting to MySQL: {err}")
        sys.exit(1)

def create_database():
    conn = get_db_connection()
    cursor = conn.cursor()

    db_name = 'royzemah'

    try:
        # Create Database
        #cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
        #cursor.execute(f"CREATE DATABASE {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        #print(f"Database {db_name} created.")

        cursor.execute(f"USE {db_name}")

        # 1. Movies Table
        # Indices: Full-text on overview, title. B-tree on release_date, vote_average.
        cursor.execute("""
            CREATE TABLE Movies (
                movie_id INT NOT NULL PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                release_date DATE,
                popularity FLOAT,
                vote_average FLOAT,
                vote_count INT,
                overview TEXT,
                original_language VARCHAR(10),
                FULLTEXT (title),
                INDEX (release_date),
                INDEX (vote_average)
            )
        """)
        print("Table 'Movies' created.")

        # 2. Genres Table
        cursor.execute("""
            CREATE TABLE Genres (
                genre_id INT NOT NULL PRIMARY KEY,
                name VARCHAR(100) NOT NULL
            )
        """)
        print("Table 'Genres' created.")

        # 3. Actors Table (renamed from People)
        cursor.execute("""
            CREATE TABLE Actors (
                actor_id INT NOT NULL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                gender INT,
                INDEX (name),
                FULLTEXT (name)
            )
        """)
        print("Table 'Actors' created.")

        # 4. Producers Table
        cursor.execute("""
            CREATE TABLE Producers (
                producer_id INT NOT NULL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                INDEX (name)
            )
        """)
        print("Table 'Producers' created.")

        # 5. Movie_Genres Table
        cursor.execute("""
            CREATE TABLE Movie_Genres (
                movie_id INT NOT NULL,
                genre_id INT NOT NULL,
                PRIMARY KEY (movie_id, genre_id),
                FOREIGN KEY (movie_id) REFERENCES Movies(movie_id) ON DELETE CASCADE,
                FOREIGN KEY (genre_id) REFERENCES Genres(genre_id) ON DELETE CASCADE
            )
        """)
        print("Table 'Movie_Genres' created.")

        # 6. Movie_Actors Table
        cursor.execute("""
            CREATE TABLE Movie_Actors (
                movie_id INT NOT NULL,
                actor_id INT NOT NULL,
                character_name VARCHAR(255),
                cast_order INT,
                PRIMARY KEY (movie_id, actor_id),
                FOREIGN KEY (movie_id) REFERENCES Movies(movie_id) ON DELETE CASCADE,
                FOREIGN KEY (actor_id) REFERENCES Actors(actor_id) ON DELETE CASCADE
            )
        """)
        print("Table 'Movie_Actors' created.")

        # 7. Movie_Producers Table
        cursor.execute("""
            CREATE TABLE Movie_Producers (
                movie_id INT NOT NULL,
                producer_id INT NOT NULL,
                PRIMARY KEY (movie_id, producer_id),
                FOREIGN KEY (movie_id) REFERENCES Movies(movie_id) ON DELETE CASCADE,
                FOREIGN KEY (producer_id) REFERENCES Producers(producer_id) ON DELETE CASCADE
            )
        """)
        print("Table 'Movie_Producers' created.")

    except mysql.connector.Error as err:
        print(f"Error creating database schemas: {err}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    create_database()
