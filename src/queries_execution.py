import queries_db_script
import sys

def main():
    print("--- Executing Queries for Movie Database ---")

    # Query 1: Full text search on Movies.title (top 10)
    keyword_1 = "Man"
    print(f"\n[Query 1] Full-text search in Movies.title for '{keyword_1}' (top 10)")
    try:
        queries_db_script.query_1(keyword_1)
    except Exception as e:
        print(f"Error executing Query 1: {e}")

    # Query 2: Full text search on Actors.name (top 3 + movie count)
    keyword_2 = "Tom"
    print(f"\n[Query 2] Full-text search in Actors.name for '{keyword_2}' (top 3 + movies_in_db)")
    try:
        queries_db_script.query_2(keyword_2)
    except Exception as e:
        print(f"Error executing Query 2: {e}")


    # Query 3: Genres with total votes > X
    min_votes = 50000
    print(f"\n[Query 3] Genres with > {min_votes} votes")
    try:
        queries_db_script.query_3(min_votes)
    except Exception as e:
        print(f"Error executing Query 3: {e}")

    # Query 4: Producers who worked with Actor X
    actor_name = "Tom Hanks"
    print(f"\n[Query 4] Producers who worked with '{actor_name}'")
    try:
        queries_db_script.query_4(actor_name)
    except Exception as e:
        print(f"Error executing Query 4: {e}")

    # Query 5: Top 5 Actors in popular movies
    print("\n[Query 5] Top 5 Actors in popular movies (>10 popularity)")
    try:
        queries_db_script.query_5()
    except Exception as e:
        print(f"Error executing Query 5: {e}")

if __name__ == "__main__":
    main()
