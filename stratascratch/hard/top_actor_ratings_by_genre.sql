-- Problem: Top Actor Ratings by Genre
-- Goal:
-- 1) For each actor, find the genre they appear in most often.
-- 2) Break ties by highest average rating for that actor.
-- 3) If still tied on both count and average, keep all tied genres.
-- 4) Globally rank resulting actor+genre pairs by average rating (no rank gaps) and keep top 3 ranks.

WITH genre_stats AS (
    SELECT
        actor_name,
        genre,
        COUNT(*) AS genre_count,
        AVG(movie_rating) AS avg_rating
    FROM top_actors_rating
    GROUP BY actor_name, genre
),
best_genre_per_actor AS (
    SELECT
        actor_name,
        genre,
        genre_count,
        avg_rating,
        DENSE_RANK() OVER (
            PARTITION BY actor_name
            ORDER BY genre_count DESC, avg_rating DESC
        ) AS actor_genre_rank
    FROM genre_stats
),
actor_genre_candidates AS (
    SELECT
        actor_name,
        genre,
        avg_rating
    FROM best_genre_per_actor
    WHERE actor_genre_rank = 1
)
SELECT
    actor_name,
    genre,
    avg_rating,
    actor_rank
FROM (
    SELECT
        actor_name,
        genre,
        avg_rating,
        DENSE_RANK() OVER (ORDER BY avg_rating DESC) AS actor_rank
    FROM actor_genre_candidates
) ranked
WHERE actor_rank <= 3
ORDER BY actor_rank, actor_name, genre;
