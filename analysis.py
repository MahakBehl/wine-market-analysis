import sqlite3
import pandas as pd

connexion = sqlite3.connect("DB/vivino.db")
cursor = connexion.cursor()



#We want to highlight 10 wines to increase our sales. Which ones should we choose and why?
## Normalize the average rating to a common scale, usually between 0 and 1 (ratings_average/max rating value) 
## Multiply the normalized average rating by the number of ratings. This gives more weight to products with both high ratings and a large number of ratings.
## Sort the products based on their weighted score, with higher scores indicating better products.
cursor.execute("""
            SELECT 
                name,
                ROUND((ratings_average / 5) * ratings_count, 1) AS weighted_score,
                ratings_average,
                ratings_count
            FROM 
                wines 
            ORDER BY 
                weighted_score DESC
            LIMIT 10;
    """)
print(cursor.fetchall())


#We have a limited marketing budget for this year. Which country should we prioritise and why?
## Normalize each data point to a common scale between 0 and 1 to ensure equal weighting. 
## Assign weights to each normalized data point based on its importance
## Multiply each normalized data point by its respective weight and sum the results to get a priority score for each country
## Rank countries based on their priority scores, with higher scores indicating higher priority
cursor.execute("""
                WITH max_values AS (
                    SELECT 
                        MAX(users_count) AS max_users,
                        MAX(regions_count) AS max_regions,
                        MAX(wines_count) AS max_wines,
                        MAX(wineries_count) AS max_wineries 
                    FROM 
                        countries
                )
                SELECT 
                    name,
                    ((regions_count / max_regions) * 0.1) + 
                    ((users_count / max_users) * 0.4) +
                    ((wines_count / max_wines) * 0.3) +
                    ((wineries_count / max_wineries) * 0.2) AS priority_score
                FROM 
                    countries, 
                    max_values 
                ORDER BY 
                    priority_score DESC;
    """)
print(cursor.fetchall())

#We detected that a big cluster of customers likes a specific combination of tastes. We identified a few keywords that match these tastes: coffee, toast, green apple, cream, and 
#citrus. We would like you to find all the wines that are related to these keywords. Check that at least 10 users confirm those keywords, to ensure the accuracy of the selection.
## Selected 5 tastes from Keywords table and make sure more then 10 users  confirm by checking the count in keyword_wine table
## Joined keywords, keywords_wine and wines table to get all the list of wine IDs & Names with atleast one taste mentioned above.
cursor.execute("""
                SELECT
                    wines.id AS wine_id,
                    wines.name AS wine_name,
                    COUNT(DISTINCT(keywords.name)) AS tastes_count,
                    keywords_wine.count AS number_of_users_confirm_keywords,
                    GROUP_CONCAT (DISTINCT(keywords.name)) AS list_of_tastes
                FROM
                    keywords
                JOIN
                    keywords_wine ON keywords.id = keywords_wine.keyword_id
                JOIN
                    wines ON keywords_wine.wine_id = wines.id
                WHERE
                    LOWER(keywords.name) IN ('coffee', 'toast', 'green apple', 'cream', 'citrus')
                    AND keywords_wine.count > 10
                GROUP BY
                    wines.id,
                    wines.name
                ORDER BY
                    tastes_count DESC;
    """)
print(cursor.fetchall())


## Joined keywords, keywords_wine and wines table to get all the list of wine IDs & Names with all taste mentioned above.
cursor.execute("""
                SELECT
                    wines.id AS wine_id,
                    wines.name AS wine_name,
                    COUNT(DISTINCT(keywords.name)) AS tastes_count,
                    keywords_wine.count AS number_of_users_confirm_keywords,
                    GROUP_CONCAT (DISTINCT(keywords.name)) AS list_of_tastes
                FROM
                    keywords
                JOIN
                    keywords_wine ON keywords.id = keywords_wine.keyword_id
                JOIN
                    wines ON keywords_wine.wine_id = wines.id
                WHERE
                    LOWER(keywords.name) IN ('coffee', 'toast', 'green apple', 'cream', 'citrus')
                    AND keywords_wine.count > 10
                GROUP BY
                    wines.id,
                    wines.name
				HAVING 
					tastes_count = 5;
    """)
print(cursor.fetchall())


#Find the top 3 most common grapes all over the world
cursor.execute("""
                SELECT
                    COUNT(country_code) AS number_of_countries_using_grape,
                    GROUP_CONCAT(country_code) AS country_codes,
                    grape_id,
                    name AS grape_name
                FROM
                    most_used_grapes_per_country
                JOIN
                    grapes ON most_used_grapes_per_country.grape_id = grapes.id
                GROUP BY
                    grape_id
                ORDER BY
                    number_of_countries_using_grape DESC
                LIMIT 3;
    """)
print(cursor.fetchall())

#Find the 5 top rated wine from wines table where grape name is Cabernet Sauvignon which one of the top 3 used grape across world
cursor.execute("""
                SELECT
                    wines.name,
                    ROUND((ratings_average / 5) * ratings_count, 1) AS weighted_score,
                    ratings_average,
                    ratings_count
                FROM
                    wines 
                where 
                    name like '%Cabernet Sauvignon%'
                ORDER BY 
                    weighted_score DESC
                LIMIT 5
    """)
print(cursor.fetchall())


#Find the 5 top rated wine from wines table where grape name is Cabernet Sauvignon which one of the top 3 used grape across world
cursor.execute("""
                SELECT
                    wines.name,
                    ROUND((ratings_average / 5) * ratings_count, 1) AS weighted_score,
                    ratings_average,
                    ratings_count
                FROM
                    wines 
                where 
                    name like '%Cabernet Sauvignon%'
                ORDER BY 
                    weighted_score DESC
                LIMIT 5
    """)
print(cursor.fetchall())

#Find the 5 top rated wine from wines table where grape name is Merlot which one of the top 3 used grape across world
cursor.execute("""
                WITH grapes_list AS (
                        SELECT
                            COUNT(country_code) AS number_of_countries_using_grape,
                            GROUP_CONCAT(country_code) AS country_codes,
                            grape_id,
                            grapes.name AS grape_name
                        FROM
                            most_used_grapes_per_country
                        JOIN
                            grapes ON most_used_grapes_per_country.grape_id = grapes.id
                        GROUP BY
                            grape_id
                        ORDER BY
                            number_of_countries_using_grape DESC
                        LIMIT 3
                    )
                    SELECT *
                    FROM (SELECT
                        wines.name,
                        ROUND((ratings_average / 5) * ratings_count, 1) AS weighted_score,
                        ratings_average,
                        ratings_count,
                        grapes_list.grape_name,
                        ROW_NUMBER() OVER (PARTITION BY CASE 
                                                                WHEN name LIKE  '%' || grapes_list.grape_name || '%' THEN grapes_list.grape_name
                                                            END 
                                                ORDER BY (SELECT NULL)) AS row_num
                    FROM
                        wines , grapes_list
                    where 
                        name like '%' || grapes_list.grape_name || '%'  
                    ORDER BY 
                        weighted_score DESC
                    ) AS subquery
                    WHERE row_num <= 5;
    """)
print(cursor.fetchall())

#a country leaderboard. A visual that shows the average wine rating for each country
cursor.execute("""
                SELECT 
                    countries.name AS country_name,
                    ROUND(AVG(ratings_average), 2) AS average_ratings
                FROM 
                    countries
                JOIN 
                    regions ON countries.code = regions.country_code
                JOIN 
                    wines ON wines.region_id = regions.id
                GROUP BY 
                    country_name
                ORDER BY 
                    average_ratings DESC;
""")
print(cursor.fetchall())

country_leaderboard = """SELECT 
                    countries.name AS country_name,
                    ROUND(AVG(ratings_average), 2) AS average_ratings
                FROM 
                    countries
                JOIN 
                    regions ON countries.code = regions.country_code
                JOIN 
                    wines ON wines.region_id = regions.id
                GROUP BY 
                    country_name
                ORDER BY 
                    average_ratings DESC;"""

country_leaderboard_df = pd.read_sql_query(country_leaderboard,connexion)
country_leaderboard_df

import plotly.express as px

fig = px.bar(country_leaderboard_df, x = "country_name", y = "average_ratings", title= "country Leaderboard For Average Wine Ratings")
fig.show()


#a vintage leaderboard. Average wine rating for all toprating vintages
cursor.execute("""
                SELECT 
                    vintages.name AS vintage_name,
                    ROUND(AVG(wines.ratings_average), 2) AS wines_average_ratings,
                    wine_id
                FROM 
                    vintages
                JOIN 
                    wines ON wines.id = vintages.wine_id
                JOIN 
                    vintage_toplists_rankings ON vintages.id = vintage_toplists_rankings.vintage_id
                GROUP BY 
                    wine_id
                ORDER BY 
                    wines_average_ratings DESC;
""")
print(cursor.fetchall())


#One of our VIP clients likes Cabernet Sauvignon and would like our top 5 recommendations.
cursor.execute("""
                SELECT
                    wines.name AS wine_name,
                    grapes.name AS grape_name,
                    wines.ratings_average AS average_rating,
                    wines.ratings_count AS rating_count,
                    ROUND((wines.ratings_average / 5) * wines.ratings_count, 1) AS weighted_score
                FROM
                    wines
                JOIN
                    regions ON regions.id = wines.region_id
                JOIN
                    most_used_grapes_per_country ON most_used_grapes_per_country.country_code = regions.country_code
                JOIN
                    grapes ON grapes.id = most_used_grapes_per_country.grape_id
                WHERE
                    grape_name = 'Cabernet Sauvignon'
                ORDER BY
                    weighted_score DESC
                LIMIT 5;
""")
print(cursor.fetchall())

