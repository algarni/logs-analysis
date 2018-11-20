#!/usr/bin/env python3

import psycopg2

DBNAME = 'news'

try:
	db = psycopg2.connect(database=DBNAME)
except:
    print("Unable to connect to the databas")

c = db.cursor()

print("What are the most popular three articles of all time?")
query = """
SELECT a.title, count(l.id) AS views
FROM articles AS a LEFT JOIN log AS l
ON l.path LIKE concat('%', a.slug)
GROUP BY a.title
ORDER BY views
DESC LIMIT 3
"""
c.execute(query)
results = c.fetchall()
for r in results:
    print('"' + r[0] + '"', ' -- ', r[1], ' views')
print()

print("Who are the most popular article authors of all time?")

query = """
SELECT name, count(l.id) AS views
FROM authors AS w
LEFT JOIN articles AS a ON w.id = a.author
LEFT JOIN log AS l ON l.path LIKE concat('%', a.slug)
GROUP BY name
ORDER BY views DESC
"""

c.execute(query)
results = c.fetchall()
for r in results:
    print(r[0], ' -- ', r[1], ' views')
print()

print("On which days did more than 1% of requests lead to errors?")

query = """
SELECT a.log_date, ROUND(b.log_count::decimal/a.log_count*100, 2) AS error_rate
FROM
(SELECT date(time) AS log_date, COUNT(*) AS log_count
FROM log GROUP BY log_date) AS a,
(SELECT DATE(time) AS log_date, COUNT(*) AS log_count
FROM log WHERE status LIKE '404%' GROUP BY log_date) AS b
WHERE a.log_date = b.log_date AND 1 < b.log_count::DECIMAL/a.log_count*100;
"""

c.execute(query)
results = c.fetchall()
for r in results:
    print(r[0], ' -- ',  str(r[1]) + '%', ' errors')
print()

db.close()
