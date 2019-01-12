import psycopg2

DBNAME = "news"

def get_db_data(sql_query):
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    c.execute(sql_query)
    sql_return = c.fetchall()
    db.close()
    return sql_return

### Return top 3 most popular articles ###
query = '''SELECT articles.title, count(*) as num from articles join log
            on log.path like concat('/article/%', articles.slug) group by articles.title
            order by num desc limit 3;'''

print(get_db_data(query))

print('######')

### Return most popular authors, highest total views first ###
query = '''SELECT authors.name, count(*) as num from articles join authors
            on authors.id=articles.author join log on log.path like
            concat('/article/%', articles.slug) group by authors.name
            order by num desc limit 3;'''

print(get_db_data(query))

print('######')

### Return days with more than 1% requests ending in error ###
query = '''SELECT all_reqs.day, round(((errors.req_errors*1.0) / all_reqs.reqs), 3)
            as error_perc from (select date_trunc('day', time) "day", count(*) as req_errors
            from log where status like '404%' group by day)
            as errors join (select date_trunc('day', time) "day", count(*) as reqs
            from log group by day) as all_reqs on all_reqs.day = errors.day
            where (ROUND(((errors.req_errors*1.0) / all_reqs.reqs), 3) > 0.01)
            order by error_perc desc;'''

print(get_db_data(query))

print('######')
