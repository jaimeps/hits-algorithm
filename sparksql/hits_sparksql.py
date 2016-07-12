
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
import time


# TABLE OUT-LINKS
def create_out_links(path):
    lines = sc.textFile(path)
    # Split and convert to array of integers and remove dead-links
    parts = lines.map(lambda l: l.split(":")) \
        .map(
        lambda p: (int(p[0]), [x for x in p[1].split(' ') if x != '']))
    # Explode and create dataframe
    out_links = parts.flatMapValues(lambda x: [int(y) for y in x]) \
        .map(lambda p: Row(from_page=p[0], to_page=p[1])) \
        .toDF()
    # Register table
    out_links.registerTempTable("out_links")


# TABLES HUBS and AUTHS
def create_hubs_auths(path):
    lines = sc.textFile(path).zipWithIndex()
    # Table with titles
    titles = lines.map(lambda p: Row(page_id=p[1]+1, page_title=p[0])).toDF()
    titles.registerTempTable("titles")
    # Table hubs
    df_hubs = lines.map(lambda p: Row(page_id=p[1]+1, hub_score=1.0)).toDF()
    df_hubs.cache()
    # df_hubs.repartition(200)
    df_hubs.registerTempTable("hubs")


# GENERIC QUERY FUNCTION
def query_table(query_string):
    query = sqlContext.sql(query_string)
    query.show()

# UPDATE AUTHS
def update_auths():
    query_string = '''SELECT to_page as page_id, sum(hub_score) as auth_score FROM
         (SELECT * FROM out_links) as Q1
       JOIN
         (SELECT page_id, hub_score FROM hubs) as Q2
        ON Q1.from_page = Q2.page_id
    GROUP BY to_page'''
    df_auths = sqlContext.sql(query_string)
    df_auths.cache()
    df_auths.registerTempTable("auths")

# UPDATE HUBS
def update_hubs():
    query_string = '''SELECT from_page as page_id, sum(auth_score) as hub_score FROM
        (SELECT * FROM out_links) as Q1
      JOIN
        (SELECT page_id, auth_score FROM auths) as Q2
       ON Q1.to_page = Q2.page_id
       GROUP BY from_page'''
    df_hubs = sqlContext.sql(query_string)
    df_hubs.cache()
    df_hubs.registerTempTable("hubs")


# NORMALIZING AUTHS
def normalize_auths():
    query_norm = '''SELECT sqrt(sum(power(auth_score,2))) as norm from auths'''
    norm = sqlContext.sql(query_norm).collect()[0][0]

    query_string = '''SELECT page_id, auth_score / ''' + str(norm) + ''' as auth_score FROM auths'''
    df_auths = sqlContext.sql(query_string)
    df_auths.cache()
    df_auths.registerTempTable("auths")

# NORMALIZING HUBS
def normalize_hubs():
    query_norm = '''SELECT sqrt(sum(power(hub_score,2))) as norm from hubs'''
    norm = sqlContext.sql(query_norm).collect()[0][0]

    query_string = '''SELECT page_id, hub_score / ''' + str(norm) + ''' as hub_score FROM hubs'''
    df_hubs = sqlContext.sql(query_string)
    df_hubs.cache()
    df_hubs.registerTempTable("hubs")

# QUERY AUTHS AND HUBS (to ensure execution)
def query_auths():
    query_string = '''SELECT auths.page_id, page_title, auth_score FROM
        auths
    JOIN
        titles
    ON auths.page_id = titles.page_id
    ORDER BY auth_score DESC limit 20'''
    query_table(query_string)

def query_hubs():
    query_string = '''SELECT hubs.page_id, page_title, hub_score FROM
        hubs
    JOIN
        titles
    ON hubs.page_id = titles.page_id
    ORDER BY hub_score DESC limit 20'''
    query_table(query_string)

# ENCAPSULATE ITERATION
def iteration(n):
    print '\n\n===== ITERATION %s =====\n\n' % (n + 1)
    # Update auths
    print '\n\n===== ITERATION %s UPDATING AUTHS =====\n\n' % (n + 1)
    update_auths()
    # Update hubs
    print '\n\n===== ITERATION %s UPDATING HUBS =====\n\n' % (n + 1)
    update_hubs()
    # Normalize auths
    print '\n\n===== ITERATION %s NORMALIZING AUTHS =====\n\n' % (n + 1)
    normalize_auths()
    query_auths()
    print '\n\n===== ITERATION %s - AUTHS OUTPUT =====\n\n' % (n + 1)
    # Normalize hubs
    print '\n\n===== ITERATION %s NORMALIZING HUBs =====\n\n' % (n + 1)
    normalize_hubs()
    query_hubs()
    print '\n\n===== ITERATION %s - HUBS OUTPUT =====\n\n' % (n + 1)


if __name__ == "__main__":

    sc = SparkContext()
    sqlContext = SQLContext(sc)

    # Create table out_links
    print '\nCreating table out_links...\n'
    path = "s3://data/links-simple-sorted.txt"
    create_out_links(path)

    # Create tables hubs and auths
    print '\nCreating tables auths and hubs...\n'
    path = "s3://data/titles-sorted.txt"
    create_hubs_auths(path)

    # Iterate 5 times to remove dead links
    print '\nCounting links...\n'
    query_string = '''SELECT count(*) FROM out_links'''
    query_table(query_string)

    # Update auths, hubs and normalize
    for i in range(8):
        start = time.time()
        iteration(i)
        elapsed = time.time() - start
        print 'Elapsed time: ', elapsed

    print '\n\n=== OK ===\n\n'
    sc.stop()
