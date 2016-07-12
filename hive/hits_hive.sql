/* HITS algorithm - Hive  */

/* In EMR, put file into hdfs
	hdfs dfs -mkdir /movies
	hdfs dfs -mkdir /ratings
	hdfs dfs -cp s3://data/movies.txt /user/hadoop/movies/
	hdfs dfs -cp s3://data/ratings.txt /user/hadoop/ratings/
	hive
*/

set hive.cli.print.header=true;

/* CREATING TABLE OUTLINKS */
DROP TABLE IF EXISTS temp;

CREATE EXTERNAL TABLE temp(from_page1 INT, to_page1 ARRAY<INT>)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ': '
COLLECTION ITEMS TERMINATED BY ' '
STORED AS TEXTFILE
LOCATION '/links/';

DROP TABLE IF EXISTS  out_links;

CREATE EXTERNAL TABLE out_links(from_page INT, to_page INT)
CLUSTERED BY (from_page) SORTED BY (from_page) INTO 32 BUCKETS
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE out_links
SELECT from_page1 as from_page, to_page2 as to_page
FROM temp
LATERAL VIEW explode(to_page1) test as to_page2
WHERE to_page2 IS NOT null;

/* CREATING TABLE HUBS */
DROP TABLE IF EXISTS temp;

CREATE EXTERNAL TABLE temp(title STRING)
STORED AS TEXTFILE
LOCATION '/titles/';

DROP TABLE IF EXISTS hubs;

CREATE EXTERNAL TABLE hubs(page_id INT, page_title STRING, hub_score FLOAT)
CLUSTERED BY (page_id) SORTED BY (page_id) INTO 32 BUCKETS
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE hubs
SELECT row_number() over (ORDER BY title) as page_id, title as page_title, 1.0 as hub_score
FROM temp;

/* CREATING TABLE AUTHS */
DROP TABLE IF EXISTS auths;

CREATE EXTERNAL TABLE auths(page_id INT, page_title STRING, auth_score FLOAT)
CLUSTERED BY (page_id) SORTED BY (page_id) INTO 32 BUCKETS
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE auths
SELECT * FROM hubs;

DROP TABLE IF EXISTS temp;

/* SUMMARY */
SELECT * from out_links limit 10;
SELECT * from auths limit 10;
SELECT * from hubs limit 10;

/* COUNTING NUMBER OF ROWS IN OUT-LINKS */
SELECT COUNT(*) as N FROM out_links;
/* 130160392 */

/* UPDATING AUTHS */
INSERT OVERWRITE TABLE auths
SELECT page_id, page_title, CASE WHEN new_score is null THEN 0 ELSE new_score END FROM
     (SELECT page_id, page_title FROM auths) as Q4
LEFT JOIN
     (SELECT to_page, sum(hub_score) as new_score FROM
          (SELECT * FROM out_links) as Q1
     LEFT JOIN
          (SELECT page_id, hub_score FROM hubs) as Q2
     ON Q1.from_page = Q2.page_id
     GROUP BY to_page) as Q3
ON Q4.page_id = Q3.to_page;

/* UPDATING HUBS */
INSERT OVERWRITE TABLE hubs
SELECT page_id, page_title, CASE WHEN new_score is null THEN 0 ELSE new_score END FROM
     (SELECT page_id, page_title FROM hubs) as Q4
LEFT JOIN
     (SELECT from_page, sum(auth_score) as new_score FROM
          (SELECT * FROM out_links) as Q1
     LEFT JOIN
          (SELECT page_id, auth_score FROM auths) as Q2
     ON Q1.to_page = Q2.page_id
     GROUP BY from_page) as Q3
ON Q4.page_id = Q3.from_page;

/* NORMALIZING AUTHS */
INSERT OVERWRITE TABLE auths
SELECT page_id, page_title, auth_score / norm FROM
     (SELECT * from auths) as Q1
JOIN
     (SELECT sqrt(sum(power(auth_score,2))) as norm from auths) as Q2;

/* NORMALIZING HUBS */
INSERT OVERWRITE TABLE hubs
SELECT page_id, page_title, hub_score / norm FROM
     (SELECT * from hubs) as Q1
JOIN
     (SELECT sqrt(sum(power(hub_score,2))) as norm from hubs) as Q2;


/* SHOW TOP 20 PAGES BY AUTH SCORE  */
SELECT auth_score, hub_score, page_title FROM
(SELECT * FROM auths ORDER BY auth_score DESC limit 20) as Q1
LEFT JOIN
(SELECT page_id, hub_score FROM hubs) as Q2
ON Q1.page_id = Q2.page_id
ORDER BY auth_score DESC limit 20;


/* SHOW TOP 20 PAGES BY HUB SCORE  */
SELECT hub_score, auth_score, page_title FROM
(SELECT * FROM hubs ORDER BY hub_score DESC limit 20) as Q1
LEFT JOIN
(SELECT page_id, auth_score FROM auths) as Q2
ON Q1.page_id = Q2.page_id
ORDER BY hub_score DESC limit 20;






