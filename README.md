## Implementation of HITS algorithm in Hive, Spark and SparkSQL

<p align="center">
	<img src="https://github.com/jaimeps/hits-algorithm/blob/master/images/logos.png" width="250">
</p>

### Description

This project consists of implemting the HITS algorithm using data from Wikipedia, in three different distributed computing environments: Hive, Spark and SparkSQL. The goal is to analyze the links of the articles to determine the most important Wikipedia pages.

### Data

The dataset consists of two files:
- Titles: A list of titles of Wikipedia articles (one title per row)
- Links: A list of links in the format "from1: to11 to12 ..."
<img src="https://github.com/jaimeps/hits-algorithm/blob/master/images/data.png" width="400">

### Methods

Two scores were computed for each page, the *authority* score and the *hub* score (initialized to 1). A good hub represents a page that pointed to many other pages, and a good authority represents a page that is linked by many different hubs.

After initialization, the process consists of:
- Updating the authority scores:
<img src="https://github.com/jaimeps/hits-algorithm/blob/master/images/update_auths.png" width="400"> <br />
- Updating the hub scores:
<img src="https://github.com/jaimeps/hits-algorithm/blob/master/images/update_hubs.png" width="400"> <br />
- Normalizing the authority scores
<img src="https://github.com/jaimeps/hits-algorithm/blob/master/images/norm_auths.png" width="400"> <br />
where
<img src="https://github.com/jaimeps/hits-algorithm/blob/master/images/norm_auths2.png" width="400"> <br />
- Normalizing the hub scores
<img src="https://github.com/jaimeps/hits-algorithm/blob/master/images/norm_hubs.png" width="400"> <br />
where
<img src="https://github.com/jaimeps/hits-algorithm/blob/master/images/norm_hubs2.png" width="400"> <br />
- Iterate

### References
- Maria Daltayanni - *Distributed Computing - Course notes*