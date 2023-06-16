# ETL and recommendation system of combined Reddit and Spotify data
This project uses the social network Reddit to obtain information about the latest music recommended by users and then query the track and artist data involved that Spotify. The entire process is managed using widely recognized tools within the field of Big Data.
## Description
This project implements a **ETL process** to collect data from Reddit. The steps of the process are:
1. Retrieved posts from Subreddit r/music, r/IndieHeads, r/PopHeads, r/ElectronicMusic using **Reddit API**. Extracted relevant information such as song names and artists' names.
2. Fetched song details using **Spotify API**. Retrieved audio features, and popularity ratings of the songs.
3. Ingest data into HDFS as a data lake in Parquet format.
4. Transformed and loaded data into Hive as a data warehouse. Created Dashboard with Superset to derive insights about music trends.
5. Utilized data from the data lake to train K-means based recommendation system and deployed a simple web application.
<img src="https://github.com/trquoctoann/Subreddit-ETL-Recommendation-System/blob/main/img/data%20pipeline.png">
The project has been built using Docker and Docker Compose to run the following containers:

- **Apache Hadoop** containers were configured for data storage as a datalake: 1 master and 2 workers node. The image was built from scratch using ubuntu image (https://hub.docker.com/_/ubuntu).
- **Apache Spark** containers were configured for data processing: 1 master and reused 2 workers from hadoop cluster. Spark setup required a custom image with the following packages installed via PyPI as additional requirements: "pandas", "requests", "pyspark", "mysql-connector-python", "flask". The image was built based on previous hadoop image.
- **Apache Hive** containers were configured for data storage as a data warehouse: hiveserver2 and metastore-server. The image was built based on previous hadoop image.
- **Apache Superset** container was configured for data visualization. The image used was the official Superset (latest version) image found on DockerHub (https://hub.docker.com/r/apache/superset) and no additional requirements were needed.
- **MySQL** containers were configured to store history. The image used was the official MySQL (latest version) image found on DockerHub (https://hub.docker.com/_/mysql)
## Prerequisites
- **Git**
- **Docker** version 24.0.2
- **Docker-compose** version 1.29.2
- **Python** 3.10.6
- Reddit and Spotify APIs **developer keys** added in a file **project.conf** following the format of the **project.conf.example** file and located in the same folder.
## Usage 
```sh
$ git clone https://github.com/trquoctoann/Subreddit-ETL-Recommendation-System.git
$ cd Subreddit-ETL-Recommendation-System
$ sudo ./build-image.sh
$ sudo docker-compose up --build -d
```
Once the project is deployed, three visual interfaces can be accessed:
1. **Apache Hadoop** interface: It is accessible through port 50070 (http://localhost:8080), being able to check cluster's status or data stored inside.
<img src="https://github.com/trquoctoann/Subreddit-ETL-Recommendation-System/blob/main/img/datalake.png">

2. **Apache Spark** history interface. It is accessible through port 18080 (http://localhost:8181) and allows to observe performance of previous tasks.
<img src="https://github.com/trquoctoann/Subreddit-ETL-Recommendation-System/blob/main/img/spark%20history.png">

3. **Recommendation System** interface. It is accessible through port 5000 (http://localhost:5000) and contains two views:
    1. **Home**: It displays several criteria of tracks, which you can choose according to your preferences. <img src="https://github.com/trquoctoann/Subreddit-ETL-Recommendation-System/blob/main/img/web%20app%20home.png">
    
    2. **Result**: System generates 5 songs that you may like based on your previous selection. <img src="https://github.com/trquoctoann/Subreddit-ETL-Recommendation-System/blob/main/img/web%20app%20result.gif">

To stop the project, run the following command within the docker folder:
```sh
$ sudo docker-compose down
```
