# PySpark-Boilerplate
A boilerplate for writing PySpark Jobs

For details see accompanyiong blog post at https://developerzen.com/best-practices-writing-production-grade-pyspark-jobs-cb688ac4d20f#.wg3iv4kie

# Pre-requisite
- Setup the following environment files which resides at `./src/config/fetch_from_mongo` folder.
- Config.env: Credentials are stored in this file. THIS FILE IS NOT PUSHED TO GIT.

    ` 
    
      MONGO_USER_NAME=<mongo-db-user-name>
      MONGO_USER_PWD=<password>
      POSTGRES_USER_NAME=<username> usually postgres
      POSTGRES_USER_PWD=<password> usually postgres
     
     
    `
- Config.json: This configuration varies as per job, information except (credentials) are stored in this file.



# How to run the job in the repository

- Creates the dist folder which has config file, libraries and source code.
   `sh deploy.sh`
- Go to dist folder
    `cd dist`
- Run the job
   `spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 --jars ~/Downloads/postgresql-42.2.24.jar --py-files jobs.zip,libs.zip main.py --job fetch_from_mongo`
