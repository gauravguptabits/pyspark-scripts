echo '======= STEP 1: Cleaning environment ========'
make clean
rm -rf src/libs
mkdir src/libs
echo '======= STEP 2: Installing deps ============='
pip install -r requirements.txt -t src/libs
pip install -r dev_requirements.txt -t src/libs
echo '======= STEP 3: Preparing distribution ========='
make build
cd ./dist
echo '=============================================== BUILD PROCESS COMPLETED==============================================================='
echo 'Run spark submit command to execute the build. Sample command is as follows:'
echo 'spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 --py-files jobs.zip,libs.zip main.py --job fetch_from_mongo'
echo 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
