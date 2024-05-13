''' 
#########################################################################
################# Create Database and Load Datasets #####################
#########################################################################
'''
# Import Libraries
import psycopg2
import pandas as pd
from sqlalchemy import create_engine

# Define the database connection parameters
db_params = {
    'host': 'localhost',
    'database': 'noaat',
    'user': 'postgres',
    'password': 'password',
    'port': 5432
}

# Create a connection to the PostgreSQL server
connection = psycopg2.connect(
    host = db_params['host'],
    dbname = db_params['database'],
    user = db_params['user'],
    password = db_params['password'],
    port = db_params['port'])

# Create a cursor object
cur = connection.cursor()

# Set automatic commit to be true, so that each action is committed without having to call conn.committ() after each command
connection.set_session(autocommit=True)

# Commit the changes and close the connection to the default database
connection.commit()
cur.close()
connection.close()

# Connect to the 'noaat' database
engine = create_engine(f'postgresql://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}/{db_params["database"]}')

# Define the file paths for your CSV files
csv_files = {
    'promo_code_dataset_v2' : 'D:\\Professional\\Projects for work\\Noaat\\Promo-Abuser\\promo_code_dataset_v2.csv',
    'user_dataset_test_v2'  : 'D:\\Professional\\Projects for work\\Noaat\\Promo-Abuser\\user_dataset_test_v2.csv',
    'user_dataset_train_v2' : 'D:\\Professional\\Projects for work\\Noaat\\Promo-Abuser\\user_dataset_train_v2.csv',
    'user_promo_dataset'    : 'D:\\Professional\\Projects for work\\Noaat\\Promo-Abuser\\user_promo_dataset.csv'
}

# Loop through the CSV files and import them into PostgreSQL
for table_name, file_path in csv_files.items():
    df = pd.read_csv(file_path)
    df.to_sql(table_name, engine, if_exists='replace', index=False)


''' 
#############################################################
################ Extract Data By PySpark  ###################
#############################################################
'''

# Import Libraries
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, concat, col, lit

# Make pyspark importable as a regular library
findspark.init()

# Start Spark Session
spark = SparkSession.builder\
        .config('spark.jars', './postgresql-42.7.3.jar')\
        .master('local')\
        .appName('noaat')\
        .getOrCreate()

# DB Configurations
db_url = "jdbc:postgresql://localhost:5432/noaat"
db_properties = {
    "user": "postgres",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

# Extract tables
user_promo = spark.read.jdbc(db_url, 'user_promo_dataset', properties=db_properties)
promo_code = spark.read.jdbc(db_url, 'promo_code_dataset_v2', properties=db_properties)
users = spark.read.jdbc(db_url, 'user_dataset_train_v2', properties=db_properties)



'''
#######################################################################
################# Transform Data and Apply Queries ####################
#######################################################################
'''
# Query 1: Top 10 users with the highest promo code usage
top_10_users_with_highest_promocodes_usages = user_promo.join(users, 'user_id', 'left')\
                                                        .withColumn('full_name', concat(col('first_name'), lit(' '), col('last_name')))\
                                                        .groupBy('full_name')\
                                                        .count()\
                                                        .orderBy('count', ascending=False)\
                                                        .limit(10)
print(top_10_users_with_highest_promocodes_usages.show())


# Query 2: Top 10 promo codes with the most usage
top_10_promocodes_most_used = user_promo.groupBy('promo_code').count().orderBy('count', ascending=False).limit(10)
print(top_10_promocodes_most_used.show())

# Query 3: Timeline of promo code usage
promocodes_usage_timeline = user_promo.withColumn('y_used_at', date_format('used_at', 'yyyy')).groupBy('y_used_at').count().orderBy('y_used_at', ascending=False)
print(promocodes_usage_timeline.show())

# Query 4: Timeline of user registrations
user_registrations_timline = users.withColumn('y_created_at', date_format('created_at', 'yyyy')).groupBy('y_created_at').count().orderBy('y_created_at', ascending=False)
print(user_registrations_timline.show())



'''
################################################################
################# Create the Data Warehouse ####################
################################################################
'''

# Define the database connection parameters
db_params = {
    'host': 'localhost',
    'database': 'noaat_dw',
    'user': 'postgres',
    'password': 'password',
    'port': 5432
}

# Create a connection to the PostgreSQL server
connection = psycopg2.connect(
    host = db_params['host'],
    dbname = db_params['database'],
    user = db_params['user'],
    password = db_params['password'],
    port = db_params['port'])

# Create a cursor object
cur = connection.cursor()

# Set automatic commit to be true, so that each action is committed without having to call conn.committ() after each command
connection.set_session(autocommit=True)

# Add 4 Tables
connection.execute('\
                   CREATE TABLE IF NOT EXISTS top_10_users_with_highest_promocodes_usages(\
                   full_name VARCHAR(255) PRIMARY KEY\
                   count INT);\
')
connection.execute('\
                   CREATE TABLE IF NOT EXISTS top_10_promocodes_most_used(\
                   promo_code VARCHAR(255) PRIMARY KEY\
                   count INT);\
')
connection.execute('\
                   CREATE TABLE IF NOT EXISTS promocodes_usage_timeline(\
                   y_used_at VARCHAR(255) PRIMARY KEY\
                   count INT);\
')
connection.execute('\
                   CREATE TABLE IF NOT EXISTS user_registrations_timline(\
                   y_created_at VARCHAR(255) PRIMARY KEY\
                   count INT);\
')

# Commit the changes and close the connection to the default database
connection.commit()
cur.close()
connection.close()


'''
##################################################################
################# Load Data To Data Warehouse ####################
##################################################################
'''

# DB Configurations
dw_url = 'jdbc:postgresql://localhost:5432/noaat_dw'
dw_properties = {
    "user": "postgres",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

# Load to data warehouse
top_10_users_with_highest_promocodes_usages.write.format("jdbc")\
         .option("url", dw_url)\
         .option("driver", dw_properties['driver'])\
         .option("dbtable", "top_10_users_with_highest_promocodes_usages")\
         .option("user", dw_properties['user'])\
         .option("password", dw_properties['password'])\
         .save()

top_10_promocodes_most_used.write.format("jdbc")\
         .option("url", dw_url)\
         .option("driver", dw_properties['driver'])\
         .option("dbtable", "top_10_promocodes_most_used")\
         .option("user", dw_properties['user'])\
         .option("password", dw_properties['password'])\
         .save()

promocodes_usage_timeline.write.format("jdbc")\
         .option("url", dw_url)\
         .option("driver", dw_properties['driver'])\
         .option("dbtable", "promocodes_usage_timeline")\
         .option("user", dw_properties['user'])\
         .option("password", dw_properties['password'])\
         .save()

         
user_registrations_timline.write.format("jdbc")\
         .option("url", dw_url)\
         .option("driver", dw_properties['driver'])\
         .option("dbtable", "user_registrations_timline")\
         .option("user", dw_properties['user'])\
         .option("password", dw_properties['password'])\
         .save()

     