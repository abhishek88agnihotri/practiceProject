from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, row_number, monotonically_increasing_id
from pyspark.sql import Window
import requests
from Config import url,Conn_details
#import psycopg2


conf = SparkConf().setMaster("local").setAppName("reading json")
sc = SparkContext(conf = conf)

spark = SparkSession \
        .builder \
        .appName("reading json") \
        .getOrCreate()


def get_word_details(url):
    response = requests.get(url)
    return response


def data_response(response):
    response_list = []
    response_list.append(response.text)
    json_rdd = sc.parallelize(response_list)
    df = spark.read.json(json_rdd)

    data_df = df.select(col("MRData.*")).select("series",col("RaceTable.*")).select("series",explode("Races").alias("Races"))

    races_df = data_df.select("series","Races.*")
    circuit_df = races_df.select("series","season", "round", "raceName", "Circuit.*", explode("Results").alias("Results"))

    results_df = circuit_df.select(circuit_df['*'], "Results.*")
    info_df = results_df.select("series","season", "round", "raceName", "Location.country", "circuitId", "circuitName",
                               "Results.position", "Results.points", "Results.Driver",
                               "Results.Constructor", "Results.grid", "Results.laps", "Results.status", "Results.FastestLap")


    final_df = info_df.select("series", "season", "round", "raceName", "country", "circuitId", "circuitName",
                              "position",
                              "points",
                              "Driver.driverId", col("Driver.givenName").alias("driverFirstName"),
                              col("Driver.familyName").alias("driverFamilyName"), "Driver.nationality",
                              "Constructor.constructorId",
                              col("Constructor.name").alias("teamName"), "grid", "laps", "status", col("FastestLap.lap").alias("FastestLap"),
                              col("FastestLap.Time.time").alias("FastestTime")) \
                        .withColumn("monotonically_increasing_id", monotonically_increasing_id())

    window = Window.orderBy(col('monotonically_increasing_id'))
    index_df = final_df.withColumn('Unique_id', row_number().over(window)).drop("monotonically_increasing_id")

    return index_df

def save_data(datadf,Conn_details):

    url = Conn_details.get("url")
    dbtable = Conn_details.get("dbtable")
    user = Conn_details.get("user")
    password = Conn_details.get("password")
    driver = Conn_details.get("driver")

    datadf.show()
    datadf.write \
        .mode("overwrite") \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", dbtable) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", driver) \
        .save()


def main(url):
    #url = "http://ergast.com/api/f1/2019/1/results.json"
    response = get_word_details(url)
    datadf = data_response(response)
    save_data(datadf,Conn_details)


if __name__ == "__main__":
    main(url)
