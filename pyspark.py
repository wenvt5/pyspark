from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def sparkStream():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")

    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")
    ts = time.time()
    counts = stream(ssc, pwords, nwords, 100)
    make_plot(counts)

def spark_query():
    # Print the tables in the catalog
    print(spark.catalog.listTables())
    query = "SELECT origin, dest, COUNT(*) as N FROM flights GROUP BY origin, dest"
    # Get the first 10 rows of flights
    flight_counts = spark.sql(query)
    # Show the results
    flight_counts.show()
    # Convert the results to a pandas DataFrame
    pd_counts = flight_counts.toPandas()
    print(pd_counts.head())


def dataFrame_Catalog():
    # Create pd_temp
    pd_temp = pd.DataFrame(np.random.random(10))
    # Create spark_temp from pd_temp
    spark_temp = spark.createDataFrame(pd_temp)
    # Examine the tables in the catalog
    print(spark.catalog.listTables())
    # Add spark_temp to the catalog
    spark_temp.createOrReplaceTempView("temp")
    # Examine the tables in the catalog again
    print(spark.catalog.listTables())


def spark_from_csv():
    file_path = "/usr/local/share/datasets/airports.csv"
    # Read in the airports data
    airports = spark.read.csv(file_path, header=True)
    # Show the data
    airports.show()


def add_column():
    # Create the DataFrame flights
    flights = spark.table("flights")
    # Show the head
    print(flights.show())
    # Add duration_hrs
    flights = flights.withColumn("duration_hrs", flights.air_time/60)


def filter_dataFrame():
    # Filter flights with a SQL string
    long_flights1 = flights.filter("distance > 1000")
    # Filter flights with a boolean column
    long_flights2 = flights.filter(flights.distance > 1000)
    # Examine the data to check they're equal
    print(long_flights1.show())
    print(long_flights2.show())
