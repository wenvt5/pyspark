from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession

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

def sparkTest():
    my_spark = SparkSession.builder.getOrCreate()
    # Print the tables in the catalog
    print(my_spark.catalog.listTables())
    query = "SELECT origin, dest, COUNT(*) as N FROM flights GROUP BY origin, dest"
    # Get the first 10 rows of flights
    flight_counts = my_spark.sql(query)
    # Show the results
    flight_counts.show()
    # Convert the results to a pandas DataFrame
    pd_counts = flight_counts.toPandas()
    print(pd_counts.head())
