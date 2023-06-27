from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.types import DataType

spark = SparkSession.builder.appName("Hotel_Bookings").master("yarn").getOrCreate()

'''Read the file as an RDD of strings'''
lines_rdd = spark.sparkContext.textFile("s3a://mybucket/input/bookings.csv")

''' Process the RDD as needed
    For example, you can split each line by comma (",") to get individual values'''
values_rdd = lines_rdd.map(lambda line: line.split(","))

'''Get the first row of the RDD'''
first_row = values_rdd.first()

''' Filter out the first row from the RDD'''
data_rdd = values_rdd.filter(lambda x: x != first_row)

'''Convert the first row to columns'''
header_rdd = spark.sparkContext.parallelize([first_row])
columns_rdd = header_rdd.union(data_rdd)

'''Display the RDD with the first row as columns'''
df = data_rdd.toDF(first_row)
df.show()

'''here extract every booking that the Tour Operators as Market Segment designations'''
to_bookings_df = df.where(df.market_segment == "Offline TA/TO")
to_bookings_df.show()


int_df = to_bookings_df.withColumn("stays_in_weekend_nights", col("stays_in_weekend_nights").cast("int"))\
    .withColumn("stays_in_week_nights", col("stays_in_week_nights").cast("int"))

int_df.show()

'''Add the arrival_date field
 Add the departure_date field'''
add_df = int_df.withColumn("arrival_date", to_date(concat(col("arrival_date_year"), col("arrival_date_month"), col("arrival_date_day_of_month")), "yyyyMMMMd"))\
    .withColumn("departure_date", expr("date_add(arrival_date, stays_in_weekend_nights + stays_in_week_nights)"))

add_df.printSchema()
add_df.show()

'''Read from CSV files Finally, an extra field with_family_breakfast . The with_family_breakfast field will have Yes if
the sum of children and babies is greater than zero otherwise No '''

family_df = add_df.withColumn("with_family_breakfast", when((col("children") + col("babies")) > 0, "Yes").otherwise("No"))
family_df.printSchema()
family_df.show()

# Write DataFrame to Parquet file with additional options
data="s3a://mybucket/output/final_hotel.parquet"
family_df.write.format("parquet").mode("overwrite").option("compression", "snappy").save("data")

'''below is the spark submit code run on yarn'''
#usr/bin/spark-submit --master yarn --deploy-mode client --driver-memory 3g --executor-memory
#2g --num-executors 1 --executor-cores 1 /home/hadoop/hotel.py




