from pyspark.sql import SparkSession
from pyspark.sql.functions import count, max, col, when

# Create a SparkSession
spark = SparkSession.builder.master("local").getOrCreate()

# Read a large JSON file (>10 TB)
df = spark.read.json("C/rnu_4_5_json")

# Group by 'id_pk' and count 'dt_account', exclude 'id_pk' 117
dt_accountCount = df.groupBy("id_pk").agg(count("dt_account").alias("dtCount")).filter(col("id_pk") != 117)

# Group by 'id_pk' and count 'kt_account', exclude 'id_pk' 117
kt_accountCount = df.groupBy("id_pk").agg(count("kt_account").alias("ktCount")).filter(col("id_pk") != 117)

# Find max of 'dt_account' for each 'id_pk'
dt_max = df.groupBy("id_pk").agg(max("dt_account").alias("dtCountMax"))

# Find max of 'kt_account' for each 'id_pk'
kt_max = df.groupBy("id_pk").agg(max("kt_account").alias("ktCountMax"))

# Join the dataframes and exclude 'id_pk' 7747
resultDf = dt_accountCount \
    .join(kt_accountCount, ["id_pk"]) \
    .join(dt_max, ["id_pk"]) \
    .join(kt_max, ["id_pk"]) \
    .filter(col("id_pk") != 7747)

# Read a small JSON file (smaller than 10 MB)
df2Dict = spark.read.json("C/dictionary")

# Create a list from df2Dict 'id_pk'
ListFromDF2 = [row["id_pk"] for row in df2Dict.select("id_pk").collect()]

# Filter 'resultDf' to exclude rows present in 'ListFromDF2'
veryFinalResult = resultDf.filter(~col("id_pk").isin(ListFromDF2))

# Show the first 10 rows
veryFinalResult.show(10, truncate=False)

# Read another small JSON file (smaller than 10 MB)
dict2 = spark.read.json("C/dict2")

# Join 'veryFinalResult' with 'dict2' and handle nulls in 'izz_number'
finalDf = veryFinalResult.join(dict2, ["id_pk"], "left") \
    .withColumn("izz_number", when(col("izz_number").isNull(), "dssf111").otherwise(col("izz_number")))

# Write the result to a CSV file
finalDf.write.csv("folder")
