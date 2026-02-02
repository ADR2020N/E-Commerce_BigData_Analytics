from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, collect_set, col, count
from itertools import combinations
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import udf

# Create Spark session
spark = SparkSession.builder \
    .appName("EcommerceProductAffinity") \
    .getOrCreate()

# Load transactions JSON (✅ FIXED PATH)
transactions_df = spark.read.option("multiline", "true") \
    .json("/project/data_raw/transactions.json")

print("=== Transactions Schema ===")
transactions_df.printSchema()

# Explode items array
items_df = transactions_df.select(
    "transaction_id",
    explode("items").alias("item")
).select(
    "transaction_id",
    "item.product_id"
)

print("=== Exploded Items Sample ===")
items_df.show(5, truncate=False)

# Group products per transaction
products_per_txn = items_df.groupBy("transaction_id") \
    .agg(collect_set("product_id").alias("products"))

# Generate product pairs
def generate_pairs(products):
    if len(products) < 2:
        return []
    return list(combinations(sorted(products), 2))

pair_udf = udf(generate_pairs, ArrayType(ArrayType(StringType())))

pairs_df = products_per_txn \
    .withColumn("pairs", pair_udf(col("products"))) \
    .select(explode("pairs").alias("pair"))

# Count frequency of pairs
affinity_df = pairs_df.groupBy("pair") \
    .agg(count("*").alias("frequency")) \
    .orderBy(col("frequency").desc())

print("=== Top Product Affinities ===")
affinity_df.show(10, truncate=False)



# Save results to file
affinity_df \
    .coalesce(1) \
    .write.mode("overwrite") \
    .json("/project/report/product_affinity")

spark.stop()
