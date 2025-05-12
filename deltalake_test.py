from pyspark.sql import SparkSession

# Define configurations as a dictionary
spark_conf = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.access.key": "3JQWQCTIFWVBOHJFBVB2",  # Replace with your access key
    "spark.hadoop.fs.s3a.secret.key": "AVUmAIzJ8upmliyW5w03LBndXoelP7cWAu3zVeea",  # Replace with your secret key
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.endpoint": "s3.eu-central-1.wasabisys.com",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
}

# Initialize SparkSession with Delta and S3 configurations
spark = (
    SparkSession.builder
    .appName("DeltaLakeWithS3")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-s3:1.12.372")
)

# Apply configurations
for key, value in spark_conf.items():
    spark = spark.config(key, value)

# Start the session
spark = spark.getOrCreate()

# Example: Create a Delta table in S3 (equivalent to your CREATE TABLE AS SELECT)
df = spark.createDataFrame([(0,), (1,), (2,), (3,), (4,)], ["id"])
df.write.format("delta").save("s3a://lbr-files/GEX/Delta3")

# Example: Read the Delta table
spark.read.format("delta").load("s3a://lbr-files/GEX/Delta3").show()
