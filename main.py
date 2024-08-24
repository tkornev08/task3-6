# !pip install pyspark
# !pip install install-jdk
# !pip install findspark

import random
import string
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

year = 2024
start_date = datetime(year, 1, 1)
end_date = datetime(year + 1, 1, 1)
def generate_random_name(str):
    return str + ''.join(random.choices(string.ascii_letters, k=6))

num_buy = random.randint(1000,10000)
num_product = random.randint(5,20)
product = [(generate_random_name('Продукт: ')) for i in range(1, num_product + 1)]
buy = [(start_date + timedelta(days=random.randint(0, (end_date - start_date).days)), generate_random_name(''), random.choice(product), random.randint(1, 100), random.randint(100, 10000)) for i in range(1, num_buy + 1)]

spark = SparkSession.builder \
    .appName("Spark SQL Example") \
    .getOrCreate()

data_buy = spark.createDataFrame(buy,['date','user_id','product','quantity','price']) 
data_buy.createOrReplaceTempView("buy")
data_buy.write.option("header",True).format("csv").mode('overwrite').save("/output.csv")
result = spark.sql("SELECT count(product) FROM buy")

result.show()

spark.stop()