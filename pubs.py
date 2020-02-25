
"""## **Import the csv file contents**"""

import pyspark.sql.functions as sf
from pyspark.sql.types import *

# Defining the schema based on given description
schema = StructType([
    StructField("fsa_id", IntegerType()),
    StructField("name", StringType()),
    StructField("address", StringType()),
    StructField("postcode", StringType()),
    StructField("easting", IntegerType()),
    StructField("northing", IntegerType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("local_authority", StringType())    
    ])

dataset = spark.read.csv("pubs_in_england.csv", header = True, schema=schema, \
                         mode = 'DROPMALFORMED')

# """*Reading data and checking number of records:*"""

# dataset.show(5)
# dataset.count()

# """**Checking the schema and data statistics**"""

# dataset.printSchema()

# dataset.describe().show()

# """We can see that the count here is **51494** which is lesser than the total number of rows in the given CSV file, which means there were **72** rows not conforming to the schema, so we need to clean it:"""

print("Initial count:", dataset.count())
dataset = dataset.dropna()
print("Cleaned count:", dataset.count())

# """**Checking data for any missing / duplicate entries**"""

# dataset.filter(dataset.fsa_id == '').show()
# dataset.filter(dataset.name == '').show()
# dataset.filter(dataset.address == '').show()
# dataset.filter(dataset.postcode == '').show()
# dataset.filter(dataset.easting == '').show()
# dataset.filter(dataset.northing == '').show()
# dataset.filter(dataset.latitude == '').show()
# dataset.filter(dataset.longitude == '').show()
# dataset.filter(dataset.local_authority == '').show()

dataset = dataset.dropDuplicates()
print("Count after removing duplicates:", dataset.count())

"""As we can see the dataset no longer has any malformed, duplicate or missing data. We can now proceed to perform analysis on the same:

# **2) Which local_authority has the least number of pubs?**
"""

# Grouping by local_authority and counting number of pubs in each group to then order by the least number of pubs
q2 = dataset \
    .groupBy('local_authority') \
    .count() \
    .select(
        'local_authority', \
        sf.col("count").alias("Number of Pubs")
        ).orderBy('Number of Pubs') 

q2.show(1) 

#Alternatively we can use q2.collect()[0] for extracting values.

"""# **4) Which Street in England has the highest number of pubs?**"""

# Splitting the address on ',' and grouping by the street and count. Followed by sorting in descending order to get highest number of pubs for a street.
q4 = dataset.select(
        sf.split("address", ", ")[0].alias("Street in England")
    ).groupBy('Street in England').count().select(
        'Street in England', \
        sf.col("count").alias("Number of Pubs")
        ).orderBy('Number of Pubs', ascending = False) 

q4.show(1)