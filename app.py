# import os

# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.12:0.12.0 pyspark-shell'

try:
    # init findspark if available
    import findspark
    findspark.init()
except ModuleNotFoundError:
    pass

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

UK_FILE_PATH = r'ConList.xml'
US_FILE_PATH = r'sdn.xml'

def write_parquet(spark_df, path='output.parquet'):
    '''
    This function is a bit of a hack to get around the fact that I wasn't able 
    to get PySpark to write Parquet on this machine.
    
    In practice, it is suboptimal because it requires the whole data set to be 
    loaded into a Pandas DataFrame before it can be written out.
    
    This is acceptable for this exercise since the whole data set is < 50 MB 
    but would not work for a large dataset.
    
    (It may be worth noting that Spark is, in fact, overkill for such a small 
    dataset and that Pandas would be sufficient on its own in this case.)
    '''
    from py4j.protocol import Py4JJavaError
    try:
        spark_df.write.parquet(path, 'overwrite')
    except Py4JJavaError:
        import pyarrow as pa
        import pyarrow.parquet as pq
        pandas_df = spark_df.toPandas()
        table = pa.Table.from_pandas(pandas_df)
        pq.write_table(table, f'{path}/{path}')

# Get or Create Spark Session
spark = SparkSession.builder.getOrCreate()


uk = spark.read.format('xml').option('rowTag', 'ConsolidatedList').load(UK_FILE_PATH)

us = spark.read.format('xml').option('rowTag', 'sdnEntry').load(US_FILE_PATH)

ukNorm = uk.selectExpr(
    "AliasType", 
    "ID", 
    "name1._VALUE as firstName", 
    "Name6 as lastName",
    "GroupID",
    "GroupTypeDescription",
    "'UK' as source")

usPrime = us.selectExpr(
    "'Prime Alias' as AliasType", 
    "uid as ID", 
    "firstName", 
    "lastName", 
    "uid as GroupID",
    "sdnType as GroupTypeDescription",
    "'US' as source")

usAlias = us.withColumn("aka", explode(us.akaList.aka)).selectExpr(
    "upper(replace(aka.type, '.', '')) as AliasType", 
    "aka.uid as ID", 
    "aka.firstName", 
    "aka.lastName", 
    "uid as GroupID",
    "sdnType as GroupTypeDescription", 
    "'US' as source")

output = ukNorm.union(usPrime).union(usAlias)

write_parquet(output)
