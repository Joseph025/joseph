from pyspark.sql import SparkSession, functions



if __name__ == '__main__':
    # 从hive读取数据 shuffle 并行度，默认200,修改为250
    spark = SparkSession.builder \
        .appName("crm_pre_sync5") \
        .config("spark.sql.crossJoin.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "180") \
        .enableHiveSupport() \
        .getOrCreate()
    df=spark.read.parquet(r'hdfs://TestBigdata/apps/hdfs/wufan.parquet')
    df.show(10)
    df_p=df.toPandas()
    i=df_p[df_p.partitiontime == '2020-07-10'].index.tolist()
    print(i)
    f=df_p.drop(labels=i,inplace=True)
    print(df_p)