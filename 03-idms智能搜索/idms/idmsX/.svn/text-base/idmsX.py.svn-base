from pyspark.sql import SparkSession

#build spark session
se = SparkSession.builder.appName("IDMS X extract").enableHiveSupport().getOrCreate()

df1 = se.sql("select * from quality_outbound_hive.idms_search_log_recall")
df2 = se.sql("select defectid,richtextid,nodecode from quality_outbound_hive.hive_ml_model_data_prepare_v2")
df3 = se.sql("select richtextid,attached_file_num,attached_img_num from quality_outbound_hive.richtext_attached_info where richtextid is not null")
df4 = se.sql("select a.defectid,a.submitdate from quality_landing_hive.defectmain_all a \
    left join quality_landing_hive.defectmain_incr b on a.defectid = b.defectid \
    where b.defectid is null union all \
    select defectid,submitdate from quality_landing_hive.defectmain_incr")

final_df = df1.join(df4,df1.defectid == df4.defectid,'left_outer').drop(df4.defectid)
final_df = final_df.join(df2,final_df.defectid == df2.defectid,'left_outer').drop(df2.defectid)
final_df = final_df.join(df3,final_df.richtextid == df3.richtextid,'left_outer')
final_df = final_df.drop('richtextid')

df_headers = ["^|~".join(final_df.columns)]
df2rdd = final_df.rdd.map(lambda rows: ["^|~".join([str(row) for row in rows])])
se.createDataFrame(df2rdd,df_headers).write.csv("/test/idmsX",quote='',escape='',escapeQuotes=False,header=True)
