from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import pandas as pd
from pyspark.sql.functions import _collect_list_doc
from pandas.core.frame import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructField,StringType,StructType,IntegerType
import numpy as np
import re
from bs4 import BeautifulSoup


def data_clean(s):
    s = str(s)
    s = s.replace('"',"'")
    s = s.replace(',',"。")
    s = s.replace('\n',"")
    s = s.replace('\r',"")
    return s


def clean_content(s):
    s=str(s)
    src_soup = BeautifulSoup(s, 'html5lib')
    if src_soup is not None:
        # get_text得到html内容
        src_soup_text = src_soup.get_text()
        if src_soup_text:
            destStr = src_soup_text.replace('\n', '')
            destStr = destStr.replace('\t', '')
            destStr = re.sub('\\s+', ' ', destStr)
    return destStr


if __name__ == '__main__':
    input_table="quality_carbon_new.es_idms_defect_v4"
    conf = SparkConf()
    cluster_warehouse = 'warehouse_dir'
    local_warehouse = 'file:\\C:\\Users\\wys3160\\software\\tmp\hive2'
    conf.set("spark.sql.warehouse.dir", cluster_warehouse)
    conf.set("spark.debug.maxToStringFields", 1000)
    # conf.set("spark.sql.parquet.binaryAsString","true")
    spark = SparkSession.builder.appName("test") \
        .config('spark.executor.extraJavaOptions', '-Dfile.encoding=utf-8') \
        .config('spark.driver.extraJavaOptionsservice_contract_id', '-Dfile.encoding=utf-8') \
       .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .enableHiveSupport().getOrCreate()
    # list_test=['system-view','screen-lengthdisable']
    # df_test=spark.createDataFrame(DataFrame(list_test),["white"])
    # df_all=df_test.withColumn("name",f.lit("白名单"))
    # df_all.registerTempTable("tmp_all")
    # spark.sql("select concat_ws(',',collect_list(white)) as new_col from tmp_all group by name").show()


    # list=(["201412090248","【8042B54分支】自定义context显示 mad 信息不完整。","<p><span style=""color:#ff0000""><span style=""color:#ff0000"">现场情况与相关操作：<br /></span><span style=""color:#0070c0"">[版本情况]</span></span></p><p><span style=""font-size:12px""></span><span style=""font-size:12px"">[M9014]display&nbsp;version&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<br />H3C&nbsp;Comware&nbsp;Software,&nbsp;Version&nbsp;7.1.054,&nbsp;Release&nbsp;9115&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<br />Copyright&nbsp;(c)&nbsp;2004-2014&nbsp;Hangzhou&nbsp;H3C&nbsp;Tech.&nbsp;Co.,&nbsp;Ltd.&nbsp;All&nbsp;rights&nbsp;reserved.&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<br />H3C&nbsp;SecPath&nbsp;M9014&nbsp;uptime&nbsp;is&nbsp;0&nbsp;weeks,&nbsp;2&nbsp;days,&nbsp;6&nbsp;hours,&nbsp;27&nbsp;minutes&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<br />Last&nbsp;reboot&nbsp;reason&nbsp;:&nbsp;User&nbsp;reboot&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<br /><br />Boot&nbsp;image:&nbsp;flash:/M9000-CMW710-BOOT-R9115.bin&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<br />Boot&nbsp;image&nbsp;version:&nbsp;7.1.054,&nbsp;Release&nbsp;9115&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<br />&nbsp;&nbsp;Compiled&nbsp;Oct&nbsp;22&nbsp;2014&nbsp;10:02:24&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<br />System&nbsp;image:&nbsp;flash:/M9000-CMW710-SYSTEM-R9115.bin&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<br />System&nbsp;image&nbsp;version:&nbsp;7.1.054,&nbsp;Release&nbsp;9115&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<br />&nbsp;&nbsp;Compiled&nbsp;Oct&nbsp;22&nbsp;2014&nbsp;10:02:24&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<br />Feature&nbsp;image(s)&nbsp;list:&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<br />&nbsp;&nbsp;flash:/M9000-CMW710-DEVKIT-R9115.bin,&nbsp;version:&nbsp;7.1.054&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<br />&nbsp;&nbsp;&nbsp;&nbsp;Compiled&nbsp;Oct&nbsp;22&nbsp;2014&nbsp;10:02:24&nbsp;&nbsp;</span></p><p><span style=""font-size:12px""></span>&nbsp;</p><p><span style=""color:#0070c0"">[配置信息]</span></p><p style=""line-height:16px""><img src=""/Ueditor5/dialogs/attachment/fileTypeImages/icon_default.png"" /><a style=""font-size:12px;text-decoration:underline"" href=""http://idms.h3c.com/DownLoad/Index?fileName=9C53944F731234658AAF41F387494D1F9898221929922F79ECD6F9236911F89FBAB4A0B93F139949748D0263F1F503DE865E47F082815A044837B1B790F5F5F630318693BCC76B53""><span style=""font-size:12px"">abc1017.cfg</span></a></p><p><img src=""/Ueditor5/dialogs/attachment/fileTypeImages/icon_default.png"" /><a style=""font-size:12px;text-decoration:underline"" href=""http://idms.h3c.com/DownLoad/Index?fileName=22633E8B3F9BFE48F7BB5831B23592DD2BDCDAF4308F1824ABB636960902A2D4EB0030DAF5A0656A8635B76D17F06026DB55433A7976DCA1E64AF586E2009CC034948F6879774A42""><span style=""font-size:12px"">back.cfg</span></a><span style=""font-size:12px"">--------------自定义context&nbsp;“vlan200”&nbsp;的配置</span></p><p>&nbsp;</p><p style=""line-height:16px""><img src=""/Ueditor5/dialogs/attachment/fileTypeImages/icon_default.png"" /><a style=""font-size:12px;text-decoration:underline"" href=""http://idms.h3c.com/DownLoad/Index?fileName=E77F1D814BF5C091B19A3BB7FFB10420683BB5DDA5A3366C35002C94D178B392399C187941C2BB292DD509EDCE3481F3C5B62938099C0350460E338177EFBA233D2844807459B55217E01EB76769E7EB7E96C0C1153CFD07""><span style=""font-size:12px"">undo-context-vlan200.cfg</span></a><span style=""font-size:12px"">----------------取消vlan&nbsp;200&nbsp;、201到自定义，增加配置vlan&nbsp;400、401的配置</span></p><p style=""line-height:16px"">&nbsp;</p><p style=""line-height:16px"">&nbsp;</p><p style=""line-height:16px""><a href=""http://idms.h3c.com/DownLoad/Index?fileName=22633E8B3F9BFE48F7BB5831B23592DD2BDCDAF4308F1824ABB636960902A2D4EB0030DAF5A0656A8635B76D17F06026DB55433A7976DCA1E64AF586E2009CC034948F6879774A42""></a></p><p><span style=""color:#0070c0"">[组网环境图]</span></p><p><strong><span style=""font-size:12px"">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</span></strong><span style=""font-size:12px"">&nbsp;84.1.1.1&nbsp;&nbsp;&nbsp;85.1.1.1&nbsp;&nbsp;&nbsp;</span></p><p><span style=""color:#000000;font-size:12px"">BPS&nbsp;-------------------58n----------------ten2/7/0/11+M9K&nbsp;+ten2/7/0/13-----------------ar1-----------BPS</span></p><p><span style=""color:#000000""><span style=""color:#000000;font-size:12px"">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;81.1.1.1&nbsp;&nbsp;&nbsp;82.1.1.1&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</span><span style=""color:#000000""></span><span style=""color:#000000""></span><span style=""color:#ff0000""><br /></span><span style=""color:#0070c0"">[测试结论]</span></p><p>&nbsp;【8042B54分支】自定义context显示&nbsp;mad&nbsp;信息不完整。</p><p>&nbsp;</p><p><span style=""color:#0070c0"">[测试步骤]</span></p><p>一、设备使能LACP&nbsp;MAD&nbsp;在默认context显示如下：</p><p style=""text-indent:2em""><span style=""font-size:12px"">[zjzw]dis&nbsp;mad</span></p><p style=""text-indent:2em""><span style=""font-size:12px"">MAD&nbsp;ARP&nbsp;disabled.</span></p><p style=""text-indent:2em""><span style=""font-size:12px"">MAD&nbsp;ND&nbsp;disabled.</span></p><p style=""text-indent:2em""><span style=""color:#c00000;font-size:12px"">MAD&nbsp;LACP&nbsp;enabled.</span></p><p style=""text-indent:2em""><span style=""font-size:12px"">MAD&nbsp;BFD&nbsp;disabled.</span></p><p>&nbsp;</p><p>二&nbsp;、设备使能LACP&nbsp;MAD&nbsp;在自定义context显示如下：显示不完整。</p><p style=""text-indent:2em"">&lt;<span style=""font-size:12px"">zjzw_15&gt;dis&nbsp;mad</span></p><p style=""text-indent:2em""><span style=""font-size:12px"">MAD&nbsp;ARP&nbsp;disabled.</span></p><p style=""text-indent:2em""><span style=""font-size:12px"">MAD&nbsp;ND&nbsp;disabled.</span></p><p style=""text-indent:2em""><span style=""font-size:12px"">MAD&nbsp;BFD&nbsp;disabled.</span></p><p>&nbsp;</p><p><span style=""color:#0070c0"">[初步定位]</span></p><p>建议：&nbsp;irf&nbsp;mad&nbsp;仅在默认context下配置并生效，在自定义context下可以不关注，</p><p>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;如果要显示建议和默认context保持一致。</p>"],
    #       ["201512240172","【艺霖待处理问题单】【同步201512230470】【VDI】【虚拟化管理平台】【D004SP06】本地用户登陆密码没有区分大小写","<p><br /></p><p><span style=""color:#ff0000"">现场情况与相关操作：<br /></span><strong><span style=""color:#00b050"">测试版本：</span></strong><em><span style=""color:#7f7f7f"">（必填，被测对象的详细版本号）</span></em></p><p>E0102P05<br /><strong><span style=""color:#00b050"">配套版本：</span></strong><em><span style=""color:#7f7f7f"">（必填，给出所有配套产品的详细版本号）</span></em></p><p>CAS E0218H06</p><p><strong><span style=""color:#00b050"">组 网 图：</span></strong><em><span style=""color:#7f7f7f"">（必填，若与组网无关只需说明无关即可）</span></em></p><p><br /></p><p><strong><span style=""color:#00b050"">设备脚本：</span></strong><em><span style=""color:#7f7f7f"">（必填，请提供发现问题的当前设备脚本，若问题单与设备MIB无关，填写‘无’）</span></em></p><p><br /></p><p><strong><span style=""color:#00b050"">测试环境：</span></strong><em><span style=""color:#7f7f7f"">（必填，包括计算机配置、操作系统、数据库、浏览器信息等）</span></em></p><p>192.168.0.26</p><p><span style=""color:#ff0000"">详细描述：<br /></span><strong><span style=""color:#00b050"">测试步骤：</span></strong><em><span style=""color:#7f7f7f"">（必填，要求能让其他读者明白如何操作能使问题复现。对于很难复现的问题要尽量详细说明问题出现时的所有操作）\</span></em></p><p>1、本地用户登陆密码没有区分大小写</p><p>比如用户h3c\密码iMC123 使用imc123一样可以登陆获取到虚拟机列表</p><p><strong><span style=""color:#00b050"">问题现象：</span></strong><em><span style=""color:#7f7f7f"">（必填，要求描述出问题的现象，并尽量指明问题所在，可根据需要提供抓图、抓包、日志、设备调试信息。问题现象不仅仅将问题展现出来，还要给出其周边（操作日志、配套产品）有利于定位的相关现象）</span></em></p><p><br /><strong><span style=""color:#00b050"">联机帮助：</span></strong><em><span style=""color:#7f7f7f"">（必填，说明受到影响的联机帮助，如不影响联机帮助填‘无’）</span></em></p><p><br /><strong><span style=""color:#00b050"">初步定位：</span></strong><em><span style=""color:#7f7f7f"">（可选，如果开发人员进行了初步定位，应在此注明定位员工姓名和工号，并给出初步定位结论。如果是提单人自己确定的&quot;初步定位&quot;结果，须给出自己定位的理由和相应的调试信息）<br /></span></em></p><p><br /></p><p><br /></p>"])
    # schema=StructType([StructField("defectNo",StringType(),True),
    #                    StructField("summary",StringType(),True),
    #                    StructField("content",StringType(),True)])
    # df=spark.createDataFrame(list,schema)

    # df.registerTempTable("wufan")
    # spark.udf.register("data_clean",data_clean)
    # spark.sql("select data_clean(name) name,describe,detail from wufan").registerTempTable("tb_content")
    #
    # spark.udf.register("clean_content", clean_content)
    # df_sec = spark.sql("select name,describe,case when detail is not null or detail != null then clean_content(detail) else  detail end detail from tb_content")
    # # df_sec.show()
    # dict(df_sec)

    # # 必须是pandas才能用这个
    # np_list=np.array(df.toPandas()).tolist()
    # print(np.array(np_list))

    spark.sql("""select  OWNER,
                    PMAnalysis,
                    Rname,
                    Vname,
                    adminAdvice,
                    approverComments,
                    att_file_num1,
                    att_file_num3,
                    att_img_num1,
                    att_img_num3,
                    baseline,
                    category,
                    categoryStr,
                    causeAnalysis,
                    content,
                    creationdate,
                    currentNode,
                    currentPerson,
                    cut_words,
                    defectID,
                    defectModifier,
                    defectNo,
                    defect_ODCSeverity,
                    developerComments,
                    issueProcessor,
                    lastProcessed,
                    lastupdateTimestamp,
                    lengthofstay,
                    nodeCode,
                    nodeName,
                    operation_type,
                    productLineName,
                    productName,
                    refresh_timestamp,
                    solution,
                    status,
                    submitBy,
                    submitDate,
                    summary,
                    suspendReason,
                    testReport,
                    testTool,
                    testToolStr,
                    testerComments from quality_carbon_new.es_idms_defect_v4""").write.saveAsTable("wufan.es_idms_defect_v4", mode='overwrite')



    # spark.sql("select defectNo,summary,content from quality_carbon_new.es_idms_defect_v4 limit 10").write.saveAsTable("wufan.es_idms_defect_v4", mode='overwrite')
    spark.stop()