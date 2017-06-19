package com.bqss.analyze

import java.text.SimpleDateFormat
import java.util.Locale


/**
  * Created by darrenfantasy on 2017/6/13.
  */
object Test extends RunLocally {
  override def getAppName: String = "Test"

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        """
          |
          |Arguments:
          |apiLogPath
          |qiniuLogPath
          |resultOutputPath
          |numPartitions    The number of RDD partitions.
          |01_1497456083217599625_63454419.snappy.parquet
        """.stripMargin)
      System.exit(1)
    }
    val apiLogPath = args(0)
    val qiniuLogPath = args(1)
    val resultOutputPath = args(2)
    val numPartitions = args(3).toInt

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    sqlContext.setConf("spark.sql.parquet.binaryAsString", "true")
    val loc = new Locale("en")
    val fm = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", loc)

    val APIIuputDataFrame = sqlContext.read.parquet(apiLogPath)
    APIIuputDataFrame.registerTempTable("apiTemp")
    sqlContext.sql("select * from apiTemp t where t.code = 200 and t.api LIKE '%/gifs/search%' or t.api LIKE '%emojis/net/search%' or t.api LIKE '%/stickers/search%'").show(11)
    val apiRdd = sqlContext.sql("select * from apiTemp t where t.code = 200 and t.api LIKE '%/gifs/search%' or t.api LIKE '%emojis/net/search%' or t.api LIKE '%/stickers/search%'").rdd
    val apis = apiRdd.map(row => {
      val api = new SearchKeyItem
      api.setmId(0L)
      api.setmIP(row.getString(0))
      api.setmTime(fm.parse(row.getString(2)).getTime)
      val log = row.getString(5)
      if (log.indexOf("q=") != -1 && log.indexOf("&", log.indexOf("q=")) != -1) {
        var mKey = log.substring(log.indexOf("q=") + 2, log.indexOf("&", log.indexOf("q=")))
        if (mKey.endsWith("%")){
          mKey = mKey + "25"
        }
        api.setmKey(java.net.URLDecoder.decode(mKey, "UTF-8"))
      }else{
        api.setmKey("")
      }
      if (log.indexOf("app_id=") != -1 && log.indexOf("&", log.indexOf("app_id=")) != -1) {
        api.setmAppName(MyTools.getAppNameByAppId(java.net.URLDecoder.decode(log.substring(log.indexOf("app_id=") + 7, log.indexOf("&", log.indexOf("app_id="))), "UTF-8")))
      }else{
        api.setmAppName("None")
      }
      api
    })

    val APIDataFrame = sqlContext.createDataFrame(apis, new SearchKeyItem().getClass)
    APIDataFrame.registerTempTable("SearchReusltsTable")
    sqlContext.sql("select * from SearchReusltsTable").show(20,false)


    val QiNiuInputDataFrame = sqlContext.read.parquet(qiniuLogPath)
    QiNiuInputDataFrame.registerTempTable("qiniuTemp")
    val qiniuRdd = sqlContext.sql("select * from qiniuTemp").rdd
    val qinius = qiniuRdd.map(row => {
      val qiniu = new QiNiuResultItem
      qiniu.setmId(0L)
      qiniu.setmIP(row.getString(0))
      qiniu.setmTime(fm.parse(row.getString(3)).getTime)
      qiniu.setmUrl(row.getString(5))
      qiniu
    }
    )
    val QiNiuDataFrame = sqlContext.createDataFrame(qinius, new QiNiuResultItem().getClass)
    QiNiuDataFrame.registerTempTable("QiniuResultsTable")
    sqlContext.sql("select * from QiniuResultsTable").show(17)
    sqlContext.sql("select * from SearchReusltsTable s,QiniuResultsTable q where s.mIP = q.mIP and q.mTime - s.mTime <10000 and q.mTime - s.mTime >0").show(11)
    val senderRdd = sqlContext.sql("select q.mId,q.mIP,q.mTime,q.mUrl from SearchReusltsTable s,QiniuResultsTable q where s.mIP = q.mIP and q.mTime - s.mTime <10000 and q.mTime - s.mTime >0").rdd
    val senders = senderRdd.map(row => {
      val analyze = new QiNiuResultItem
      analyze.setmId(row.getLong(0))
      analyze.setmIP(row.getString(1))
      analyze.setmTime(row.getLong(2))
      analyze.setmUrl(row.getString(3))
      analyze
    })
    val SenderDataFrame = sqlContext.createDataFrame(senders, new QiNiuResultItem().getClass)
    SenderDataFrame.registerTempTable("SenderTable")

    val ReceiverDataFrame = QiNiuDataFrame.except(SenderDataFrame)
    ReceiverDataFrame.registerTempTable("ReceiverTable")

    val analyzeRdd = sqlContext.sql("select s.mId,s.mIP,q.mTime,s.mKey,q.mUrl,s.mAppName from SearchReusltsTable s,SenderTable q where s.mIP = q.mIP and q.mTime - s.mTime <10000 and q.mTime - s.mTime >0").rdd
    val analyzes = analyzeRdd.map(row => {
      val analyze = new AnalyzeResultItem
      analyze.setmId(row.getLong(0))
      analyze.setmIP(row.getString(1))
      analyze.setmTime(row.getLong(2))
      analyze.setmKey(row.getString(3))
      analyze.setmUrl(row.getString(4))
      analyze.setmAppName(row.getString(5))
      analyze
    })
    val AnalyzeFrame = sqlContext.createDataFrame(analyzes, new AnalyzeResultItem().getClass)
    AnalyzeFrame.registerTempTable("AnalyzeTable")
    print("QiniuResultsTableahh")
    val finalResult = sqlContext.sql("select * from ReceiverTable q ,AnalyzeTable a where q.mUrl = a.mUrl and q.mTime>a.mTime").rdd
    val finalRes = finalResult.map(u => {
      val finalRe = new AnalyzeResultItem
      finalRe.setmId(u.getLong(6))
      finalRe.setmIP(u.getString(0))
      finalRe.setmKey(u.getString(7))
      finalRe.setmTime(u.getLong(2))
      finalRe.setmUrl(u.getString(3))
      finalRe.setmAppName(u.getString(4))
      finalRe
    }
    )
    val output = finalRes.map { v => ("![](" + v.getmUrl() + ")  " + v.getmKey() + "  " + v.getmAppName(), 1) }.reduceByKey(_ + _)
    output.saveAsTextFile(resultOutputPath)













    //    sqlContext.udf.register("dateToLong",(input:String)=>
    //    fm.parse(input).getTime()
    //    )
    //    sqlContext.udf.register("getKeyFromUrl",(input:String)=>
    //      java.net.URLDecoder.decode(input.substring(input.indexOf("q=")+2,input.indexOf("&",input.indexOf("q="))),"UTF-8")
    //    )
    ////    sqlContext.sql("select * from qiniuTemp q,ApiTable s where s.srcIP = q.ip and dateToLong(q.time) - dateToLong(s.sTime)<10000").registerTempTable("SenderTable")
    //    sqlContext.sql("select * from qiniuTemp q,ApiTable s where s.srcIP = q.ip and dateToLong(q.time) - dateToLong(s.sTime)<10000").show(15)
    //    val senderRdd = sqlContext.sql("select * from qiniuTemp q,ApiTable s where s.srcIP = q.ip and dateToLong(q.time) - dateToLong(s.sTime)<10000").rdd
    //    val senders = senderRdd.map(row =>{
    //      val analyze = new QiNiuResultItem
    //      analyze.setmIP(row.getString(0))
    //      analyze.setmTime(fm.parse(row.getString(1)).getTime)
    //      analyze.setmUrl(row.getString(3))
    //      analyze
    //    })
    //
    //    val SenderDataFrame = sqlContext.createDataFrame(senderRdd,new QiNiuResultItem().getClass)
    //    val ReceiverDataFrame = QiNiuDataFrame.except(SenderDataFrame)
    //    ReceiverDataFrame.registerTempTable("ReceiverTable")
    //    sqlContext.sql("select * from ReceiverTable").show(10)
    ////    sqlContext.sql("select s.srcIP,s.params,q.url from ApiTable s,SenderTable q where s.srcIP = q.ip and dateToLong(q.time) - dateToLong(s.sTime)<10000 and dateToLong(q.time) - dateToLong(s.sTime) >0").show()
    ////    val analyzeRdd = sqlContext.sql("select s.srcIP,q.time,s.params,q.url from ApiTable s,SenderTable q where s.srcIP = q.ip and dateToLong(q.time) - dateToLong(s.sTime)<10000 and dateToLong(q.time) - dateToLong(s.sTime) >0").rdd
    //    sqlContext.sql("select s.srcIP,q.time,s.params,q.url from ApiTable s,SenderTable q where s.srcIP = q.ip").show()
    //    val analyzeRdd = sqlContext.sql("select s.srcIP,q.time,s.params,q.url from ApiTable s,SenderTable q where s.srcIP = q.ip ").rdd
    //    println("AnalyzeLength:"+analyzeRdd.collect().length)
    //    val analyzes = analyzeRdd.map(row =>{
    //      val analyze = new AnalyzeResultItem
    //      analyze.setmIP(row.getString(0))
    //      println(row.getString(1))
    //      analyze.setmTime(fm.parse(row.getString(1)).getTime)
    //      analyze.setmKey(java.net.URLDecoder.decode(row.getString(2).substring(row.getString(2).indexOf("q=")+2,row.getString(2).indexOf("&",row.getString(2).indexOf("q="))),"UTF-8"))
    //      analyze.setmUrl(row.getString(3))
    //      val log = row.getString(2)
    //      if(log.indexOf("app_id=") != -1 && log.indexOf("&",log.indexOf("app_id=")) != -1){
    //        var appId=java.net.URLDecoder.decode(log.substring(log.indexOf("app_id=")+7,log.indexOf("&",log.indexOf("app_id="))),"UTF-8")
    //        print(appId)
    //        print(MyTools.getAppNameByAppId(appId))
    //        analyze.setmAppName(MyTools.getAppNameByAppId(appId))
    //      }
    //      analyze
    //    })
    //
    //    val AnalyzeFrame = sqlContext.createDataFrame(analyzes,new AnalyzeResultItem().getClass)
    //    AnalyzeFrame.registerTempTable("AnalyzeTable")
    //    sqlContext.sql("select * from ReceiverTable q ,AnalyzeTable a where q.url= a.mUrl and q.time>a.mTime").show(30)
    //    val finalResult = sqlContext.sql("select * from ReceiverTable q ,AnalyzeTable a where q.url= a.mUrl and q.time>a.mTime").rdd
    //    val finalRes = finalResult.map(u=>{
    //      val finalRe = new AnalyzeResultItem
    //      finalRe.setmKey(u.getString(0))
    //      finalRe.setmUrl(u.getString(1))
    //      finalRe.setmAppName(u.getString(2))
    //      finalRe
    //    }
    //    )
    //    val output = finalRes.map{v=>("![]("+v.getmUrl()+")  "+v.getmKey()+"  "+v.getmAppName(),1)}.reduceByKey(_+_)
    //    output.saveAsTextFile(resultOutputPath)
  }

}
