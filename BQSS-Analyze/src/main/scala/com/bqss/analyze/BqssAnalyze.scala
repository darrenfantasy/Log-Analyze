package com.bqss.analyze


import java.text.SimpleDateFormat
import java.util.Locale



/**
  * Created by darrenfantasy on 2017/5/17.
  */
object BqssAnalyze extends RunLocally{
  override def getAppName = "BqssAnalyze"
  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println(
        """
          |
          |Arguments:
          |apiLogPath
          |qiniuLogPath
          |resultOutputPath
          |numPartitions    The number of RDD partitions.
          |
        """.stripMargin)
      System.exit(1)
    }

    val apiLogPath = args(0)
    val qiniuLogPath = args(1)
    val resultOutputPath = args(2)
    val numPartitions = args(3).toInt

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val loc = new Locale("en")
    val fm = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss",loc)
    var item = 0L
    val searchs = sc.textFile(apiLogPath,numPartitions).map(log =>{
      item = item+1
      if (log.indexOf("q=") != -1 && log.indexOf("&",log.indexOf("q=")) != -1 && log.indexOf("[") != -1 && log.indexOf("]") != -1){
        val search = new SearchKeyItem
        search.setmId(item)
        search.setmIP(log.substring(0,log.indexOf(" ")))
        val dt = fm.parse(log.substring(log.indexOf("[")+1,log.indexOf("]")))
        search.setmTime(dt.getTime())
        search.setmKey(java.net.URLDecoder.decode(log.substring(log.indexOf("q=")+2,log.indexOf("&",log.indexOf("q="))),"UTF-8"))
        if(log.indexOf("app_id=") != -1 && log.indexOf("&",log.indexOf("app_id=")) != -1){
          var appId=java.net.URLDecoder.decode(log.substring(log.indexOf("app_id=")+7,log.indexOf("&",log.indexOf("app_id="))),"UTF-8")
          print(appId)
          print(MyTools.getAppNameByAppId(appId))
          search.setmAppName(MyTools.getAppNameByAppId(appId))
        }
        search
      }
      else{
        val search = new SearchKeyItem
        search.setmId(item)
        search.setmIP("0.0.0.0")
        search.setmTime(9999999L)
        search.setmKey("hhhhhh")
        search
      }
    }
    )

    sqlContext.createDataFrame(searchs,new SearchKeyItem().getClass).registerTempTable("SearchReuslts")
    val searchsRDD = sqlContext.sql("select * from SearchReuslts").rdd
    val apiResult = searchsRDD.map(row =>{
      val search = new SearchKeyItem
      search.setmIP(row.getString(1))
      search.setmTime(row.getLong(3))
      search.setmKey(row.getString(2))
      search.setmAppName(row.getString(0))
      search
    })

    var qiniuItemNum = 0L
    val qiniuResults= sc.textFile(qiniuLogPath,numPartitions).map(line =>{
      qiniuItemNum = qiniuItemNum+1
      if (line.indexOf("[") != -1 && line.indexOf("]") != -1 && line.indexOf("GET ") != -1 && line.indexOf(" HTTP") != -1){
        val qiniuItem = new QiNiuResultItem
        qiniuItem.setmId(qiniuItemNum)
        qiniuItem.setmIP(line.substring(0,line.indexOf(" ")))
        val dt = fm.parse(line.substring(line.indexOf("[")+1,line.indexOf("]")))
        qiniuItem.setmTime(dt.getTime())
        qiniuItem.setmUrl(line.substring(line.indexOf("GET ")+4,line.indexOf(" HTTP")))
        qiniuItem
      }
      else{
        val qiniuItem = new QiNiuResultItem
        qiniuItem.setmId(qiniuItemNum)
        qiniuItem.setmIP("0.0.0.0")
        qiniuItem.setmTime(1999999909L)
        qiniuItem.setmUrl("hahdadhj")
        qiniuItem
      }
    }
    )
    println("qiniuItemCount"+qiniuItemNum)
    val QiNiuDataFrame = sqlContext.createDataFrame(qiniuResults,new QiNiuResultItem().getClass)
    QiNiuDataFrame.registerTempTable("QiniuResultsTable")



    val senderRdd = sqlContext.sql("select q.mId,q.mIP,q.mTime,q.mUrl from SearchReuslts s,QiniuResultsTable q where s.mIP = q.mIP and q.mTime - s.mTime <10000 and q.mTime - s.mTime >0").rdd
    val senders = senderRdd.map(row =>{
      val analyze = new QiNiuResultItem
      analyze.setmId(row.getLong(0))
      analyze.setmIP(row.getString(1))
      analyze.setmTime(row.getLong(2))
      analyze.setmUrl(row.getString(3))
      analyze
    })
    val SenderDataFrame = sqlContext.createDataFrame(senders,new QiNiuResultItem().getClass)
    SenderDataFrame.registerTempTable("SenderTable")

    val ReceiverDataFrame = QiNiuDataFrame.except(SenderDataFrame)
    ReceiverDataFrame.registerTempTable("ReceiverTable")

//    val analyzeRdd = sqlContext.sql("select first(s.mId),first(s.mIP),first(q.mTime),first(s.mKey),first(q.mUrl),first(s.mAppName) from SearchReuslts s,QiniuResultsTable q where s.mIP = q.mIP and s.mTime - q.mTime <3000 and s.mTime - q.mTime >0 group by q.mId ").rdd
    val analyzeRdd = sqlContext.sql("select s.mId,s.mIP,q.mTime,s.mKey,q.mUrl,s.mAppName from SearchReuslts s,SenderTable q where s.mIP = q.mIP and q.mTime - s.mTime <10000 and q.mTime - s.mTime >0").rdd
    val analyzes = analyzeRdd.map(row =>{
      val analyze = new AnalyzeResultItem
      analyze.setmId(row.getLong(0))
      analyze.setmIP(row.getString(1))
      analyze.setmTime(row.getLong(2))
      analyze.setmKey(row.getString(3))
      analyze.setmUrl(row.getString(4))
      analyze.setmAppName(row.getString(5))
      analyze
    })
    val AnalyzeFrame = sqlContext.createDataFrame(analyzes,new AnalyzeResultItem().getClass)
    AnalyzeFrame.registerTempTable("AnalyzeTable")
    print("QiniuResultsTableahh")

//    sqlContext.sql("select * from QiniuResultsTable").show()
//    sqlContext.sql("select * from ReceiverTable q ,AnalyzeTable a where q.mUrl = a.mUrl and q.mTime>a.mTime").show(20,false)
    val finalResult = sqlContext.sql("select * from ReceiverTable q ,AnalyzeTable a where q.mUrl = a.mUrl and q.mTime>a.mTime").rdd
    val finalRes = finalResult.map(u=>{
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
    val output = finalRes.map{v=>("![]("+v.getmUrl()+")  "+v.getmKey()+"  "+v.getmAppName(),1)}.reduceByKey(_+_)
//    val output = finalRes.map{v=>(v.getmUrl()+v.getmKey()+v.getmAppName(),1)}.reduceByKey(_+_)
    output.saveAsTextFile(resultOutputPath)

  }
}
