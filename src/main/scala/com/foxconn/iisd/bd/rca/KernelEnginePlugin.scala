package com.foxconn.iisd.bd.rca

import java.time.format.DateTimeFormatter
import java.util.Locale

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._

object KernelEnginePlugin {

  var configLoader = new ConfigLoader()
  val datetimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.US)

  def main(args: Array[String]): Unit = {

    val limit = 1
    var count = 0

    println("v1")

    while(count < limit) {
      println(s"count: $count")

      try {
        configLoader.setDefaultConfigPath("""conf/default.yaml""")
        if(args.length == 1) {
          configLoader.setDefaultConfigPath(args(0))
        }
        KernelEnginePlugin.start()
      } catch {
        case ex: Exception => {
          ex.printStackTrace()
        }
      }

      count = count + 1

      Thread.sleep(5000)
    }
  }

  def start(): Unit = {

    var date: java.util.Date = new java.util.Date();
    val flag = date.getTime().toString
    println(s"flag: $flag")

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    //val sparkConfigMap = configLoader.getString("minio", "bucket")

    val sparkBuilder = SparkSession
      .builder
      .appName(configLoader.getString("spark", "job_name"))
      .master(configLoader.getString("spark", "master"))

    val confStr = configLoader.getString("spark", "conf")

    val confAry = confStr.split(";").map(_.trim)
    for (i <- 0 until confAry.length) {
      val configKeyValue = confAry(i).split("=").map(_.trim)
      println("conf ===> " + configKeyValue(0) + " : " + configKeyValue(1))
      sparkBuilder.config(configKeyValue(0), configKeyValue(1))
    }

    val spark = sparkBuilder.getOrCreate()

    val configMap = spark.conf.getAll
    for ((k, v) <- configMap) {
      println("[" + k + " = " + v + "]")
    }

    configLoader.setConfig2SparkAddFile(spark)

    var logPathSection = "local_log_path"
    val isFromMinio = configLoader.getString("general", "from_minio").toBoolean
    println("isFromMinio : " + isFromMinio)

    if (isFromMinio) {
      logPathSection = "minio_log_path"

      val endpoint = configLoader.getString("minio", "endpoint")
      val accessKey = configLoader.getString("minio", "accessKey")
      val secretKey = configLoader.getString("minio", "secretKey")
      val bucket = configLoader.getString("minio", "bucket")

      spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", endpoint)
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", "false")
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", accessKey)
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secretKey)
    }

    spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    import spark.implicits._

    val bobcatPath = configLoader.getString(logPathSection, "bobcat_path")

    val bobcatFileLmits = configLoader.getString(logPathSection, "bobcat_file_limits").toInt

    //"sn,station,stationcode,machine,start_time,end_time,istestfail,symptom,desc,uploadtime,emp,ver1,ver2"
    //CN8BS7C334_||_TLEOL_||_TLEOL_||_LC_TLEOL_41_||_11/29/2018 8:33:10 AM_||_11/29/2018 8:38:46 AM_||_0_||__||__||_11/29/2018 8:38:46 AM_||_ _||_ _||_
    val bobcatColumns = configLoader.getString("log_prop", "bobcat_col")

    val dataSeperator = configLoader.getString("log_prop", "log_seperator")

    ///////////
    //載入資料//
    ///////////

    try {
      val bobcatDestPaths = IoUtils.flatMinioFiles(spark,
        flag,
        bobcatPath,
        bobcatFileLmits)

      var bobcatSourceDf = IoUtils.getDfFromPath(spark, bobcatDestPaths.toString, bobcatColumns, dataSeperator)
//      println("bobcatSourceDf count : " + bobcatSourceDf.count())

      val maxIstestfailSourceDf = bobcatSourceDf
                       .distinct()
                       .orderBy("sn","station")
                       .orderBy($"start_time".desc)
                       .groupBy("sn")
                       .agg(max("istestfail").alias("new_istestfail"))
//      println("maxIstestfailSourceDf count : " + maxIstestfailSourceDf.count())

      val joinMaxIstestfailDf = bobcatSourceDf.join(maxIstestfailSourceDf, "sn")
//      println("joinMaxIstestfailDf count : " + joinMaxIstestfailDf.count())

      val symptomAndDescValueDf = joinMaxIstestfailDf
        .filter($"istestfail" === $"new_istestfail")
        .withColumn("descValue", concat($"desc_", lit(":"), $"c3"))
//        .where(bobcatSourceDf("symptom") !== "")
        .groupBy($"sn")
        .agg(concat_ws(";", collect_list($"symptom")).alias("new_symptom"), concat_ws(";", collect_list($"descValue")).alias("descValue"))

//      println("symptomAndDescValueDf count : " + symptomAndDescValueDf.count())
//      symptomAndDescValueDf.show(false)
//      println("symptomAndDescValueDf total rows : " + symptomAndDescValueDf.count())

      val istestfailUdf = udf((istestfail: Integer, symptom: String) => {
        if (istestfail == 0) {
          ""
        } else {
          symptom
        }
      })

      val resultDf = symptomAndDescValueDf
                  .join(joinMaxIstestfailDf, "sn")
                  .dropDuplicates("sn")
                  .withColumnRenamed("istestfail","istestfail_tmp")
                  .withColumnRenamed("new_istestfail","istestfail")
                  .withColumnRenamed("symptom","symptom_tmp")
                  .withColumnRenamed("desc_","desc_tmp")
                  .withColumnRenamed("c3","c3_tmp")
                  .withColumn("symptom", istestfailUdf($"istestfail", $"new_symptom"))
                  .withColumn("desc_", istestfailUdf($"istestfail", $"descValue"))
                  .withColumn("c3", lit("": String))
                  .drop("new_symptom")
                  .drop("descValue")
                  .drop("istestfail_tmp")
                  .drop("symptom_tmp")
                  .drop("desc_tmp")
                  .drop("c3_tmp")

//      println("resultDf count : " + resultDf.count())

//      resultDf.show(false)
//      println("resultDf total rows : " + resultDf.count())

      val bobcat_cols: List[String] = bobcatColumns.split(",").toList
      val bobcat_col: List[Column] = bobcat_cols.map(resultDf(_))

      val resultMacthColDf = resultDf
        .select(bobcat_col:_*)

      resultMacthColDf.printSchema()
      resultMacthColDf.show(false)

      //暫存 , 下次可以從這邊開始
//      resultMacthColDf.coalesce(1).write.format("csv").option("header","false").save("s3a://test/cache")

//      resultMacthColDf.write.format("csv").option("header","false").option("maxRecordsPerFile", 5000).save("s3a://test/" + flag)
//      println("resultMacthColDf count : " + resultMacthColDf.count())

//      val resultMacthColDf = spark.read.format("csv").option("header", "false").load("s3a://test/cache/part-00000-08951b7f-5748-4675-b333-f0833ef49eab-c000.csv")

      val MAX_ROWS = configLoader.getString("general", "max_records_within_a_file").toDouble
      // 計算陣列長度 (看總筆數要分幾份 , 一份5000筆)
      val aryLeng = Math.ceil(resultMacthColDf.count().toDouble / MAX_ROWS).toInt
      println("aryLeng : " + aryLeng)
      // 1 / 長度
      val randonSplitValue: Double = 1.0 / aryLeng.toDouble
      println("randonSplitValue : " + randonSplitValue)
      // 取到小數點第三位
      val randonSplitValue3f =  randonSplitValue.formatted("%.3f")
      println("randonSplitValue3f : " + randonSplitValue3f)
      val splitAry = new Array[Double](aryLeng)
      var sum = 0.0
      for(i <- 0 to splitAry.length-2) {
        splitAry(i) = randonSplitValue3f.toDouble
        sum = sum + randonSplitValue3f.toDouble
      }
      println("sum : " + sum)
      // 1 - 全部前面相加 , 也就是最後一份的比例
      val lastValue = 1.0 - sum
      println("lastValue : " + lastValue)
      splitAry(splitAry.length-1) = lastValue

      // dataframe按照給的比例去做分割
      val outputDataSet = resultMacthColDf.randomSplit(splitAry)
      println("outputDataSet length : " + outputDataSet.length)
      for(i <- 0 to outputDataSet.length-1) {
        outputDataSet(i)
          .map(x => x.mkString("", "#", "").replaceAll("#", dataSeperator))
          .coalesce(1)
          .write
          .text(configLoader.getString(logPathSection, "bobcat_d_tmp_path") + flag + "/" + i)
      }

      IoUtils.moveFileFromBobcatD(spark, flag, configLoader.getString(logPathSection, "bobcat_d_tmp_path") ,
        configLoader.getString(logPathSection, "bobcat_output_path"))


    } catch {
    case ex: Exception => {
      println("===> " + ex.printStackTrace())
    }
  }

  }

}

