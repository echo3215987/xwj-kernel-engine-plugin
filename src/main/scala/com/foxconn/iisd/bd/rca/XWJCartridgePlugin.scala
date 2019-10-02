package com.foxconn.iisd.bd.rca

import java.net.{InetAddress, URI}
import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.util.{Date, Locale}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{regexp_extract, when, _}
import org.apache.spark.sql.types._

import scala.collection.mutable.WrappedArray
import scala.collection.mutable.Seq
import com.foxconn.iisd.bd.rca.SparkUDF._
import org.apache.spark.storage.StorageLevel
import org.apache.hadoop.fs.{FileSystem, Path}

object XWJCartridgePlugin {

  var configLoader = new ConfigLoader()
  val datetimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.US)

  var totalRawDataSize: Long = 0
  val mb = 1024*1024
  val runtime = Runtime.getRuntime
  var jobId = ""
  var jobYear = ""
  var jobMonth = ""
  var jobDay = ""
  var jobHour = ""
  var jobMinute = ""
  var jobSecond = ""
  var jobStatus = false
  var bobcatXWJPath = ""
  var testDetailPath = ""

  val ctrlACode = "\001"
  val ctrlCCode = "\003"
  val downLimit = "_DOWNLIMIT"
  val upLimit = "_UPLIMIT"
  val lowerLimit = "_LOWER_LIMIT"
  val upperLimit = "_UPPER_LIMIT"

  def main(args: Array[String]): Unit = {

    val limit = 1
    var count = 0

    println("xwj-plugin-cartridge-nesta-v3")

    while(count < limit) {

      println(s"count: $count")
      configLoader.setDefaultConfigPath("""conf/default.yaml""")
      if(args.length == 1) {
        configLoader.setDefaultConfigPath(args(0))
      }

      jobId = getHostName()
      println("job id : " + jobId)

      val sparkBuilder = SparkSession
        .builder
        .appName(configLoader.getString("spark", "job_name"))
        .master(configLoader.getString("spark", "master"))

      val confStr = configLoader.getString("spark", "conf")

      val confAry = confStr.split(";").map(_.trim)
      for(i <- 0 until confAry.length) {
        val configKeyValue = confAry(i).split("=").map(_.trim)
        println("conf ===> " + configKeyValue(0) + " : " + configKeyValue(1))
        sparkBuilder.config(configKeyValue(0), configKeyValue(1))
      }

      val spark = sparkBuilder.getOrCreate()

      val configMap = spark.conf.getAll
      for ((k,v) <- configMap) {
        println("[" + k + " = " + v + "]")
      }

      try {
//        jobId = "rca-ke-dev-uuid-20190830100000-driver"
        jobYear = jobId.split("-uuid-")(1).split("-")(0).slice(0, 4)
        jobMonth = jobId.split("-uuid-")(1).split("-")(0).slice(4, 6)
        jobDay = jobId.split("-uuid-")(1).split("-")(0).slice(6, 8)
        jobHour = jobId.split("-uuid-")(1).split("-")(0).slice(8, 10)
        jobMinute = jobId.split("-uuid-")(1).split("-")(0).slice(10, 12)
        jobSecond = jobId.split("-uuid-")(1).split("-")(0).slice(12, 14)
        //        Summary.setJobId(jobId) TODO
        XWJCartridgePlugin.start(spark)
      } catch {
        case ex: Exception => {
          ex.printStackTrace()
        }
      }
      finally {
        IoUtils.moveFilesByJobStatus(
          spark,
          bobcatXWJPath,
          jobStatus,
          jobId,
          jobYear + jobMonth,
          jobDay,
          jobHour + jobMinute + jobSecond)
      }

      count = count + 1

      Thread.sleep(5000)
    }
  }

  def start(spark: SparkSession): Unit = {

    println("** Used Memory:  " + (runtime.totalMemory - runtime.freeMemory) / mb + " MB")
    println("** Free Memory:  " + runtime.freeMemory / mb + " MB")
    println("** Total Memory: " + runtime.totalMemory / mb + " MB")
    println("** Max Memory:   " + runtime.maxMemory / mb + " MB")

    var date: java.util.Date = new java.util.Date()
    val flag = date.getTime().toString

    val jobStartTime: String = new SimpleDateFormat(
      configLoader.getString("summary_log_path","job_fmt")).format(date.getTime())
    println("job start time : " + jobStartTime)
    println(s"flag: $flag")

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

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

    import spark.implicits._
//    val numExecutors = spark.conf.get("spark.executor.instances", "1").toInt
    val testDetailDTFmt = configLoader.getString("log_prop", "test_detail_dt_fmt")

    val factory = configLoader.getString("general", "factory")

    //Bobcat_d_xwj
    val bobcatXWJColumnStr = configLoader.getString("log_prop", "bobcat_xwj_col")
    bobcatXWJPath = configLoader.getString(logPathSection, "bobcat_d_xwj_path")
//    val bobcatDXWJTempPath = configLoader.getString(logPathSection, "bobcat_d_xwj_tmp_path")
    val bobcatXWJFileLmits = configLoader.getString(logPathSection, "bobcat_d_xwj_file_limits").toInt

    testDetailPath = configLoader.getString(logPathSection, "test_detail_path")
    val testDetailFileLmits = configLoader.getString(logPathSection, "test_detail_file_limits").toInt

    //"sn,build_name,build_description,unit_number,station_id,test_status,test_starttime,test_endtime,list_of_failure,list_of_failure_detail,test_phase,machine_id,factory_code,floor,line_id,test_item,test_value,test_unit,test_lower,test_upper,create_time,update_time,station_name,start_date,product,test_version"
    //CN95I870ZC06MD_||_SOR_||_SOR_||_CN95I870ZC06MD_||_L7_TLEOL_06_||_Exception_||_2019/05/18 06:36_||_2019/05/18 06:36_||_PcaVerifyFirmwareRev_||_Error_||_MP_||__||_CQ_||_D62_||_2_||_ProcPCClockSync^DResultInfo^APcaVerifyFirmwareRev^DResultInfo^APcaVerifyFirmwareRev^DExpectedVersion^APcaVerifyFirmwareRev^DReadVersion^APcaVerifyFirmwareRev^DDateTimeStarted^APcaVerifyFirmwareRev^DActualFWUpdate^APcaVerifyFirmwareRev^DFWUpdateDSIDFirst_||_ProcPCClockSync^DResultInfo^C^APcaVerifyFirmwareRev^DResultInfo^C^APcaVerifyFirmwareRev^DExpectedVersion^C^APcaVerifyFirmwareRev^DReadVersion^C^APcaVerifyFirmwareRev^DDateTimeStarted^C5/18/2019 5:29:48 AM^APcaVerifyFirmwareRev^DActualFWUpdate^C^APcaVerifyFirmwareRev^DFWUpdateDSIDFirst^C_||_ProcPCClockSync^DResultInfo^C^APcaVerifyFirmwareRev^DResultInfo^C^APcaVerifyFirmwareRev^DExpectedVersion^C^APcaVerifyFirmwareRev^DReadVersion^C^APcaVerifyFirmwareRev^DDateTimeStarted^C^APcaVerifyFirmwareRev^DActualFWUpdate^C^APcaVerifyFirmwareRev^DFWUpdateDSIDFirst^C_||_ProcPCClockSync^DResultInfo^C^APcaVerifyFirmwareRev^DResultInfo^C^APcaVerifyFirmwareRev^DExpectedVersion^C^APcaVerifyFirmwareRev^DReadVersion^CTJP1FN1845AR^APcaVerifyFirmwareRev^DDateTimeStarted^C^APcaVerifyFirmwareRev^DActualFWUpdate^C169^APcaVerifyFirmwareRev^DFWUpdateDSIDFirst^C_||_ProcPCClockSync^DResultInfo^C^APcaVerifyFirmwareRev^DResultInfo^C^APcaVerifyFirmwareRev^DExpectedVersion^C^APcaVerifyFirmwareRev^DReadVersion^CTJP1FN1845AR^APcaVerifyFirmwareRev^DDateTimeStarted^C^APcaVerifyFirmwareRev^DActualFWUpdate^C169^APcaVerifyFirmwareRev^DFWUpdateDSIDFirst^C_||_2019/05/18 06:36_||_2019/05/18 06:36_||_TLEOL_||_2019/05/18 06:36_||_TaiJi Base_||_42.3.8 REV_37_Taiji25
    val testDetailColumnStr = configLoader.getString("log_prop", "test_detail_col")

    val dataSeperator = configLoader.getString("log_prop", "log_seperator")
    val dataSeperatorNonEscape = configLoader.getString("log_prop", "log_seperator_non_escape")

    val mbLimits = configLoader.getString("log_prop", "mb_limits").toInt

    ///////////
    //載入資料//
    ///////////

    try {

//      val bobcatDestPaths = IoUtils.flatMinioFiles(spark,
//        flag,
//        bobcatXWJPath, //source
//        bobcatXWJFileLmits
//      )

      val bobcatDestPath = IoUtils.flatMinioFiles(spark,
        bobcatXWJPath,
        (mbLimits * 1024 * 1024),
        jobYear + jobMonth,
        jobDay,
        jobHour + jobMinute + jobSecond)

//        val bobcatDestPaths = new Path("s3a://rca-dev/Cartridge-Nesta/Data/Bobcat_D_XWJ/20190725/*")
//        val bobcatDestPaths = new Path("s3a://rca-dev/Cartridge-Nesta/Data/Bobcat_D_XWJ/20190725/F6U16-30001_bob_0_TEST_R_20190826085047891.txt")
        var bobcatSourceDf = IoUtils.getDfFromPath(spark, bobcatDestPath.toString, bobcatXWJColumnStr, dataSeperator)

        println("bobcatSourceDf from file, Used Memory:  " + (runtime.totalMemory - runtime.freeMemory) / mb + " MB")

        var sourceCount = bobcatSourceDf.count()
        println("source_count: " + bobcatSourceDf.count())

        if(sourceCount > 0){
          //        bobcatSourceDf.show(1, false)

          val specs = List(downLimit, upLimit, lowerLimit, upperLimit)
          val lowerSpecs = List(downLimit, lowerLimit)
          val upperSpecs = List(upLimit, upperLimit)
          val excludeItems = List("PRE_WEIGHT:PRE_WEIGHT", "POST_WEIGHT:POST_WEIGHT")
          //filter exclude test item(PRE_WEIGHT:PRE_WEIGHT, POST_WEIGHT:POST_WEIGHT)
          bobcatSourceDf = bobcatSourceDf.filter(row => !excludeItems.exists(row.getAs("test_item").toString.contains))
            .withColumn("product", split(getLast(split(expr("input_file_name()"), "/")), "_").getItem(0))
            .withColumn("test_version", lit("VER"))
            //沒有unit的値, 先留空值
            .withColumn("test_value", concat_ws(ctrlCCode, col("test_item"), col("test_value_temp")))
            .withColumn("test_unit", lit(""))
            .withColumn("test_unit", concat_ws(ctrlCCode, col("test_item"), col("test_unit")))
            .withColumn("test_item_result", replaceTestResult(col("test_item_result_temp")))
            .withColumn("test_item_result", concat_ws(ctrlCCode, col("test_item"), col("test_item_result")))
            .withColumn("test_item_result_detail_temp", replaceTestResultDetail(col("test_item_result_detail_temp")))
            .withColumn("test_item_result_detail", col("test_item_result_detail_temp"))
            .withColumn("test_item_result_detail", concat_ws(ctrlCCode, col("test_item"), col("test_item_result_detail")))

          val specCols = List("product", "sn", "station_name", "test_starttime", "test_version", "test_item", "test_value_temp")
          val specDf = bobcatSourceDf.filter(row => specs.exists(row.getAs("test_item").toString.contains))
            //product, sn, station_name, test_starttime, test_version, test_item
            .selectExpr(specCols: _*)
            .dropDuplicates()
            .withColumnRenamed("test_item", "test_item_spec")
            .withColumn("test_item", regexp_replace(col("test_item_spec"), lowerLimit + "|" + downLimit + "|" + upperLimit + "|" + upLimit, ""))
            .withColumn("test_lower", when(col("test_item_spec").contains(downLimit).or(col("test_item_spec").contains(lowerLimit)), col("test_value_temp")))
            .withColumn("test_upper", when(col("test_item_spec").contains(upLimit).or(col("test_item_spec").contains(upperLimit)), col("test_value_temp")))
            .withColumn("test_lower", when(col("test_lower").isNotNull, concat_ws(ctrlCCode, col("test_item"), col("test_lower"))))
            .withColumn("test_upper", when(col("test_upper").isNotNull, concat_ws(ctrlCCode, col("test_item"), col("test_upper"))))

          //        specDf.show(false)

          //exclude test_item_spec
          bobcatSourceDf = bobcatSourceDf.filter(row => !specs.exists(row.getAs("test_item").toString.contains))

          val bobcatDistinctDf = bobcatSourceDf.dropDuplicates("product", "sn", "station_name", "test_starttime", "test_version", "test_item")

          val specGroupbyCols = List("product", "sn", "station_name", "test_starttime", "test_version", "test_item")
          val cols = List("test_lower", "test_upper")
          var specTempJoinBobDf = spark.emptyDataFrame
          for(colName <- cols){
            //add colName element into specGroupbyCols list
            val tempCols = colName :: specGroupbyCols
            val specTempDf = specDf.selectExpr(tempCols: _*).filter(col(colName).isNotNull)
            if(specTempJoinBobDf.isEmpty)
              specTempJoinBobDf = bobcatDistinctDf.join(specTempDf, specGroupbyCols, "left")
            else
              specTempJoinBobDf = specTempJoinBobDf.join(specTempDf, specGroupbyCols, "left")
          }

          specTempJoinBobDf = specTempJoinBobDf.groupBy("product", "sn", "station_name", "test_starttime", "test_version")
            .agg(concat_ws(ctrlACode, collect_list("test_item")).as("test_item"),
              concat_ws(ctrlACode, collect_list("test_upper")).as("test_upper"),
              concat_ws(ctrlACode, collect_list("test_lower")).as("test_lower"),
              concat_ws(ctrlACode, collect_list("test_unit")).as("test_unit"),
              concat_ws(ctrlACode, collect_list("test_value")).as("test_value"),
              concat_ws(ctrlACode, collect_list("test_item_result")).as("test_item_result"),
              concat_ws(ctrlACode, collect_list("test_item_result_detail")).as("test_item_result_detail"),
              max(col("test_item_result_temp")).as("test_status")
            ).persist(StorageLevel.MEMORY_AND_DISK)

          //failure list, use ^A(SOH) separation
          val failDf = bobcatSourceDf.filter(col("test_item_result_temp").equalTo(1))
            .groupBy("product", "sn", "station_name", "test_starttime", "test_version")
            .agg(concat_ws(ctrlACode, collect_list("test_item")).as("list_of_failure"),
              concat_ws(ctrlACode, collect_set("test_item_result_detail_temp")).as("list_of_failure_detail"))

          val groupByCols = List("product", "sn", "station_name", "test_starttime", "test_version")
          var finalResultsDF = specTempJoinBobDf.join(failDf, groupByCols, "left")

          //gen output file
          //(1)add fixed value column
          finalResultsDF = finalResultsDF.withColumn("build_name", lit("MP"))
            .withColumn("build_description", lit("MP"))
            .withColumn("unit_number", col("sn"))
            .withColumn("station_id", col("station_name"))
            .withColumn("test_endtime", col("test_starttime"))
            .withColumn("test_phase", lit(""))
            .withColumn("factory_code", lit("LH"))
            .withColumn("floor", lit("D2-4F"))
            .withColumn("line_id", lit("null"))
            .withColumn("create_time", col("test_starttime"))
            .withColumn("update_time", col("test_starttime"))
            .withColumn("start_date", col("test_starttime"))
          finalResultsDF = finalResultsDF.join(bobcatDistinctDf.dropDuplicates(groupByCols)
            .select("product", "sn", "station_name", "test_starttime", "test_version", "machine_id"),
            groupByCols)

          //(2)replace test_status value
          finalResultsDF = finalResultsDF.withColumn("test_status", replaceTestResult(col("test_status")))

          //(3)if test_upper or test_lower is null, give 'test_item^'
          finalResultsDF = finalResultsDF.withColumn("test_upper",
            when(col("test_upper").equalTo(""), genTestItemSpec(col("test_item"))).otherwise(col("test_upper")))
            .withColumn("test_lower", when(col("test_lower").equalTo(""), genTestItemSpec(col("test_item"))).otherwise(col("test_lower")))
          //(4)if list_of_failure or list_of_failure_detail is null, give ''
          finalResultsDF = finalResultsDF.withColumn("list_of_failure",
            when(col("list_of_failure").isNull, lit("")).otherwise(col("list_of_failure")))
            .withColumn("list_of_failure_detail",
              when(col("list_of_failure_detail").isNull, lit("")).otherwise(col("list_of_failure_detail")))
            .persist(StorageLevel.MEMORY_AND_DISK)
          //        finalResultsDF.show(false)
          //        finalResultsDF.where(col("test_status").equalTo("fail")).show(false)

          val testDetailColumns = testDetailColumnStr.toUpperCase.split(",")
          println("final_count: " + finalResultsDF.count())

          finalResultsDF
            .selectExpr(testDetailColumns: _*)
            .map(x => x.mkString("", dataSeperatorNonEscape, ""))
            .coalesce(1)
            .write
            .text(testDetailPath + jobYear + "/" + jobMonth + "/" + jobDay + "/"
              + jobHour + "/" + jobMinute + "/" + jobSecond)
          println("save test_detail output:" + testDetailPath + jobYear + "/" + jobMonth + "/" + jobDay + "/"
            + jobHour + "/" + jobMinute + "/" + jobSecond)
        }

        val jobEndTime: String = new SimpleDateFormat(
          configLoader.getString("summary_log_path", "job_fmt")).format(new Date().getTime())
        println("job end time : " + jobEndTime)
        jobStatus = true

      } catch {
      case ex: Exception => {
        println("===> " + ex.printStackTrace())

      }

    }

  }

  def getHostName(): String = {
    var hostName = ""
    try {
      val ip = InetAddress.getLocalHost()
      hostName = ip.getHostName
    } catch {
      case ex: Exception => {
        println("===> Get Pod Hostname Exception !!!")
        ex.printStackTrace()
      }
    }
    hostName
  }

}

