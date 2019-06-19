package com.foxconn.iisd.bd.rca

import java.io.File
import java.nio.file.{Files, Paths}
import java.time.format.DateTimeFormatter
import java.util.Locale

import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, Encoders, SparkSession}
import org.apache.spark.sql.functions.{regexp_extract, _}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}

import scala.collection.mutable.WrappedArray
import scala.collection.mutable.Seq
import com.foxconn.iisd.bd.rca.SparkUDF._
import org.apache.spark.sql.SaveMode

object XWJKernelEnginePlugin {

  var configLoader = new ConfigLoader()
  val datetimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.US)

  def main(args: Array[String]): Unit = {

    val limit = 1
    var count = 0

    println("xwj-v1")

    while(count < limit) {
      println(s"count: $count")

      try {
        configLoader.setDefaultConfigPath("""conf/default.yaml""")
        if(args.length == 1) {
          configLoader.setDefaultConfigPath(args(0))
        }
        XWJKernelEnginePlugin.start()
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

    var date: java.util.Date = new java.util.Date()
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
    val numExecutors = spark.conf.get("spark.executor.instances", "1").toInt
    val testDetailDTFmt =  configLoader.getString("log_prop", "test_detail_dt_fmt")

    val factory = configLoader.getString("general", "factory")

    //val failCondition: Int = configLoader.getString("analysis", "fail_condition").toInt

    //s3a://" + bucket + "/

    val testDetailCompressionPath = configLoader.getString(logPathSection, "test_detail_compression_path")
    val testDetailCompressionTmpPath = configLoader.getString(logPathSection, "test_detail_compression_tmp_path")
    //val testDetailCompressionOutputPath = configLoader.getString(logPathSection, "test_detail_compression_output_path")
    val testDetailCompressionSuccessfulPath = configLoader.getString(logPathSection, "test_detail_compression_output_successful_path")
    val testDetailCompressionFailedPath = configLoader.getString(logPathSection, "test_detail_compression_output_failed_path")
    val testDetailCompressionResultPath = configLoader.getString(logPathSection, "test_detail_compression_output_result_path")

    val testDetailPath = configLoader.getString(logPathSection, "test_detail_path")
    val testDetailFileLmits = configLoader.getString(logPathSection, "test_detail_file_limits").toInt

    //"sn,build_name,build_description,unit_number,station_id,test_status,test_starttime,test_endtime,list_of_failure,list_of_failure_detail,test_phase,machine_id,factory_code,floor,line_id,test_item,test_value,test_unit,test_lower,test_upper,create_time,update_time,station_name,start_date,product,test_version"
    //CN95I870ZC06MD_||_SOR_||_SOR_||_CN95I870ZC06MD_||_L7_TLEOL_06_||_Exception_||_2019/05/18 06:36_||_2019/05/18 06:36_||_PcaVerifyFirmwareRev_||_Error_||_MP_||__||_CQ_||_D62_||_2_||_ProcPCClockSync^DResultInfo^APcaVerifyFirmwareRev^DResultInfo^APcaVerifyFirmwareRev^DExpectedVersion^APcaVerifyFirmwareRev^DReadVersion^APcaVerifyFirmwareRev^DDateTimeStarted^APcaVerifyFirmwareRev^DActualFWUpdate^APcaVerifyFirmwareRev^DFWUpdateDSIDFirst_||_ProcPCClockSync^DResultInfo^C^APcaVerifyFirmwareRev^DResultInfo^C^APcaVerifyFirmwareRev^DExpectedVersion^C^APcaVerifyFirmwareRev^DReadVersion^C^APcaVerifyFirmwareRev^DDateTimeStarted^C5/18/2019 5:29:48 AM^APcaVerifyFirmwareRev^DActualFWUpdate^C^APcaVerifyFirmwareRev^DFWUpdateDSIDFirst^C_||_ProcPCClockSync^DResultInfo^C^APcaVerifyFirmwareRev^DResultInfo^C^APcaVerifyFirmwareRev^DExpectedVersion^C^APcaVerifyFirmwareRev^DReadVersion^C^APcaVerifyFirmwareRev^DDateTimeStarted^C^APcaVerifyFirmwareRev^DActualFWUpdate^C^APcaVerifyFirmwareRev^DFWUpdateDSIDFirst^C_||_ProcPCClockSync^DResultInfo^C^APcaVerifyFirmwareRev^DResultInfo^C^APcaVerifyFirmwareRev^DExpectedVersion^C^APcaVerifyFirmwareRev^DReadVersion^CTJP1FN1845AR^APcaVerifyFirmwareRev^DDateTimeStarted^C^APcaVerifyFirmwareRev^DActualFWUpdate^C169^APcaVerifyFirmwareRev^DFWUpdateDSIDFirst^C_||_ProcPCClockSync^DResultInfo^C^APcaVerifyFirmwareRev^DResultInfo^C^APcaVerifyFirmwareRev^DExpectedVersion^C^APcaVerifyFirmwareRev^DReadVersion^CTJP1FN1845AR^APcaVerifyFirmwareRev^DDateTimeStarted^C^APcaVerifyFirmwareRev^DActualFWUpdate^C169^APcaVerifyFirmwareRev^DFWUpdateDSIDFirst^C_||_2019/05/18 06:36_||_2019/05/18 06:36_||_TLEOL_||_2019/05/18 06:36_||_TaiJi Base_||_42.3.8 REV_37_Taiji25
    val testDetailColumns = configLoader.getString("log_prop", "test_detail_col")

    //val dataSeperator = configLoader.getString("log_prop", "log_seperator")
    val dataSeperatorNonEscape = configLoader.getString("log_prop", "log_seperator_non_escape")
    ///////////
    //載入資料//
    ///////////

    try {

      val testDetailDestPath = IoUtils.decompressMinioFiles(spark,
        flag,
        testDetailCompressionPath, //source
        testDetailFileLmits,
        testDetailCompressionResultPath //result
      )


      //將xml分成三部分解析, 1.最外層的tag CIMProjectResults, 2.tag sequence 3.step測項
      var rawdataDF = spark.read.text(testDetailDestPath.toString+"/*.xml")
        .withColumn("filename", getLast(split(expr("input_file_name()"), "/")))
        .groupBy("filename").agg(concat_ws("", collect_list("value")).as("value"))

      var CIMProjectResultsDF = rawdataDF
        .withColumn("SN", regexp_extract(col("value"),"(SerialNumber=\")(\\w+)(\")" ,2))
        .withColumn("BUILD_NAME", lit("SOR"))
        .withColumn("BUILD_DESCRIPTION", lit("SOR"))
        .withColumn("UNIT_NUMBER", col("SN"))
        .withColumn("STATION_ID", regexp_extract(col("value"),"(StationNumber=\")(\\w+)" ,2))
        .withColumn("TEST_STATUS", regexp_extract(col("value"),"(RunResult=\")(\\w+)" ,2))
        .withColumn("TEST_STARTTIME_TEMP", regexp_extract(col("value"),"(RunDateTimeStarted=\")((?:[^\"\\\\]+|\\\\.)*)" ,2))
        .withColumn("TEST_STARTTIME", from_unixtime(unix_timestamp($"TEST_STARTTIME_TEMP","MM/dd/yyyy hh:mm:ss aa"), testDetailDTFmt))
        .withColumn("TEST_ENDTIME_TEMP", regexp_extract(col("value"),"(SeqDateTimeStarted=\")((?:[^\"\\\\]+|\\\\.)*)" ,2))
        .withColumn("TEST_ENDTIME", from_unixtime(unix_timestamp($"TEST_ENDTIME_TEMP", "MM/dd/yyyy hh:mm:ss aa"), testDetailDTFmt))
        //list failure
        .withColumn("TEST_PHASE", lit("MP"))
        .withColumn("MACHINE_ID", lit("null"))
        .withColumn("FACTORY_CODE", lit(factory))
        //取得floor_line對應碼, 利用STATION_ID的第二位數對應測試樓層跟線體
        .withColumn("FLOOR_LINE", getFloorLine(col("STATION_ID").substr(2,1)))
        .withColumn("FLOOR", col("FLOOR_LINE").substr(1,3))
        .withColumn("LINE_ID", col("FLOOR_LINE").substr(4,1))
        //五個測項
        .withColumn("CREATE_TIME_TEMP", regexp_replace(regexp_replace(regexp_replace(regexp_extract(col("filename"),
        "([A-Z]{3})_([0-9]+)_([0-9]{4})_([0-9]+h)_([0-9]+m)_([0-9]+s)", 0), "h", ""),
        "m", ""), "s", ""))
        .withColumn("CREATE_TIME", from_unixtime(unix_timestamp($"CREATE_TIME_TEMP", "MMM_dd_yyyy_HH_mm_ss"), testDetailDTFmt))
          //expr("from_unixtime(UNIX_TIMESTAMP(CREATE_TIME_TEMP, 'MMM_dd_yyyy_HH_mm_ss'), 'yyyy/MM/dd HH:mm:ss')"))
        .withColumn("UPDATE_TIME", col("CREATE_TIME"))
        .withColumn("STATION_NAME", regexp_extract(col("value"),"(StationName=\")(\\w+)" ,2))
        .withColumn("START_DATE", col("TEST_STARTTIME"))
        .withColumn("PRODUCT", lit("TaiJi Base"))
        .withColumn("TEST_VERSION", regexp_extract(col("value"),"(ProjectVersion=\")([^\"]+)" ,2))

      //只留下RunResult等於Exception　或 Pass　或 Fail的三種狀態
      val originCount = CIMProjectResultsDF.count()
      var newCIMProjectResultsDF = CIMProjectResultsDF.filter(col("TEST_STATUS").equalTo("Exception")
        .or(col("TEST_STATUS").equalTo("Pass"))
        .or(col("TEST_STATUS").equalTo("Fail")))

      val StepDF = rawdataDF
        .selectExpr("split(value, '<Step ') as Step", "filename")
        .selectExpr("explode(Step) as Step", "filename")
        .filter(col("Step").contains("StepName=\""))
        .withColumn("MAIN_TEST_ITEM", regexp_extract(col("Step"), "(StepName=\")(.+)(\"\\sStepNumber=)", 2))
        .withColumn("TEST_RESULT", regexp_extract(col("Step"), "(TestResult=\")(\\w+)", 2))//TestDateTimeStarted=)
        .withColumn("TEST_RESULT_INFO", regexp_extract(col("Step"), "(TestResultInfo=\")(.+)(\"\\sTestDateTimeStarted=)", 2))
      //測試值
        .withColumn("TEST_VALUE_RAW", regexp_extract(col("Step"), "(PayLoad)([^>]+)", 2))
        .withColumn("TEST_VALUE_RAW", expr("trim(substring(TEST_VALUE_RAW, 1, length(TEST_VALUE_RAW)-1))"))
        .withColumn("TEST_VALUE_RAW", split(col("TEST_VALUE_RAW"), "\" "))
        .withColumn("TEST_SPEC_RAW", regexp_extract(col("Step"), "(<TestParms>)(.+)(</TestParms>)", 2))
        .withColumn("TEST_SPEC_KEY", splitKey(col("TEST_SPEC_RAW")))
        .withColumn("TEST_SPEC_VALUE", splitValue(col("TEST_SPEC_RAW")))
        .withColumn("TEST_SPEC_MAP", arrays_zip(col("TEST_SPEC_KEY"), col("TEST_SPEC_VALUE")))
        .withColumn("TEST_SPEC_MAP", expr("transform(TEST_SPEC_MAP, x -> concat_ws('=', x.TEST_SPEC_KEY, x.TEST_SPEC_VALUE))"))

      var StepItemValueDF = StepDF.selectExpr("filename", "MAIN_TEST_ITEM", "explode(TEST_VALUE_RAW) as TEST_VALUE_RAW", "TEST_SPEC_MAP")
        .withColumn("SUB_TEST_ITEM", split(col("TEST_VALUE_RAW"), "=").getItem(0))
        .withColumn("TEST_VALUE_TEMP", regexp_replace(split(col("TEST_VALUE_RAW"), "=").getItem(1), "\"", ""))
        .withColumn("TEST_ITEM", concat(col("MAIN_TEST_ITEM"), lit("\004"), col("SUB_TEST_ITEM")))
        .withColumn("TEST_VALUE", concat(col("TEST_ITEM"), lit("\003"), col("TEST_VALUE_TEMP")))
        .withColumn("TEST_UNIT", concat(col("TEST_ITEM"), lit("\003"), lit("")))
        .withColumn("TEST_UPPER_LOWER", getSpec(col("TEST_ITEM"), col("TEST_SPEC_MAP")))
        .withColumn("TEST_UPPER", concat(col("TEST_ITEM"), lit("\003"), col("TEST_UPPER_LOWER").getItem(0)))
        .withColumn("TEST_LOWER", concat(col("TEST_ITEM"), lit("\003"), col("TEST_UPPER_LOWER").getItem(1)))

      //最後^A分隔各個測項.測試上下界.測試值,測試單位
      StepItemValueDF = StepItemValueDF.groupBy("filename")
        .agg(concat_ws("\001", collect_list("TEST_ITEM")).as("TEST_ITEM"),
          concat_ws("\001", collect_list("TEST_UPPER")).as("TEST_UPPER"),
        concat_ws("\001", collect_list("TEST_LOWER")).as("TEST_LOWER"),
        concat_ws("\001", collect_list("TEST_UNIT")).as("TEST_UNIT"),
        concat_ws("\001", collect_list("TEST_VALUE")).as("TEST_VALUE"))

      //取得測試失敗項目清單(CIMProjectResults.Sequence.Step.StepName 当 CIMProjectResults.Sequence.Step.（TestResult='Fail' or TestResult='Exception' ）),
      //取得測試失敗項目清單說明(CIMProjectResults.Sequence.Step.TestResultInfo 当 CIMProjectResults.Sequence.Step.（TestResult='Fail' or TestResult='Exception' ）)
      var failStepDF = StepDF.filter(col("TEST_RESULT").equalTo("Fail").or(col("TEST_RESULT").equalTo("Exception")))
        .groupBy("filename").agg(concat_ws("\001", collect_list("MAIN_TEST_ITEM")).as("LIST_OF_FAILURE"),
        concat_ws("\001", collect_list("TEST_RESULT_INFO")).as("LIST_OF_FAILURE_DETAIL"))


      var finalResultsDF = newCIMProjectResultsDF.join(StepItemValueDF, "filename")
      if(failStepDF.count()>0){
        finalResultsDF = finalResultsDF.join(failStepDF, "filename")
      }else{
        finalResultsDF = finalResultsDF
          .withColumn("LIST_OF_FAILURE", lit("")).withColumn("LIST_OF_FAILURE_DETAIL", lit(""))
      }

      finalResultsDF
        .select("SN", "BUILD_NAME", "BUILD_DESCRIPTION", "UNIT_NUMBER", "STATION_ID", "TEST_STATUS", "TEST_STARTTIME", "TEST_ENDTIME",
        "LIST_OF_FAILURE", "LIST_OF_FAILURE_DETAIL", "TEST_PHASE", "MACHINE_ID", "FACTORY_CODE", "FLOOR", "LINE_ID", "TEST_ITEM", "TEST_VALUE",
        "TEST_UNIT", "TEST_LOWER", "TEST_UPPER", "CREATE_TIME", "UPDATE_TIME", "STATION_NAME", "START_DATE", "PRODUCT", "TEST_VERSION")
        .map(x => x.mkString("", dataSeperatorNonEscape, ""))//.replaceAll("#", dataSeperator))
        .coalesce(1)
        .write
        .text(testDetailCompressionTmpPath + flag + "/" + "temp")
      //println(testDetailCompressionTmpPath + flag + "/" + "temp")

      //刪除RunResult不等於Exception　或 Pass　或 Fail的三種狀態的XML
      val afterFilterCount = newCIMProjectResultsDF.count()
      if(originCount != afterFilterCount){
        val removedCIMDF =
        CIMProjectResultsDF.join(newCIMProjectResultsDF,
          CIMProjectResultsDF("filename") === newCIMProjectResultsDF("filename"), "leftanti")
        val list = removedCIMDF.select("filename").map(row => row.mkString(""))(Encoders.STRING)collect()

        for(filename <- list){
          //delete RunResult !=  Exception, Pass, Fail
          println("delete RunResult !=  Exception, Pass, Fail file: " + filename)
          IoUtils.deleteFileFromCompression_Temp(spark,
            flag,
            testDetailCompressionPath)
          //FileUtils.deleteQuietly(new File(filename.replace("file:/", "")))
        }
      }
      //成功
      IoUtils.moveFileToTestDetail(spark, flag, testDetailCompressionTmpPath, testDetailCompressionSuccessfulPath, testDetailPath)

    } catch {
    case ex: Exception => {
      println("===> " + ex.printStackTrace())
      //失敗
      IoUtils.moveFileToFailed(spark, flag, testDetailCompressionTmpPath, testDetailCompressionFailedPath, testDetailPath)
    }
  }

  }

}

