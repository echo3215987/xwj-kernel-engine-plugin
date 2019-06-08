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
      /*
      val bobcatDestPaths = IoUtils.flatMinioFiles(spark,
        flag,
        bobcatPath,
        bobcatFileLmits)

      var bobcatSourceDf = IoUtils.getDfFromPath(spark, bobcatDestPaths.toString, bobcatColumns, dataSeperator)

      val maxIstestfailSourceDf = bobcatSourceDf
                       .distinct()
                       .orderBy("sn","station")
                       .orderBy($"start_time".desc)
                       .groupBy("sn")
                       .agg(max("istestfail").alias("new_istestfail"))


      val joinMaxIstestfailDf = bobcatSourceDf.join(maxIstestfailSourceDf, "sn")

      val symptomAndDescValueDf = joinMaxIstestfailDf
        .filter($"istestfail" === $"new_istestfail")
        .withColumn("descValue", concat($"desc_", lit(":"), $"c3"))

        .groupBy($"sn")
        .agg(concat_ws(";", collect_list($"symptom")).alias("new_symptom"), concat_ws(";", collect_list($"descValue")).alias("descValue"))


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

      val bobcat_cols: List[String] = bobcatColumns.split(",").toList
      val bobcat_col: List[Column] = bobcat_cols.map(resultDf(_))

      val resultMacthColDf = resultDf
        .select(bobcat_col:_*)

      resultMacthColDf.printSchema()
      resultMacthColDf.show(false)

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
*/


      val filepath_xz = "C:\\Users\\foxconn\\Desktop\\vfpa_trans_fail_list_20190513-20190520.tar.xz"
      //val filepath_xzcompress = "E:\\untar\\"
      //val filepath_xzcompress = "C:\\Users\\foxconn\\Desktop\\RCA\\ask\\new\\newspec_subSeq_PRNU_SiriusFW\\"
      val filepath_xzcompress = "C:\\Users\\foxconn\\Desktop\\RCA\\ask\\"
      val filepath_output = "C:\\Users\\foxconn\\Desktop\\RCA\\ask\\"

      //val files = filepath_xzcompress + "*.xml"
      val files = filepath_xzcompress + "new\\WuDang_L7_TLEOL_11_CN94M8702J06MD_1_APR_21_2019_0h_48m_46s.xml"


      //從CIMation.tar.xz壓縮檔, 解壓縮找出產品:Taiji Base的CIMation xml, 並去除維護(repair)資料
      /*val fin_tar = Files.newInputStream(Paths.get(filepath_xz))
      IoUtils.unxzfile(fin_tar, filepath_xzcompress)
*/
      //將xml分成三部分解析, 1.最外層的tag CIMProjectResults, 2.tag sequence 3.step測項
      var rawdataDF = spark.read.text(files)
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
        .withColumn("TEST_STARTTIME", from_unixtime(expr("UNIX_TIMESTAMP(TEST_STARTTIME_TEMP,'MM/dd/yyyy hh:mm:ss aa')"), "yyyy/MM/dd HH:mm:ss"))
        .withColumn("TEST_ENDTIME_TEMP", regexp_extract(col("value"),"(SeqDateTimeStarted=\")((?:[^\"\\\\]+|\\\\.)*)" ,2))
        .withColumn("TEST_ENDTIME", from_unixtime(expr("UNIX_TIMESTAMP(TEST_ENDTIME_TEMP,'MM/dd/yyyy hh:mm:ss aa')"), "yyyy/MM/dd HH:mm:ss"))
        //list failure


        .withColumn("TEST_PHASE", lit("MP"))
        .withColumn("MACHINE_ID", lit("null"))
        .withColumn("FACTORY_CODE", lit("CQ"))
        //取得floor_line對應碼, 利用STATION_ID的第二位數對應測試樓層跟線體
        .withColumn("FLOOR_LINE", getFloorLine(col("STATION_ID").substr(2,1)))
        .withColumn("FLOOR", col("FLOOR_LINE").substr(1,3))
        .withColumn("LINE_ID", col("FLOOR_LINE").substr(4,1))
        //五個測項


        .withColumn("CREATE_TIME_TEMP", regexp_replace(regexp_replace(regexp_replace(regexp_extract(col("filename"),
        "([A-Z]{3})_([0-9]+)_([0-9]{4})_([0-9]+h)_([0-9]+m)_([0-9]+s)", 0), "h", ""),
        "m", ""), "s", ""))
        .withColumn("CREATE_TIME",
          expr("from_unixtime(UNIX_TIMESTAMP(CREATE_TIME_TEMP, 'MMM_dd_yyyy_HH_mm_ss'), 'yyyy/MM/dd HH:mm:ss')"))
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
        .withColumn("MAIN_TEST_ITEM", regexp_extract(col("Step"), "(StepName=\")(\\w+)", 2))
        .withColumn("TEST_RESULT", regexp_extract(col("Step"), "(TestResult=\")(\\w+)", 2))
        .withColumn("TEST_RESULT_INFO", regexp_extract(col("Step"), "(TestResultInfo=\")([^\"]+)", 2))
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
        .withColumn("TEST_UNIT", concat(col("TEST_ITEM"), lit("\003"), lit("null")))
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
      StepItemValueDF.show(false)

      //取得測試失敗項目清單(CIMProjectResults.Sequence.Step.StepName 当 CIMProjectResults.Sequence.Step.（TestResult='Fail' or TestResult='Exception' ）),
      //取得測試失敗項目清單說明(CIMProjectResults.Sequence.Step.TestResultInfo 当 CIMProjectResults.Sequence.Step.（TestResult='Fail' or TestResult='Exception' ）)
      var failStepDF = StepDF.filter(col("TEST_RESULT").equalTo("Fail").or(col("TEST_RESULT").equalTo("Exception")))
        .groupBy("filename").agg(concat_ws(";", collect_list("MAIN_TEST_ITEM")).as("LIST_OF_FAILURE"),
        concat_ws(";", collect_list("TEST_RESULT_INFO")).as("LIST_OF_FAILURE_DETAIL"))

      var finalResultsDF = newCIMProjectResultsDF.join(StepItemValueDF, "filename")
        .join(failStepDF, "filename")

//println(StepDF.count())
      //StepDF.drop("Step", "filename").show(120, false)
      //newCIMProjectResultsDF.drop("filename", "value", "TEST_STARTTIME_TEMP", "TEST_ENDTIME_TEMP", "CREATE_TIME_TEMP", "FLOOR_LINE").show(false)

      finalResultsDF
        .select("SN", "BUILD_NAME", "BUILD_DESCRIPTION", "UNIT_NUMBER", "STATION_ID", "TEST_STATUS", "TEST_STARTTIME", "TEST_ENDTIME",
        "LIST_OF_FAILURE", "LIST_OF_FAILURE_DETAIL", "TEST_PHASE", "MACHINE_ID", "FACTORY_CODE", "FLOOR", "LINE_ID", "TEST_ITEM", "TEST_VALUE",
        "TEST_UNIT", "TEST_LOWER", "TEST_UPPER", "CREATE_TIME", "UPDATE_TIME", "STATION_NAME", "START_DATE", "PRODUCT", "TEST_VERSION")
        .map(x => x.mkString("", "#", "").replaceAll("#", dataSeperator))
        .coalesce(1)
        .write
        .mode(SaveMode.Overwrite)
        .text(filepath_output+".txt")
      finalResultsDF.printSchema()

      //刪除RunResult不等於Exception　或 Pass　或 Fail的三種狀態的XML
      val afterFilterCount = newCIMProjectResultsDF.count()
      if(originCount != afterFilterCount){
        //CIMProjectResultsDF = CIMProjectResultsDF.selectExpr("input_file_name() as filename")
        //var newCIMProjectResultsTempDF = newCIMProjectResultsDF.selectExpr("input_file_name() as filename")
        val removedCIMDF =
        CIMProjectResultsDF.join(newCIMProjectResultsDF,
          CIMProjectResultsDF("filename") === newCIMProjectResultsDF("filename"), "leftanti")
        val list = removedCIMDF.select("filename").map(row => row.mkString(""))(Encoders.STRING)collect()

        for(filename <- list){
          //delete RunResult !=  Exception, Pass, Fail
          println("delete RunResult !=  Exception, Pass, Fail file: " + filename)
          FileUtils.deleteQuietly(new File(filename.replace("file:/", "")))
        }
      }



      /*var CIMProjectResultsDF = spark.read.format("com.databricks.spark.xml")
        .option("rowTag", "CIMProjectResults")
        .schema(resultSchema)
        .load(files)
      val originCount = CIMProjectResultsDF.count()
      var newCIMProjectResultsDF = CIMProjectResultsDF.filter(col("_RunResult").equalTo("Exception")
        .or(col("_RunResult").equalTo("Pass"))
        .or(col("_RunResult").equalTo("Fail"))
      )
      CIMProjectResultsDF.show(25, false)
      */

/*
      val afterFilterCount = newCIMProjectResultsDF.count()
      if(originCount != afterFilterCount){
        CIMProjectResultsDF = CIMProjectResultsDF.selectExpr("input_file_name() as filename")
        var newCIMProjectResultsTempDF = newCIMProjectResultsDF.selectExpr("input_file_name() as filename")
        val removedCIMDF =
          CIMProjectResultsDF.join(newCIMProjectResultsTempDF,
            CIMProjectResultsDF("filename") === newCIMProjectResultsTempDF("filename"), "leftanti")
        val list = removedCIMDF.select("filename").map(row => row.mkString(""))(Encoders.STRING)collect()

        for(filename <- list){
          //delete RunResult !=  Exception, Pass, Fail
          println("delete RunResult !=  Exception, Pass, Fail file: " + filename)
          FileUtils.deleteQuietly(new File(filename.replace("file:/", "")))
        }
        //removedCIMDF.show(false)
      }

      val getFloorLine = udf {
        s: String =>
          configLoader.getString("test_floor_line", "code"+s)
      }

      newCIMProjectResultsDF = newCIMProjectResultsDF.selectExpr("input_file_name() as filename", "_SerialNumber as SN",
        "_StationNumber as STATION_ID", "_RunResult as TEST_STATUS", //"_RunDateTimeStarted as TEST_STARTTIME_TEMP",
        "from_unixtime(UNIX_TIMESTAMP(_RunDateTimeStarted,'MM/dd/yyyy hh:mm:ss aa'), 'yyyy/MM/dd HH:mm') as TEST_STARTTIME",
        "_StationName as STATION_NAME", "_ProjectVersion as TEST_VERSION")
        .withColumn("BUILD_NAME", lit("SOR"))
        .withColumn("BUILD_DESCRIPTION", lit("SOR"))
        .withColumn("UNIT_NUMBER", col("SN"))
        .withColumn("TEST_PHASE", lit("MP"))
        //.withColumn("MACHINE_ID", lit(null)) //不存就是null
        .withColumn("FACTORY_CODE", lit("CQ"))
        //取得floor_line對應碼, 利用STATION_ID的第二位數對應測試樓層跟線體
        .withColumn("FLOOR_LINE", getFloorLine(col("STATION_ID").substr(2,1)))
        .withColumn("FLOOR", col("FLOOR_LINE").substr(1,3))
        .withColumn("LINE_ID", col("FLOOR_LINE").substr(4,1))
        .withColumn("CREATE_TIME_BACKUP", expr("regexp_extract(filename, " +
          "'([A-Z]{3})_([0-9]+)_([0-9]{4})_([0-9]+h)_([0-9]+m)_([0-9]+s)', 0)"))
        .withColumn("CREATE_TIME_TEMP", regexp_replace(regexp_replace(regexp_replace(regexp_extract(col("filename"),
          "([A-Z]{3})_([0-9]+)_([0-9]{4})_([0-9]+h)_([0-9]+m)_([0-9]+s)", 0), "h", ""),
          "m", ""), "s", ""))
        .withColumn("CREATE_TIME",
          expr("from_unixtime(UNIX_TIMESTAMP(CREATE_TIME_TEMP, 'MMM_dd_yyyy_HH_mm_ss'), 'yyyy/MM/dd HH:mm')"))
        .withColumn("UPDATE_TIME", col("CREATE_TIME"))
        .withColumn("START_DATE", col("TEST_STARTTIME"))
        .withColumn("PRODUCT", lit("TaiJi Base"))

      newCIMProjectResultsDF.show(25, false)



      var SequenceDF = spark.read.text(files)
        .withColumn("filename", get_last(split(expr("input_file_name()"), "/")))
        .groupBy("filename").agg(concat_ws("", collect_list("value")).as("value"))
        .selectExpr("split(value, '<Sequence') as Sequence", "filename")
        .selectExpr("explode(Sequence) as Sequence", "filename")
        .filter(col("Sequence").contains("SeqDateTimeStarted=\""))
        .withColumn("TEST_ENDTIME_TEMP",
          regexp_extract(col("Sequence"),
            "SeqDateTimeStarted=\"([0-9]+)/([0-9]+)/([0-9]{4})(\\s)([0-9]+):([0-9]+):([0-9]+)(\\s)([A-Z]{2})\"", 0))  //"5/28/2019 2:41:10 PM"
        .withColumn("TEST_ENDTIME", split(col("TEST_ENDTIME_TEMP"), "\"").getItem(1))
      SequenceDF.show(false)
      SequenceDF = SequenceDF.selectExpr("filename", //"_SeqDateTimeStarted as TEST_ENDTIME_TEMP",
        "from_unixtime(UNIX_TIMESTAMP(TEST_ENDTIME,'MM/dd/yyyy hh:mm:ss aa'), 'yyyy/MM/dd HH:mm') as TEST_ENDTIME")
        .groupBy(col("filename")).agg(max("TEST_ENDTIME").as("TEST_ENDTIME"))

      newCIMProjectResultsDF = newCIMProjectResultsDF.withColumn("filename_temp", get_last(split(col("filename"), "/")))

      newCIMProjectResultsDF = newCIMProjectResultsDF.join(SequenceDF,
        SequenceDF.col("filename") === newCIMProjectResultsDF.col("filename_temp"), "inner")
      SequenceDF.show(false)
      newCIMProjectResultsDF.show(false)

      //取得測試失敗項目清單(CIMProjectResults.Sequence.Step.StepName 当 CIMProjectResults.Sequence.Step.（TestResult='Fail' or TestResult='Exception' ）),
      //取得測試失敗項目清單說明(CIMProjectResults.Sequence.Step.TestResultInfo 当 CIMProjectResults.Sequence.Step.（TestResult='Fail' or TestResult='Exception' ）)
      val stepSchema =
      StructType(
        Array(
          StructField("_GUIResponseTime", StringType),
          StructField("_StepDescription", StringType),
          StructField("_StepName", StringType),
          StructField("_StepNumber", StringType),
          StructField("_TestAsset", StringType),
          StructField("_TestDateTimeStarted", StringType),
          StructField("_TestElapsedTimeSec", StringType),
          StructField("_TestResult", StringType),
          StructField("_TestResultInfo", StringType),
          StructField("_TestRetryCount", StringType),
          StructField("_TestType", StringType)
        )
      )

      var StepDF = spark.read.format("com.databricks.spark.xml")
        .option("rootTag", "CIMProjectResults")
        .option("rowTag", "Step")
        .schema(stepSchema)
        .load(files)
      StepDF.show(5, false)

      val StepFailureListDF = StepDF.selectExpr("_TestResult", "_StepName", "_TestResultInfo")
        .filter(col("_TestResult").equalTo("Fail").or(col("_TestResult").equalTo("Exception")))
        .withColumn("filename", get_last(split(input_file_name(), "/")))
        .groupBy("filename").agg(concat_ws(";", collect_list("_StepName")).as("LIST_OF_FAILURE"),
        concat_ws(";", collect_list("_TestResultInfo")).as("LIST_OF_FAILURE_DETAIL"))
      //測試失敗項目
      newCIMProjectResultsDF = newCIMProjectResultsDF.join(StepFailureListDF,
        StepFailureListDF.col("filename") === newCIMProjectResultsDF.col("filename_temp"), "inner")
      //測試項目
      val StepNameDF = StepDF.selectExpr("_StepName").withColumn("filename", get_last(split(input_file_name(), "/")))
        .groupBy("filename").agg(concat_ws("^A", collect_list("_StepName")).as("TEST_ITEM"))

      newCIMProjectResultsDF = newCIMProjectResultsDF.join(StepNameDF,
        StepNameDF.col("filename") === newCIMProjectResultsDF.col("filename_temp"), "inner")

      StepFailureListDF.show(25, false)
      StepNameDF.show(25, false)
      newCIMProjectResultsDF.show(25, false)

      //測試項目測試值, 上下界
      var StepItemDF = spark.read.text(files)
          .withColumn("filename", get_last(split(expr("input_file_name()"), "/")))
          .groupBy("filename").agg(concat_ws("", collect_list("value")).as("value"))
          .selectExpr("split(value, '<Step') as Step", "filename")
          .selectExpr("explode(Step) as Step", "filename") //單位by Step
          .filter(col("Step").contains("StepName="))
          .withColumn("TEST_VALUE", regexp_extract(col("Step"), "<DataLog (.+)</DataLog>", 0))
      StepItemDF.show(100,false)

*/


    } catch {
    case ex: Exception => {
      println("===> " + ex.printStackTrace())
    }
  }

  }

}

