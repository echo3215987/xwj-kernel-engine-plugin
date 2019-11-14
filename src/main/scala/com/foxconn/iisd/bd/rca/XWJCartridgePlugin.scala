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
  // create job object
  val job = new Job()
  // create configContext object
  val configContext = new ConfigContext()

//  val datetimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.US)
//  var totalRawDataSize: Long = 0
//  val mb = 1024*1024
//  val runtime = Runtime.getRuntime
//  var jobId = ""
//  var jobYear = ""
//  var jobMonth = ""
//  var jobDay = ""
//  var jobHour = ""
//  var jobMinute = ""
//  var jobSecond = ""
////  var jobStatus = false
//  var bobcatXWJPath = ""
//  var testDetailPath = ""
//
//  val ctrlACode = "\001"
//  val ctrlCCode = "\003"
//  val downLimit = "_DOWNLIMIT"
//  val upLimit = "_UPLIMIT"
//  val lowerLimit = "_LOWER_LIMIT"
//  val upperLimit = "_UPPER_LIMIT"

  def main(args: Array[String]): Unit = {
    println("xwj-plugin-cartridge-nesta-v4")

    //關閉不必要的Log資訊
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    //Job開始
    println("======> XWJ Kernel Engine Plugin Job Start")
    job.jobStartTime = new java.util.Date()

    ////////////////////////////////////////////////////////////////////////////////////////
    //初始化作業
    ////////////////////////////////////////////////////////////////////////////////////////
    initialize(args)
    job.setJobId(configContext)
    val jobStartTime: String = new SimpleDateFormat(configContext.jobDateFmt).format(job.jobStartTime.getTime())
    println("job start time : " + jobStartTime)

    ////////////////////////////////////////////////////////////////////////////////////////
    //XWJ Plugin處理
    ////////////////////////////////////////////////////////////////////////////////////////
    val sparkSession = configContext.sparkSession
    import sparkSession.implicits._

    TestDetailProcessing.calculation(configContext)

    //Job結束
    job.jobEndTime = new java.util.Date()
    println("======> XWJ Kernel Engine Plugin Job End")
    configContext.isJobState = true
    val jobEndTime: String = new SimpleDateFormat(configContext.jobDateFmt).format(job.jobEndTime.getTime())
    println("job end time : " + jobEndTime)


    ////////////////////////////////////////////////////////////////////////////////////////
    //將TMP檔案依照Job State搬到Succeeded or Failed
    ////////////////////////////////////////////////////////////////////////////////////////
    val minioIo = new MinioIo(configContext)
    minioIo.moveFilesByJobStatus()

    ////////////////////////////////////////////////////////////////////////////////////////
    //SummaryFile輸出
    ////////////////////////////////////////////////////////////////////////////////////////
    SummaryFile.save(configContext)



  }

  /*
 *
 *
 * @author JasonLai
 * @date 2019/9/27 下午4:50
 * @param [args]
 * @return void
 * @description 初始化 configloader, spark, configContext
 */
  def initialize(args: Array[String]): Unit = {

    //load config yaml
    configLoader.setDefaultConfigPath("""conf/default.yaml""")
    if(args.length == 1) {
      configLoader.setDefaultConfigPath(args(0))
    }

    //env
    configContext.env = configLoader.getString("general", "env")

    //spark
    configContext.sparkJobName = configLoader.getString("spark", "job_name")
    configContext.sparkMaster = configLoader.getString("spark", "master")

    //create spark session object
    val sparkBuilder = SparkSession
      .builder
      .appName(configContext.sparkJobName)

    if(configContext.env.equals(XWJKEPluginConstants.ENV_LOCAL)) {
      sparkBuilder.master(configContext.sparkMaster)
    }

    val spark = sparkBuilder.getOrCreate()

    //spark executor add config file
    configLoader.setConfig2SparkAddFile(spark)

    //executor instances number
    configContext.sparkNumExcutors = spark.conf.get("spark.executor.instances", "1").toInt

    //add sparkSession into configContext
    configContext.sparkSession = spark

    //初始化context
    initializeconfigContext()

    println(s"======> Job Start Time : ${configContext.job.jobStartTime}")
    println(s"======> Job Flag : ${configContext.flag}")
  }

  /*
   *
   *
   * @author EchoLee
   * @date 2019/9/19 上午9:37
   * @description configContext初始化基本設定值
   */
  def initializeconfigContext(): Unit = {
    //job
    configContext.job = job
    configContext.jobDateFmt = configLoader.getString("summary_log_path", "job_fmt")
    //flag
    configContext.flag = job.jobStartTime.getTime.toString

    //minio
    configContext.minioEndpoint = configLoader.getString("minio", "endpoint")
    configContext.minioConnectionSslEnabled = configLoader.getString("minio", "connectionSslEnabled").toBoolean
    configContext.minioAccessKey = configLoader.getString("minio", "accessKey")
    configContext.minioSecretKey = configLoader.getString("minio", "secretKey")
    //to summaryfile use
    configContext.minioBucket = configLoader.getString("minio", "bucket")
    //summary file
    configContext.summaryFileLogBasePath = configLoader.getString("summary_log_path", "data_base_path")
    configContext.summaryFileLogTag = configLoader.getString("summary_log_path", "tag")
    configContext.summaryFileExtension = configLoader.getString("summary_log_path", "file_extension")
    configContext.summaryFileJobFmt = configLoader.getString("summary_log_path", "job_fmt")
    configContext.summaryFileBuName = configLoader.getString("summary_log_path", "bu_name")

    //Bobcat_d_xwj
    configContext.testDetailDTFmt = configLoader.getString("log_prop", "test_detail_dt_fmt")
    configContext.bobcatXWJTempPath = configLoader.getString("minio_log_path", "bobcat_d_xwj_tmp_path")
    configContext.bobcatXWJOutputPath = configLoader.getString("minio_log_path", "bobcat_d_xwj_output_path")
    configContext.bobcatXWJSucceededPath = configLoader.getString("minio_log_path", "bobcat_d_xwj_succeeded_path")
    configContext.bobcatXWJFailedPath = configLoader.getString("minio_log_path", "bobcat_d_xwj_failed_path")
    configContext.bobcatXWJColumns = configLoader.getString("log_prop", "bobcat_xwj_col")
    configContext.bobcatXWJPath = configLoader.getString("minio_log_path", "bobcat_d_xwj_path")
    configContext.testDetailPath = configLoader.getString("minio_log_path", "test_detail_path")
    configContext.testDetailColumns = configLoader.getString("log_prop", "test_detail_col")
    configContext.dataSeperator = configLoader.getString("log_prop", "log_seperator")
    configContext.dataSeperatorNonEscape = configLoader.getString("log_prop", "log_seperator_non_escape")
    configContext.mbLimits = configLoader.getString("log_prop", "mb_limits").toInt

    //object
    val fileDataSource = new FileSource(configContext)
    fileDataSource.init()
    configContext.fileDataSource = fileDataSource

  }
}

