package com.foxconn.iisd.bd.rca

import com.foxconn.iisd.bd.rca.SparkUDF.getLast
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{expr, split}

/*
 *
 *
 * @author EchoLee
 * @date 2019/11/7 上午9:47
 * @param
 * @return
 * @description 針對檔案形式來源的資料進行存取操作
 */
@SerialVersionUID(114L)
class FileSource(configContext: ConfigContext) extends BaseDataSource with Serializable {

  var bobcatXWJPath = ""
  var testDetailPath = ""
  var bobcatXWJColumns = ""
  var testDetailColumns = ""
  var dataSeperator = ""
  var mbLimits = 0
  var job = new Job()
  val minioIo = new MinioIo(configContext)

  /*
   *
   *
   * @author EchoLee
   * @date 2019/11/7 上午9:39
   * @description 初始化File所需的參數
   */
  override def init(): Unit = {
    bobcatXWJPath = configContext.bobcatXWJPath
    bobcatXWJColumns = configContext.bobcatXWJColumns
    testDetailPath = configContext.testDetailPath
    dataSeperator = configContext.dataSeperator
    mbLimits = configContext.mbLimits
    //use minio service
    setMinioS3()
    job = configContext.job
  }

  /*
   *
   *
   * @author EchoLee
   * @date 2019/11/7 上午9:40
   * @param []
   * @return org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
   * @description 從minio xwj bobcat d file取得測試表資料，並轉換成 spark dataframe
   */
  override def fetchBobcatXWJDataDf(): DataFrame = {
    println("======> test detail data from file")

    val testDetailDestPath = minioIo.flatMinioFiles(configContext.sparkSession,
      bobcatXWJPath,
      mbLimits * XWJKEPluginConstants.mb,
      minioIo.getJobIdTime(0, 4).toString + minioIo.getJobIdTime(4, 6).toString,
      minioIo.getJobIdTime(6, 8).toString,
      minioIo.getJobIdTime(8, 10).toString + minioIo.getJobIdTime(10, 12).toString + minioIo.getJobIdTime(12, 14).toString)

    minioIo.getDfFromPath(configContext.sparkSession, testDetailDestPath.toString, bobcatXWJColumns, dataSeperator)
  }

  /*
   *
   *
   * @author EchoLee
   * @date 2019/11/7 上午9:40
   * @param []
   * @return void
   * @description 將spark dataframe 存到 test detail 目錄
   */
  def saveDfToTestDetailDir(dataFrame: Dataset[String]): Unit = {
    var jobYear = minioIo.getJobIdTime(0, 4).toString
    var jobMonth = minioIo.getJobIdTime(4, 6).toString
    var jobDay = minioIo.getJobIdTime(6, 8).toString
    var jobHour = minioIo.getJobIdTime(8, 10).toString
    var jobMinute = minioIo.getJobIdTime(10, 12).toString
    var jobSecond = minioIo.getJobIdTime(12, 14).toString

    println("======> plugin data save to test detail dir:" + testDetailPath + jobYear + "/" + jobMonth + "/" + jobDay + "/"
      + jobHour + "/" + jobMinute + "/" + jobSecond)

    dataFrame
      .write
      .text(testDetailPath + jobYear + "/" + jobMonth + "/" + jobDay + "/"
        + jobHour + "/" + jobMinute + "/" + jobSecond)

  }

  /*
  *
  *
  * @author EchoLee
  * @date 2019/11/7 上午9:46
  * @param []
  * @return void
  * @description sparkSession 設定minio s3參數
  */
  def setMinioS3(): Unit = {
    configContext.sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", configContext.minioEndpoint)
    configContext.sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", configContext.minioConnectionSslEnabled.toString)
    configContext.sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", configContext.minioAccessKey)
    configContext.sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", configContext.minioSecretKey)
  }

}
