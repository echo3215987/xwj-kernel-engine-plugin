package com.foxconn.iisd.bd.rca

import com.foxconn.iisd.bd.rca.XWJCartridgePlugin.{configContext, configLoader}
import org.apache.spark.sql.SparkSession

class ConfigContext extends Serializable {
  //env
  var env = ""
  //flag
  var flag = ""
  //job
  var job: Job = null
  var jobDateFmt = ""
  var isJobState = false
  //Bobcat_d_xwj
  var mbLimits = 0
  var bobcatXWJTempPath = ""
  var bobcatXWJOutputPath = ""
  var bobcatXWJSucceededPath = ""
  var bobcatXWJFailedPath = ""
  var bobcatXWJPath = ""
  var testDetailPath = ""
  var bobcatXWJColumns = ""
  var testDetailColumns = ""
  var testDetailDTFmt = ""
  var dataSeperator = ""
  var dataSeperatorNonEscape = ""
  // spark
  var sparkJobName = ""
  var sparkMaster = ""
  var sparkSession: SparkSession = null
  var sparkNumExcutors = 1
  //minio
  var minioEndpoint = ""
  var minioAccessKey = ""
  var minioSecretKey = ""
  var minioBucket = ""
  var minioConnectionSslEnabled = false
  //datasource
  var fileDataSource: FileSource = null
  //summaryfile
  var summaryFileLogBasePath = ""
  var summaryFileLogTag = ""
  var summaryFileExtension = ""
  var summaryFileJobFmt = ""
  var summaryFileBuName = ""


}
