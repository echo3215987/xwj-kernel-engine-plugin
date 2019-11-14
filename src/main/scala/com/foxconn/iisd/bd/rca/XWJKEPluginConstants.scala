package com.foxconn.iisd.bd.rca

object XWJKEPluginConstants {

  //job
  val JOB_SUCCEEDED = "@Job_Result_200"
  val JOB_FAILED = "@Job_Result_300"
  val JOB_EMPTY_DATA = "@Job_Result_301"

  //MINIO FILE PATH
  val MINIO_BOBCAT_D_XWJ_PATH = "Bobcat_D_XWJ"
  val MINIO_TEST_DETAIL_PATH = "TEST_DETAIL"

  //env
  val ENV_LOCAL = "local"
  val ENV_DEV = "dev"
  val ENV_PROD = "prod"

  //summaryfile
  val SUMMARYFILE_SUCCEEDED = "SUCCEEDED"
  val SUMMARYFILE_WARRING = "WARRING"
  val SUMMARYFILE_FAILED = "FAILED"

  //column split code
  val ctrlACode = "\001"
  val ctrlCCode = "\003"
  val downLimit = "_DOWNLIMIT"
  val upLimit = "_UPLIMIT"
  val lowerLimit = "_LOWER_LIMIT"
  val upperLimit = "_UPPER_LIMIT"

  //mb
  val mb = 1024 * 1024
}
