package com.foxconn.iisd.bd.rca

import java.text.SimpleDateFormat
import java.util

import com.google.gson.Gson

object SummaryFile {
  case class InputFileNames(bobcatXWJFilesNameList: util.ArrayList[String])

  case class XWJKEPluginSummaryJson(template_json: String, xwjKEPlugin: util.ArrayList[SummaryJson])

  case class SummaryJson(inputFileNames: InputFileNames, totalRowsInRawData: Long, totalRowsInXWJBobcatD: Long, job: Job, bu: String)

  case class Job(id: String, startTime: String, endTime: String, status: String, message: String)

  var bobcatXWJFilesNameList: util.ArrayList[String] = null

  var totalRowsInRawData: Long = 0

  var totalRowsInXWJBobcatD: Long = 0

  var summaryFileJobFmt = ""
  var summaryFileBuName = ""

  var id = ""

  var startTime = ""

  var endTime = ""

  var status = ""

  var message = ""

  def save(configContext: ConfigContext): Unit = {
    summaryFileBuName = configContext.summaryFileBuName

    //get job
    this.id = configContext.job.jobId
    this.startTime = ((new SimpleDateFormat(configContext.summaryFileJobFmt)).format(configContext.job.jobStartTime))
    this.endTime = ((new SimpleDateFormat(configContext.summaryFileJobFmt)).format(configContext.job.jobEndTime))
    configContext.isJobState match {
      case true =>
        this.status = XWJKEPluginConstants.SUMMARYFILE_SUCCEEDED
      case false =>
        this.status = XWJKEPluginConstants.SUMMARYFILE_FAILED
    }
    this.message = ""

    //empty data
    val minioIo = new MinioIo(configContext)
    minioIo.saveSummaryFileToMinio(configContext.sparkSession, getJsonString())
  }


  def getJsonString(): String = {
    val inputFileNames = new InputFileNames(bobcatXWJFilesNameList)
    val job = new Job(id, startTime, endTime, status, message)
    val summaryJson = new SummaryJson(inputFileNames, totalRowsInRawData, totalRowsInXWJBobcatD, job, summaryFileBuName)
    val summaryJsonList = new util.ArrayList[SummaryJson]()
    summaryJsonList.add(summaryJson)
    val xwjKEPluginSummaryJson = new XWJKEPluginSummaryJson("rca-xwj-ke-plugin", summaryJsonList)
    val gson = new Gson
    val jsonString = gson.toJson(xwjKEPluginSummaryJson)
    jsonString
  }
}
