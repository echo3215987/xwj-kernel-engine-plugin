package com.foxconn.iisd.bd.rca

import java.text.SimpleDateFormat

import com.foxconn.iisd.bd.rca.SparkUDF._
import com.foxconn.iisd.bd.rca.XWJCartridgePlugin.configContext
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{when, _}
import org.apache.spark.storage.StorageLevel

object TestDetailProcessing {

  def calculation(configContext: ConfigContext): Unit = {
    val sparkSession = configContext.sparkSession
    import sparkSession.implicits._
    val minioIo = new MinioIo(configContext)

    var bobcatSourceDf = configContext.fileDataSource.fetchBobcatXWJDataDf()
          .withColumn("product", split(getLast(split(expr("input_file_name()"), "/")), "_").getItem(0))
//     val testDetailDestPath = minioIo.flatMinioFiles(configContext.sparkSession, configContext.bobcatXWJPath,
//       configContext.mbLimits * XWJKEPluginConstants.mb,
//       minioIo.getJobIdTime(0, 4).toString +  minioIo.getJobIdTime(4, 6).toString,
//       minioIo.getJobIdTime(6, 8).toString,
//       minioIo.getJobIdTime(8, 10).toString +  minioIo.getJobIdTime(10, 12).toString +  minioIo.getJobIdTime(12, 14).toString)
//
//
//    var bobcatSourceDf = minioIo.getDfFromPath(configContext.sparkSession, testDetailDestPath.toString, configContext.bobcatXWJColumns, configContext.dataSeperator)
//      .withColumn("product", split(getLast(split(expr("input_file_name()"), "/")), "_").getItem(0))
    bobcatSourceDf = bobcatSourceDf.distinct()

    val sourceCount = bobcatSourceDf.count()
    println("source_count: " + sourceCount)

    SummaryFile.totalRowsInRawData = sourceCount

    if (sourceCount == 0) {
      println(XWJKEPluginConstants.JOB_EMPTY_DATA)
    } else {
      val specs = List(XWJKEPluginConstants.downLimit, XWJKEPluginConstants.upLimit,
        XWJKEPluginConstants.lowerLimit, XWJKEPluginConstants.upperLimit)
      val lowerSpecs = List(XWJKEPluginConstants.downLimit, XWJKEPluginConstants.lowerLimit)
      val upperSpecs = List(XWJKEPluginConstants.upLimit, XWJKEPluginConstants.upperLimit)
      val excludeItems = List("PRE_WEIGHT:PRE_WEIGHT", "POST_WEIGHT:POST_WEIGHT")
      //filter exclude test item(PRE_WEIGHT:PRE_WEIGHT, POST_WEIGHT:POST_WEIGHT)
      bobcatSourceDf = bobcatSourceDf.filter(row => !excludeItems.exists(row.getAs("test_item").toString.contains))
        .withColumn("test_version", lit("VER"))
        //沒有unit的値, 先留空值
        .withColumn("test_value", concat_ws(XWJKEPluginConstants.ctrlCCode, col("test_item"), col("test_value_temp")))
        .withColumn("test_unit", lit(""))
        .withColumn("test_unit", concat_ws(XWJKEPluginConstants.ctrlCCode, col("test_item"), col("test_unit")))
        .withColumn("test_item_result", replaceTestResult(col("test_item_result_temp")))
        .withColumn("test_item_result", concat_ws(XWJKEPluginConstants.ctrlCCode, col("test_item"), col("test_item_result")))
        .withColumn("test_item_result_detail_temp", replaceTestResultDetail(col("test_item_result_detail_temp")))
        .withColumn("test_item_result_detail", col("test_item_result_detail_temp"))
        .withColumn("test_item_result_detail", concat_ws(XWJKEPluginConstants.ctrlCCode, col("test_item"), col("test_item_result_detail")))

      val specCols = List("product", "sn", "station_name", "test_starttime", "test_version", "test_item", "test_value_temp")
      val specDf = bobcatSourceDf.filter(row => specs.exists(row.getAs("test_item").toString.contains))
        //product, sn, station_name, test_starttime, test_version, test_item
        .selectExpr(specCols: _*)
        .dropDuplicates()
        .withColumnRenamed("test_item", "test_item_spec")
        .withColumn("test_item",
          regexp_replace(col("test_item_spec"), XWJKEPluginConstants.lowerLimit + "|"
            + XWJKEPluginConstants.downLimit + "|" + XWJKEPluginConstants.upperLimit + "|"
            + XWJKEPluginConstants.upLimit, ""))
        .withColumn("test_lower", when(col("test_item_spec").contains(XWJKEPluginConstants.downLimit)
          .or(col("test_item_spec").contains(XWJKEPluginConstants.lowerLimit)), col("test_value_temp")))
        .withColumn("test_upper", when(col("test_item_spec").contains(XWJKEPluginConstants.upLimit)
          .or(col("test_item_spec").contains(XWJKEPluginConstants.upperLimit)), col("test_value_temp")))
        .withColumn("test_lower", when(col("test_lower").isNotNull,
          concat_ws(XWJKEPluginConstants.ctrlCCode, col("test_item"), col("test_lower"))))
        .withColumn("test_upper", when(col("test_upper").isNotNull,
          concat_ws(XWJKEPluginConstants.ctrlCCode, col("test_item"), col("test_upper"))))

      //exclude test_item_spec
      bobcatSourceDf = bobcatSourceDf.filter(row => !specs.exists(row.getAs("test_item").toString.contains))

      val bobcatDistinctDf = bobcatSourceDf.dropDuplicates("product", "sn", "station_name", "test_starttime", "test_version", "test_item")

      val specGroupbyCols = List("product", "sn", "station_name", "test_starttime", "test_version", "test_item")
      val cols = List("test_lower", "test_upper")
      var specTempJoinBobDf = configContext.sparkSession.emptyDataFrame
      for (colName <- cols) {
        //add colName element into specGroupbyCols list
        val tempCols = colName :: specGroupbyCols
        val specTempDf = specDf.selectExpr(tempCols: _*).filter(col(colName).isNotNull)
        if (specTempJoinBobDf.isEmpty)
          specTempJoinBobDf = bobcatDistinctDf.join(specTempDf, specGroupbyCols, "left")
        else
          specTempJoinBobDf = specTempJoinBobDf.join(specTempDf, specGroupbyCols, "left")
      }

      specTempJoinBobDf = specTempJoinBobDf.groupBy("product", "sn", "station_name", "test_starttime", "test_version")
        .agg(concat_ws(XWJKEPluginConstants.ctrlACode, collect_list("test_item")).as("test_item"),
          concat_ws(XWJKEPluginConstants.ctrlACode, collect_list("test_upper")).as("test_upper"),
          concat_ws(XWJKEPluginConstants.ctrlACode, collect_list("test_lower")).as("test_lower"),
          concat_ws(XWJKEPluginConstants.ctrlACode, collect_list("test_unit")).as("test_unit"),
          concat_ws(XWJKEPluginConstants.ctrlACode, collect_list("test_value")).as("test_value"),
          concat_ws(XWJKEPluginConstants.ctrlACode, collect_list("test_item_result")).as("test_item_result"),
          concat_ws(XWJKEPluginConstants.ctrlACode, collect_list("test_item_result_detail")).as("test_item_result_detail"),
          max(col("test_item_result_temp")).as("test_status")
        ).persist(StorageLevel.MEMORY_AND_DISK)

      //failure list, use ^A(SOH) separation
      val failDf = bobcatSourceDf.filter(col("test_item_result_temp").equalTo(1))
        .groupBy("product", "sn", "station_name", "test_starttime", "test_version")
        .agg(concat_ws(XWJKEPluginConstants.ctrlACode, collect_list("test_item")).as("list_of_failure"),
          concat_ws(XWJKEPluginConstants.ctrlACode, collect_set("test_item_result_detail_temp")).as("list_of_failure_detail"))

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


      val testDetailColumns = configContext.testDetailColumns.toUpperCase.split(",")
      val finalResultCount = finalResultsDF.count()
      println("final_count: " + finalResultCount)

      SummaryFile.totalRowsInXWJBobcatD = finalResultCount

      val finalResultsDFString = finalResultsDF
        .selectExpr(testDetailColumns: _*)
        .map(x => x.mkString("", configContext.dataSeperatorNonEscape, ""))
        .coalesce(1)

      configContext.fileDataSource.saveDfToTestDetailDir(finalResultsDFString)

    }

  }

}

