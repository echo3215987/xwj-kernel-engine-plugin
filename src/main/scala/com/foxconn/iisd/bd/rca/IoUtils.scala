package com.foxconn.iisd.bd.rca

import java.io._
import java.net.URI
import java.nio.file.{Files, Paths}
import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.Date

import org.apache.hadoop.fs.{FileSystem, Path, FileUtil}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object IoUtils {

    def flatMinioFiles(spark: SparkSession, srcPathStr: String, fileLimits: Long,
                       yearMonth: String, day: String, hourMinuteSecond: String): Path = {
        var count = 1
        var totalSize: Long = 0

        val fileSystem = FileSystem.get(URI.create(srcPathStr), spark.sparkContext.hadoopConfiguration)

        val srcPath = new Path(srcPathStr)
        val destPath =
            new Path(
                new Path(
                    new Path(
                        new Path(srcPath.getParent, s"${srcPath.getName}_TMP"),
                        s"${yearMonth}"),
                    s"${day}"),
                s"${hourMinuteSecond}")

        if(!fileSystem.exists(destPath)){
            fileSystem.mkdirs(destPath)
        }

        try {
            val pathFiles = fileSystem.listFiles(srcPath, true)
            while (totalSize <= fileLimits && pathFiles.hasNext()) {
                val file = pathFiles.next()
                val filename = file.getPath.getName
                val tmpFilePath = new Path(destPath, filename)
                if (file.getLen > 0) {
                    println("number : " + count +
                      " , [MOVE] " + file.getPath + " -> " + tmpFilePath.toString +
                      " , file size : " + IoUtils.getNetFileSizeDescription(file.getLen))
//                    FileUtil.copy(fileSystem, file.getPath, fileSystem, tmpFilePath, false, true, spark.sparkContext.hadoopConfiguration)
                    fileSystem.rename(file.getPath, tmpFilePath)
                    count = count + 1
                    totalSize = totalSize + file.getLen
                    println("totalSize add getLen : " + IoUtils.getNetFileSizeDescription(totalSize))
                    Thread.sleep(2000)
                }
            }
            XWJCartridgePlugin.totalRawDataSize = totalSize
            println("files total size : " + IoUtils.getNetFileSizeDescription(totalSize))

        } catch {
            case ex: FileNotFoundException => {
                println("===> FileNotFoundException !!!")
            }
        }
        return destPath
    }

//    def flatMinioFiles(spark: SparkSession, flag:String, srcPathStr: String, fileLimits: Integer): Path = {
//        var count = 0
//
//        val fileSystem = FileSystem.get(URI.create(srcPathStr), spark.sparkContext.hadoopConfiguration)
//
//        val srcPath = new Path(srcPathStr)
//        val destPath = new Path(new Path(srcPath.getParent, s"${srcPath.getName}_TMP"), flag)
//
//        if(!fileSystem.exists(destPath)){
//            fileSystem.mkdirs(destPath)
//        }
//
//        try {
//            val wipPathFiles = fileSystem.listFiles(srcPath, true)
//            while (count < fileLimits && wipPathFiles.hasNext()) {
//                val file = wipPathFiles.next()
//
//                val filename = file.getPath.getName
//                val tmpFilePath = new Path(destPath, filename)
//
//                if (file.getLen > 0) {
//                    println(s"[MOVE] ${file.getPath} -> ${tmpFilePath.toString} : ${file.getLen}")
//
//                    fileSystem.rename(file.getPath, tmpFilePath)
//
//                    count = count + 1
//                    Thread.sleep(2000)
//
//                }
//            }
//        } catch {
//            case ex: FileNotFoundException => {
//                println("===> FileNotFoundException !!!")
//            }
//        }
//
//        return destPath
//    }


    def getDfFromPath(spark: SparkSession, path: String, columns: String, dataSeperator: String): DataFrame = {

        val schema = StructType(columns
          .split(",")
          .map(fieldName => StructField(fieldName,StringType, true)))
        val rdd = spark
          .sparkContext
          .textFile(path)
          .map(_.replace("'", "、"))
          .map(_.split(dataSeperator, schema.fields.length).map(field => {
              if(field.isEmpty)
                  ""
              else
                  field.trim
          }))
          .map(p => Row(p: _*))

        rdd.take(10).map(println)

        return spark.createDataFrame(rdd, schema)
    }

    def moveFilesByJobStatus(spark: SparkSession, pluginPath: String, jobStatus: Boolean, jobId: String, yearMonth: String,
                             day: String, hourMinuteSecond: String): Unit = {
        println(s"--> moveFilesByJobStatus")
        try {
            var status = ""
            if(jobStatus) {
                println(s"Job Status : Succeeded")
                status = "Succeeded"
            } else {
                println(s"Job Status : Failed")
                status = "Failed"
            }
            val fileSystem = FileSystem.get(URI.create(pluginPath), spark.sparkContext.hadoopConfiguration)
            moveFiles(pluginPath, fileSystem, yearMonth, day, hourMinuteSecond, jobId, status)
        } catch {
            case ex: Exception => {
                println("===> Exception !!!")
            }
        }
    }


    def moveFiles(srcFilePath: String, fileSystem: FileSystem,
                  yearMonth: String, day: String, hourMinuteSecond: String,
                  jobId: String, status: String): Unit = {
        try {

            val srcPath = new Path(srcFilePath)
            val srcTmpPath =
                new Path(
                    new Path(
                        new Path(
                            new Path(srcPath.getParent, s"${srcPath.getName}_TMP"),
                            s"${yearMonth}"),
                        s"${day}"),
                    s"${hourMinuteSecond}")
            val destPath =
                new Path(
                    new Path(
                        new Path(
                            new Path(srcPath.getParent, s"${srcPath.getName}_${status}"),
                            s"${yearMonth}"),
                        s"${day}"),
                    s"${jobId}")
            if(!fileSystem.exists(destPath)){
                fileSystem.mkdirs(destPath)
            }
            val pathFiles = fileSystem.listFiles(srcTmpPath, true)
            var count = 1
            while (pathFiles.hasNext()) {
                val file = pathFiles.next()
                val filename = file.getPath.getName
                val destFilePath = new Path(destPath, filename)
                if (file.getLen > 0) {
                    println("number : " + count + " , [MOVE] " + file.getPath + " -> " + destFilePath.toString)
                    fileSystem.rename(file.getPath, destFilePath)
                    Thread.sleep(2000)
                }
                count = count + 1
            }
            // no file situation
            if(count == 1) {
                fileSystem.delete(destPath, true)
            }
            fileSystem.delete(srcTmpPath, true)
        } catch {
            case ex: Exception => {
                println("===> Exception !!!")
            }
        }
    }

    def getNetFileSizeDescription(size :Long): String = {
        val bytes = new StringBuffer()
        val format = new DecimalFormat("###.0")
        if (size >= 1024 * 1024 * 1024) {
            val i = (size / (1024.0 * 1024.0 * 1024.0))
            bytes.append(format.format(i)).append("GB")
        }
        else if (size >= 1024 * 1024) {
            val i = (size / (1024.0 * 1024.0));
            bytes.append(format.format(i)).append("MB")
        }
        else if (size >= 1024) {
            val i = (size / (1024.0));
            bytes.append(format.format(i)).append("KB")
        }
        else if (size < 1024) {
            if (size <= 0) {
                bytes.append("0B")
            }
            else {
                bytes.append(size.toInt).append("B")
            }
        }
        bytes.toString()
    }
}
