package com.foxconn.iisd.bd.rca

import java.io._
import java.net.URI
import java.nio.file.{Files, Paths}
import java.text.SimpleDateFormat
import java.util.Date

import com.foxconn.iisd.bd.rca.XWJCartridgePlugin.configLoader
import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveInputStream}
import org.apache.commons.compress.compressors.xz.XZCompressorInputStream
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object IoUtils {
    val BUFFERSIZE = 4096
    val TAIJIBASE_MAPPING = configLoader.getString("taiji_base", "code").split(",")


    def flatMinioFiles(spark: SparkSession, flag:String, srcPathStr: String, fileLimits: Integer): Path = {
        var count = 0

        val fileSystem = FileSystem.get(URI.create(srcPathStr), spark.sparkContext.hadoopConfiguration)

        val srcPath = new Path(srcPathStr)
        val destPath = new Path(new Path(srcPath.getParent, s"${srcPath.getName}_TMP"), flag)

        if(!fileSystem.exists(destPath)){
            fileSystem.mkdirs(destPath)
        }

        try {
            val wipPathFiles = fileSystem.listFiles(srcPath, true)
            while (count < fileLimits && wipPathFiles.hasNext()) {
                val file = wipPathFiles.next()

                val filename = file.getPath.getName
                val tmpFilePath = new Path(destPath, filename)

                if (file.getLen > 0) {
                    println(s"[MOVE] ${file.getPath} -> ${tmpFilePath.toString} : ${file.getLen}")

                    fileSystem.rename(file.getPath, tmpFilePath)

                    count = count + 1
                    Thread.sleep(2000)

                }
            }
        } catch {
            case ex: FileNotFoundException => {
                println("===> FileNotFoundException !!!")
            }
        }

        return destPath
    }


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


}
