package com.foxconn.iisd.bd.rca

import java.io._
import java.net.URI
import java.nio.file.{Files, Paths}

import com.foxconn.iisd.bd.rca.XWJKernelEnginePlugin.configLoader
import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveInputStream}
import org.apache.commons.compress.compressors.xz.XZCompressorInputStream
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object IoUtils {
    val BUFFERSIZE = 4096
    val TAIJIBASE_MAPPING = configLoader.getString("taiji_base", "code").split(",")

    def decompressMinioFiles(spark: SparkSession, flag:String, srcPathStr: String, fileLimits: Integer, resultPathStr: String): Path = {
        var count = 0

        val fileSystem = FileSystem.get(URI.create(srcPathStr), spark.sparkContext.hadoopConfiguration)
        val srcPath = new Path(srcPathStr)
        val destPathStr = s"${srcPath.getName}_TMP"
        val destPath = new Path(new Path(srcPath.getParent, destPathStr), flag)
        val resultPath = new Path(new Path(srcPath.getParent, resultPathStr), flag)

        if(!fileSystem.exists(destPath)){
            fileSystem.mkdirs(destPath)
        }

        if(!fileSystem.exists(resultPath)){
            fileSystem.mkdirs(resultPath)
        }

        try {
            val wipPathFiles = fileSystem.listFiles(srcPath, true)
            while (count < fileLimits && wipPathFiles.hasNext()) {
                val file = wipPathFiles.next()

                val filename = file.getPath.getName
                val filePath = new Path(srcPath, filename)
                val tmpFilePath = new Path(destPath, filename)
                val resultFilePath = new Path(resultPath, filename)


                if (file.getLen > 0) {
                    //val fin_tar = Files.newInputStream(Paths.get(srcPathStr))
                    val fin_tar = fileSystem.open(filePath)
                    IoUtils.unxzfile(fin_tar, destPath.toString, fileSystem)

                    println(s"[MOVE] ${file.getPath} -> ${resultPath.toString} : ${file.getLen}")
                    fileSystem.rename(file.getPath, resultFilePath)

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

    def moveFileToTestDetail(spark: SparkSession, flag: String, srcPathStr: String, statusPathStr: String, destPathStr: String): Unit = {
        var count = 0
        val fileSystem = FileSystem.get(URI.create(srcPathStr), spark.sparkContext.hadoopConfiguration)

        val srcPath = new Path(srcPathStr, flag) //temp

        val statusPath = new Path(statusPathStr, flag) //status : successful
        if(!fileSystem.exists(statusPath)){
            fileSystem.mkdirs(statusPath)
        }

        val destPath = new Path(destPathStr, flag) //test_detail
        if(!fileSystem.exists(destPath)){
            fileSystem.mkdirs(destPath)
        }

        try {

            val wipPathFiles = fileSystem.listFiles(srcPath, true)
            while (wipPathFiles.hasNext()) {
                val file = wipPathFiles.next()
                val filename = file.getPath.getName
                val tmpFilePath = new Path(destPath, filename)
                if (file.getLen > 0) {
                    if(filename.indexOf("part-") == 0){
                        //將result txt搬到test_detail
                        println(s"[MOVE] ${file.getPath} -> ${tmpFilePath.toString} : ${file.getLen}")
                        fileSystem.rename(file.getPath, tmpFilePath)

                        println(s"[DELETE] ${file.getPath.getParent}")
                        if(fileSystem.exists(file.getPath.getParent)) {
                            fileSystem.delete(file.getPath.getParent, true)
                        }

                    }else{
                        //將xml搬到successful
                        println(s"[MOVE] ${file.getPath} -> ${statusPath.toString} : ${file.getLen}")
                        fileSystem.rename(file.getPath, statusPath)
                    }

                    count = count + 1
                    Thread.sleep(2000)
                }
            }

        } catch {
            case ex: Exception => {
                println("ex:" + ex.printStackTrace())
            }
        }

    }

    def moveFileToFailed(spark: SparkSession, flag: String, srcPathStr: String, statusPathStr: String, destPathStr: String): Unit = {
        var count = 0
        val fileSystem = FileSystem.get(URI.create(srcPathStr), spark.sparkContext.hadoopConfiguration)

        val srcPath = new Path(srcPathStr, flag) //temp

        val statusPath = new Path(statusPathStr, flag) //status : failed
        if(!fileSystem.exists(statusPath)){
            fileSystem.mkdirs(statusPath)
        }

        val destPath = new Path(destPathStr, flag) //test_detail
        if(!fileSystem.exists(destPath)){
            fileSystem.mkdirs(destPath)
        }

        try {

            val wipPathFiles = fileSystem.listFiles(srcPath, true)
            while (wipPathFiles.hasNext()) {
                val file = wipPathFiles.next()
                val filename = file.getPath.getName
                val tmpFilePath = new Path(destPath, filename)

                if (file.getLen > 0) {

                    //將xml搬到failed
                    println(s"[MOVE] ${file.getPath} -> ${statusPath.toString} : ${file.getLen}")
                    fileSystem.rename(file.getPath, statusPath)

                    count = count + 1
                    Thread.sleep(2000)
                }
            }

        } catch {
            case ex: Exception => {
                println("ex:" + ex.printStackTrace())
            }
        }

    }

    def deleteFileFromCompression_Temp(spark: SparkSession, flag: String, srcPathStr: String): Unit = {
        var count = 0
        val fileSystem = FileSystem.get(URI.create(srcPathStr), spark.sparkContext.hadoopConfiguration)

        val srcPath = new Path(srcPathStr, flag)

        try {
            val wipPathFiles = fileSystem.listFiles(srcPath, true)
            while (wipPathFiles.hasNext()) {
                val file = wipPathFiles.next()
                val filename = file.getPath.getName
                val filePath = new Path(srcPath, filename)

                if (file.getLen > 0) {

                    println(s"[DELETE]  ${file.getPath} : ${file.getLen}")
                    if(fileSystem.exists(filePath)) {
                        fileSystem.delete(filePath, true)
                    }

                    count = count + 1
                    Thread.sleep(2000)
                }
            }

        } catch {
            case ex: Exception => {
                println("ex:" + ex.printStackTrace())
            }
        }

    }

    //parse xz file
    def unxzfile( in: InputStream, destinationDir: String, fs: FileSystem ) {
        try{
            def processTar( tarIn: TarArchiveInputStream ): Unit = {
                def processFileInTar( dest: BufferedOutputStream ): Unit = {
                    val data = new Array[ Byte ]( BUFFERSIZE)
                    val count = tarIn.read( data, 0, BUFFERSIZE )
                    count match {
                        case -1 =>
                            dest.close( )
                        case _ =>
                            dest.write( data, 0, count )
                            processFileInTar( dest )
                    }
                }
                tarIn.getNextEntry.asInstanceOf[ TarArchiveEntry ]
                match {
                    case null =>
                    case a: TarArchiveEntry if a.isDirectory =>
                        val f: File = new File( a.getName )
                        f.mkdirs( )
                        processTar( tarIn )
                    case a: TarArchiveEntry if a.isFile =>

                        if(a.getName.contains("WuDang") && !a.getName().contains("Repair")){
                            import scala.util.control._
                            val loop = new Breaks
                            loop.breakable {
                                for (code <- TAIJIBASE_MAPPING) {
                                    if (a.getName().contains(code)) {
                                        println(a.getName)
                                        println(a.getName.split("/").last)
                                        println(destinationDir + "/" + a.getName.split("/").last)
                                        /*val fos: FileOutputStream = new FileOutputStream(destinationDir + "/" + a.getName.split("/").last)
                                        val dest: BufferedOutputStream = new BufferedOutputStream(fos,
                                            BUFFERSIZE)*/

                                        val filePath = new Path(destinationDir, a.getName.split("/").last)
                                        val dest: BufferedOutputStream = new BufferedOutputStream(new BufferedOutputStream(fs.create(filePath)))

                                        processFileInTar(dest)
                                        println("Extracting: "+a.getName)
                                        loop.break
                                    }
                                }
                            }
                        }
                        processTar( tarIn )
                    case a: TarArchiveEntry => processTar( tarIn )
                }
            }

            val xzIn: XZCompressorInputStream = new XZCompressorInputStream( in )
            val tarIn: TarArchiveInputStream = new TarArchiveInputStream( xzIn )
            processTar( tarIn )
            tarIn.close( )
            println( "unxz completed successfully." )
        }
        catch {
            case ex: FileNotFoundException => {
                // ex.printStackTrace()
                println("===> FileNotFoundException !!!")
            }
        }
    }

}
