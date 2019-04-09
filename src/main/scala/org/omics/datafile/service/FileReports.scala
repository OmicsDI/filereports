package org.omics.datafile.service

import com.mongodb.spark._
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.sql.{Column, Encoders, SparkSession}
import com.mongodb.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions.explode

object FileReports {


  def main(args:Array[String]): Unit ={
    val sparkSession = SparkSession.builder()
      .appName("mongozips")
      .master("local[*]")
      .getOrCreate()


    val zipDf = sparkSession.read.mongo(readConfig)


    val explodedMapDf =  zipDf.select(explode(zipDf.col("files")) as (Seq("acc", "fileList")))

    val explodedFiles =   explodedMapDf.select(explode(explodedMapDf.col("fileList")) as "files")

    import org.apache.spark.sql.functions.udf
    val getExtensionUdf = udf[String, String](getExtension)

    val getWholeExtensionUdf = udf[String, String](getSecondPart)

    explodedFiles
      .withColumn("fileExtension",getWholeExtensionUdf(explodedFiles("files")))
      .groupBy("fileExtension").count().coalesce(1).write.csv("/home/gaur/extensions.csv")

  }

  val reg_ex = """.*\.(\w+)""".r

  def getExtension(dt:String):String={
    dt match {
      case reg_ex(ext) => ext
      case _ =>  "NoExtension"
    }
  }

  def getSecondPart(in:String):String = {
    val fileName = in.split("/").last
    if(fileName.contains("."))
      fileName.substring(fileName.indexOf('.'), fileName.length)
    else
      "NoExtension"
  }

}
