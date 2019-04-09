package org.omics.datafile.service

import com.mongodb.spark._
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.sql.{Column, Encoders, SparkSession}
import com.mongodb.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions.explode
import org.omics.datafile.utils.Utilities

object FileReports {


  def main(args:Array[String]): Unit ={

    val sparkSession = SparkService.getSparkSession()

    val zipDf = sparkSession.read.mongo(MongoSparkService.getMongoReadConfig)

    val explodedMapDf =  zipDf.select(explode(zipDf.col("files")) as (Seq("acc", "fileList")))

    val explodedFiles =   explodedMapDf.select(explode(explodedMapDf.col("fileList")) as "files")

    import org.apache.spark.sql.functions.udf
    val getExtensionUdf = udf[String, String](Utilities.getExtension)

    val getWholeExtensionUdf = udf[String, String](Utilities.getSecondPart)

    explodedFiles
      .withColumn("fileExtension",getWholeExtensionUdf(explodedFiles("files")))
      .groupBy("fileExtension").count().coalesce(1).write.csv("/home/gaur/extensions.csv")

  }

}
