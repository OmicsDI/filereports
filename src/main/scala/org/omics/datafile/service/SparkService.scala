package org.omics.datafile.service

import org.apache.spark.sql.SparkSession

object SparkService {

  def getSparkSession(): SparkSession ={
    SparkSession.builder()
      .appName("mongozips")
      .master("local[*]")
      .getOrCreate()

  }

}
