package org.omics.datafile.utils

import org.omics.datafile.service.FileReports

object Utilities {

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
