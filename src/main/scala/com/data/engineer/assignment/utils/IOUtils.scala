package com.data.engineer.assignment.utils

import java.io.{File, FileWriter}

object IOUtils {

  def writeToFile(filePath: String, content: AnyRef): Unit = {
      val fileWriter = new FileWriter(new File(filePath))
      try {
        fileWriter.write(content.toString)
      } finally {
        fileWriter.close()
      }
  }

  def writeArrayToFile(filePath: String, array: Array[_ <: AnyRef]): Unit = {
    val content = array.map(row => row.toString).mkString("\n")
    writeToFile(filePath, content)
  }

}