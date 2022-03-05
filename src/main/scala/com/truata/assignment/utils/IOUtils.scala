package com.truata.assignment.utils

import java.io.{File, FileWriter}
import scala.util.Try

object IOUtils {

  def writeToFile(filePath: String, content: AnyRef): Try[Unit] = {
    Try {
      val fileWriter = new FileWriter(new File(filePath))
      try {
        fileWriter.write(content.toString)
      } finally {
        fileWriter.close()
      }
    }
  }

  def writeArrayToFile(filePath: String, array: Array[_ <: AnyRef]): Try[Unit] = {
    val content = array.map(row => row.toString).mkString("\n")
    writeToFile(filePath, content)
  }

}