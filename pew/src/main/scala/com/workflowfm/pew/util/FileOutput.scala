package com.workflowfm.pew.util

/** Helper to write stuff to a file.
  */
trait FileOutput {
  import java.io._

  def writeToFile(filePath: String, output: String): Option[Exception] = try {
    val file = new File(filePath)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(output)
    bw.close()
    None
  } catch {
    case e: Exception => Some(e)
  }
}
