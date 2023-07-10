package com.github.mjakubowski84.parquet4s

import org.apache.hadoop.fs.FileAlreadyExistsException
import org.apache.parquet.io.{InputFile, OutputFile, PositionOutputStream}

import java.io.ByteArrayOutputStream
import scala.collection.mutable
import scala.language.reflectiveCalls
import scala.util.control.NoStackTrace

object InMemoryOutputFile {
  def create(initSize: Int): InMemoryOutputFile = create(initSize, 3 * initSize)
  def create(initSize: Int, maxSize: Int): InMemoryOutputFile = new InMemoryOutputFile(initSize, maxSize)
}

class InMemoryOutputFile(initSize: Int, maxSize: Int) extends OutputFile {
  private val os = new ByteArrayOutputStream(initSize) {
    def takeAndReuse: Array[Byte] = {
      val content = toByteArray()
      if (buf.length > maxSize) {
        buf = new Array[Byte](initSize)
      }
      count = 0
      content
    }
  }

  override def create(blockSizeHint: Long): PositionOutputStream = {
    if (os.size() > 0) throw new FileAlreadyExistsException(s"In-memory file already exists")
    new PositionOutputStream {
      override def getPos: Long                                    = os.size()
      override def write(b: Int): Unit                             = os.write(b)
      override def write(b: Array[Byte], off: Int, len: Int): Unit = os.write(b, off, len)
    }
  }

  override def createOrOverwrite(blockSizeHint: Long): PositionOutputStream = {
    os.reset()
    create(blockSizeHint)
  }

  override def supportsBlockSize(): Boolean = false

  override def defaultBlockSize(): Long =
    throw new UnsupportedOperationException("Block size is not supported by InMemoryOutputFile") with NoStackTrace

  def takeAndReuse(): Array[Byte] = os.takeAndReuse

  def contentLength: Int = os.size()
}
