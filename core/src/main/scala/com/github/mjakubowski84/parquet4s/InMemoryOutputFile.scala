package com.github.mjakubowski84.parquet4s

import org.apache.hadoop.fs.FileAlreadyExistsException
import org.apache.parquet.io.{OutputFile, PositionOutputStream}

import java.io.ByteArrayOutputStream
import scala.language.reflectiveCalls

object InMemoryOutputFile {
  val DefaultBlockSize: Int = 64 << 10

  def create(
      initBufferSize: Int,
      maxBufferSize: Option[Int] = None,
      blockSize: Int             = DefaultBlockSize
  ): InMemoryOutputFile =
    new InMemoryOutputFile(initBufferSize, maxBufferSize.getOrElse(3 * initBufferSize), blockSize)
}

class InMemoryOutputFile(initBufferSize: Int, maxBufferSize: Int, blockSize: Int = InMemoryOutputFile.DefaultBlockSize)
    extends OutputFile {
  private val os = new ByteArrayOutputStream(initBufferSize) {
    def takeAndReuse: Array[Byte] = {
      val content = toByteArray()
      if (buf.length > maxBufferSize) {
        buf = new Array[Byte](initBufferSize)
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

  override def supportsBlockSize(): Boolean = true

  override def defaultBlockSize(): Long = blockSize

  def takeAndReuse(): Array[Byte] = os.takeAndReuse

  def contentLength: Int = os.size()
}
