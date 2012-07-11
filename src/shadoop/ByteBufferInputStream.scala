package shadoop

import java.nio.ByteBuffer
import java.io.InputStream

class ByteBufferInputStream(buffer: ByteBuffer) extends InputStream {
  override def read(): Int = {
    if (!buffer.hasRemaining()) {
      return -1
    }
    return buffer.get() & 0xFF;
  }
  
  override def read(dest: Array[Byte], offset: Int, length: Int): Int = {
    if (!buffer.hasRemaining()) {
      return -1;
    }
    val amountToGet = math.min(buffer.remaining(), length)
    buffer.get(dest, offset, amountToGet)
    return amountToGet
  }
  
  override def skip(bytes: Long): Long = {
    val amountToSkip = math.min(bytes, buffer.remaining).toInt
    buffer.position(buffer.position + amountToSkip)
    return amountToSkip
  }
}

