package shadoop

import java.nio.ByteBuffer
import java.io.OutputStream

class ByteBufferOutputStream(buffer: ByteBuffer) extends OutputStream {
  override def write(b: Int) {
    buffer.put(b.toByte);
  }

  override def write(bytes: Array[Byte], off: Int, len: Int) {
    buffer.put(bytes, off, len);
  } 
}