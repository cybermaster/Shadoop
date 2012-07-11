package shadoop

import java.nio.ByteBuffer
import java.io.DataOutputStream
import java.io.DataInputStream
import org.apache.hadoop.io.ObjectWritable
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.NullWritable
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.{Serializer => KSerializer}
import spark.KryoRegistrator
import spark.KryoSerializer
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.util.ReflectionUtils

class WritableSerializer[T <: Writable: ClassManifest]() extends KSerializer { 
  lazy val jobConf = new JobConf()
  
  override def writeObjectData(buf: ByteBuffer, obj: AnyRef) {
    val outputStream = new DataOutputStream(new ByteBufferOutputStream(buf))
    obj.asInstanceOf[Writable].write(outputStream)
  }
  
  override def readObjectData[S](buf: ByteBuffer, cls: Class[S]): S = {
    //val instance = implicitly[ClassManifest[T]].erasure.newInstance().asInstanceOf[T]
    val instance = ReflectionUtils.newInstance(implicitly[ClassManifest[T]].erasure, null).asInstanceOf[Writable]
    instance.readFields(new DataInputStream(new ByteBufferInputStream(buf)))
    instance.asInstanceOf[S]
  }
} 

class NullWritableSerializer() extends KSerializer {
  override def writeObjectData(buf: ByteBuffer, obj: AnyRef) { }
  override def readObjectData[T](buf: ByteBuffer, cls: Class[T]): T = NullWritable.get().asInstanceOf[T]
}

class WritableRegistrator extends KryoRegistrator {
  def registerClasses(kryo: Kryo) {
    kryo.register(classOf[NullWritable], new NullWritableSerializer())
    kryo.register(classOf[IntWritable], new WritableSerializer[IntWritable]())
    kryo.register(classOf[LongWritable], new WritableSerializer[LongWritable]())
    kryo.register(classOf[Text], new WritableSerializer[Text]())
  }
}

object WritableRegistrator {
  def main(args: Array[String]) {
    /*
    val item = new IntWritable(1234)
    val buf = ByteBuffer.allocate(1024 * 1024)
    buf.clear()
    val writable = new ObjectWritable(item)
    val outputStream = new DataOutputStream(new ByteBufferOutputStream(buf))
    writable.write(outputStream)
    buf.flip()
    println("serialized to " + buf.remaining() + " bytes")
    while(buf.hasRemaining()) {
      val byte = buf.get()
      print(byte + "(" + byte.toChar + ") ")
    }
    println("\ndone")
    buf.rewind()
    val instance = new ObjectWritable()
    instance.setConf(new JobConf())
    instance.readFields(new DataInputStream(new ByteBufferInputStream(buf)))
    println("deserialized")
    println(instance.get().asInstanceOf[IntWritable])
    */
    
    System.setProperty("spark.kryo.registrator", "testscala.WritableRegistrator")
    //val list = (0 until 10).map(x => new Text(x.toString)).toIterator
    val list = (0 until 10).map(x => NullWritable.get()).toIterator
    val serializer = new KryoSerializer()
    val serializerInstance = serializer.newInstance()
    val buffer = ByteBuffer.allocate(1024 * 1024)
    serializerInstance.serializeStream(new ByteBufferOutputStream(buffer)).writeAll(list)
    buffer.flip()
    println("serialized to " + buffer.remaining() + " bytes")
    while(buffer.hasRemaining()) {
      print(buffer.get() + " ")
    }
    println("\ndone")
    buffer.rewind()
    val serializerInstance1 = serializer.newInstance()
    val list1 = serializerInstance1.deserializeStream(new ByteBufferInputStream(buffer)).toIterator
    println("deserialized")
    list1.foreach(println)
    println("\ndone")
    
  }
}

