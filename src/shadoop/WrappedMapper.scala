package shadoop

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.RawKeyValueIterator
import org.apache.hadoop.mapreduce.{Job => HadoopJob}
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.JobContext
import org.apache.hadoop.mapreduce.TaskAttemptID
import org.apache.hadoop.util.ReflectionUtils
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.WritableUtils


/**
 * This class is a wrapper around Hadoop Mapper (new API). It implements a custom Mapper.Context
 * that is given to the mapper object for processing the data.
 */
class WrappedMapper[
  MapInputKey : ClassManifest, MapInputValue : ClassManifest, 
  MapOutputKey : ClassManifest, MapOutputValue : ClassManifest](
    mapperClass: Class[Mapper[MapInputKey, MapInputValue, MapOutputKey, MapOutputValue]],
    job: WrappedJob
  ) extends Serializable {
  
  if (job.conf.getMapOutputKeyClass() != implicitly[ClassManifest[MapOutputKey]].erasure) {
    throw new Exception("Specified type of MapOutputKey in WrappedMapper does not match MapOutputKey class in job conf")
  }
  
  if (job.conf.getMapOutputValueClass() != implicitly[ClassManifest[MapOutputValue]].erasure) {
    throw new Exception("Specified type of MapOutputValue in WrappedMapper does not match MapOutputValue class in job conf")
  }
 
  def run(inputIterator: Iterator[(MapInputKey, MapInputValue)]): Iterator[(MapOutputKey, MapOutputValue)] = {
    
    val mapper = ReflectionUtils.newInstance(mapperClass, job.conf)
    val useClone = !job.isMapperKeyValueCloneDisabled()
    
    val mapperContext = new mapper.Context(
        job.conf, new TaskAttemptID(), null, null, null, new WrappedReporter(), null) {
      
      var currentKeyValue: (MapInputKey, MapInputValue) = null
      val outputBuffer = new ArrayBuffer[(MapOutputKey, MapOutputValue)]() 

      override def nextKeyValue() = if (inputIterator.hasNext) {
        currentKeyValue = inputIterator.next()
        true
      } else {
        currentKeyValue = null
        false
      }      

      override def getCurrentKey(): MapInputKey = if (currentKeyValue == null) 
        null.asInstanceOf[MapInputKey] else currentKeyValue._1       

      override def getCurrentValue(): MapInputValue = if (currentKeyValue == null) 
        null.asInstanceOf[MapInputValue] else currentKeyValue._2

      override def write(key: MapOutputKey, value: MapOutputValue) { 
        if (useClone) {
          val clonedKey = if (key != null) {
            WritableUtils.clone(key.asInstanceOf[Writable], job.conf).asInstanceOf[MapOutputKey]
          } else {
            null.asInstanceOf[MapOutputKey]
          }
          val clonedValue = if (value != null) {
            WritableUtils.clone(value.asInstanceOf[Writable], job.conf).asInstanceOf[MapOutputValue]
          } else {
            null.asInstanceOf[MapOutputValue]
          }
          outputBuffer += ((clonedKey, clonedValue))
        } else {
          outputBuffer += ((key, value))
        }    
        
        
      }

      def outputIterator() = {
        println("# map output key-value pairs = " + outputBuffer.length)        
        outputBuffer.toIterator
      }
    }
    
    mapper.run(mapperContext)
    mapperContext.outputIterator()       
  }
}
