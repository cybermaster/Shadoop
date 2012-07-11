package shadoop

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.RawKeyValueIterator
import org.apache.hadoop.mapreduce.{Job => HadoopJob}
import org.apache.hadoop.mapreduce.JobContext
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.TaskAttemptID
import org.apache.hadoop.io.RawComparator
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.DataInputBuffer
import org.apache.hadoop.io.WritableUtils
import org.apache.hadoop.io.WritableComparable
import org.apache.hadoop.util.ReflectionUtils
import org.apache.hadoop.util.Progress

/**
 * This class is a wrapper around Hadoop Reducer (new API). It implements a custom Reducer.Context
 * that is given to the mapper object for processing the data.
 */
class WrappedReducer[
  MapOutputKey : ClassManifest, MapOutputValue : ClassManifest, 
  ReduceOutputKey : ClassManifest, ReduceOutputValue : ClassManifest](
    reducerClass: Class[Reducer[MapOutputKey, MapOutputValue, ReduceOutputKey, ReduceOutputValue]],          
    job: WrappedJob
  ) extends Serializable {
  
  if (job.conf.getMapOutputKeyClass() != implicitly[ClassManifest[MapOutputKey]].erasure) {
    throw new Exception("Specified type of MapOutputKey in WrappedReducer does not match MapOutputKey class in job conf")
  }
  
  if (job.conf.getMapOutputValueClass() != implicitly[ClassManifest[MapOutputValue]].erasure) {
    throw new Exception("Specified type of MapOutputValue in WrappedReducer does not match MapOutputValue class in job conf")
  }

  def run(iterator: Iterator[(MapOutputKey, Seq[MapOutputValue])]): Iterator[(ReduceOutputKey, ReduceOutputValue)] = {    
    
    // Sorting the keys using the job's comparator, just like Hadoop does
    //val useSort = job.conf.get("mapred.sort.disable", "false") != "true"    
    val useSort = !job.isSortDisabled
    val inputIterator = if (useSort) {
      val comparator: RawComparator[MapOutputKey] = job.conf.getOutputKeyComparator().asInstanceOf[RawComparator[MapOutputKey]]    
      val isWritableComparable = classOf[WritableComparable[MapOutputKey]].isAssignableFrom(job.conf.getMapOutputKeyClass())    
      val isWritable = classOf[Writable].isAssignableFrom(job.conf.getMapOutputKeyClass())
      val seq = iterator.toSeq
      println("Sorting " + seq.size + " keys")
      val startTime = System.currentTimeMillis()      
      val sortedSeq = seq.sortWith( (x: (MapOutputKey, Seq[MapOutputValue]), y: (MapOutputKey, Seq[MapOutputValue])) => {
        if (isWritableComparable) {
          x._1.asInstanceOf[WritableComparable[MapOutputKey]].compareTo(y._1) < 0
        } else if (isWritable) {
          val xBytes = WritableUtils.toByteArray(x._1.asInstanceOf[Writable])
          val yBytes = WritableUtils.toByteArray(y._1.asInstanceOf[Writable])
          comparator.compare(xBytes, 0, xBytes.length, yBytes, 0, yBytes.length) < 0
        } else {
          throw new Exception("Cannot sort, no idea how to compare objects of type " + job.conf.getMapOutputKeyClass())
        }
      })
      println("Sorted " + seq.size + " keys in " + (System.currentTimeMillis() - startTime) + " ms")
      sortedSeq.iterator        
    } else {
      iterator
    }
    
    val reducer = ReflectionUtils.newInstance(reducerClass, job.conf)
    val useClone = !job.isReducerKeyValueCloneDisabled()
    
    
    // This dummy iterator is used in the creation of reducer context as ReduceContext checks 
    // the availability of key-values in the constructor. It is not used anywhere else in this 
    // custom Reducer.Context
    val dummyIterator = new RawKeyValueIterator {
      def getKey(): DataInputBuffer = null
      def getValue(): DataInputBuffer = null
      def getProgress(): Progress = null
      def next() = inputIterator.hasNext
      def close() { }
    }
    
    // This is a custom Reducer.Context object that feeds data to the reducer object from Spark's iterator
    val reducerContext = new reducer.Context(job.conf, new TaskAttemptID(), dummyIterator, 
        null, null, null, null, new WrappedReporter(), null, 
        job.conf.getMapOutputKeyClass().asInstanceOf[Class[MapOutputKey]], 
        job.conf.getMapOutputKeyClass().asInstanceOf[Class[MapOutputValue]]) {
      // Option and None used instead of null because of this issue: 
      // http://stackoverflow.com/questions/7830731/parameter-type-in-structural-refinement-may-not-refer-to-an-abstract-type-defin
      var currentKey: Option[MapOutputKey] = None
      var currentValue: Option[MapOutputValue] = None
      var currentValueIterator: java.util.Iterator[MapOutputValue] = null
      val outputBuffer = new ArrayBuffer[(ReduceOutputKey, ReduceOutputValue)]()
      
      override def nextKey() = if (inputIterator.hasNext) {          
        val (key, values) = inputIterator.next()
        currentKey = Some(key)
        currentValueIterator = new java.util.Iterator[MapOutputValue]() {
          val iterator = values.iterator
          def hasNext() = iterator.hasNext
          def next() = iterator.next()              
          def remove() { }
        }
        true
      } else {
        currentKey = None
        currentValueIterator = null
        false
      }
      
      override def nextKeyValue() = {
        if (currentKey == null) {
          if (!nextKey()) {        
            false
          } else {
            if (currentValueIterator.hasNext()) {
              currentValue = Some(currentValueIterator.next())
              true
            } else {
              currentValue = None
              false
            }
          }        
        } else {          
          if (currentValueIterator.hasNext()) {
            currentValue = Some(currentValueIterator.next())
            true
          } else {
            currentValue = None
            false
          }
        }
      }
      
      override def getCurrentKey(): MapOutputKey = 
        if (currentKey.isDefined) currentKey.get else null.asInstanceOf[MapOutputKey]
      
      override def getCurrentValue(): MapOutputValue = 
        if (currentValue.isDefined) currentValue.get else null.asInstanceOf[MapOutputValue] 
      
      override def getValues(): java.lang.Iterable[MapOutputValue] = 
        new java.lang.Iterable[MapOutputValue]() {
          def iterator(): java.util.Iterator[MapOutputValue] = currentValueIterator
        }
      
      override def write(key: ReduceOutputKey, value: ReduceOutputValue) {
        if (useClone) {          
          val clonedKey = if (key != null) {
            WritableUtils.clone(key.asInstanceOf[Writable], job.conf).asInstanceOf[ReduceOutputKey]
          } else {
            null.asInstanceOf[ReduceOutputKey]
          }
          val clonedValue= if (value != null) {
            WritableUtils.clone(value.asInstanceOf[Writable], job.conf).asInstanceOf[ReduceOutputValue]
          } else {
            null.asInstanceOf[ReduceOutputValue]
          }
          outputBuffer += ((clonedKey, clonedValue))
        } else {
          outputBuffer += ((key, value))
        } 
      }
      
      def outputIterator() = outputBuffer.toIterator
    }
      
    reducer.run(reducerContext)
    reducerContext.outputIterator
  }
}
  

