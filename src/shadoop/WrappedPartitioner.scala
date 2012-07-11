package shadoop

import spark.Partitioner

import org.apache.hadoop.mapreduce.{Job => HadoopJob}
import org.apache.hadoop.mapreduce.{Partitioner => HadoopPartitioner}
import org.apache.hadoop.util.ReflectionUtils



/**
 * This class is a wrapper around Hadoop Partitioner (new API).
 */  
class WrappedPartitioner[MapOutputKey, MapOutputValue](
    partitionerClass: Class[HadoopPartitioner[MapOutputKey, MapOutputValue]],
    numReducers: Int, 
    job: WrappedJob) extends Partitioner() {
  
  //val job = new WrappedJob(hadoopJob) 
  @transient var hPartitioner: HadoopPartitioner[MapOutputKey, MapOutputValue] = null
  
  def hadoopPartitioner(): HadoopPartitioner[MapOutputKey, MapOutputValue] = {
    if (hPartitioner == null) {
      hPartitioner = ReflectionUtils.newInstance(partitionerClass, job.conf)
    }
    hPartitioner
  }
  
  //lazy val set = new HashSet[Any]() 
  
  override def numPartitions = numReducers
  
  override def getPartition(key: Any): Int = {      
    //if (set.add(key.hashCode())) println("" + set.size() + " unique keys")
    hadoopPartitioner.getPartition(
      key.asInstanceOf[MapOutputKey], null.asInstanceOf[MapOutputValue], numPartitions)
  }
}