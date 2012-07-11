package shadoop

import spark.SerializableWritable

import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.{Job => HadoopJob}
import org.apache.hadoop.mapreduce.JobContext
import org.apache.hadoop.mapreduce.JobID

class WrappedJob(@transient hadoopJob: HadoopJob) extends Serializable {
  // This is wrapper around the Job configuration object so that this can be 
  // serialized and sent along with the task 
  private val confSerializable = new SerializableWritable[JobConf](
      hadoopJob.getConfiguration().asInstanceOf[JobConf])
  
  @transient 
  private var contextTransient: JobContext = null
  
  def conf(): JobConf = confSerializable.value 
  
  def context(): JobContext = {
    if (contextTransient == null) {
      contextTransient = new JobContext(conf, new JobID()) 
    }
    contextTransient
  }
  
  def disableSort() {
    set("spark.sort.disabled", "true")
  }
  
  def isSortDisabled(): Boolean = {
    get("spark.sort.disabled") == "true"
  }
  
  def disableMapperKeyValueClone() {
    set("spark.mapper.keyvalueclone.disabled", "true")
  }
  
  def isMapperKeyValueCloneDisabled(): Boolean = {
    get("spark.mapper.keyvalueclone.disabled") == "true"
  }
  
  def disableReducerKeyValueClone() {
    set("spark.reducer.keyvalueclone.disabled", "true")
  }
  
  def isReducerKeyValueCloneDisabled(): Boolean = {
    get("spark.reducer.keyvalueclone.disabled") == "true"
  }
  
  def set(key: String, value: String) {
    conf.set(key, value)
  }
  
  def get(key: String): String = {
    conf.get(key) 
  }
} 
