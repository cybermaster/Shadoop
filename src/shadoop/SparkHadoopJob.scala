package shadoop

import spark.RDD
import spark.UnionRDD
import spark.SparkContext
import spark.SparkContext._
import spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.lib.MultipleInputs
import org.apache.hadoop.mapreduce.{Job => HadoopJob}
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.hadoop.mapreduce.OutputFormat
import org.apache.hadoop.mapreduce.{Partitioner => HadoopPartitioner}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.ReflectionUtils

 

/**
 * This class is wrapper around a Hadoop job (new API), that allows it to be run as a Spark job.
 */
class SparkHadoopJob[
     MapInputKey : ClassManifest, MapInputValue : ClassManifest, 
     MapOutputKey : ClassManifest, MapOutputValue : ClassManifest, 
     ReduceOutputKey : ClassManifest, ReduceOutputValue : ClassManifest
   ] (@transient hadoopJob: HadoopJob) extends Serializable { 
  
  protected val job = new WrappedJob(hadoopJob)
  
  protected var inputRDD: RDD[(MapInputKey, MapInputValue)] = null
  protected var mappedRDDStorageLevel: StorageLevel = null
  protected var reducedRDDStorageLevel: StorageLevel = null
  protected var saveReducedRDD = true
  
  var mappedRDD: RDD[(MapOutputKey, MapOutputValue)] = null
  var reducedRDD: RDD[(ReduceOutputKey, ReduceOutputValue)] = null
  
  /**
   *  This function performs the map stage of Hadoop. It reads input sources with the same 
   * input format, unifies them and applies the Hadoop mapper (as specified by the job conf) 
   * to all of them to create a mapped RDD.
   */ 
  protected def createMappedRDD(sc: SparkContext): RDD[(MapOutputKey, MapOutputValue)] = {

    // Create Spark's wrapper around the Hadoop mapper class to do the map phase
    val mapperClass = job.context.getMapperClass()
      .asInstanceOf[Class[Mapper[MapInputKey, MapInputValue, MapOutputKey, MapOutputValue]]]
    val wrappedMapper = new WrappedMapper(mapperClass, job) 
      
    if (inputRDD != null) {
      // Map the input RDD using the WrappedMapper
      inputRDD.mapPartitions(wrappedMapper.run)
    
    } else {
      // Create HadoopRDDs from input paths, unify them and then map them using
      // the WrappedMapper
      val inputPaths: Iterable[String] = FileInputFormat.getInputPaths(job.context).map(_.toString)
      
      val mapInputKeyClass: Class[MapInputKey] = 
        implicitly[ClassManifest[MapInputKey]].erasure.asInstanceOf[Class[MapInputKey]]
      val mapInputValueClass: Class[MapInputValue] = 
        implicitly[ClassManifest[MapInputValue]].erasure.asInstanceOf[Class[MapInputValue]]
      val inputFormatClass: Class[InputFormat[MapInputKey, MapInputValue]] =
        job.context.getInputFormatClass().asInstanceOf[Class[InputFormat[MapInputKey, MapInputValue]]]
      
      val inputRDDs = inputPaths.map(path => {
        sc.newAPIHadoopFile[MapInputKey, MapInputValue, InputFormat[MapInputKey, MapInputValue]](
          path, inputFormatClass, mapInputKeyClass, mapInputValueClass, job.conf)
      })
      
      val unifiedInputRDDs = new UnionRDD(sc, inputRDDs.toSeq) 
      unifiedInputRDDs.mapPartitions(wrappedMapper.run)      
    }         
  }
  
  /**
   *  This function performs Hadoop-style reduce stage on the given mapped RDD. It uses the 
   *  specified partitioner to partition the data, then uses the specified comparator and 
   *  reducer to sort and reduce the data.
   */
  protected def createReducedRDD(mappedRDD: RDD[(MapOutputKey, MapOutputValue)]): RDD[(ReduceOutputKey, ReduceOutputValue)] = {
    // Create Spark's Partitioner object to custom partition the map output RDD
    val numReducers = job.context.getNumReduceTasks()
    val partitionerClass = job.context.getPartitionerClass()
            .asInstanceOf[Class[HadoopPartitioner[MapOutputKey, MapOutputValue]]]   
    val wrappedPartitioner = new WrappedPartitioner(partitionerClass, numReducers, job)
    val reducerClass = job.context.getReducerClass()
      .asInstanceOf[Class[Reducer[MapOutputKey, MapOutputValue, ReduceOutputKey, ReduceOutputValue]]]
    val wrappedReducer = new WrappedReducer(reducerClass, job)    
    
    mappedRDD.groupByKey(wrappedPartitioner)
             .mapPartitions(wrappedReducer.run)
  }
  
  /**
   *  This function saves the reduced RDD to external storage as specified by the job conf.
   */
  protected def saveRDD(reducedRDD: RDD[(ReduceOutputKey, ReduceOutputValue)]) {
    val outputPath: String = FileOutputFormat.getOutputPath(job.context).toString()
        
    val reduceOutputKeyClass: Class[ReduceOutputKey] = 
      implicitly[ClassManifest[ReduceOutputKey]].erasure.asInstanceOf[Class[ReduceOutputKey]]   
    val reduceOutputValueClass: Class[ReduceOutputValue] = 
      implicitly[ClassManifest[ReduceOutputValue]].erasure.asInstanceOf[Class[ReduceOutputValue]]
    val outputFormatClass: Class[OutputFormat[_, _]] =
      job.context.getOutputFormatClass().asInstanceOf[Class[OutputFormat[_, _]]]    
    
    reducedRDD.saveAsNewAPIHadoopFile(outputPath, reduceOutputKeyClass, reduceOutputValueClass, outputFormatClass)
  }
  
  /**
   * This function runs the Hadoop job as a Spark job using the given SparkContext
   */
  def run(sc: SparkContext) {   
    mappedRDD = createMappedRDD(sc)     
    if (mappedRDDStorageLevel != null) {
      mappedRDD.persist(mappedRDDStorageLevel)
    }
    reducedRDD = createReducedRDD(mappedRDD)
    if (reducedRDDStorageLevel != null) {
      reducedRDD.persist(reducedRDDStorageLevel)
    }
    if (saveReducedRDD) {
      saveRDD(reducedRDD)
    }
  }
  
  def setInputRDD(rdd: RDD[(MapInputKey, MapInputValue)]) {
    inputRDD = rdd
  }
  
  def getMappedRDD() = mappedRDD
  
  def getReducedRDD() = reducedRDD
  
  def persistMappedRDD(level: StorageLevel) {
    mappedRDDStorageLevel = level
  }
  
  def persistReducedRDD(level: StorageLevel) {
    reducedRDDStorageLevel = level
  }
  
  def disableSort() {
    job.disableSort()
  }
  
  def disableMapperKeyValueClone() {
    job.disableMapperKeyValueClone()
  }
  
  def disableReducerKeyValueClone() {
    job.disableReducerKeyValueClone()
  }
  
  def disableSave() {
    saveReducedRDD = false 
  }
}


