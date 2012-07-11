package shadoop

import spark.RDD
import spark.UnionRDD
import spark.SparkContext
import spark.SparkContext._

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.lib.MultipleInputs
import org.apache.hadoop.mapreduce.{Job => HadoopJob}
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.hadoop.mapreduce.lib.input.DelegatingMapper
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.util.ReflectionUtils

class SparkMultiInputHadoopJob[
     MapOutputKey : ClassManifest, MapOutputValue : ClassManifest, 
     ReduceOutputKey : ClassManifest, ReduceOutputValue : ClassManifest
   ] (@transient hadoopJob: HadoopJob) extends SparkHadoopJob[
     Any, Any, MapOutputKey, MapOutputValue, ReduceOutputKey, ReduceOutputValue
   ] (hadoopJob) {
  
  @transient val inputData = new HashMap[RDD[(Any, Any)], Class[Mapper[Any, Any, MapOutputKey, MapOutputValue]]]
  
  
  override def setInputRDD(rdd: RDD[(Any, Any)]) {
    throw new Exception("Use addInputData instead of setInputRDD for SparkMultiInputHadoopJob")
  }
  
  
  override protected def createMappedRDD(sc: SparkContext): RDD[(MapOutputKey, MapOutputValue)] = {
    val isMultiInput = inputData.size > 0 || job.context.getMapperClass().getSimpleName().startsWith("DelegatingMapper")
    if (isMultiInput) {
      createMultiInputMappedRDD(sc)
    } else {
      super.createMappedRDD(sc)
    }
  }

  
  def addInputData[InputKey, InputValue] (
      rdd: RDD[(InputKey, InputValue)], 
      mapperClass: Class[_ <: Mapper[InputKey, InputValue, MapOutputKey, MapOutputValue]]) {
    inputData += ((
        rdd.asInstanceOf[RDD[(Any, Any)]], 
        mapperClass.asInstanceOf[Class[Mapper[Any, Any, MapOutputKey, MapOutputValue]]]))
  }  
  
  
  def createMultiInputMappedRDD(sc: SparkContext): RDD[(MapOutputKey, MapOutputValue)] = {
    val multipleInputPathsAndFormats = job.conf.get("mapred.input.dir.formats")
    val multipleInputPathsAndMappers = job.conf.get("mapred.input.dir.mappers")
    
    if (multipleInputPathsAndFormats != null) {
      val pathToInputFormatClass = new HashMap[String, Class[InputFormat[_, _]]] 
      pathToInputFormatClass ++= multipleInputPathsAndFormats.split(",").map(x => {
        val y = x.split(";")
        val path = y(0)
        val inputFormatClass = job.conf.getClassByName(y(1))
        (path, inputFormatClass.asInstanceOf[Class[InputFormat[_, _]]])      
      })
      
      val pathToMapper = new HashMap[String, Class[Mapper[Any, Any, MapOutputKey, MapOutputValue]]] 
      pathToMapper ++= multipleInputPathsAndMappers.split(",").map(x => {
        val y = x.split(";")
        val path = y(0)
        val mapperClass = job.conf.getClassByName(y(1))
        (path, mapperClass.asInstanceOf[Class[Mapper[Any, Any, MapOutputKey, MapOutputValue]]])      
      })
      
      if (pathToInputFormatClass.size != pathToMapper.size) {
        throw new Exception("Job with multiple input has different numbers of input formats and mappers")
      }
      
      pathToInputFormatClass.keySet.foreach(path => {
        println("Processing " + path)
        val inputFormatClass = pathToInputFormatClass(path).asInstanceOf[Class[InputFormat[Any, Any]]]
        val mapperClass = pathToMapper(path)
        val newJob = new HadoopJob(job.conf)
        FileInputFormat.addInputPath(newJob, new Path(path))
        val inputRDD = sc.newAPIHadoopFile[Any, Any, InputFormat[Any, Any]](
            path, 
            inputFormatClass, 
            null.asInstanceOf[Class[Any]], 
            null.asInstanceOf[Class[Any]], 
            newJob.getConfiguration())
        inputData += ((inputRDD, mapperClass))
      })
    }
        
    val mappedRDDs = inputData.map(data => {
      val inputRDD = data._1
      val mapperClass = data._2
      val wrappedMapper = new WrappedMapper(mapperClass, job)
      inputRDD.mapPartitions(wrappedMapper.run)         
    })
    new UnionRDD(sc, mappedRDDs.toSeq)
  }

  
  /**
   *  This function handles the MultipleInput case. It reads multiple input sources (each 
   * with different input formats and mappers as specified by the job conf) and 
   * applies their corresponding Hadoop mapper to create a unified mapped RDD.
   */
  
  /*
   def createMultiInputMappedRDD(sc: SparkContext): RDD[(MapOutputKey, MapOutputValue)] = {
    val pathToInputFormatClass = new HashMap[String, Class[InputFormat[_, _]]] 
    pathToInputFormatClass ++= job.conf.get("mapred.input.dir.formats").split(",").map(x => {
      val y = x.split(";")
      val path = y(0)
      val inputFormatClass = job.conf.getClassByName(y(1))
      (path, inputFormatClass.asInstanceOf[Class[InputFormat[_, _]]])      
    })
    
    val pathToMapper = new HashMap[String, Class[Mapper[Any, Any, MapOutputKey, MapOutputValue]]] 
    pathToMapper ++= job.conf.get("mapred.input.dir.mappers").split(",").map(x => {
      val y = x.split(";")
      val path = y(0)
      val mapperClass = job.conf.getClassByName(y(1))
      (path, mapperClass.asInstanceOf[Class[Mapper[Any, Any, MapOutputKey, MapOutputValue]]])      
    })
    
    if (pathToInputFormatClass.size != pathToMapper.size) {
      throw new Exception("Job with multiple input has different numbers of input formats and mappers")
    }
    
    val mappedRDDs = pathToInputFormatClass.keySet.map(path => {
      println("Processing " + path)
      val inputFormatClass = pathToInputFormatClass(path).asInstanceOf[Class[InputFormat[Any, Any]]]
      val mapperClass = pathToMapper(path)
      val newJob = new HadoopJob(job.conf)
      FileInputFormat.addInputPath(newJob, new Path(path))
      val inputRDD = sc.newAPIHadoopFile[Any, Any, InputFormat[Any, Any]](
          path, 
          inputFormatClass, 
          null.asInstanceOf[Class[Any]], 
          null.asInstanceOf[Class[Any]], 
          newJob.getConfiguration())
      val wrappedMapper = new WrappedMapper(hadoopJob, mapperClass)
      val mappedRDD = inputRDD.mapPartitions(wrappedMapper.run) 
      mappedRDD
    })
    val unifiedMappedRDDs = new UnionRDD(sc, mappedRDDs.toSeq)
    unifiedMappedRDDs
  } 
  */  
} 





