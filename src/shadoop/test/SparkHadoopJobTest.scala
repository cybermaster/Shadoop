package shadoop.test

import shadoop._

import spark.SparkContext
import spark.SparkContext._

import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text


object SparkHadoopJobTest {
  
  def runTestJob() {
    val sc = new SparkContext("local[2]", "Test")
    sc.parallelize(1 to 100, 10).map(x => (x % 10, 1)).reduceByKey(_ + _).collect().foreach(println)
    sc.stop()
  }
  
  def runWordCount(args: Array[String]) {
    if (args.length < 1) {
      println("WordCountJob <plain text file> ")
      System.exit(1)
    }
    val inputFile = args(0)
    val outputFile = inputFile + "_counts_spark"
    System.setProperty("spark.kryo.registrator", "shadoop.WritableRegistrator")
      
    try {
      val wcJob = new WordCount()
      wcJob.generateJob(inputFile, outputFile)  
      wcJob.prepareJob()
      
      val sc = new SparkContext("local[2]", "WordCount")
      val hadoopJob = wcJob.getJob()
      //hadoopJob.getConfiguration().set("mapred.sort.disable", "true")
      val sparkHadoopJob = new SparkHadoopJob[LongWritable, Text, Text, LongWritable, Text, NullWritable](hadoopJob)
      sparkHadoopJob.run(sc)
      sc.stop()
    } catch {
      case e: java.io.IOException => e.printStackTrace()
      case ie: InterruptedException => ie.printStackTrace();
      case ce: ClassNotFoundException => ce.printStackTrace();
    }
  }
  
  def runMultiInputWordCount(args: Array[String]) {
    if (args.length < 2) {
      println("WordCountJob <plain text file>  <sequence file of type <NullWritable, Text> >")
      System.exit(1)
    }
    val inputFile1 = args(0)
    val inputFile2 = args(1)
    val outputFile = inputFile1 + "_counts_spark"
    System.setProperty("spark.kryo.registrator", "shadoop.WritableRegistrator")
      
    try {
      val wcJob = new WordCount()
      wcJob.generateMultiInputJob(inputFile1, inputFile2, outputFile)      
      wcJob.prepareJob()
      
      val sc = new SparkContext("local[2]", "WordCount")
      val hadoopJob = wcJob.getJob()
      //hadoopJob.getConfiguration().set("mapred.sort.disable", "true")
      val sparkHadoopJob = new SparkMultiInputHadoopJob[Text, LongWritable, Text, NullWritable](hadoopJob)
      sparkHadoopJob.run(sc)
      sc.stop()
    } catch {
      case e: java.io.IOException => e.printStackTrace()
      case ie: InterruptedException => ie.printStackTrace();
      case ce: ClassNotFoundException => ce.printStackTrace();
    }
  }
  
  
  def main(args: Array[String]) {
    //runTestJob()
    
    /**
     * This tests the SparkHadoopJob class. Execute the runWordCount in WordCount.main() 
     * and runWordCount here, both on the same input text file (as command line argument),
     * and compare the results (generated in same directory as input file). They should 
     * be exactly same.
     */
    //runWordCount(args)
    
    /**
     * This tests the extended version of SparkHadoopJob, that is, SparkMultipleInputHadoopJob, 
     * which has support for MultipleInputs in Hadoop jobs (ref: 
     * http://hadoop.apache.org/common/docs/r0.20.2/api/org/apache/hadoop/mapred/lib/MultipleInputs.html). 
     * Same as before, execute runMultiInputWordCount in WordCount class and here, giving 
     * it two files (first argument is a text file, second one is a sequence file of type 
     * <Text, NullWritable>) and the compare the results. They should again be exactly same.
     */
    runMultiInputWordCount(args)
    
    System.exit(0)
  }
}
