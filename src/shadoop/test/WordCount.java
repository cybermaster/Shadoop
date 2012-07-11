package shadoop.test;


import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

public class WordCount {
  private final static LongWritable one = new LongWritable(1); 
  private Path outputFile = null;
  private Job job = null;
         
  public static class WordCountTextMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private Text word = new Text();
    @Override
    protected void setup(Context context) {
      System.out.println("Mapper.setup called");
    }
   
    @Override
    protected void map(LongWritable key, Text value, Context context) {
      try {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
          word.set(itr.nextToken());
          context.write(word, one);
        }
      } catch (IOException e) {
        e.printStackTrace();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    
    @Override 
    protected void cleanup(Context context) {
      System.out.println("Mapper.cleanup called");
    }
  }
  
  public static class WordCountSeqMapper extends Mapper<NullWritable, Text, Text, LongWritable> {
    private Text word = new Text();
    @Override
    protected void setup(Context context) {
      System.out.println("Mapper.setup called");
    }
   
    @Override
    protected void map(NullWritable key, Text value, Context context) {
      try {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
          word.set(itr.nextToken());
          context.write(word, one);
        }
      } catch (IOException e) {
        e.printStackTrace();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    
    @Override 
    protected void cleanup(Context context) {
      System.out.println("Mapper.cleanup called");
    }
  }
  
  public static class WordCountReducer extends Reducer<Text, LongWritable, Text, NullWritable> {
    private Text count = new Text();
    
    @Override
    protected void setup(Context context) {
      System.out.println("Reducer.setup called");
    }
    
    
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) {
      try {
        long sum = 0l;
        for (LongWritable val : values) {
          sum += val.get();
        }
        count.set(key + "," + sum);
        context.write(count, null);
        
        //System.out.println(count);
      } catch (IOException e) {
        e.printStackTrace();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    
    @Override 
    protected void cleanup(Context context) {
      System.out.println("Reducer.cleanup called");
    }
  }
  
  public void generateJob(String inFile, String outFile) {
    try {
      Path inputFile = new Path(inFile);
      outputFile = new Path(outFile);
      job = new Job();
      
      job.setMapperClass(WordCountTextMapper.class);
      job.setReducerClass(WordCountReducer.class);

      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(LongWritable.class);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(LongWritable.class);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.setInputPaths(job, inputFile);
      
      FileOutputFormat.setOutputPath(job, outputFile);
      
      job.setNumReduceTasks(1);
      job.setJarByClass(WordCount.class);
      System.out.println(job.getPartitionerClass());
      
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }
  
  
  public void generateMultiInputJob(String inFile1, String inFile2, String outFile) {
    try {
      Path inputFile1 = new Path(inFile1);
      Path inputFile2 = new Path(inFile2);
      outputFile = new Path(outFile);
      job = new Job();
      
      //job.setMapperClass(WordCountMapper.class);
      job.setReducerClass(WordCountReducer.class);

      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(LongWritable.class);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(LongWritable.class);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      //FileInputFormat.setInputPaths(job, inputFile);
      MultipleInputs.addInputPath(job, inputFile1, TextInputFormat.class, WordCountTextMapper.class);
      if (inputFile2 != null) {
        MultipleInputs.addInputPath(job, inputFile2, SequenceFileInputFormat.class, WordCountSeqMapper.class);
      }
      
      FileOutputFormat.setOutputPath(job, outputFile);
      
      job.setNumReduceTasks(1);
      job.setJarByClass(WordCount.class);
      System.out.println(job.getPartitionerClass());
      
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }
  
  public Job getJob() { return job; } 
  
  public void prepareJob() {
    try {
      FileSystem fs = outputFile.getFileSystem(job.getConfiguration());
      fs.delete(outputFile, true);
    } catch (IOException e) {
      e.printStackTrace();
    }
  
  }
  
  public static void runWordCount(String[] args) {
    if (args.length < 1) {
      System.out.println("WordCountJob <plain text file>");
      System.exit(1);
    }
    String inputFile = args[0];
    String outputFile = inputFile + "_counts_hadoop";
    try {
      WordCount wcJob = new WordCount();
      wcJob.generateJob(inputFile, outputFile);
      wcJob.prepareJob();
      Job job = wcJob.getJob();
      job.submit();
      job.waitForCompletion(true);
      System.out.println("Output generated in " + outputFile);      
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }
  
  public static void runMultiInputWordCount(String[] args) {
    if (args.length < 2) {
      System.out.println("WordCountJob <plain text file>  <sequence file of type <NullWritable, Text> >");
      System.exit(1);
    }
    String inputFile1 = args[0];
    String inputFile2 = args[1];
    String outputFile = inputFile1 + "_counts_hadoop";
    try {
      WordCount wcJob = new WordCount();
      wcJob.generateMultiInputJob(inputFile1, inputFile2, outputFile);
      wcJob.prepareJob();
      Job job = wcJob.getJob();
      job.submit();
      job.waitForCompletion(true);
      System.out.println("Output generated in " + outputFile);      
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }
  
  
  public static void main(String[] args) {
    //runWordCount(args);
    runMultiInputWordCount(args);
  }
}
