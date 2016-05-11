// Greg Misicko
// Feb 19 2016
// e63 Big Data Analytics HW03 Problem 3
// This class will read the output of the previous problem, which listed
// counts of words in descending order, and then build a list of how
// many times each count value occurred. ie - how many times did a 
// word appear once, how many times did a word appear twice, etc

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount2 {

	// For this problem we are reading in the count + word from the 
	// previous problem. We want to output the count + <number of 
	// times that count appeared>
	// Output format needs to change to int int
  public static class TokenizerMapper3 
       extends Mapper<Object, Text, IntWritable, IntWritable>{
    
	  // just like in the orginal word count, we want to link a 
	  // single count to every instance of the number count. Reduce
	  // will do the summation. 
      private final static IntWritable one = new IntWritable(1);
	  
	  // we no longer care about the word, only the count
      // private Text word = new Text();
      private IntWritable count = new IntWritable();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	    
      String[] line = value.toString().split("\t");
      
      // take the count and ignore the word
      count = new IntWritable(Integer.parseInt(line[0]));
      
      context.write(count, one);
      
    }
  }
  
  // The reducer counts the occurrences of counts, identical to what 
  // WordCount.java did for words
  public static class IntSumReducer3 
       extends Reducer<IntWritable, IntWritable, IntWritable , IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(IntWritable key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
    	
      for (IntWritable val : values) {
          sum += val.get();
        }
        result.set(sum);
        
        context.write(key, result); 
      
    }
  }
  
  // comparator does comparisons for the hadoop system and determines the order of output
  public static class IntComparator extends WritableComparator {

	    public IntComparator() {
	        super(IntWritable.class, true);
	    }
	    
	    // compare the two keys and return for ascending order
	    @Override
	    public int compare(WritableComparable w1, WritableComparable w2) {
	    	// just going to switch the -1 to +1 to switch from desc order to asc
	        return w1.compareTo(w2) * (1);
	    	
	    }
	}
  

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: wordsorter <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "wordsorter");
    job.setJarByClass(WordCount2.class);
    job.setMapperClass(TokenizerMapper3.class);
    job.setCombinerClass(IntSumReducer3.class);
    
 //   job.setInputFormatClass(KeyValueTextInputFormat.class);
    job.setMapOutputKeyClass(IntWritable.class); 
    job.setMapOutputValueClass(IntWritable.class); 

    
    // add the comparator, which will perform the sort for the reducer
//    job.setSortComparatorClass(IntComparator.class);
    
    job.setReducerClass(IntSumReducer3.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}