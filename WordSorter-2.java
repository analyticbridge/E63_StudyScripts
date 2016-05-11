// Greg Misicko
// Feb 19 2016
// e63 Big Data Analytics HW03 Problem 2
// This class will read the output of the previous problem and sort
// the word counts in descending order. 

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

public class WordSorter {

  public static class TokenizerMapper2 
       extends Mapper<Object, Text, IntWritable, Text>{
    
      //private final static IntWritable one = new IntWritable(1);
      private Text word = new Text();
      private IntWritable count = new IntWritable();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	    
 //     StringTokenizer itr = new StringTokenizer(value.toString());
      String[] line = value.toString().split("\t");
      
      word = new Text(line[0]);
      count = new IntWritable(Integer.parseInt(line[1]));
 //     System.out.println(word+" "+count);
      
      context.write(count, word);
 
    }
  }
  
  // In this problem the reducer is essentially a pass-through since it doesn't
  // do any work. However it is still necessary because the comparator will be
  // called from it.
  public static class IntSumReducer2 
       extends Reducer<IntWritable, Text, IntWritable , Text> {
//    private IntWritable result = new IntWritable();

    public void reduce(IntWritable key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
 //     int sum = 0;
    	
      for (Text val : values) {
 //       sum += val.get();
    	  context.write(key, val);
      }
 //     result.set(sum);
      
    }
  }
  
  // comparator tells hadoop how to order keys in output
  public static class IntComparator extends WritableComparator {

	    public IntComparator() {
	        super(IntWritable.class, true);
	    }
	    
	    // compare the keys and sort them in descending order
	    @Override
	    public int compare(WritableComparable w1, WritableComparable w2) {
	    	// The compareTo method returns an integer result of the comparison.
	    	// Multiplying it by -1 gives the reverse truth, or in other words
	    	// forces the sort to be descending
	        return w1.compareTo(w2) * (-1);   	
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
    job.setJarByClass(WordSorter.class);
    job.setMapperClass(TokenizerMapper2.class);
    job.setCombinerClass(IntSumReducer2.class);
    
 //   job.setInputFormatClass(KeyValueTextInputFormat.class);
    job.setMapOutputKeyClass(IntWritable.class); 
    job.setMapOutputValueClass(Text.class); 

    
    // add the comparator, which will perform the sort for the reducer
    job.setSortComparatorClass(IntComparator.class);
    
    job.setReducerClass(IntSumReducer2.class);
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