// Greg Misicko
// Feb 19 2016
// e63 Big Data Analytics HW03 Problem 4
// This will combine the operations of problem 1 and
// problem 3 by chaining two jobs. 


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Combine {
	
	//private static final String INTERMEDIATE_OUTPUT_PATH = "/home/greg/vmware/vmShare/intermediate_output";
	private static final String INTERMEDIATE_OUTPUT_PATH = "intermediate_output";
	
	// set a global value that will contain my list of stop words.
	// It cannot be changed. 
	public static List<String> stopWordList = getStopWords("");

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        
        // I'll convert the word to a String in order to run a few operations on it
        String keyString = word.toString();
        // scrub out any non-alpha characters
  	  	Text cleanKey = new Text(keyString.replaceAll("[^a-zA-Z]", ""));
        
        // Only track this word if its not a stop word
        if(!stopWordList.contains(cleanKey.toString().toLowerCase())) {
      	  	context.write(cleanKey, one); 
      	  }          
      }
           
    }
  }
  
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
  	    
//     StringTokenizer itr = new StringTokenizer(value.toString());
    String[] line = value.toString().split("\t");
    
    //word = new Text(line[0]);
    count = new IntWritable(Integer.parseInt(line[1]));
//     System.out.println(word+" "+count);
    
    context.write(count, one);
    
    

//     System.out.println("key: "+key+" count: "+value);
    
//     while (itr.hasMoreTokens()) {
//       word.set(itr.nextToken());
//       count.set(Integer.parseInt(itr.nextToken()));
      
      
//    }
  }
}
  
  public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
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
  
  // In this problem the reducer is essentially a pass-through since it doesn't
  // do any work. However it is still necessary because the comparator will be
  // called from it.
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
  
  
  	// Reads in the stop words from the identified stop words file. These are words that
  	// are too common and should be ignored in the final list. 
  	// If you are too lazy to create an external stopWordsFile or if you give an incorrect
  	// path, a default list will be used. 
	private static List<String> getStopWords(String stopWordsFile) {
  	BufferedReader in;
  	List<String> stopWordList = new ArrayList<String>();
  	
		try {
			in = new BufferedReader(new FileReader(stopWordsFile));
			// Initialize the input line
			String line = "";
			
			// While there are new lines remaining..
			while ((line = in.readLine()) != null) {    	
				stopWordList.add(line);
			}
			in.close();
		}
		catch (IOException ioe) {
			stopWordList = Arrays.asList("i", "a", "about", "an", "are", "as", 
					"at", "be", "by", "com", "for", "from", "how", "in", "is", 
					"it", "of", "on", "or", "that", "the", "this", "to", "was", 
					"what", "when", "where", "who", "will", "with", "the", 
					"www");
		}
		
		return stopWordList;
  }

  public static void main(String[] args) throws Exception {
	  
	  Configuration conf = new Configuration();
	  
	  String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	  if (otherArgs.length < 2) {
		  System.err.println("HW03 WordCount Usage: wordcount <in> [<in>...] <out>");
		  System.exit(2);
	  }
	  
	  Path temp = new Path(INTERMEDIATE_OUTPUT_PATH);
	  
	  Job job1 = Job.getInstance(conf, "wordcount");
	  job1.setJarByClass(Combine.class);
	  job1.setMapperClass(TokenizerMapper.class);
	  job1.setCombinerClass(IntSumReducer.class);
	  job1.setReducerClass(IntSumReducer.class);
	  job1.setOutputKeyClass(Text.class);
	  job1.setOutputValueClass(IntWritable.class);
	  FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
	  FileOutputFormat.setOutputPath(job1, temp);
	  job1.waitForCompletion(true);
	  
	  Job job2 = Job.getInstance(conf, "wordsorter");
	  job2.setJarByClass(Combine.class);
	  job2.setMapperClass(TokenizerMapper3.class);
	  job2.setCombinerClass(IntSumReducer3.class);
	  job2.setMapOutputKeyClass(IntWritable.class); 
	  job2.setMapOutputValueClass(IntWritable.class); 
	  job2.setReducerClass(IntSumReducer3.class);
	  job2.setOutputKeyClass(IntWritable.class);
	  job2.setOutputValueClass(Text.class);
	  FileInputFormat.addInputPath(job2, temp);
	  FileOutputFormat.setOutputPath(job2, new Path(args[1]));
	  job2.waitForCompletion(true);
	  
	  try {
		  FileSystem fs = temp.getFileSystem(conf);
		  fs.delete(temp, true);
	  } catch (IOException ioe){}
	  
	  //System.exit(true);

  	}
}
