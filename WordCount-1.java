// Greg Misicko
// Feb 19 2016
// e63 Big Data Analytics HW03 Problem 1
// This class will read an input file and perform a count of every unique
// word. Non-alpha characters are removed, and some stop words are ignored. 


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {
	
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
	  
//	  List<String> stopWordList = new ArrayList<String>();
//	  stopWordList = getStopWords("");
	  
	  Job job = Job.getInstance(conf, "wordcount");
	  
	  job.setJarByClass(WordCount.class);
	  
	  job.setMapperClass(TokenizerMapper.class);
	  
	  job.setCombinerClass(IntSumReducer.class);
	  
	  job.setReducerClass(IntSumReducer.class);
	  
	  job.setOutputKeyClass(Text.class);
	  
	  job.setOutputValueClass(IntWritable.class);
	  
	  for (int i = 0; i < otherArgs.length - 1; ++i) {
		  FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
	  }
	  
	  FileOutputFormat.setOutputPath(job,
			  new Path(otherArgs[otherArgs.length - 1]));
	  
	  System.exit(job.waitForCompletion(true) ? 0 : 1);

  	}
}
