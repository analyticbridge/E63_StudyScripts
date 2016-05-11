package cscie63.assignment3;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
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

public class Problem1 {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
	// stop words currently we will filter out.
	private final String[] stopWords = {"i", "a", "an", "are", "as", "at", "be", "by",
                "com", "for", "from", "how", "in", "is", "it",
                "of", "on", "or", "that", "the", "this", "to",
                "was", "what", "when", "where", "who", "will",
                "with", "www"};
	// use HashSet to improve performance of the application.
	private HashSet<String> hashStopWords = new HashSet<String>(Arrays.asList(stopWords));
    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
    	  // We need to remove unwanted characters here, and skip
    	  // all stop words. therefore we save it to a temporary variable.
    	  String temp = itr.nextToken();
    	  // To remove unwanted characters.
    	  temp = procssWord(temp);
    	  
    	  if (temp.length() > 0 && !isStopWords(temp)) {
    		  // Only store the result if it is not a stop word.
	        word.set(temp);
	        context.write(word, one);
    	  }
      }
    }
    /**
     * To determine if the word is a stop word or not.
     * @param temp input string, to be determined if it is a stop word
     * @return true if it is a stop word; false otherwise.
     */
	private boolean isStopWords(String temp) {
		// TODO Auto-generated method stub
		if (hashStopWords.contains(temp.toLowerCase())) {
			// if lower case of input is contained in HashSet, it is a stop word.
			return true;
		} else {
			return false;
		}
	}
	/**
	 * To remove unwanted characters at the begin or at the end of the given word.
	 * @param temp the given word, unwanted character at the beginning and end
	 *             of it will be removed.
	 * @return string without unwanted character at the beginning and end of the input.
	 */
	private String procssWord(String temp) {
		// TODO Auto-generated method stub
		// This is to remove any unwanted character at the start/end
		// of the string. ^[^a-zA-Z0-9]+ is to remove unwanted characters
		// at the start of the string; and [^a-zA-Z0-9]+$ is to remove
		// unwanted characters at the end of the string.
		temp = temp.replaceAll("^[^a-zA-Z0-9]+|[^a-zA-Z0-9]+$", "");
		return temp;
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

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: Problem1 <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "wordcount");
    job.setJarByClass(Problem1.class);
    job.setMapperClass(TokenizerMapper.class);
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

