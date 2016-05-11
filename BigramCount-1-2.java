/**
 * CSCI E-63 Assignment 4 - Problem 4
 * @author nobuhiko_tamagawa
 */

package edu.hu.ntamagawa;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.lang.Iterable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class BigramCount {

    @SuppressWarnings("serial")
    public static void main(String[] args) throws Exception {

        // As specified in the problem, force to use local files by
        // appending the URI scheme of file://. Please note that the argument
        // to the program needs a full path, starting with /. That way, the
        // path string will be "file:///path/to/file", starting with file:///
        // (three slashes are used).
        String inputFile = "file://" + args[0];
        String outputFile = "file://" + args[1];

        // Create a Java Spark Context. AppName is now 'BigramCount'
        SparkConf conf = new SparkConf().setAppName("BigramCount");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load our input data, separated by 2 new line characters. This will read the file
        // one *paragraph* at a time.
        Configuration hadoopConf = new Configuration();

        // Set the delimiter as 2 new lines
        hadoopConf.set("textinputformat.record.delimiter", "\r\n\r\n");

        // Read the file using the TextInputFormat class with the delimiter specified above.
        // Each Text contains a paragraph.
        JavaPairRDD<LongWritable, Text> input =
                sc.newAPIHadoopFile(inputFile, TextInputFormat.class,
                                    LongWritable.class, Text.class, hadoopConf);

        // The input is now key/value pair of a number and a paragraph.
        // We only need paragraphs, so get the values from input.
        JavaRDD<Text> paragraphs = input.values();

        // For each paragraph, flat Map using the custom FlatMapFunction.
        // This will give us all the bigrams (1 string contains a bigram).
        JavaRDD<String> bigrams = paragraphs
                .flatMap(new ParagraphToBigramFlatMapFunction());

        // Count the occurrence of the bigrams by map to a pair of
        // bigram --> occurrence key/value pair.
        // Then reduce by adding all counts per key.
        JavaPairRDD<String, Integer> counts = bigrams.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String x) {
                        return new Tuple2<>(x, 1);
                    }
                }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer x, Integer y) {
                return x + y;
            }
        });

        // Save all the bigram to a text file.
        counts.saveAsTextFile(outputFile);

        // Get the count of all bigram
        long bigramCount = counts.count();

        // Save a message telling total bigram count into a new file.
        List<String> msgs = Arrays.asList("Total bigram count = " + bigramCount);
        JavaRDD<String> allCount = sc.parallelize(msgs);
        allCount.saveAsTextFile(outputFile + "_count");

        // Save the first 20 bigram
        List<Tuple2<String, Integer>> list20 = counts.take(20);
        JavaPairRDD<String, Integer>  bigram20 = sc.parallelizePairs(list20);
        bigram20.saveAsTextFile(outputFile + "20");

        // Filter bigrams that contain word "heaven"
        JavaPairRDD<String, Integer> heavens = counts.filter(
                new Function<Tuple2<String, Integer>, Boolean> () {
            @Override
            public Boolean call(Tuple2<String, Integer> t) throws Exception {

                // Split the bigram into 2 words
                String bigram = t._1();
                String[] tokens = bigram.split(" ");
                if (tokens.length != 2) {
                    throw new Exception("Not bigram");
                }

                // If either of the word in the bigram contains heaven, return true
                return tokens[0].compareTo("heaven") == 0 || tokens[1].compareTo("heaven") == 0;

            }
        });

        // Save the text file that contains bigram with heaven.
        heavens.saveAsTextFile(outputFile + "_heaven");
    }

    /***
     * Takes a Text as input which holds a paragraph, and returns all bigrams in the paragraph
     * as Iterable strings (List).
     *
     */
    @SuppressWarnings("serial")
    public static class ParagraphToBigramFlatMapFunction implements FlatMapFunction<Text, String> {

        @Override
        public Iterable<String> call(Text txt) throws Exception {

            // Convert Text to Java String
            String line = txt.toString();

            // Strip new lines
            line = line.replaceAll("\\r\\n", " ");

            // Split a paragraph into sentences, which is separated by
            // a period, question mark, or exclamation mark
            List<String> sentences =  Arrays.asList(line.split("[.!?]"));

            // For each sentence, get bigrams. By processing each sentence
            // (not line separated by a new line), this meets the requirement
            // of adding the bigram in which the first words is the last word
            // on the line and the second word is the first word on the
            // subsequent line.
            List<String> bigrams = new ArrayList<>();
            for(String sentence : sentences) {
                // Add all bigrams from all the sentences.
                bigrams.addAll(getBigram(sentence));
            }

            // Return the bigrams from the paragraph.
            return bigrams;
        }

        /***
         * This method returns a list of bigrams in the given sentence.
         * @param sentence
         * @return
         */
        private List<String> getBigram(String sentence) {
            // Before tokenizing, replace all punctuation to a space.
            sentence = sentence.replaceAll("\\p{Punct}", " ");

            // Split a sentence into words
            String[] words = sentence.split(" ");

            // Prepare clean words after removing punctuation
            // and made lower case.
            List<String> cleanWords = new ArrayList<>();
            for(String word : words) {
                // If the word is empty, that must be only punctuation(s) that are removed above.
                // Skip it.
                if(word.isEmpty()) {
                    continue;
                }

                // Store the lower-cased word
                cleanWords.add(word.toLowerCase());
            }

            // From the list of clean words, create bigrams
            List<String> bigrams = new ArrayList<>();

            // From all words, create bigrams by concatenating the word in the previous
            // position, a space character, and the word in the current position.
            // Skip the first word (i=1), as it can't be a bigram because there's
            // no previous word.
            for(int i = 1; i < cleanWords.size(); i++) {
                String word1 = cleanWords.get(i - 1);   // previous word
                String word2 = cleanWords.get(i);       // current word

                // Make bigram and store it
                bigrams.add(word1 + " " + word2);
            }

            // Return the list of bigrams in the sentence.
            return bigrams;
        }
    }
}
