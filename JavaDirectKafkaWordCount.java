/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples.streaming;

/**
* Modified bv Dongyow Lin on 3/31/2016
* Course: CSCI E63 - Big Dat Analytics
* For Homework Assignment 08
* Problem 03 - Consumer Client program 
* Student: Dongyow Lin
*/

import java.util.HashMap;
import java.util.HashSet;
import java.util.Arrays;
import java.util.regex.Pattern;

import scala.Tuple2;

import com.google.common.collect.Lists;
import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.Durations;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount. Usage:
 * JavaDirectKafkaWordCount <brokers> <topics> <brokers> is a list of one or
 * more Kafka brokers <topics> is a list of one or more kafka topics to consume
 * from
 *
 * Example: $ bin/run-example streaming.JavaDirectKafkaWordCount
 * broker1-host:port,broker2-host:port topic1,topic2
 */

public final class JavaDirectKafkaWordCount {
	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("Usage: JavaDirectKafkaWordCount <brokers> <topics>\n"
					+ "  <brokers> is a list of one or more Kafka brokers\n"
					+ "  <topics> is a list of one or more kafka topics to consume from\n\n");
			System.exit(1);
		}

		// StreamingExamples.setStreamingLogLevels();

		String brokers = args[0];
		String topics = args[1];
		
		// Create context with a 5 seconds batch interval
		// Change (1) for problem3
		// 1. Replace the following original statement to use JavaSparkContent class
		// 2. Modified the batch internal to 5 seconds as indicated in the problem requirement
		// SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount");
		JavaSparkContext sparkConf = new JavaSparkContext("local[5]", "JavaDirectKafkaWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

		HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
		HashMap<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", brokers);

		// Create direct kafka stream with brokers and topics
		JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jssc, String.class, String.class,
				StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);

		// Get the lines, split them into words, count the words and print
		JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
			@Override
			public String call(Tuple2<String, String> tuple2) {
				return tuple2._2();
			}
		});
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterable<String> call(String x) {
				return Lists.newArrayList(SPACE.split(x));
			}
		});
		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(s, 1);
			}
		});

		// Change (2) for problem3
		// The following original codes were removed for problem3 as reduceByKey() method
		// is no longer needed
		/*
		 * .reduceByKey( new Function2<Integer, Integer, Integer>() {
		 * 
		 * @Override public Integer call(Integer i1, Integer i2) { return i1 +
		 * i2; } });
		 */
		// Instead, create a separate Reduce function for use in
		// .reduceByKeyAndWindows method.
		Function2<Integer, Integer, Integer> reduceFunc = new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		};

		// Change (3) for problem3 
		// Add following reduceByKeyAndWindow() method as the new reducer 
		// Set window duration to 30 seconds, and slide interval to 5 seconds as indicated in the problem requirement 
		JavaPairDStream<String, Integer> windowedWordCounts = wordCounts.reduceByKeyAndWindow(reduceFunc,
				Durations.seconds(30), Durations.seconds(5));

		// Change (4) for problem3
		// Change the final JavaPairDStream to print to the new one after .reduceByKeyAndWindow()
		windowedWordCounts.print();
				
		// Start the computation
		jssc.start();
		jssc.awaitTermination();
	}
}
