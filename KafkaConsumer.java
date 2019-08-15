package com.upgrad.creditcardfrauddetection;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

public class KafkaConsumer {

	/*
	 * Setup GROUP_ID in such a way that unique every time
	 */
	public static String GROUP_ID = "amitgoelkafkaspark"
			+ new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());

	public static void main(String[] args) throws Exception {

		/*
		 * Check if 1 argument is passed to the program
		 */
		if (args.length != 1) {
			System.out.println("Please enter 1st argument as host server IP");
			return;
		}
		/*
		 * Print GROUP_ID for current Kafka stream
		 */
		System.out.println("Using group ID : " + GROUP_ID + "  for current Kafka stream.");

		/*
		 * Set Logger to OFF
		 */
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		/*
		 * Set Spark Configuration object
		 */
		SparkConf sparkConf = new SparkConf().setAppName("CreditCardFraudDetection").setMaster("local[*]");

		/*
		 * Set Streaming Context
		 */
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

		/*
		 * Initialize static variable hostServerIP in CreditCardFraudDetection class.
		 * The HBase master and ZooKeeper are dependent on this IP, which keeps on
		 * changing with restart of EC2 instance every time.
		 */
		CreditCardFraudDetection.hostServerIP = args[0];

		/*
		 * Set parameters for Kafka stream
		 */
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "100.24.223.181:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", GROUP_ID);
		kafkaParams.put("auto.offset.reset", "earliest");
		kafkaParams.put("enable.auto.commit", true);

		/*
		 * Subscribe to Kafka topic
		 */
		Collection<String> topics = Arrays.asList("transactions-topic-verified");

		/*
		 * Consume input Kafka Stream and convert into Spark DStreams
		 */
		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

		/*
		 * Use map transformation on input stream and get the value part
		 */
		JavaDStream<String> jds = stream.map(x -> x.value());

		/*
		 * Print count of records present
		 */
		jds.foreachRDD(x -> System.out.println("Total Number of Transactions to be Processed : " + x.count() + "\n"));

		/*
		 * Print all the data so can be used for verification later if needed
		 */
		jds.foreachRDD(new VoidFunction<JavaRDD<String>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaRDD<String> rdd) {

				rdd.foreach(a -> System.out.println(a));
			}
		});

		/*
		 * Map data from input stream to CreditCardFraudDetection class constructor
		 */
		JavaDStream<CreditCardFraudDetection> jds_mapped = jds.map(x -> new CreditCardFraudDetection(x));

		/*
		 * Call FraudDetection method in CreditCardFraudDetection class
		 */
		jds_mapped.foreachRDD(new VoidFunction<JavaRDD<CreditCardFraudDetection>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaRDD<CreditCardFraudDetection> rdd) {
				rdd.foreach(x -> x.FraudDetection(x));
			}

		});

		/*
		 * Print current time stamp before starting
		 */
		System.out.println("\nStart Time : " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + "\n");

		/*
		 * Start Spark Streaming
		 */
		jssc.start();

		/*
		 * Await Termination to respond to Ctrl+C and gracefully close Spark Streaming
		 */
		jssc.awaitTermination();

		/*
		 * Print current time stamp before closing
		 */
		System.out.println("\nEnd Time : " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + "\n");

		/*
		 * Close Spark Streaming
		 */
		jssc.close();

	}
}
