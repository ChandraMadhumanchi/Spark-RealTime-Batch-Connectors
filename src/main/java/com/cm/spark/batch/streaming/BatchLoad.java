package com.cm.spark.batch.streaming;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
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

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.servicebus.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.RetryPolicy;

import scala.Tuple2;

public class BatchLoad {

	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		
		// Eventhub
					final String namespaceName = ""; 
					final String eventHubName = ""; 
					final String sasKeyName = "";
					final String sasKey = ""; 
		  
		  SparkConf conf = new SparkConf()
					.setMaster("local[2]")
					.setAppName("BatchLoad")
					.set("spark.driver.allowMultipleContexts", "true")
					// .set("spark.streaming.concurrentJobs", "50")
					// .set("spark.streaming.kafka.maxRatePerPartition","100")
					.set("spark.streaming.unpersist", "true").set("spark.streaming.backpressure.enabled", "false")
					.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
					.set("spark.executor.extraJavaOptions", "-XX:+UseG1GC");
		  
	      JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
	    
	      
	      JavaDStream<String> lines = jssc.textFileStream(args[0]);
	     
	      //final String output = args[1];  
	      lines.print();
	     
	      lines.foreachRDD(rdd -> {

				rdd.foreachPartition(consumerRecords -> {
					ConnectionStringBuilder connStr = new ConnectionStringBuilder(namespaceName, eventHubName,
							sasKeyName, sasKey);

					EventHubClient ehClient;
					ehClient = EventHubClient.createFromConnectionStringSync(connStr.toString(),
							RetryPolicy.getDefault());

					while (consumerRecords.hasNext()) {
						// System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>"+consumerRecords.next());
						String crMessage = consumerRecords.next();
						
						System.out.println("===>>>>>>"+crMessage);



							byte[] payloadBytes = crMessage.getBytes("UTF-8");
							EventData sendEvent = new EventData(payloadBytes);
							ehClient.sendSync(sendEvent);


					}
				});
			});
			jssc.start();
			jssc.awaitTermination();
			jssc.stop();
		
	}

}
