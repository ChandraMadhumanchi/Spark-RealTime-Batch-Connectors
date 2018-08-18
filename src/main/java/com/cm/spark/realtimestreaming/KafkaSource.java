package com.cm.spark.realtimestreaming;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

import com.google.common.io.ByteStreams;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventDataBatch;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;


public class KafkaSource implements SourceAdaptor, Serializable {

	public String streamDataFromSource(String kafkaparams, final String ehubparams,final String gzip_data_flag) throws InterruptedException, EventHubException, IOException {
	
		System.out.println("we have entered kafka code");
		System.out.println("kafkaparams:" + kafkaparams);
		System.out.println("ehubparams:" + ehubparams);
		System.out.println("gzipdataflag:" + gzip_data_flag);

	
		String[] Kafka_Params = kafkaparams.split("#");
		
		// Below are the MQ Paramters from wrapper
		String TOPICNAME = Kafka_Params[0];
		String ZOOKEEPER = Kafka_Params[1];
		String BROKERID = Kafka_Params[2];
		String GROUPID = Kafka_Params[3];
		String SPARK_BATCHDURATION = Kafka_Params[4];
		
		String SPARK_MAXRATEPERPARTITION  = Kafka_Params[5];
		String KAFKA_SSL_KEY_PASSWORD  = Kafka_Params[6];
		
		// To increase throughput, your must increase the number of Kafka partitions in the topic.
		SparkConf sparkConf = new SparkConf()
			//.setMaster("local[2]")
			.setAppName("KafkaDirectEventHub")
			.set("spark.driver.allowMultipleContexts", "true")
			.set("spark.streaming.kafka.maxRatePerPartition",SPARK_MAXRATEPERPARTITION) //"40"
			.set("spark.streaming.unpersist","true")
			.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
			.set("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
			.set("spark.speculation", "false")
			.set("spark.streaming.backpressure.enabled","true")
			.set("spark.streaming.backpressure.pid.minRate","80");

		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf,
				Durations.seconds(Integer.parseInt(SPARK_BATCHDURATION)));
		
		
		// Get certificates
				//String SPARK_YARN_STAGING_DIR = System.getenv("SPARK_YARN_STAGING_DIR");
				//System.out.println("path:"+ SPARK_YARN_STAGING_DIR);

				String truststoreLocation = "./client.truststore.jks";
				String keystoreLocation = "./client.keystore.jks";


				// Kafka config
				Map<String, Object> kafkaParams = new HashMap<String, Object>();
				kafkaParams.put("zookeeper", ZOOKEEPER);
				kafkaParams.put("bootstrap.servers", BROKERID);
				kafkaParams.put("key.deserializer", StringDeserializer.class);
				kafkaParams.put("value.deserializer", StringDeserializer.class);
				kafkaParams.put("group.id", GROUPID);
				kafkaParams.put("auto.offset.reset", "latest");
				//kafkaParams.put("auto.offset.reset", "earliest");
				kafkaParams.put("enable.auto.commit", false);

				// Protocol
				kafkaParams.put("security.protocol", "SSL");
				kafkaParams.put("ssl.truststore.location", truststoreLocation);
				kafkaParams.put("ssl.truststore.password", KAFKA_SSL_KEY_PASSWORD);

				kafkaParams.put("ssl.keystore.location", keystoreLocation);
				kafkaParams.put("ssl.keystore.password", KAFKA_SSL_KEY_PASSWORD);
				kafkaParams.put("ssl.key.password", KAFKA_SSL_KEY_PASSWORD);

				kafkaParams.put("ssl.enabled.protocols", "TLSv1.2,TLSv1.1,TLSv1");
				kafkaParams.put("ssl.truststore.type", "JKS");
				kafkaParams.put("ssl.keystore.type", "JKS");

				Collection<String> topics = Collections.singletonList(TOPICNAME);

				// Create direct kafka stream with brokers and topics
				final JavaInputDStream<ConsumerRecord<String, String>> stream =
					KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(),
						ConsumerStrategies.<String,String>Subscribe(topics,kafkaParams));

				stream.foreachRDD(rdd -> {
					   OffsetRange[] kafkaOffsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
					   
					   JavaRDD<String> lines = rdd.map((Function<ConsumerRecord<String, String>, String>) ConsumerRecord::value);
							   
					   // Compress and convert to EH payload format
					   JavaRDD<EventData> payloads = lines.map(line -> {
					      byte[] lineBytes = line.getBytes("UTF-8");
					      ByteArrayInputStream sourceStream = new ByteArrayInputStream(lineBytes);
					      ByteArrayOutputStream outputStream = new ByteArrayOutputStream((lineBytes.length * 3) / 5);
					      
					      if(gzip_data_flag.equalsIgnoreCase("Y")){
					    	  try (GZIPOutputStream gzip = new GZIPOutputStream(outputStream)) {
					    		  ByteStreams.copy(sourceStream, gzip);
					    	  }
					      }

					      return new EventData(outputStream.toByteArray());
					   });

					   // Write all partitions into EventHub as batches
					   payloads.foreachPartition(partitionOfRecords -> {
					      if (partitionOfRecords.hasNext()) {
					         Event_Hub eb = new Event_Hub();
					         EventHubClient client = eb.getClientConnection(ehubparams);

					         EventDataBatch batch = client.createBatch();
					         while (partitionOfRecords.hasNext()) {
					            EventData event = partitionOfRecords.next();

					            if (!batch.tryAdd(event)) {
					               if (batch.getSize() > 0) {
					                  // Send filled batch to EventHub
					                  final EventDataBatch temp = batch;
					                  client.sendSync(temp::iterator);

					                  // Initialize a new empty batch
					                  batch = client.createBatch();
					               }

					               // Retry adding event into empty batch
					               if (!batch.tryAdd(event)) {
					                  // Single event > 256KB. For now, send alone to trigger correct message too big error.
					                  // TODO: upload to blob and send message with blob handle here
					                  client.sendSync(event);
					               }
					            }
					         }

					         if (batch.getSize() > 0) {
					            // Send batch to EventHub
					            client.sendSync(batch::iterator);
					         }
					      }
					   });

					   // All partitions have been written. Mark offsets as completed in Kafka.
					   ((CanCommitOffsets) stream.inputDStream()).commitAsync(kafkaOffsetRanges);
					});

			
				jssc.start();
				jssc.awaitTermination();
				
				return null;
			}
		}

