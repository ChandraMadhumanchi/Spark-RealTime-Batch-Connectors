package com.cm.spark.batch.streaming;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import com.microsoft.azure.eventprocessorhost.*;
import com.microsoft.azure.servicebus.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.*;
import com.microsoft.azure.servicebus.*;

import java.util.concurrent.ExecutionException;

public class EventHubConsumer {
	public static void main(String args[])
			throws ServiceBusException, ExecutionException, InterruptedException, IOException {
		final String consumerGroupName = "$Default";
		
		final String namespaceName = "xxxx"; // 
		final String eventHubName = "xxxx";
		final String sasKeyName = "xxx";
		final String sasKey = "xxxxxxx";

		ConnectionStringBuilder connStr = new ConnectionStringBuilder(namespaceName, eventHubName, sasKeyName, sasKey);

		EventHubClient ehClient = EventHubClient.createFromConnectionStringSync(connStr.toString());

		String partitionId = "0";
		PartitionReceiver receiver = ehClient.createReceiverSync(EventHubClient.DEFAULT_CONSUMER_GROUP_NAME,
				partitionId, PartitionReceiver.START_OF_STREAM, false);

		receiver.setReceiveTimeout(Duration.ofSeconds(120));

		while (true) {
			try {
				Iterable<EventData> receivedEvents = receiver.receiveSync(1);

				if (receivedEvents != null) {
					Iterator itr = receivedEvents.iterator();

					while (itr.hasNext()) {
						EventData data = (EventData) itr.next();

						byte[] bytes = data.getBytes();

						String s = new String(bytes);
						System.out.println(s);
					}
				}
			} catch (Exception ex) {
				System.out.println("" + ex.getMessage());
			}
		}

	}
}