package com.cm.spark.realtimestreaming;

import java.io.IOException;

import com.microsoft.azure.eventhubs.EventHubException;
import org.json.simple.parser.ParseException;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.ConnectionStringBuilder;

public class Event_Hub {

	  public static int i=0;
	  
	public void send_payload_ehub(String payload,String ehubparams) throws EventHubException, IOException, ParseException
	{
		String[] Eventhub_Params = ehubparams.split(",");
		
		  final String NAMESPACE = Eventhub_Params[0];
		  final String EVENT_HUB = Eventhub_Params[1];
		  final String SASKEYNAME = Eventhub_Params[2];
		  final String SASKEY = Eventhub_Params[3];
		  System.out.println("***********************************BEFORE data being written to event hub");
		  writedatatoeventhub(payload,NAMESPACE,EVENT_HUB,SASKEYNAME,SASKEY);
		  System.out.println("***********************************AFTER data being written to event hub");
	}
	
	public static void writedatatoeventhub(String message, String namespaceName, String eventHubName, String sasKeyName, String sasKey) throws EventHubException, IOException, ParseException
	  {
		 
		  i++;
			
		    ConnectionStringBuilder connStr = new ConnectionStringBuilder(namespaceName, eventHubName, sasKeyName, sasKey);
			
		    byte[] payloadBytes = message.getBytes("UTF-8");
		    EventData sendEvent = new EventData(payloadBytes);

		    EventHubClient ehClient = EventHubClient.createFromConnectionStringSync(connStr.toString());
		    ehClient.sendSync(sendEvent);
		    ehClient.close();
			System.out.println("Event Sent");
			System.out.println("This is message number - "+i);
			
			
	  }

	public EventHubClient getClientConnection(String ehubparams) throws EventHubException, IOException {
		// TODO Auto-generated method stub
		String[] Eventhub_Params = ehubparams.split(",");
		
		final String NAMESPACE = Eventhub_Params[0];
		final String EVENT_HUB = Eventhub_Params[1];
		final String SASKEYNAME = Eventhub_Params[2];
		final String SASKEY = Eventhub_Params[3];

		ConnectionStringBuilder connStr = new ConnectionStringBuilder(NAMESPACE, EVENT_HUB, SASKEYNAME, SASKEY);
		return EventHubClientFactory.getOrCreate(connStr.toString());
	}
}
