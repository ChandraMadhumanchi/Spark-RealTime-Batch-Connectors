package com.cm.spark.batch.streaming;
 
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
 
import com.microsoft.azure.eventhubs.*;
import com.microsoft.azure.servicebus.*;

import org.apache.spark.streaming.eventhubs.EventHubDirectDStream;
import org.apache.spark.streaming.eventhubs.EventHubDirectDStream$;
import org.apache.spark.streaming.eventhubs.EventHubsUtils;
 
//Microsoft.ServiceBus.Messaging

public class EventHubProducer
{
    public static void main(String[] args) throws ServiceBusException, ExecutionException, InterruptedException, IOException {
       try
       {
    	
    	   
    	  final String namespaceName = "xxx";
          final String eventHubName = "xx";
          final String sasKeyName = "xx";
          final String sasKey = "xxx";
           
    	   
           
        ConnectionStringBuilder connStr = new ConnectionStringBuilder(namespaceName, eventHubName, sasKeyName, sasKey);
      
        BufferedReader br = new BufferedReader(new FileReader("/xx/DimProduct.json"));

		String sCurrentLine = null;
		String message = null;
		while ((sCurrentLine = br.readLine()) != null) {
			message = sCurrentLine;
		

      
        
        EventData sendEvent = new EventData(message.getBytes());
 
        EventHubClient ehClient = EventHubClient.createFromConnectionStringSync(connStr.toString(), RetryPolicy.getDefault());
        ehClient.sendSync(sendEvent);
		
		}
        
        
        System.out.println("Messages Send");
       }
       catch(Exception ex){
    	   System.out.println("" + ex.getMessage());
       }
        
    }
}