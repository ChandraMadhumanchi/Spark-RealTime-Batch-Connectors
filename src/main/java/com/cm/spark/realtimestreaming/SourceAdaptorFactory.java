package com.cm.spark.realtimestreaming;

public class SourceAdaptorFactory {
	
public SourceAdaptor getsourcetype(String source_Type) {
		
		// TODO Auto-generated method stub
		if(source_Type ==null)
			return null;
		
		if(source_Type.equalsIgnoreCase("kafka"))
		{
			return new KafkaSource();
		}
		return null;
	}

}
