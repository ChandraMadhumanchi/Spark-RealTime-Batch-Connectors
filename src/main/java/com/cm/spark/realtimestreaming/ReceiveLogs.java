package com.cm.spark.realtimestreaming;

public class ReceiveLogs {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String source_Type = args[0];
		String gzip_flag = args[3];
		if(gzip_flag == null || gzip_flag == "")
			gzip_flag = "Y";
		if(!gzip_flag.equalsIgnoreCase("N"))
			gzip_flag ="Y";
		
		SourceAdaptorFactory source = new SourceAdaptorFactory();
		final SourceAdaptor S = source.getsourcetype(source_Type);
		try {
			String MetaJson = S.streamDataFromSource(args[1], args[2], gzip_flag);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
