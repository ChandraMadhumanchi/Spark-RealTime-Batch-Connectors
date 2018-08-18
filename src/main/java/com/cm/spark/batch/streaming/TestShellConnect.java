package com.cm.spark.batch.streaming;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class TestShellConnect {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		try {
            Runtime rt = Runtime.getRuntime();
            Process pr = rt.exec(new String[]{"/bin/sh", "/Users/nisum/Desktop/test.sh"});

            BufferedReader input = new BufferedReader(new InputStreamReader(pr.getInputStream()));
            String line = "";
            while ((line = input.readLine()) != null) {
                System.out.println(line);
            }
        } catch (Exception e) {
            System.out.println(e.toString());
            e.printStackTrace();
        }
		
	}

}
