package com.cm.spark.batch.streaming;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

public class CompressFileGzip {
    public static void main(String[] args) {
        
    	String source_filepath = "xxxxx/single_msg.json";
        String destinaton_zip_filepath = "xxxxxx/single_msg.gzip";

        CompressFileGzip gZipFile = new CompressFileGzip();

        gZipFile.gzipFile(source_filepath, destinaton_zip_filepath);

    }

    public void gzipFile(String source_filepath, String destinaton_zip_filepath) {

        byte[] buffer = new byte[2048];

        try {

            FileOutputStream fileOutputStream =new FileOutputStream(destinaton_zip_filepath);

            GZIPOutputStream gzipOuputStream = new GZIPOutputStream(fileOutputStream);

            FileInputStream fileInput = new FileInputStream(source_filepath);

            int bytes_read;

            while ((bytes_read = fileInput.read(buffer)) > 0) {

                gzipOuputStream.write(buffer, 0, bytes_read);

            }

            fileInput.close();

            gzipOuputStream.finish();
            gzipOuputStream.close();
            System.out.println("The file was compressed successfully!");
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}