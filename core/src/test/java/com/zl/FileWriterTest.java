package com.zl;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class FileWriterTest {
    public static void main(String[] args) throws IOException {
        String path = "/opt/a/spark_test/shuffle.txt" ;

        File file = new File(path);

        OutputStream outputStream = new FileOutputStream(file);
        for (int i = 0; i < 1000 * 10000 ; i++) {
            outputStream.write(String.valueOf(i%1000 + "\n").getBytes(StandardCharsets.UTF_8));
            if(i % 1000 == 0){
                outputStream.flush();
            }
        }

        outputStream.flush();
        outputStream.close();

    }
}
