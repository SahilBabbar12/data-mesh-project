package com.knoldus.util;
import com.knoldus.config.Config;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.SerializableFunction;
import java.util.Date;

public class WriteToGcs  {

    public static FileIO.Write<String,String> writeToGCSbucket() {

        Config config = Config.load();
        Date currentDate = new Date();
        String timeStamp= String.valueOf(currentDate.getTime());
        String fileName=config.getGcpfilePrefix()+timeStamp;
        String fileSuffix=config.getGcpfileSuffix();

        return FileIO.<String, String>writeDynamic()
                 .by((SerializableFunction<String, String>) input -> {
                     return input.getClass().toString();
                        })
                        .to(config.getGcpBucketName())
                        .withNumShards(1)
                        .via(TextIO.sink())
                        .withDestinationCoder(StringUtf8Coder.of())
                        .withNaming(key -> FileIO.Write.defaultNaming(fileName, fileSuffix));
    }
}
