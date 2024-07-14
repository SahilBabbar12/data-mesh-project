package com.knoldus.util;
import com.knoldus.config.Config;
import org.apache.beam.io.requestresponse.Result;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;

public class WriteToGcs  {

    public static FileIO.Write<Void,String> writeToGCSbucket() {

        Config config = Config.load();
        return FileIO.<String>write()
                .via(TextIO.sink())
                .to(config.getGcpBucketName())
                .withSuffix(config.getGcpfileSuffix());
    }
}
