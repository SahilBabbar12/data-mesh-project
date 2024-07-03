package com.knoldus;

import com.knoldus.Config.Config;
import org.apache.beam.io.requestresponse.RequestResponseIO;
import org.apache.beam.io.requestresponse.Result;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.PCollection;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * ApachePipeline is the main class for executing an Apache Beam pipeline that fetches data from Mockaroo API
 * and writes it to Google Cloud Storage.
 */
public class ApachePipeline {

    public static void main(String[] args) {

        try {
            Config config = Config.load();
            GcpOptions options = PipelineOptionsFactory.create().as(GcpOptions.class);
            Pipeline pipeline = Pipeline.create(options);

            options.setProject(config.getGcpProjectId());
            PCollection<String> requests = pipeline.apply("CreateRequests",
                    Create.of(config.getMockarooUrl()));

            Coder<String> responseCoder = StringUtf8Coder.of();
            Result<String> result = requests.apply("CallMockarooAPI",
                    RequestResponseIO.of(new MockarooCaller(), responseCoder));
            WriteFilesResult<Void> writeResult = result.getResponses().apply("WriteToGCS",
                    FileIO.<String>write()
                            .via(TextIO.sink())
                            .to("gs://" + config.getGcpBucketName() + "/" + config.getGcpBucketPath())
                            .withSuffix(".json"));
            PCollection<String> fileInfo = writeResult.getPerDestinationOutputFilenames()
                    .apply("ExtractFileNames", Values.create());


            PCollection<PubsubMessage> fileMessages = fileInfo.apply("CreatePubsubMessages", ParDo.of(new DoFn<String, PubsubMessage>() {
                @ProcessElement
                public void processElement(@Element String filePath, OutputReceiver<PubsubMessage> out) {
                    String timestamp = new Date().toString();

                    // Creating the payload
                    String payload = String.format(filePath, timestamp);

                    Map<String, String> attributes = new HashMap<>();
                    attributes.put("filePath", filePath);
                    attributes.put("timestamp", timestamp);

                    out.output(new PubsubMessage(payload.getBytes(StandardCharsets.UTF_8), attributes));
                }
            }));

            // Writing the message to pub/sub
            fileMessages.apply(PubsubIO.writeMessages().to(config.getGcpPubSubTopic()));
            pipeline.run().waitUntilFinish();

        } catch (IllegalArgumentException illegalArgumentException) {
            System.err.println("Invalid argument: " + illegalArgumentException.getMessage());
        } catch (RuntimeException runtimeException) {
            System.err.println("Runtime error occurred: " + runtimeException.getMessage());
        } catch (Exception exception) {
            System.err.println("An unexpected error occurred: " + exception.getMessage());
            exception.printStackTrace();
        }
    }
}
