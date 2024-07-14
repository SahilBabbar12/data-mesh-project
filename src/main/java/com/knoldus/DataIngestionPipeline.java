package com.knoldus;

import com.knoldus.config.Config;
import com.knoldus.model.Failure;
import com.knoldus.pubsub.CreatePubsubMessages;
import com.knoldus.util.HttpRequestHandler;
import com.knoldus.util.WriteToGcs;
import org.apache.beam.io.requestresponse.RequestResponseIO;
import org.apache.beam.io.requestresponse.Result;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ApachePipeline is the main class for executing an Apache Beam pipeline that fetches data from Mockaroo API
 * and writes it to Google Cloud Storage.
 */
public class DataIngestionPipeline {
    private static final Logger LOG = LoggerFactory.getLogger(DataIngestionPipeline.class);
    public static void main(String[] args) {

        try {
            Config config = Config.load();
            GcpOptions options = PipelineOptionsFactory.create().as(GcpOptions.class);
            Pipeline pipeline = Pipeline.create(options);

            options.setProject(config.getGcpProjectId());


            Result<String> results = pipeline.apply("CreateRequests",
                    Create.of(config.getMockarooUrl()))
                    .apply("handleHttpRequest",
                    RequestResponseIO.of(new HttpRequestHandler(), StringUtf8Coder.of()));

            WriteFilesResult<Void> writeResult = results.getResponses().apply("WriteToGCS",
                   WriteToGcs.writeToGCSbucket());

            PCollection<String> fileInfo = writeResult.getPerDestinationOutputFilenames()
                    .apply("ExtractFileNames", Values.create());

            CreatePubsubMessages pubsubMessages= new
                    CreatePubsubMessages();
            PCollectionTuple tuple = fileInfo.apply("CreatePubsubMessages",
                    ParDo.of(pubsubMessages).withOutputTags(pubsubMessages.getOutputTag(),
                            TupleTagList.of(pubsubMessages.getFailuresTag())));

            PCollection<PubsubMessage> fileMessages= tuple.get(pubsubMessages.getOutputTag());
            PCollection<Failure> failurePCollection= tuple.get(pubsubMessages.getFailuresTag());

            fileMessages.apply(PubsubIO.writeMessages().to(config.getGcpPubSubTopic()));

            LOG.error(failurePCollection.toString());
            pipeline.run().waitUntilFinish();
        } catch (IllegalArgumentException illegalArgumentException) {
            LOG.error("Invalid argument: " + illegalArgumentException.getMessage());
        } catch (RuntimeException runtimeException) {
            LOG.error("Runtime error occurred: " + runtimeException.getMessage());
        } catch (Exception exception) {
            LOG.error("An unexpected error occurred: " + exception.getMessage());
            exception.printStackTrace();
        }
    }
}
