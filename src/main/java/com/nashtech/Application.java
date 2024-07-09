package com.nashtech;

import com.nashtech.options.GCPOptions;
import com.nashtech.options.PipelineFactory;
import com.nashtech.transformations.DataInfo;
import com.nashtech.transformations.SendDataToGCSBucket;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;

import static com.nashtech.utils.PipelineConstants.errorTag;
import static com.nashtech.utils.PipelineConstants.successTag;

public class Application {
    final static Logger logger = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        Pipeline pipeline = PipelineFactory.createPipeline(args);
        GCPOptions options = pipeline.getOptions().as(GCPOptions.class);

        PCollection<PubsubMessage> messages = options.getUseSubscription() ?
                pipeline.apply("Read From Subscription", PubsubIO.readMessagesWithAttributes().fromSubscription(options.getInputSubscription())) :
                pipeline.apply("Read From Topic", PubsubIO.readMessagesWithAttributes().fromTopic(options.getInputTopic()));

        PCollection<String> stringMessages = messages.apply("Convert to String", ParDo.of(new DoFn<PubsubMessage, String>() {
            @ProcessElement
            public void process(ProcessContext processContext) {
                PubsubMessage pubsubMessage = processContext.element();
                Optional<String> messageString = Optional.ofNullable(pubsubMessage)
                        .map(message -> new String(message.getPayload(), StandardCharsets.UTF_8));
                messageString.ifPresent(processContext::output);
            }
        }));

        PCollectionTuple dataInfoMap = stringMessages.apply("Fetch Data location and Domain Team", ParDo.of(new DataInfo())
                .withOutputTags(successTag, TupleTagList.of(errorTag)));

        PCollection<String> domainDataBucketURI = dataInfoMap.get(successTag);
        PCollection<String> errors = dataInfoMap.get(errorTag);

        errors.apply("Log GCS Errors", ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                logger.error("Failed to read data info : " + c.element());
            }
        }));

        PCollectionTuple gcsBucketUri = domainDataBucketURI.apply("Write to GCS bucket", ParDo.of(new SendDataToGCSBucket())
                .withOutputTags(successTag, TupleTagList.of(errorTag)));

        PCollection<String> happyGCSBucketURI = gcsBucketUri.get(successTag);
        PCollection<String> gcsErrors = gcsBucketUri.get(errorTag);

        if (options.getUseSubscription()) {
            logger.info("Publishing GCS URIs to output Pub/Sub subscription");
            happyGCSBucketURI.apply("Write to Pub/Sub subscription", PubsubIO.writeStrings().to(options.getOutputSubscription()));
        } else {
            logger.info("Publishing GCS URIs to output Pub/Sub topic");
            happyGCSBucketURI.apply("Write to Pub/Sub topic", PubsubIO.writeStrings().to(options.getOutputTopic()));
        }

        gcsErrors.apply("Log GCS Errors", ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                logger.error("Failed to write to GCS: " + c.element());
            }
        }));

        pipeline.run().waitUntilFinish();
    }
}