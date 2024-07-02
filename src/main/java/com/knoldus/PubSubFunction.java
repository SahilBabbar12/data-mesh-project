package com.knoldus;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;

public class PubSubFunction {

    public static void applyPubSubFunction(Pipeline pipeline, List<GcsMessage> messages) {
        PCollection<PubsubMessage> pubsubMessages = pipeline.apply(Create.of(messages))
                .apply(MapElements
                        .into(TypeDescriptor.of(PubsubMessage.class))
                        .via(message -> {
                            byte[] payload = new byte[0]; // Empty payload
                            HashMap<String, String> attributes = new HashMap<>();

                            attributes.put("FilePath", message.path);
                            attributes.put("Domain", message.domain);
                            attributes.put("Timestamp", Long.toString(message.timestamp));
                            return new PubsubMessage(payload, attributes);
                        }));

        // Printing Pub/Sub messages to the console and convert them to strings
        PCollection<String> pubsubMessageStrings = pubsubMessages.apply("Process and Format Messages", ParDo.of(new DoFn<PubsubMessage, String>() {
            @ProcessElement
            public void processElement(@Element PubsubMessage message, OutputReceiver<String> out) {
                String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
                String attributes = message.getAttributeMap().toString();
                String formattedMessage = "Pub/Sub Message payload: " + payload + "\n" +
                        "Pub/Sub Message attributes: " + attributes;

                // Print to console
                System.out.println(formattedMessage);

                // Output the formatted string
                out.output(formattedMessage);
            }
        }));

        // Writing the formatted message to a file
        pubsubMessageStrings.apply("Write PubSub Messages", TextIO.write().to("pubsubprefix").withSuffix(".txt"));
    }
}
