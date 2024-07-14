package com.knoldus.pubsub;

import com.knoldus.model.Failure;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class CreatePubsubMessagesFn extends DoFn<String, PubsubMessage> {

    public static final TupleTag<PubsubMessage> SUCCESS_TAG = new TupleTag<PubsubMessage>(){};
    public static final TupleTag<Failure> FAILURE_TAG = new TupleTag<Failure>(){};

    @ProcessElement
    public void processElement(@Element String filePath, MultiOutputReceiver out) {
        try {
            String timestamp = new Date().toString();
            String payload = String.format(filePath, timestamp);
            Map<String, String> attributes = new HashMap<>();
            attributes.put("filePath", filePath);
            attributes.put("timestamp", timestamp);
            PubsubMessage message = new PubsubMessage(payload.getBytes(StandardCharsets.UTF_8), attributes);
            out.get(SUCCESS_TAG).output(message);
        } catch (Exception exception) {
            final Failure failure = Failure.from(filePath,
                    "Unexpected error while creating PubsubMessage" , exception);
            out.get(FAILURE_TAG).output(failure);
        }
    }

    public TupleTag<PubsubMessage> getOutputTag() {
        return SUCCESS_TAG;
    }

    public TupleTag<Failure> getFailuresTag() {
        return FAILURE_TAG;
    }

}