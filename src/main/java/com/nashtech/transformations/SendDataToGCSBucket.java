package com.nashtech.transformations;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.nashtech.options.GCPOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;

import static com.nashtech.utils.PipelineConstants.errorTag;
import static com.nashtech.utils.PipelineConstants.successTag;
import static java.nio.charset.StandardCharsets.UTF_8;

public class SendDataToGCSBucket extends DoFn<String, String> {
    final Logger logger = LoggerFactory.getLogger(SendDataToGCSBucket.class);

    @ProcessElement
    public void processElement(ProcessContext context, PipelineOptions options) {
        String element = context.element();
        GCPOptions gcpOptions = options.as(GCPOptions.class);
        Storage storage = gcpOptions.getGcsClient();
        String targetBucketName = gcpOptions.getGcsBucketName();
        String uuid = String.valueOf(UUID.randomUUID());

        try {
            ObjectMapper mapper = gcpOptions.getDefaultObjectMapper();

            TypeReference<HashMap<String, String>> typeRef
                    = new TypeReference<HashMap<String, String>>() {
            };
            HashMap<String, String> messageMap = mapper.readValue(element, typeRef);
            String path = messageMap.get("path");
            String domainTeamName = messageMap.get("domain");
            if (path.startsWith("gs://")) {
                String[] split = path.split("/");
                String sourceBucketName = split[2];
                String objectName = path.split(sourceBucketName)[1].substring(1);
                String file_name = split[split.length - 1];
                BlobId source = BlobId.of(sourceBucketName, objectName);
                BlobId target =
                        BlobId.of(
                                targetBucketName, file_name);

                storage.copy(
                        Storage.CopyRequest.newBuilder().setSource(source).setTarget(target).build());
                logger.info(
                        "Copied object "
                                + objectName
                                + " from bucket "
                                + sourceBucketName
                                + " to "
                                + targetBucketName);
                HashMap<String, String> resultMap = new HashMap<>();
                resultMap.put("path", target.toGsUtilUri());
                resultMap.put("domainTeam", domainTeamName);
                resultMap.put("insertedTime", Instant.now().toString());
                context.output(mapper.writeValueAsString(resultMap));
            }
            else {
                throw new Exception("File location is not valid");
            }
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
            logger.error("Error writing to GCS: " + ex.getMessage(), ex);
            String errorMessage = String.format("Error processing element %s: %s", element, ex.getMessage());
            context.output(errorTag, errorMessage);
        }
    }
}