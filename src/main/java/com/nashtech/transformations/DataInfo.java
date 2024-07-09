package com.nashtech.transformations;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
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

import java.util.HashMap;
import java.util.UUID;

import static com.nashtech.utils.PipelineConstants.errorTag;

public class DataInfo extends DoFn<String, String> {
    final Logger logger = LoggerFactory.getLogger(DataInfo.class);

    @ProcessElement
    public void processElement(ProcessContext context, PipelineOptions options) {
        String element = context.element();
        GCPOptions gcpOptions = options.as(GCPOptions.class);

        try {
            ObjectMapper mapper = gcpOptions.getDefaultObjectMapper();

            TypeReference<HashMap<String, String>> typeRef
                    = new TypeReference<HashMap<String, String>>() {
            };
            HashMap<String, String> messageMap = mapper.readValue(element, typeRef);
            String path = messageMap.get("path");
            String domainTeamName = messageMap.get("domain");
            if (path.startsWith("gs://")) {
                HashMap<String, String> resultMap = new HashMap<>();
                resultMap.put("path", path);
                resultMap.put("domainTeam", domainTeamName);
                context.output(mapper.writeValueAsString(resultMap));
            } else {
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
