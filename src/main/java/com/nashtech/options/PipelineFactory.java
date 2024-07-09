package com.nashtech.options;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class PipelineFactory {
    public static Pipeline createPipeline(final String[] args) {

        PipelineOptionsFactory.register(GCPOptions.class);
        GCPOptions options =
                PipelineOptionsFactory.fromArgs(args)
                        .withValidation()
                        .create()
                        .as(GCPOptions.class);

        return Pipeline.create(options);
    }
}
