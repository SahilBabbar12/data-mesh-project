package com.nashtech.utils;

import org.apache.beam.sdk.values.TupleTag;

public class PipelineConstants {
    public static final TupleTag<String> successTag = new TupleTag<>() { };
    public static final TupleTag<String> errorTag = new TupleTag<>() { };
}
