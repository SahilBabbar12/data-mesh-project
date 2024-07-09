package com.nashtech.services;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

public class GcsClientFactory implements DefaultValueFactory<Storage> {

    @Override
    public Storage create(@UnknownKeyFor @NonNull @Initialized PipelineOptions options) {
        return StorageOptions.getDefaultInstance().getService();
    }
}
