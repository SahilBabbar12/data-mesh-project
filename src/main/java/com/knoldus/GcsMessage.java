package com.knoldus;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;


/**
 * Represents a Google Cloud Storage object message for use in Apache Beam pipelines.
 */
@DefaultCoder(AvroCoder.class)
public class GcsMessage {

    /** GCS object path */
    public String path;

    /** Associated domain */
    public String domain;

    /** Timestamp in milliseconds  */
    public Long timestamp;

    /** Default constructor */
    public GcsMessage() {}

    /**
     * Constructs a GcsMessage with specified values.
     */
    public GcsMessage(String path, String domain, Long timestamp) {
        this.path = path;
        this.domain = domain;
        this.timestamp = timestamp;
    }
}