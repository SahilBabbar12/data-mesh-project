package com.knoldus.config;

import io.github.cdimascio.dotenv.Dotenv;

/**
 * Config class is responsible for loading and providing access to configuration
 * settings for the application, primarily sourced from environment variables.
 */
public class Config {
    private static final String GCP_PROJECT_KEY = "GCP_PROJECT";
    private static final String GCP_BUCKET_NAME_KEY = "GCP_BUCKET_NAME";
    private static final String GCP_PUB_SUB_TOPIC_KEY = "GCP_PUB_SUB_TOPIC";
    private static final String MOCKAROO_API_URL_KEY = "MOCKAROO_API_URL";
    private static final String GCP_FILE_SUFFIX_KEY="GCP_FILE_SUFFIX";

    private final String gcpProjectId;
    private final String gcpBucketName;
    private final String gcpPubSubTopic;
    private final String mockarooUrl;
    private final String gcpfileSuffix;


    /**
     * Private constructor to enforce use of the factory method.
     *
     * @param gcpProjectId  The Google Cloud Project ID
     * @param gcpBucketName The Google Cloud Storage bucket name
     * @param mockarooUrl   The Mockaroo API URL
     * @param gcpfileSuffix
     */
    private Config(String gcpProjectId, String gcpBucketName, String gcpPubSubTopic, String mockarooUrl, String gcpfileSuffix) {
        this.gcpProjectId = gcpProjectId;
        this.gcpBucketName= gcpBucketName;
        this.gcpPubSubTopic = gcpPubSubTopic;
        this.mockarooUrl = mockarooUrl;
        this.gcpfileSuffix = gcpfileSuffix;
    }

    /**
     * method to create and load a Config instance from environment variables.
     *
     * @return A new Config instance with values loaded from environment variables
     */
    public static Config load() {
        Dotenv dotenv = Dotenv.load();
        return new Config(
                dotenv.get(GCP_PROJECT_KEY),
                dotenv.get(GCP_BUCKET_NAME_KEY),
                dotenv.get(GCP_PUB_SUB_TOPIC_KEY),
                dotenv.get(MOCKAROO_API_URL_KEY),
                dotenv.get(GCP_FILE_SUFFIX_KEY)
        );
    }

    /**
     * Gets the Google Cloud Project ID.
     *
     * @return The Google Cloud Project ID
     */
    public String getGcpProjectId() {
        return gcpProjectId;
    }

    /**
     * Gets the Google Cloud Storage bucket name.
     *
     * @return The Google Cloud Storage bucket name
     */
    public String getGcpBucketName() {
        return gcpBucketName;
    }

    /**
     * Gets pubsub topic path
     *
     * @return pubsub topic path
     */
    public String getGcpPubSubTopic() {
        return gcpPubSubTopic;
    }

    /**
     * Gets the Mockaroo API URL.
     *
     * @return The Mockaroo API URL
     */
    public String getMockarooUrl() {
        return mockarooUrl;
    }

    /**
     * Gets the file suffix
     *
     * @return the file suffix
     */
    public String getGcpfileSuffix() {
        return gcpfileSuffix;
    }
}