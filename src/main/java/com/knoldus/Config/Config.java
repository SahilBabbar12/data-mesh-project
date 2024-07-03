package com.knoldus.Config;

import io.github.cdimascio.dotenv.Dotenv;

/**
 * Config class is responsible for loading and providing access to configuration
 * settings for the application, primarily sourced from environment variables.
 */
public class Config {
    private static final String GCP_PROJECT_KEY = "GCP_PROJECT";
    private static final String GCP_BUCKET_NAME_KEY = "GCP_BUCKET_NAME";
    private static final String GCP_BUCKET_PATH_KEY = "GCP_BUCKET_PATH";
    private static final String GCP_PUB_SUB_TOPIC_KEY = "GCP_PUB_SUB_TOPIC";
    private static final String MOCKAROO_API_URL_KEY = "MOCKAROO_API_URL";

    private final String gcpProjectId;
    private final String gcpBucketName;
    private final String gcpBucketPath;
    private final String gcpPubSubTopic;
    private final String mockarooUrl;

    /**
     * Private constructor to enforce use of the factory method.
     *
     * @param gcpProjectId The Google Cloud Project ID
     * @param gcpBucketName The Google Cloud Storage bucket name
     * @param gcpBucketPath The Google Cloud Storage bucket path
     * @param mockarooUrl The Mockaroo API URL
     */
    private Config(String gcpProjectId, String gcpBucketName, String gcpBucketPath, String gcpPubSubTopic, String mockarooUrl) {
        this.gcpProjectId = gcpProjectId;
        this.gcpBucketName= gcpBucketName;
        this.gcpBucketPath = gcpBucketPath;
        this.gcpPubSubTopic = gcpPubSubTopic;
        this.mockarooUrl = mockarooUrl;
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
                dotenv.get(GCP_BUCKET_PATH_KEY),
                dotenv.get(GCP_PUB_SUB_TOPIC_KEY),
                dotenv.get(MOCKAROO_API_URL_KEY)
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
     * Gets the Google Cloud Storage bucket path.
     *
     * @return The Google Cloud Storage bucket path
     */
    public String getGcpBucketPath() {
        return gcpBucketPath;
    }

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
}