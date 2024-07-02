package com.knoldus;

import org.apache.beam.io.requestresponse.Caller;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
/**
 * Implements Caller interface to make HTTP requests to Mockaroo API.
 */
class MockarooCaller implements Caller<String, String>, Serializable {

    private transient HttpClient httpClient;

    /** Initializes the MockarooCaller with a new HttpClient. */
    public MockarooCaller() {
        initHttpClient();
    }

    /** Initializes the HttpClient with a 10-second timeout. */
    private void initHttpClient() {
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    /**
     * Makes an HTTP GET request to the provided URL.
     * @param url The URL to call
     * @return The response body as a String
     * @throws RuntimeException if the request fails
     */
    @Override
    public String call(String url) {
        if (httpClient == null) {
            initHttpClient();
        }

        HttpRequest httpRequest = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Accept", "application/json")
                .GET()
                .build();

        try {
            HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                throw new RuntimeException("API request failed with status code: " + response.statusCode());
            }

            return response.body();
        } catch (IOException | InterruptedException exception) {
            throw new RuntimeException("Error calling API: " + exception.getMessage(), exception);
        }
    }

    /**
     * Custom deserialization method to reinitialize the HttpClient.
     */
    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        initHttpClient();
    }
}
