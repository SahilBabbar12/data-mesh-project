package com.knoldus.model;

import java.io.Serializable;

public class Failure implements Serializable {
    private final String inputElement;
    private final String errorMessage;
    private final Exception exception;

    public Failure(String inputElement, String errorMessage, Exception exception) {
        this.inputElement = inputElement;
        this.errorMessage = errorMessage;
        this.exception = exception;
    }

    public static <T> Failure from(final T inputElement, String errorMessage, final Exception exception) {
        return new Failure(inputElement.toString(), errorMessage, exception);
    }
    @Override
    public String toString() {
        return "\nFailure{filePath='" + inputElement + "', errorMessage='" + errorMessage + "', exception=" + exception + "}";
    }
}