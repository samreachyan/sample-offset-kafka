package com.sakcode.producer.dto;

public class InputParameter {
    private int numberOfMessage;

    public InputParameter() {
    }

    public InputParameter(int numberOfMessage) {
        this.numberOfMessage = numberOfMessage;
    }

    public int getNumberOfMessage() {
        return numberOfMessage;
    }

    public void setNumberOfMessage(int numberOfMessage) {
        this.numberOfMessage = numberOfMessage;
    }
}
