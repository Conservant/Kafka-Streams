package ru.mitenev.streamapi.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Message {
    @JsonProperty
    private String from;
    @JsonProperty
    private String to;
    @JsonProperty
    private String text;

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public String getTo() {
        return to;
    }

    public void setTo(String to) {
        this.to = to;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }
}
