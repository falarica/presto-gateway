package io.prestosql.gateway;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;

public class ErrorInfo
{
    private final String userMessage;
    private final String devMessage;
    private final Optional<String> exceptionTrace;

    public ErrorInfo(
            @JsonProperty("userMessage") String userMessage,
            @JsonProperty("developerMessage") String developerMessage,
            @JsonProperty("exceptionTrace") Optional<String> exceptionTrace)
    {
        this.userMessage = userMessage;
        this.devMessage = developerMessage;
        this.exceptionTrace = exceptionTrace;
    }

    @JsonProperty
    public String getUserMessage()
    {
        return userMessage;
    }

    @JsonProperty
    public String getDevMessage()
    {
        return devMessage;
    }

    @JsonProperty
    public Optional<String> getExceptionTrace()
    {
        return exceptionTrace;
    }
}
