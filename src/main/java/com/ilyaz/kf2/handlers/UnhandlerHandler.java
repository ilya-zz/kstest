package com.ilyaz.kf2.handlers;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;

@Slf4j
public class UnhandlerHandler implements StreamsUncaughtExceptionHandler
{
    @Override
    public StreamThreadExceptionResponse handle(Throwable exception) {
        log.warn("unhandled", exception);
        return REPLACE_THREAD;
    }
}
