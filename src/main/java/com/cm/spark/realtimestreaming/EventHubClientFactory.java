package com.cm.spark.realtimestreaming;

import com.microsoft.azure.eventhubs.*;

import java.io.IOException;
import java.util.concurrent.*;

public class EventHubClientFactory {
    public final static RetryPolicy EHFactoryRetryPolicy = new RetryExponential(
        ClientConstants.DEFAULT_RETRY_MIN_BACKOFF,
        ClientConstants.DEFAULT_RETRY_MAX_BACKOFF,
        30,
        "EventHubClientFactory");

    /**
     * Client cache. Each Spark worker JVM process will have its own cache which will stick around
     * for the lifetime of the process. In this way, multiple executors which run on the same worker will share
     * the EventHub connection instance.
     */
    private final static ConcurrentMap<String, EventHubClient> clientCache = new ConcurrentHashMap<String, EventHubClient>();

    /**
     * Retrieves an EventHub client / connection for the given connection string. If the client already exists,
     * the existing client is returned, otherwise a new client is created and cached for future calls.
     * @param connStr The connection string for the target EventHub.
     * @return The EventHub client instance which may be used to communicate with the corresponding EventHub.
     */
    public static EventHubClient getOrCreate(String connStr) throws IOException, EventHubException {
        try {
            return clientCache.computeIfAbsent(connStr, connStr2 -> {
                try {
                    return EventHubClient.createFromConnectionStringSync(connStr, EHFactoryRetryPolicy);
                } catch (Throwable t) {
                    // Wrap the checked exceptions into a RuntimeException to get around Java's poor exception design
                    // (there's no way to forward checked exceptions through the lambda in computerIfAbsent)
                    throw new RuntimeException(t);
                }
            });
        }
        catch (RuntimeException e) {
            Throwable t = e.getCause();

            // Unwrap and rethrow exceptions that are intended to be checked by the EH client lib user.
            if (t instanceof IOException) {
                throw (IOException)t;
            } else if (t instanceof EventHubException) {
                throw (EventHubException)t;
            } else {
                throw e;
            }
        }
    }
}