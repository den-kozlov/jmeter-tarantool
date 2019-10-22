package com.bercut.handlers;

import org.tarantool.TarantoolClient;
import org.tarantool.TarantoolClientConfig;

public interface ITarantoolHandler {
    TarantoolClientConfig getConfig();

    TarantoolClient getClient();

    int getSpaceId();

    void handle();
}
