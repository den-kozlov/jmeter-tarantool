package com.bercut.handlers;

import static com.bercut.constants.TarantoolArguments.*;

import java.util.List;

import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tarantool.TarantoolClient;
import org.tarantool.TarantoolClusterClient;
import org.tarantool.TarantoolClusterClientConfig;

public abstract class AbstractTarantoolHandler implements ITarantoolHandler {
    protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractTarantoolHandler.class);
    protected static volatile TarantoolClusterClientConfig  config;
    protected static volatile TarantoolClusterClient client;
    protected static int spaceId;
    protected final JavaSamplerContext context;
    protected Boolean debug = false;

    public AbstractTarantoolHandler(JavaSamplerContext context) {
        this.context = context;
        this.debug = Boolean.valueOf(context.getParameter(DEBUG, "false"));

        setupClientConfig();
        setupTarantoolClient(getConfig());
        setupSpace();
    }

    private void setupSpace() {
        String space_name = context.getParameter(SPACE_NAME);
        if (space_name == null || space_name.isEmpty())
        {
            throw new IllegalArgumentException("Tarantool space name wasn't defined");
        }
        List<?> reply = client.syncOps().eval(
            String.format("if box.space.%1$s then return box.space.%1$s.id else return nil end", space_name)
            );
        if (reply.size() < 1 || reply.get(0) == null)
        {
            throw new IllegalArgumentException(String.format("Space %s doesn't exist on tarantool server", space_name) );
        }
        spaceId = (Integer)reply.get(0);
    }

    public TarantoolClusterClientConfig getConfig() {
        return config;
    }

    public TarantoolClient getClient() {
        return client;
    }

    public int getSpaceId() {
        return spaceId;
    }
    private synchronized void setupClientConfig() {
        if (debug) {
            LOGGER.info("Setting up Tarantool Config");
        }

        int timeout = context.getIntParameter(TIMEOUT, 2000);

        if (config == null) {
            config = new TarantoolClusterClientConfig ();
            config.username = context.getParameter(USERNAME, null);
            config.password = context.getParameter(PASSWORD, null);
            if (config.username == null || config.username.isEmpty()) {
                config.username = null;
                config.password = null;
            }
            config.clusterDiscoveryEntryFunction = context.getParameter(CLUSTER_DISCOVERY_FUNCTION, "");
            config.operationExpiryTimeMillis = timeout;
        }
    }

    private synchronized void setupTarantoolClient(TarantoolClusterClientConfig cfg) {
        if (debug) {
            LOGGER.info("Setting up Tarantool Client");
        }

        String servers = context.getParameter(SERVERS);
        if (servers == null || servers.isEmpty()) {
            throw new IllegalArgumentException("The server list is empty!");
        }
        // (Subset of) nodes in the cluster to establish a connection
        String[] hosts = servers.split("\\s*[,;]\\s*");
        client = new TarantoolClusterClient(cfg, hosts);
        client.ping();
    }
}
