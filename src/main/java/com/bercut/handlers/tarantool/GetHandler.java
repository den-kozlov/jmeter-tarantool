package com.bercut.handlers.tarantool;

import static com.bercut.constants.TarantoolArguments.KEY;
import static com.bercut.constants.TarantoolArguments.RANDOM_KEY;
import static com.bercut.constants.TarantoolArguments.RANDOM_KEY_MAX;
import static com.bercut.constants.TarantoolArguments.RANDOM_KEY_MIN;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import com.bercut.handlers.AbstractTarantoolHandler;

import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.tarantool.Iterator;

public class GetHandler extends AbstractTarantoolHandler {

    private List<String> key_tuple;
    private boolean random_key;
    private int key_range_min;
    private int key_range_max;
    private Random random;


    public GetHandler(JavaSamplerContext context) {
        super(context);
        this.key_tuple = Arrays.asList(context.getParameter(KEY));
        this.random_key = Boolean.valueOf(context.getParameter(RANDOM_KEY, "false"));
        this.key_range_min = context.getIntParameter(RANDOM_KEY_MIN,0);
        this.key_range_max = context.getIntParameter(RANDOM_KEY_MAX,0);
        this.random = new Random();
    }

    @Override
    public void handle() {
        if (random_key) {
            String key = String.valueOf(random.nextInt((int) (Math.abs(key_range_max - key_range_min + 1))) + key_range_min);
            key_tuple.set(0, key);
        }

        if (debug) {
            LOGGER.info(String.format("[%s] Getting key: %s",
                    getClass().getSimpleName(), String.valueOf(key_tuple)));
        }

        List<?> tup = client.syncOps().select(spaceId, 0, key_tuple, 0, 1, Iterator.EQ);

        if (debug && tup != null) {
            LOGGER.info("GET Result: " + String.valueOf(tup));
        }
        tup = null;
    }
}
