package com.bercut.handlers.tarantool;

import static com.bercut.constants.TarantoolArguments.KEY;
import static com.bercut.constants.TarantoolArguments.LOCAL_FILE_PATH;
import static com.bercut.constants.TarantoolArguments.RANDOM_KEY;
import static com.bercut.constants.TarantoolArguments.RANDOM_KEY_MAX;
import static com.bercut.constants.TarantoolArguments.RANDOM_KEY_MIN;
import static com.bercut.constants.TarantoolArguments.VALUE;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import com.bercut.handlers.AbstractTarantoolHandler;

import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;

public class PutHandler extends AbstractTarantoolHandler {

    private List<String> key_tuple;
    private List<String> value_tuple;
    private boolean random_key;
    private int key_range_min;
    private int key_range_max;
    private Random random;
    
    public PutHandler(JavaSamplerContext context) {
        super(context);

        this.key_tuple = Arrays.asList(context.getParameter(KEY));
        this.random_key = Boolean.valueOf(context.getParameter(RANDOM_KEY, "false"));
        this.key_range_min = context.getIntParameter(RANDOM_KEY_MIN,0);
        this.key_range_max = context.getIntParameter(RANDOM_KEY_MAX,0);
        this.random = new Random();

        String value = context.getParameter(VALUE, "");
        String file = context.getParameter(LOCAL_FILE_PATH);
        if (!(file == null || file.isEmpty()) && value.isEmpty()) {
            try {
                value = Files.readAllLines(Paths.get(file)).toString();
            } catch (IOException e) {
                LOGGER.error(getClass().getSimpleName() + ": File read error", e);
            }
        }
        this.value_tuple = Arrays.asList(context.getParameter(KEY), value);
    }

    @Override
    public void handle() {
        if (random_key) {
            String key = String.valueOf(random.nextInt((int) (Math.abs(key_range_max - key_range_min + 1))) + key_range_min);
            key_tuple.set(0, key);
            value_tuple.set(0, key);
        }

        if (debug) {
            LOGGER.info(String.format("[%s] Putting key-value: %s",
                    getClass().getSimpleName(), String.valueOf(value_tuple)));
        }

        client.syncOps().upsert(spaceId, key_tuple, value_tuple);
    }
}
