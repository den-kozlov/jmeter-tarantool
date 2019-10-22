package com.bercut.handlers;

import com.bercut.handlers.tarantool.*;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import static com.bercut.constants.TarantoolArguments.*;

import java.util.HashMap;
import java.util.Map;

public class TarantoolHandlerFactory {

    private static Map<String, Class<? extends ITarantoolHandler>> handlerMap;

    static {
        handlerMap = new HashMap<String, Class<? extends ITarantoolHandler>>();
        handlerMap.put("GET", GetHandler.class);
        handlerMap.put("PUT", PutHandler.class);
    }

    public static ITarantoolHandler getHandler(JavaSamplerContext context) throws Exception {
        String method = context.getParameter(METHOD);

        if (handlerMap.containsKey(method)) {
            Class<? extends ITarantoolHandler> cls = handlerMap.get(method.toUpperCase());
            if (cls != null) {
                return cls.getDeclaredConstructor(JavaSamplerContext.class).newInstance(context);
            } else {
                throw new Exception("Unable to instantiate method handler class");
            }
        } else {
            throw new IllegalArgumentException(method + " is not supported as a method");
        }

    }
}
