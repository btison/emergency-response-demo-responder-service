package com.redhat.cajun.navy.responder.tracing;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import io.opentracing.propagation.TextMap;


public class HeadersMapExtractAdapter implements TextMap {

    private final Map<String, String> map = new HashMap<>();

    HeadersMapExtractAdapter(Map<String, Object> headers) {
        for (String key : headers.keySet()) {
            if (headers.get(key) instanceof byte[]) {
                map.put(key, new String((byte[])(headers.get(key)), StandardCharsets.UTF_8));
            } else {
                map.put(key, headers.get(key).toString());
            }
        }
    }

    @Override
    public Iterator<Entry<String, String>> iterator() {
        return map.entrySet().iterator();
    }

    @Override
    public void put(String key, String value) {
        throw new UnsupportedOperationException(
                "HeadersMapExtractAdapter should only be used with Tracer.extract()");
    }
}
