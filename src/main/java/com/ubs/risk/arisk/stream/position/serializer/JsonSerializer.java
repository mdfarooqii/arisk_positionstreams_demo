package com.ubs.risk.arisk.stream.position.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class JsonSerializer<T> implements Serializer<T> {

    private Class<T> serializedClass;
    private ObjectMapper objectMapper;

    public JsonSerializer(Class<T> serializedClass){
        this.serializedClass = serializedClass;
        this.objectMapper = new ObjectMapper();
    }


    @Override
    public void configure(Map map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, T t) {
        try {
            return objectMapper.writeValueAsBytes(t);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {

    }
}
