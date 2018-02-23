package com.ubs.risk.arisk.stream.position.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class JsonDeserializer<T> implements Deserializer {

    private Class<T> deserializedClass;
    private ObjectMapper objectMapper;

    public JsonDeserializer(Class<T> deserializedClass) {
        this.deserializedClass = deserializedClass;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void configure(Map map, boolean b) {
        if(deserializedClass == null) {
            deserializedClass = (Class<T>) map.get("serializedClass");
        }
    }

    @Override
    public T deserialize(String s, byte[] bytes) {
        if(bytes == null){
            return null;
        }
        try {
            return objectMapper.readValue(s, this.deserializedClass);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {

    }
}
