package com.neohope.kks.demo.clickstreamenrich.model;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * JsonDeserializer
 * @author Hansen
 */
public class UserActitityDeserializer implements Deserializer<UserActivity> {

    private Gson gson = new Gson();
    
    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }
    
    @Override
    public void close() {
    }

    @Override
    public UserActivity deserialize(String s, byte[] bytes) {
        if(bytes == null){
            return null;
        }
        return gson.fromJson(new String(bytes), UserActivity.class);
    }
}