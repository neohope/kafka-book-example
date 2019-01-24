package com.neohope.kks.demo.stockstats.model;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * TradeStatsDeserializer
 * @author Hansen
 */
public class TradeStatsDeserializer implements Deserializer<TradeStats> {

    private Gson gson = new Gson();
    
    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }
    
    @Override
    public void close() {
    }

    @Override
    public TradeStats deserialize(String s, byte[] bytes) {
        if(bytes == null){
            return null;
        }
        return gson.fromJson(new String(bytes), TradeStats.class);
    }
}