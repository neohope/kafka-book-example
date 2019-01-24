package com.neohope.kks.demo.stockstats.model;

import java.nio.charset.Charset;
import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

import com.google.gson.Gson;

/**
 * 自定义序列化工具类
 * @author Hansen
 */
public class TradeSerializer implements Serializer<Trade> {
	
	private Gson gson = new Gson();

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
	}
	
	@Override
	public void close() {
	}

	@Override
	public byte[] serialize(String topic, Trade data) {
		return gson.toJson(data).getBytes(Charset.forName("UTF-8"));
	}
}
