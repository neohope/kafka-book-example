package com.neohope.kk.kkdemo.beans;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TCustomerSerializer implements Serializer<TCustomer>, Deserializer<TCustomer>{
	
	private static Logger logger = LoggerFactory.getLogger(TCustomerSerializer.class);

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
	}
	
	@Override
	public void close() {
	}

	@Override
	public byte[] serialize(String topic, TCustomer data) {
		if(data==null)return null;
		
		byte[] serializedName;
		int nameSize;
		if(data.getName()!=null){
			try {
				serializedName=data.getName().getBytes("UTF-8");
				nameSize=serializedName.length;
			} catch (UnsupportedEncodingException e) {
				logger.error(e.getMessage());
				serializedName=new byte[0];
				nameSize=0;
			}
		}
		else{
			serializedName=new byte[0];
			nameSize=0;
		}
		
		ByteBuffer buff=ByteBuffer.allocate(4+4+nameSize);
		buff.putInt(data.getID());
		buff.putInt(nameSize);
		buff.put(serializedName);
		return buff.array();
	}
	
	@Override
	public TCustomer deserialize(String topic, byte[] data) {
		if(data==null)return null;
		ByteBuffer buff=ByteBuffer.wrap(data);
		int ID=buff.getInt();
		int nameSize=buff.getInt();
		byte[] serializedName=new byte[nameSize];
		buff.get(serializedName);
		String name=new String(serializedName, StandardCharsets.UTF_8);
		return new TCustomer(ID,name);
	}

}
