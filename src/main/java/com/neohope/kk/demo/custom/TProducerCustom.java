package com.neohope.kk.demo.custom;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.neohope.kk.demo.beans.TCustomer;

/**
 * 通过自定义序列化类传递信息
 * @author Hansen
 */
public class TProducerCustom implements Callback, Closeable {
	private static Logger logger = LoggerFactory.getLogger(TProducerCustom.class);
	
	private Properties kafkaProps;
	private KafkaProducer<Integer, TCustomer> producer;
	
	@Override
	public void close() throws IOException {
		if(producer!=null)producer.close();
	}
	
	public TProducerCustom(String serverPort, String groupId) {
		kafkaProps = new Properties();
		kafkaProps.put("bootstrap.servers",serverPort);
		kafkaProps.put("key.serializer","org.apache.kafka.common.serialization.IntegerSerializer");
		kafkaProps.put("value.serializer","com.neohope.kk.demo.beans.TCustomerSerializer");
		producer = new KafkaProducer<Integer, TCustomer>(kafkaProps);
		
	}
	
	public void SendSync() {
		String topic="TCustomerCustom";
		TCustomer c=new TCustomer(1,"Tom");
		ProducerRecord<Integer, TCustomer> record = new ProducerRecord<Integer, TCustomer>(topic, c.getID(), c);
		try {
			producer.send(record).get();
			logger.info("msg sent successed!");
		} catch (InterruptedException e) {
			logger.warn(e.getMessage());
		} catch (ExecutionException e) {
			logger.warn(e.getMessage());
		}
	}
	
	public void SendAsync() {
		String topic="TCustomerCustom";
		TCustomer c=new TCustomer(2,"Jerry");
	    ProducerRecord<Integer, TCustomer> record = new ProducerRecord<Integer, TCustomer>(topic, c.getID(), c);
		producer.send(record,this);
	}
	
	@Override
	public void onCompletion(RecordMetadata metadata, Exception e) {
		if(e!=null)logger.warn(e.getMessage());
		else logger.info("msg sent successed!");
	}
	
    public static void main( String[] args ) throws IOException {
    	TProducerCustom producer = new TProducerCustom("localhost:9092", "group002");
    	producer.SendSync();
    	producer.SendAsync();
    	
    	logger.info(">>>>>>TProducer started, press enter to exit");
		System.in.read();
		logger.info(">>>>>>TProducer exited");
		
		producer.close();
    }
}
