package com.neohope.kk.kkdemo;

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

public class TProducerString implements Callback, Closeable {
	private static Logger logger = LoggerFactory.getLogger(TProducerString.class);
	
	private Properties kafkaProps;
	private KafkaProducer<String, String> producer;
	
	@Override
	public void close() throws IOException {
		if(producer!=null)producer.close();
	}
	
	public TProducerString(String serverPort, String groupId) {
		kafkaProps = new Properties();
		kafkaProps.put("bootstrap.servers",serverPort);
		kafkaProps.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
		kafkaProps.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<String, String>(kafkaProps);
		
	}
	
	public void SendSync() {
		String topic="TCustomer";
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, "id001", "Tom");
		try {
			//如果去掉get，也不会阻塞，但无法获知发送结果
			producer.send(record).get();
			logger.info("msg sent successed!");
		} catch (InterruptedException e) {
			logger.warn(e.getMessage());
		} catch (ExecutionException e) {
			logger.warn(e.getMessage());
		}
	}
	
	public void SendAsync() {
		String topic="TCustomer";
	    ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, "id002", "Jerry");
		producer.send(record,this);
	}
	
	@Override
	public void onCompletion(RecordMetadata metadata, Exception e) {
		if(e!=null)logger.warn(e.getMessage());
		else logger.info("msg sent successed!");
	}
	
    public static void main( String[] args ) throws IOException {
    	TProducerString producer = new TProducerString("localhost:9092", "group001");
    	producer.SendSync();
    	producer.SendAsync();
    	
    	logger.info(">>>>>>TProducer started, press enter to exit");
		System.in.read();
		logger.info(">>>>>>TProducer exited");
		
		producer.close();
    }
}
