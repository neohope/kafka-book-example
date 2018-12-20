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

import com.neohope.kk.kkdemo.beans.TCustomerSchema;

public class TProducerAvroSchema implements Callback, Closeable {
	private static Logger logger = LoggerFactory.getLogger(TProducerAvroSchema.class);
	
	private Properties kafkaProps;
	private KafkaProducer<Integer, TCustomerSchema> producer;
	
	@Override
	public void close() throws IOException {
		if(producer!=null)producer.close();
	}
	
	public TProducerAvroSchema(String serverPort, String groupId) {
		kafkaProps = new Properties();
		kafkaProps.put("bootstrap.servers",serverPort);
		kafkaProps.put("key.serializer","io.confluent.kafka.serializers.KafkaAvroSerializer");
		kafkaProps.put("value.serializer","io.confluent.kafka.serializers.KafkaAvroSerializer");
		kafkaProps.put("schema.registry.url", "http://localhost:8081");
		producer = new KafkaProducer<Integer, TCustomerSchema>(kafkaProps);
	}
	
	public void SendSync() {
		String topic="TCustomerAvroSchema";
		TCustomerSchema c=new TCustomerSchema();
		c.setId(1);
		c.setName("Tom");
		c.setFaxNumber("");
		ProducerRecord<Integer, TCustomerSchema> record = new ProducerRecord<Integer, TCustomerSchema>(topic, c.getId(), c);
		try {
			producer.send(record).get();
		} catch (InterruptedException e) {
			logger.warn(e.getMessage());
		} catch (ExecutionException e) {
			logger.warn(e.getMessage());
		}
	}
	
	public void SendAsync() {
		String topic="TCustomerAvroSchema";
		TCustomerSchema c=new TCustomerSchema();
		c.setId(2);
		c.setName("Jerry");
		c.setFaxNumber("");
	    ProducerRecord<Integer, TCustomerSchema> record = new ProducerRecord<Integer, TCustomerSchema>(topic, c.getId(), c);
		producer.send(record, this);
	}
	
	@Override
	public void onCompletion(RecordMetadata metadata, Exception e) {
		if(e!=null)logger.warn(e.getMessage());
		else logger.info("msg sent successed!");
	}
	
    public static void main( String[] args ) throws IOException {
    	TProducerAvroSchema producer = new TProducerAvroSchema("localhost:9092", "group003");
    	producer.SendSync();
    	producer.SendAsync();
    	
    	logger.info(">>>>>>TProducer started, press enter to exit");
		System.in.read();
		logger.info(">>>>>>TProducer exited");
		
		producer.close();
    }

}
