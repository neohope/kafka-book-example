package com.neohope.kk.kkdemo;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.neohope.kk.kkdemo.beans.TCustomer;

public class TConsumerCustom implements Callback, Closeable {
	private static Logger logger = LoggerFactory.getLogger(TConsumerCustom.class);
	
	private Properties kafkaProps;
	private KafkaConsumer<Integer, TCustomer> consumer;
	
	@Override
	public void close() throws IOException {
		if(consumer!=null)consumer.close();
	}
	
	public TConsumerCustom(String serverPort, String groupId) {
		kafkaProps = new Properties();
		kafkaProps.put("bootstrap.servers",serverPort);
		kafkaProps.put("group.id", groupId);
		kafkaProps.put("key.deserializer","org.apache.kafka.common.serialization.IntegerDeserializer");
		kafkaProps.put("value.deserializer","com.neohope.kk.kkdemo.beans.TCustomerSerializer");
		consumer = new KafkaConsumer<Integer, TCustomer>(kafkaProps);
	}
	
	public void PollSync(List<String> topics) {
		try {
            consumer.subscribe(topics);
			
			while(true){
				ConsumerRecords<Integer, TCustomer> records = consumer.poll(Duration.ofMillis(100));
				for(ConsumerRecord<Integer, TCustomer> record:records){
					logger.info(String.format("topic=%s, partition=%s, offset=%d, customerId=%s, customerName=%s",
							record.topic(), record.partition(), record.offset(), record.key(), record.value().getName()));
				}
				
				Thread.sleep(200);
			}
		} catch (InterruptedException e) {
			logger.info(e.getMessage());
		}
	}
	
	@Override
	public void onCompletion(RecordMetadata metadata, Exception e) {
		if(e!=null)logger.warn(e.getMessage());
	}
	
    public static void main( String[] args ) throws IOException {
    	TConsumerCustom consumer=new TConsumerCustom("localhost:9092","group003");
    	
		List<String> topics=new ArrayList<>();
		topics.add("TCustomerCustom");
    	consumer.PollSync(topics);
    	
    	consumer.close();
    }

}
