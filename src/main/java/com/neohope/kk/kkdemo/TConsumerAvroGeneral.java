package com.neohope.kk.kkdemo;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TConsumerAvroGeneral implements Callback, Closeable {
	private static Logger logger = LoggerFactory.getLogger(TConsumerAvroGeneral.class);
	
	private Properties kafkaProps;
	private KafkaConsumer<Object, Object> consumer;
	
	@Override
	public void close() throws IOException {
		if(consumer!=null)consumer.close();
	}
	
	public TConsumerAvroGeneral(String serverPort, String groupId) {
		kafkaProps = new Properties();
		kafkaProps.put("bootstrap.servers",serverPort);
		kafkaProps.put("group.id", groupId);
		kafkaProps.put("key.deserializer","io.confluent.kafka.serializers.KafkaAvroDeserializer");
		kafkaProps.put("value.deserializer","io.confluent.kafka.serializers.KafkaAvroDeserializer");
		kafkaProps.put("schema.registry.url", "http://localhost:8081");
		consumer = new KafkaConsumer<Object, Object>(kafkaProps);
	}
	
	public void PollSync(List<String> topics) {
		try {
			consumer.subscribe(topics);
			
			while(true){
				ConsumerRecords<Object, Object> records = consumer.poll(Duration.ofMillis(100));
				for(ConsumerRecord<Object, Object> record:records){
					logger.info(String.format("topic=%s, partition=%s, offset=%d, customerId=%s, customerName=%s",
							record.topic(), record.partition(), record.offset(), record.key(), ((GenericRecord)record.value()).get("name")));
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
		else logger.info("msg sent successed!");
	}
	
    public static void main( String[] args ) throws IOException {
    	TConsumerAvroGeneral consumer=new TConsumerAvroGeneral("localhost:9092","group004");
    	
		List<String> topics=new ArrayList<>();
		topics.add("TCustomerAvroGeneral");
    	consumer.PollSync(topics);
    	
    	consumer.close();
    }

}
