package com.neohope.kk.kkdemo;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TConsumerOffset implements Closeable {
	private static Logger logger = LoggerFactory.getLogger(TConsumerOffset.class);
	
	private Properties kafkaProps;
	private KafkaConsumer<String, String> consumer;
	
	@Override
	public void close() throws IOException {
		if(consumer!=null)consumer.close();
	}
	
	public TConsumerOffset(String serverPort, String groupId) {
		kafkaProps = new Properties();
		kafkaProps.put("bootstrap.servers",serverPort);
		kafkaProps.put("group.id", groupId);
		kafkaProps.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		kafkaProps.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		kafkaProps.put("enable.auto.commit", "false");
		consumer = new KafkaConsumer<String, String>(kafkaProps);
	}
	
	public void PollSync(List<String> topics) {
		Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
		int count=0;
		
		try {
			consumer.subscribe(topics);
			
			while(true){
				ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
				for(ConsumerRecord<String,String> record:records){
					logger.info(String.format("topic=%s, partition=%s, offset=%d, customerId=%s, customerName=%s",
							record.topic(), record.partition(), record.offset(), record.key(), record.value()));
					
					currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset()+1, "no metadata"));
					
					if(count%100==0){
						consumer.commitAsync(currentOffsets, null);
					}
				}
				consumer.commitAsync(new OffsetCommitCallback(){
					public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception e){
						if(e!=null)logger.error(e.getMessage());
					}
				});
				
				Thread.sleep(200);
			}
		} catch (InterruptedException e) {
			logger.info(e.getMessage());
		}
		finally{
			consumer.commitSync(currentOffsets);
		}
	}
	
    public static void main( String[] args ) throws IOException {
    	TConsumerOffset consumer=new TConsumerOffset("localhost:9092","group001");
    	
		List<String> topics=new ArrayList<>();
		topics.add("TCustomer");
    	consumer.PollSync(topics);
    	
    	consumer.close();
    }
}
