package com.neohope.kks.demo.clickstreamenrich;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.neohope.kks.demo.clickstreamenrich.model.UserActivity;

/**
 * 从主题wordcount-output读取分析结果，并输出到日志
 * @author Hansen
 */
public class ConsumerGson implements Closeable {
	private static Logger logger = LoggerFactory.getLogger(ConsumerGson.class);
	
	private Properties kafkaProps;
	private KafkaConsumer<Integer,UserActivity> consumer;
	
	@Override
	public void close() throws IOException {
		if(consumer!=null)consumer.close();
	}
	
	public ConsumerGson(String serverPort, String groupId) {
		kafkaProps = new Properties();
		kafkaProps.put("bootstrap.servers",serverPort);
		kafkaProps.put("group.id", groupId);
		kafkaProps.put("key.deserializer","org.apache.kafka.common.serialization.IntegerDeserializer");
		kafkaProps.put("value.deserializer","com.neohope.kks.demo.clickstreamenrich.model.UserActitityDeserializer");
		consumer = new KafkaConsumer<Integer,UserActivity>(kafkaProps);
	}
	
	public void PollSync(List<String> topics) {
		try {
			consumer.subscribe(topics);
			
			while(true){
				ConsumerRecords<Integer,UserActivity> records = consumer.poll(Duration.ofMillis(100));
				for(ConsumerRecord<Integer,UserActivity> record:records){
					UserActivity userActivity = record.value();
					logger.info(String.format("topic=%s, partition=%s, offset=%d, uid=%s, terms=%s",
							record.topic(), record.partition(), record.offset(), record.key(), userActivity.searchTerm));
				}
				consumer.commitSync();
				
				Thread.sleep(200);
			}
		} catch (InterruptedException e) {
			logger.info(e.getMessage());
		}
	}
	
    public static void main( String[] args ) throws IOException {
    	ConsumerGson consumer=new ConsumerGson("localhost:9092","group001");
    	
		List<String> topics=new ArrayList<>();
		topics.add("clicks.user.activity");
    	consumer.PollSync(topics);
    	
    	consumer.close();
    }

}
