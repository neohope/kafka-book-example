package com.neohope.kks.demo.wordcount;

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

/**
 * 从主题wordcount-output读取分析结果，并输出到日志
 * @author Hansen
 */
public class ConsumerResult implements Closeable {
	private static Logger logger = LoggerFactory.getLogger(ConsumerResult.class);
	
	private Properties kafkaProps;
	private KafkaConsumer<String, String> consumer;
	
	@Override
	public void close() throws IOException {
		if(consumer!=null)consumer.close();
	}
	
	public ConsumerResult(String serverPort, String groupId) {
		kafkaProps = new Properties();
		kafkaProps.put("bootstrap.servers",serverPort);
		kafkaProps.put("group.id", groupId);
		kafkaProps.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		kafkaProps.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		consumer = new KafkaConsumer<String, String>(kafkaProps);
	}
	
	public void PollSync(List<String> topics) {
		try {
			consumer.subscribe(topics);
			
			while(true){
				ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
				for(ConsumerRecord<String,String> record:records){
					logger.info(String.format("topic=%s, partition=%s, offset=%d, word=%s, count=%s",
							record.topic(), record.partition(), record.offset(), record.key(), record.value()));
				}
				consumer.commitSync();
				
				Thread.sleep(200);
			}
		} catch (InterruptedException e) {
			logger.info(e.getMessage());
		}
	}
	
    public static void main( String[] args ) throws IOException {
    	ConsumerResult consumer=new ConsumerResult("localhost:9092","group001");
    	
		List<String> topics=new ArrayList<>();
		topics.add("wordcount-output");
    	consumer.PollSync(topics);
    	
    	consumer.close();
    }

}
