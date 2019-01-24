package com.neohope.kks.demo.avg;

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
 * 从主题ks_avg_prices读取平均值，并输出到日志
 * @author Hansen
 */
public class ConsumerAvg implements Closeable {
	private static Logger logger = LoggerFactory.getLogger(ConsumerAvg.class);
	
	private Properties kafkaProps;
	private KafkaConsumer<String, Double> consumer;
	
	@Override
	public void close() throws IOException {
		if(consumer!=null)consumer.close();
	}
	
	public ConsumerAvg(String serverPort, String groupId) {
		kafkaProps = new Properties();
		kafkaProps.put("bootstrap.servers",serverPort);
		kafkaProps.put("group.id", groupId);
		kafkaProps.put("key.deserializer","com.neohope.kks.demo.avg.model.WindowedStringDeserializer");
		kafkaProps.put("value.deserializer","org.apache.kafka.common.serialization.DoubleDeserializer");
		consumer = new KafkaConsumer<String, Double>(kafkaProps);
	}
	
	public void PollSync(List<String> topics) {
		try {
			consumer.subscribe(topics);
			
			while(true){
				ConsumerRecords<String, Double> records = consumer.poll(Duration.ofMillis(100));
				for(ConsumerRecord<String, Double> record:records){
					logger.info(String.format("topic=%s, partition=%s, offset=%d, id=%s, avg=%s",
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
    	ConsumerAvg consumer=new ConsumerAvg("localhost:9092","group001");
    	
		List<String> topics=new ArrayList<>();
		topics.add("ks_avg_prices");
    	consumer.PollSync(topics);
    	
    	consumer.close();
    }

}
