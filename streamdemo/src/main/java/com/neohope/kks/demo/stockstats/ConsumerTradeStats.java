package com.neohope.kks.demo.stockstats;

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

import com.neohope.kks.demo.stockstats.model.TradeStats;

/**
 * 从主题stockstats-output读取分析结果，并输出到日志
 * @author Hansen
 */
public class ConsumerTradeStats implements Closeable {
	private static Logger logger = LoggerFactory.getLogger(ConsumerTradeStats.class);
	
	private Properties kafkaProps;
	private KafkaConsumer<String, TradeStats> consumer;
	
	@Override
	public void close() throws IOException {
		if(consumer!=null)consumer.close();
	}
	
	public ConsumerTradeStats(String serverPort, String groupId) {	
		kafkaProps = new Properties();
		kafkaProps.put("bootstrap.servers",serverPort);
		kafkaProps.put("group.id", groupId);
		kafkaProps.put("key.deserializer","com.neohope.kks.demo.stockstats.model.WindowedStringDeserializer");
		kafkaProps.put("value.deserializer","com.neohope.kks.demo.stockstats.model.TradeStatsDeserializer");
		consumer = new KafkaConsumer<String, TradeStats>(kafkaProps);
	}

	public void PollSync(List<String> topics) {
		try {
			consumer.subscribe(topics);
			
			while(true){
				ConsumerRecords<String, TradeStats> records = consumer.poll(Duration.ofMillis(100));
				for(ConsumerRecord<String, TradeStats> record:records){
					TradeStats tradeStats = record.value();
					logger.info(String.format("topic=%s, partition=%s, offset=%d, stock=%s, avg=%s",
							record.topic(), record.partition(), record.offset(), record.key(), tradeStats.avgPrice));
				}
				consumer.commitSync();
				
				Thread.sleep(200);
			}
		} catch (InterruptedException e) {
			logger.info(e.getMessage());
		}
	}
	
    public static void main( String[] args ) throws IOException {
    	ConsumerTradeStats consumer=new ConsumerTradeStats("localhost:9092", "group001");
    	
		List<String> topics=new ArrayList<>();
		topics.add("stockstats-output");
    	consumer.PollSync(topics);
    	
    	consumer.close();
    }
}
