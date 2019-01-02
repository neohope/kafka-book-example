package com.neohope.kk.kkdemo;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TRebalance implements Closeable, ConsumerRebalanceListener {
	
	private static Logger logger = LoggerFactory.getLogger(TRebalance.class);
	
	private Properties kafkaProps;
	private KafkaConsumer<String, String> consumer;
	
	@Override
	public void close() throws IOException {
		if(consumer!=null)consumer.close();
	}
	
	public TRebalance(String serverPort, String groupId) {
		kafkaProps = new Properties();
		kafkaProps.put("bootstrap.servers",serverPort);
		kafkaProps.put("group.id", groupId);
		kafkaProps.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		kafkaProps.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		kafkaProps.put("enable.auto.commit", "false");
		consumer = new KafkaConsumer<String, String>(kafkaProps);
	}

	//停止读取消息之后，reblance之前
	@Override
	public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
		
	}

	//reblance之后，开始读取消息之前
	@Override
	public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
		for(TopicPartition partition: partitions){
			consumer.seek(partition, getOffsetFromDB(partition));
		}
	}
	
	private long getOffsetFromDB(TopicPartition partition){
		return 0l;
	}
	
	private void saveOffsetToDB(String topic,int partition, long offset){
	}
	
	public void PollSync(List<String> topics) {
		Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
		
		try {
			consumer.subscribe(topics, this);
			consumer.poll(Duration.ofMillis(0));
			
			for(TopicPartition partition: consumer.assignment()){
				consumer.seek(partition, getOffsetFromDB(partition));
			}
			
			while(true){
				ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
				for(ConsumerRecord<String,String> record:records){
					logger.info(String.format("topic=%s, partition=%s, offset=%d, customerId=%s, customerName=%s",
							record.topic(), record.partition(), record.offset(), record.key(), record.value()));
					saveOffsetToDB(record.topic(), record.partition(), record.offset());
					currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset()+1, "no metadata"));
				}
				
				consumer.commitAsync(currentOffsets, new OffsetCommitCallback(){
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
    	TRebalance consumer=new TRebalance("localhost:9092","group001");
    	
		List<String> topics=new ArrayList<>();
		topics.add("TCustomer");
    	consumer.PollSync(topics);
    	
    	consumer.close();
    }

}
