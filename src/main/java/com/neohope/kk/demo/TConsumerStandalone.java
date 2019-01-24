package com.neohope.kk.demo;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consumer，带ShutdownHook，输出TopicPartition
 * @author Hansen
 */
public class TConsumerStandalone implements Closeable {
	private static Logger logger = LoggerFactory.getLogger(TConsumerStandalone.class);
	
	private Properties kafkaProps;
	private KafkaConsumer<String, String> consumer;
	final Thread mainThread = Thread.currentThread();
	
	@Override
	public void close() throws IOException {
		if(consumer!=null)consumer.close();
	}
	
	public TConsumerStandalone(String serverPort, String groupId) {
		kafkaProps = new Properties();
		kafkaProps.put("bootstrap.servers",serverPort);
		kafkaProps.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		kafkaProps.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		consumer = new KafkaConsumer<String, String>(kafkaProps);
	}
	
	public void AddShutdownHook() {
		 Runtime.getRuntime().addShutdownHook(new Thread() {
		     public void run() {
		         System.out.println("Starting exit...");
		         consumer.wakeup();
		         try {
		             mainThread.join();
		         } catch (InterruptedException e) {
		             e.printStackTrace();
		         }
		     }
		 });
	}

	public void PollSync(String topic) {
		try {
			List<TopicPartition> partitions=new ArrayList<>();
			List<PartitionInfo> infos=consumer.partitionsFor(topic);
			if(infos!=null){
				for(PartitionInfo info:infos){
					partitions.add(new TopicPartition(info.topic(), info.partition()));
				}
			}
			consumer.assign(partitions);
			
			while(true){
				ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
				for(ConsumerRecord<String,String> record:records){
					logger.info(String.format("topic=%s, partition=%s, offset=%d, customerId=%s, customerName=%s",
							record.topic(), record.partition(), record.offset(), record.key(), record.value()));
				}
				
				for (TopicPartition tp: consumer.assignment()){
                    System.out.println("Committing offset at position:" + consumer.position(tp));
				}
				consumer.commitSync();
				Thread.sleep(200);
			}
			
		} catch (InterruptedException e) {
			logger.info(e.getMessage());
		}
	}
	
    public static void main( String[] args ) throws IOException {
    	TConsumerStandalone consumer=new TConsumerStandalone("localhost:9092","group001");
    	consumer.AddShutdownHook();
    	consumer.PollSync("TCustomer");
    	consumer.close();
    }

}
