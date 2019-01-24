package com.neohope.kks.demo.avg;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 产生随机价格信息
 * @author Hansen
 */
public class ProducerPrice implements Closeable {
	private static Logger logger = LoggerFactory.getLogger(ProducerPrice.class);
	public static final String[] TICKERS = {"MMM", "ABT", "ABBV"};
	
	private Properties kafkaProps;
	private KafkaProducer<String, Integer> producer;
	
	@Override
	public void close() throws IOException {
		if(producer!=null)producer.close();
	}
	
	public ProducerPrice(String serverPort, String groupId) {
		kafkaProps = new Properties();
		kafkaProps.put("bootstrap.servers",serverPort);
		kafkaProps.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
		kafkaProps.put("value.serializer","org.apache.kafka.common.serialization.IntegerSerializer");
		producer = new KafkaProducer<String, Integer>(kafkaProps);
		
	}
	
	public void SendSync() throws InterruptedException {
		 Random random = new Random();
        for(int i=0; i<1000; i++) {
            for (String ticker : TICKERS) {
                ProducerRecord<String, Integer> record = new ProducerRecord<>("ks_prices", ticker, random.nextInt());
                producer.send(record, (RecordMetadata r, Exception e) -> {
                    if (e != null) {
                        System.out.println("Error producing events");
                        e.printStackTrace();
                    }
                });

                Thread.sleep(100);
            }
        }
	}

    public static void main( String[] args ) throws IOException, InterruptedException {
    	ProducerPrice producer = new ProducerPrice("localhost:9092", "group002");
    	producer.SendSync();
    	
    	logger.info(">>>>>>TProducer started, press enter to exit");
		System.in.read();
		logger.info(">>>>>>TProducer exited");
		
		producer.close();
    }
}
