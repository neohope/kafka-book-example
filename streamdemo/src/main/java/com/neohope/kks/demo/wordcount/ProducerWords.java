package com.neohope.kks.demo.wordcount;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 将分析的文档，按行进行拆分，提交到主题 wordcount-input
 * @author Hansen
 */
public class ProducerWords implements Closeable {
	private static Logger logger = LoggerFactory.getLogger(ProducerWords.class);
	
	private Properties kafkaProps;
	private KafkaProducer<String, String> producer;
	
	@Override
	public void close() throws IOException {
		if(producer!=null)producer.close();
	}
	
	public ProducerWords(String serverPort, String groupId) {
		kafkaProps = new Properties();
		kafkaProps.put("bootstrap.servers",serverPort);
		kafkaProps.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
		kafkaProps.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<String, String>(kafkaProps);
	}
	
	public void SendFile(){
		int lineno=0;
		BufferedReader br=null;
		try {
			br=new BufferedReader(new FileReader("src/main/resources/input.txt"));
			String line = br.readLine();
			while (line != null) {
				SendSync(lineno++,line);
				line = br.readLine();
			}
		} catch (FileNotFoundException e) {
			logger.warn(e.getMessage());
		} catch (IOException e) {
			logger.warn(e.getMessage());
		}
		finally{
			if(br!=null)
				try {
					br.close();
				} catch (IOException e) {
					logger.warn(e.getMessage());
				}
		}
	}
	
	public void SendSync(Integer lineid, String line) {
		String topic="wordcount-input";

		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, "id"+lineid, line);
		try {
			producer.send(record).get();
			logger.info("msg sent successed!");
		} catch (InterruptedException e) {
			logger.warn(e.getMessage());
		} catch (ExecutionException e) {
			logger.warn(e.getMessage());
		}
	}
	
    public static void main( String[] args ) throws IOException {    	
    	ProducerWords producer = new ProducerWords("localhost:9092", "group001");
    	producer.SendFile();
    	
    	logger.info(">>>>>>TProducer started, press enter to exit");
		System.in.read();
		logger.info(">>>>>>TProducer exited");
		
		producer.close();
    }
}
