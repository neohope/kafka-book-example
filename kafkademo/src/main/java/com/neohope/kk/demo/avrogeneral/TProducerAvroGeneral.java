package com.neohope.kk.demo.avrogeneral;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 通过Avro GenericRecord序列化传递信息
 * @author Hansen
 */
public class TProducerAvroGeneral implements Callback, Closeable {
	private static Logger logger = LoggerFactory.getLogger(TProducerAvroGeneral.class);

	private Properties kafkaProps;
	private KafkaProducer<Object, Object> producer;

	@Override
	public void close() throws IOException {
		if (producer != null)
			producer.close();
	}

	public TProducerAvroGeneral(String serverPort, String groupId) {
		kafkaProps = new Properties();
		kafkaProps.put("bootstrap.servers", serverPort);
		kafkaProps.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
		kafkaProps.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
		kafkaProps.put("schema.registry.url", "http://localhost:8081");
		producer = new KafkaProducer<Object, Object>(kafkaProps);
	}

	public void SendSync() {
		String topic = "TCustomerAvroGeneral";

		String tcustomerSchema = "{\"type\":\"record\",\"name\":\"TCustomer\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"}]}";
		Schema.Parser parser = new Schema.Parser();
		Schema schema = parser.parse(tcustomerSchema);
		GenericRecord avroRecord = new GenericData.Record(schema);
		avroRecord.put("id", 1);
		avroRecord.put("name", "Tom");

		ProducerRecord<Object, Object> record = new ProducerRecord<Object, Object>(topic, avroRecord.get("id"),
				avroRecord);
		try {
			producer.send(record).get();
		} catch (InterruptedException e) {
			logger.warn(e.getMessage());
		} catch (ExecutionException e) {
			logger.warn(e.getMessage());
		}
	}

	public void SendAsync() {
		String topic = "TCustomerAvroGeneral";

		String tcustomerSchema = "{\"type\":\"record\",\"name\":\"TCustomer\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"}]}";
		Schema.Parser parser = new Schema.Parser();
		Schema schema = parser.parse(tcustomerSchema);
		GenericRecord avroRecord = new GenericData.Record(schema);
		avroRecord.put("id", 2);
		avroRecord.put("name", "Jerry");

		ProducerRecord<Object, Object> record = new ProducerRecord<Object, Object>(topic, avroRecord.get("id"),
				avroRecord);
		producer.send(record, this);
	}

	@Override
	public void onCompletion(RecordMetadata metadata, Exception e) {
		if (e != null)
			logger.warn(e.getMessage());
		else
			logger.info("msg sent successed!");
	}

	public static void main(String[] args) throws IOException {
		TProducerAvroGeneral producer = new TProducerAvroGeneral("localhost:9092", "group004");
		producer.SendSync();
		producer.SendAsync();

		logger.info(">>>>>>TProducer started, press enter to exit");
		System.in.read();
		logger.info(">>>>>>TProducer exited");

		producer.close();
	}

}
