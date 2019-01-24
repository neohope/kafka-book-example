package com.neohope.kks.demo.wordcount;

import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 从主题wordcount-input读取消息，用stream进行处理，统计每个词出现的频率
 * 并将统计结果提交到主题wordcount-output
 * @author Hansen
 */
public class StreamWordCount {
	private static Logger logger = LoggerFactory.getLogger(StreamWordCount.class);
	
	public static void main(String[] args) throws Exception{

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream("wordcount-input");
        final Pattern pattern = Pattern.compile("\\W+");
        
        //每行，转为小写，通过空格分解为List
        //映射为map
        //过滤掉the
        //根据key排序
        //计数
        //重新映射为map
        KStream<Object,String> counts  = source.flatMapValues(value-> Arrays.asList(pattern.split(value.toLowerCase())))
                .map((key, value) -> new KeyValue<Object, Object>(value, value))
                .filter((key, value) -> (!value.equals("the")))
                .groupByKey()
                .count()
                .mapValues(value->Long.toString(value))
                .toStream();
        counts.to("wordcount-output");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

    	logger.info(">>>>>>WordCount started, press enter to exit");
		System.in.read();
		logger.info(">>>>>>WordCount exited");
		
        streams.close();

    }
}
