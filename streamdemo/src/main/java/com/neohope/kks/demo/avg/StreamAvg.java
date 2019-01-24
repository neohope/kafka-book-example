package com.neohope.kks.demo.avg;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.neohope.kks.demo.avg.model.AvgValue;
import com.neohope.kks.demo.serde.JsonDeserializer;
import com.neohope.kks.demo.serde.JsonSerializer;
import com.neohope.kks.demo.serde.WrapperSerde;

import java.time.Duration;
import java.util.Properties;

/**
 * 从主题ks_prices读取价格信息，按key求平均值，并输出到ks_avg_prices
 * @author Hansen
 */
public class StreamAvg {
	private static Logger logger = LoggerFactory.getLogger(StreamAvg.class);

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ks_prices");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Integer> namedPrices = builder.stream("ks_prices");
        
        Initializer<AvgValue> initializer = new Initializer<AvgValue>() {
			@Override
			public AvgValue apply() {
				return new AvgValue(0,0);
			}
		};
		
		Aggregator<String, Integer, AvgValue> aggregator = new Aggregator<String, Integer, AvgValue>() {
			@Override
			public AvgValue apply(String aggKey, Integer value, AvgValue aggregate) {
				return new AvgValue(aggregate.count + 1, aggregate.sum + value );
			}
		};
		
		Materialized<String, AvgValue, WindowStore<Bytes, byte[]>> materialized = Materialized.<String, AvgValue, WindowStore<Bytes, byte[]>>as("trade-aggregates")
        .withValueSerde(new AvgValueSerde());
        
        KStream<Windowed<String>, Double> avgStream = namedPrices
        		.groupByKey()
        		.windowedBy(TimeWindows.of(Duration.ofMillis(5000)))
        		.<AvgValue>aggregate(initializer, aggregator, materialized)
        		.toStream()
        		.<Double>mapValues((v) -> ((double)v.sum/v.count));
        
        avgStream.to("ks_avg_prices", Produced.with(WindowedSerdes.timeWindowedSerdeFrom(String.class), Serdes.Double()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        
    	logger.info(">>>>>>StreamAvg started, press enter to exit");
		System.in.read();
		logger.info(">>>>>>StreamAvg exited");

        streams.close();
    }
    
    static public final class AvgValueSerde extends WrapperSerde<AvgValue> {
        public AvgValueSerde() {
            super(new JsonSerializer<AvgValue>(), new JsonDeserializer<AvgValue>(AvgValue.class));
        }
    }
}
