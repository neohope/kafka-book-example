package com.neohope.kks.demo.stockstats;

import com.neohope.kks.demo.serde.JsonDeserializer;
import com.neohope.kks.demo.serde.JsonSerializer;
import com.neohope.kks.demo.serde.WrapperSerde;
import com.neohope.kks.demo.stockstats.model.Trade;
import com.neohope.kks.demo.stockstats.model.TradeStats;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

/**
 * 读取输入流"stocks"
 * 输出流stockstats-output：每10秒的最小和均值
 * 输出流stockstat-2-trade-aggregates-changelog：每分钟前3最小价格的股票
 * @author Hansen
 */
public class StreamStockStats {
	
	private static Logger logger = LoggerFactory.getLogger(StreamStockStats.class);
	
	public static final String STOCK_TOPIC = "stocks";

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stockstat-2");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, TradeSerde.class.getName());

        // 获取cluster大小，从而得到合适的replication
        AdminClient ac = AdminClient.create(props);
        DescribeClusterResult dcr = ac.describeCluster();
        int clusterSize = dcr.nodes().get().size();
        if(clusterSize<3) props.put("replication.factor",clusterSize);
        else props.put("replication.factor",3);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Trade> source = builder.stream(STOCK_TOPIC);
        KStream<Windowed<String>, TradeStats> stats = source
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofMillis(5000))
                .advanceBy(Duration.ofMillis(1000)))
                .<TradeStats>aggregate(() -> new TradeStats(),
                		(k, v, tradestats) -> tradestats.add(v),
		                Materialized.<String, TradeStats, WindowStore<Bytes, byte[]>>as("trade-aggregates")
		                .withValueSerde(new TradeStatsSerde()))
                .toStream()
                .mapValues((trade) -> trade.computeAvgPrice());

        stats.to("stockstats-output", Produced.keySerde(WindowedSerdes.timeWindowedSerdeFrom(String.class)));

        Topology topology = builder.build();
        System.out.println(topology.describe());
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();
        
    	logger.info(">>>>>>ClickstreamEnrichment started, press enter to exit");
		System.in.read();
		logger.info(">>>>>>ClickstreamEnrichment exited");

        streams.close();
    }

    static public final class TradeSerde extends WrapperSerde<Trade> {
        public TradeSerde() {
            super(new JsonSerializer<Trade>(), new JsonDeserializer<Trade>(Trade.class));
        }
    }

    static public final class TradeStatsSerde extends WrapperSerde<TradeStats> {
        public TradeStatsSerde() {
            super(new JsonSerializer<TradeStats>(), new JsonDeserializer<TradeStats>(TradeStats.class));
        }
    }
}
