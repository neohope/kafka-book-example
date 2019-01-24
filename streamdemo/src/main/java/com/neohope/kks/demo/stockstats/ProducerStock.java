package com.neohope.kks.demo.stockstats;

import com.neohope.kks.demo.stockstats.model.Trade;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

/**
 * 生成虚拟交易数据
 * @author Hansen
 */
public class ProducerStock {
	public static final String[] TICKERS = {"MMM", "ABT", "ABBV", "ACN", "ATVI", "AYI", "ADBE", "AAP", "AES", "AET"};
	public static final int MAX_PRICE_CHANGE = 5;
	public static final int START_PRICE = 5000;
	public static final String STOCK_TOPIC = "stocks";
	public static final int DELAY = 100;

    public static void main(String[] args) throws Exception {
        System.out.println("Start generating data");

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "com.neohope.kks.demo.stockstats.model.TradeSerializer");
        KafkaProducer<String, Trade> producer = new KafkaProducer<>(props);

        Map<String, Integer> prices = new HashMap<>();
        for (String ticker : TICKERS){
            prices.put(ticker, START_PRICE);
        }

        Random random = new Random();
        for(int i=0; i<1000; i++) {
            for (String ticker : TICKERS) {
            	
            	// 构建Trade
                double log = random.nextGaussian() * 0.25 + 1;
                int size = random.nextInt(100);
                int price = prices.get(ticker);
                if (i % 10 == 0) {
                    price = price + random.nextInt(MAX_PRICE_CHANGE * 2) - MAX_PRICE_CHANGE;
                    prices.put(ticker, price);
                }
                Trade trade = new Trade("ASK",ticker,(price+log),size);
                
                // 发送Trade
                ProducerRecord<String, Trade> record = new ProducerRecord<>(STOCK_TOPIC, ticker, trade);
                producer.send(record, (RecordMetadata r, Exception e) -> {
                    if (e != null) {
                        System.out.println("Error producing events");
                        e.printStackTrace();
                    }
                });

                Thread.sleep(DELAY);
            }
        }
        
        producer.close();
    }
}
