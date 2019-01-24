package com.neohope.kks.demo.clickstreamenrich;

import com.google.gson.Gson;
import com.neohope.kks.demo.clickstreamenrich.model.PageView;
import com.neohope.kks.demo.clickstreamenrich.model.Search;
import com.neohope.kks.demo.clickstreamenrich.model.UserProfile;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 生成模拟数据，包括用户信息，搜索和点击
 */
public class ProducerClick {
	
    public static final String USER_PROFILE_TOPIC = "clicks.user.profile";
    public static final String PAGE_VIEW_TOPIC = "clicks.pages.views";
    public static final String SEARCH_TOPIC = "clicks.search";
    public static final String USER_ACTIVITY_TOPIC = "clicks.user.activity";

    public static KafkaProducer<Integer, String> producer = null;

    public static void main(String[] args) throws Exception {

        System.out.println("Start generating data");

        // 构建消息
        List<ProducerRecord<Integer, String>> records = new ArrayList<>();
        Gson gson = new Gson();

        // 两个用户
        String[] interests1 = {"Surfing", "Hiking"};
        UserProfile user1 = new UserProfile(1, "Mathias", "94301", interests1 );

        String[] interests2 = {"Ski", "Dancing"};
        UserProfile user2 = new UserProfile(2, "Anna", "94302", interests2);

        records.add(new ProducerRecord<Integer, String>(USER_PROFILE_TOPIC, user1.getUserID(), gson.toJson(user1)));
        records.add(new ProducerRecord<Integer, String>(USER_PROFILE_TOPIC, user2.getUserID(), gson.toJson(user2)));

        // 更新用户信息
        String[] newInterests = {"Ski", "stream processing"};
        records.add(new ProducerRecord<Integer, String>(USER_PROFILE_TOPIC, user2.getUserID(), gson.toJson(user2.update("94303", newInterests))));

        // 两次搜索
        Search search1 = new Search(1, "retro wetsuit");
        Search search2 = new Search(2, "light jacket");

        records.add(new ProducerRecord<Integer, String>(SEARCH_TOPIC, search1.getUserID(), gson.toJson(search1)));
        records.add(new ProducerRecord<Integer, String>(SEARCH_TOPIC, search2.getUserID(), gson.toJson(search2)));

        // 三次点击
        PageView view1 = new PageView(1, "collections/mens-wetsuits/products/w3-worlds-warmest-wetsuit");
        PageView view2 = new PageView(2, "product/womens-dirt-craft-bike-mountain-biking-jacket");
        PageView view3 = new PageView(2, "/product/womens-ultralight-down-jacket");

        records.add(new ProducerRecord<Integer, String>(PAGE_VIEW_TOPIC, view1.getUserID(), gson.toJson(view1)));
        records.add(new ProducerRecord<Integer, String>(PAGE_VIEW_TOPIC, view2.getUserID(), gson.toJson(view2)));
        records.add(new ProducerRecord<Integer, String>(PAGE_VIEW_TOPIC, view3.getUserID(), gson.toJson(view3)));

        //发送消息
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<Integer, String>(props);

        for (ProducerRecord<Integer, String> record: records)
            producer.send(record, (RecordMetadata r, Exception e) -> {
                if (e != null) {
                    System.out.println("Error producing to topic " + r.topic());
                    e.printStackTrace();
                }
            });
        records.clear();
        
        // Sleep一段时间，成为两个Session
        Thread.sleep(5000);

        // 一次搜索
        Search search3 = new Search(2, "carbon ski boots");
        records.add(new ProducerRecord<Integer, String>(SEARCH_TOPIC, search3.getUserID(), gson.toJson(search3)));

        // 两次点击
        PageView view4 = new PageView(2, "product/salomon-quest-access-custom-heat-ski-boots-womens");
        PageView view5 = new PageView(2, "product/nordica-nxt-75-ski-boots-womens");

        records.add(new ProducerRecord<Integer, String>(PAGE_VIEW_TOPIC, view4.getUserID(), gson.toJson(view4)));
        records.add(new ProducerRecord<Integer, String>(PAGE_VIEW_TOPIC, view5.getUserID(), gson.toJson(view5)));

        // 一次未知用户点击
        PageView view6 = new PageView(-1, "product/osprey-atmos-65-ag-pack");
        records.add(new ProducerRecord<Integer, String>(PAGE_VIEW_TOPIC, view6.getUserID(), gson.toJson(view6)));

        // 发送消息
        for (ProducerRecord<Integer, String> record: records)
            producer.send(record, (RecordMetadata r, Exception e) -> {
                if (e != null) {
                    System.out.println("Error producing to topic " + r.topic());
                    e.printStackTrace();
                }
            });

        producer.close();
    }
}
