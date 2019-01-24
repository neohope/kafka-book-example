package com.neohope.kk.demo;

import kafka.consumer.BaseConsumerRecord;
import kafka.tools.MirrorMaker;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Collections;
import java.util.List;

/**
 * 两个数据中心进行镜像时，增加主题前缀
 * @author Hansen
 */
@SuppressWarnings("deprecation")
public class TopicSwitchingHandler implements MirrorMaker.MirrorMakerMessageHandler {

    private final String topicPrefix;

    public TopicSwitchingHandler(String topicPrefix) {
        this.topicPrefix = topicPrefix;
    }

	@Override
	public List<ProducerRecord<byte[], byte[]>> handle(BaseConsumerRecord record) {
		return Collections.singletonList(new ProducerRecord<byte[], byte[]>(topicPrefix + "." + record.topic(), record.partition(), record.key(), record.value()));
	}
}
