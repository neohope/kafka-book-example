package com.neohope.kk.demo;

import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 分区管理器
 * @author Hansen
 */
public class TPartition implements Partitioner {
	private static Logger logger = LoggerFactory.getLogger(TPartition.class);

	@Override
	public void configure(Map<String, ?> configs) {
	}

	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		int numPartitions = cluster.partitionCountForTopic(topic);
		
		if(keyBytes == null || !(key instanceof String)){
			logger.error("message key is not correctly seted");
			throw new InvalidRecordException("message key is not correctly seted");
		}
		
		if(numPartitions==1)return 1;
		
		//对于vip，提供特殊分区
		//相关设置应该在configure方法进行
		if(((String)key).equals("our_vip_producer")){
			return numPartitions;
		} else {
			return (Math.abs(Utils.murmur2(keyBytes))%(numPartitions-1));
		}
	}

	@Override
	public void close() {
	}
	
}
