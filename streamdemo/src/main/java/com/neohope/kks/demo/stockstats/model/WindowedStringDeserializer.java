package com.neohope.kks.demo.stockstats.model;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.UnlimitedWindow;

/**
 * WindowedStringDeserializer
 * @author Hansen
 */
public class WindowedStringDeserializer implements Deserializer<Windowed<String>>{
	
	private static final int TIMESTAMP_SIZE = 8;
    private StringDeserializer keyDeserializer = new StringDeserializer();
    
    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }

    @Override
    public Windowed<String> deserialize(final String topic, final byte[] data) {
        byte[] bytes = new byte[data.length - TIMESTAMP_SIZE];
        System.arraycopy(data, 0, bytes, 0, bytes.length);
        long start = ByteBuffer.wrap(data).getLong(data.length - TIMESTAMP_SIZE);
        return new Windowed<>(keyDeserializer.deserialize(topic, bytes), new UnlimitedWindow(start));
    }

    @Override
    public void close() {
    }
}
