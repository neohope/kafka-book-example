package com.neohope.kks.demo.stockstats.model;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.UnlimitedWindow;

/**
 * TimeWindowedStringDeserializer
 * @author Hansen
 */
public class TimeWindowedStringDeserializer implements Deserializer<Windowed<String>> {

	private static final int TIMESTAMP_SIZE = 8;
	private static final int EMPTY_SIZE = 4;
    private Deserializer<String> keyDeserializer = new StringDeserializer();
    
    public TimeWindowedStringDeserializer() {
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
    }

    @Override
    public Windowed<String> deserialize(final String topic, final byte[] data) {
        byte[] bytes = new byte[data.length - TIMESTAMP_SIZE - EMPTY_SIZE];
        System.arraycopy(data, 0, bytes, 0, bytes.length);
        long start = ByteBuffer.wrap(data).getLong(data.length - TIMESTAMP_SIZE - EMPTY_SIZE);
        return new Windowed<>(keyDeserializer.deserialize(topic, bytes), new UnlimitedWindow(start));
    }

    @Override
    public void close() {
    }
}
