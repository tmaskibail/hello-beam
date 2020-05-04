package com.maskibail.data.policy;

import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.TimestampPolicy;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;

import java.util.Optional;

/**
 * Extract event time from the message instead of the Kafka log-append-time or processing-time
 **/
public class CustomFieldTimePolicy extends TimestampPolicy<Long, String> {
    protected Instant currentWatermark;

    public CustomFieldTimePolicy(Optional<Instant> previousWatermark) {
        currentWatermark = previousWatermark.orElse(BoundedWindow.TIMESTAMP_MIN_VALUE);
    }

    private static Instant extractTimeStampFromLogEntry(String element) {
        String[] message = element.split(",");
        return Instant.parse(message[3].trim(), DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"));
    }

    @Override
    public Instant getTimestampForRecord(PartitionContext ctx, KafkaRecord<Long, String> record) {
        currentWatermark = extractTimeStampFromLogEntry(record.getKV().getValue());
        return currentWatermark;
    }

    @Override
    public Instant getWatermark(PartitionContext ctx) {
        return currentWatermark;
    }
}
