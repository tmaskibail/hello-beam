package com.maskibail.data;

import avro.shaded.com.google.common.collect.ImmutableMap;
import com.maskibail.data.policy.CustomFieldTimePolicy;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;

public class BeamKafkaStreamProcessor {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> values = pipeline.apply(KafkaIO.<Long, String>read()
                .withBootstrapServers("localhost:9092")
                .withTopic("tweets")
                .withKeyDeserializer(LongDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .withConsumerConfigUpdates(ImmutableMap.of("group.id", "beam-client"))
                .withProcessingTime() //ignoring this default timestamp
                .withTimestampPolicyFactory((tp, previousWatermark) -> new CustomFieldTimePolicy(previousWatermark))
                .withReadCommitted()
                .commitOffsetsInFinalize()
                // PCollection<KafkaRecords<Long, String>>
                .withoutMetadata())
                // PCollection<KV<Long, String>>
                .apply(Values.create());

        values
                .apply(Window.<String>into(FixedWindows.of(Duration.standardSeconds(15)))
                        .withAllowedLateness(Duration.standardSeconds(5))
                        .triggering(Repeatedly.forever(AfterProcessingTime
                                .pastFirstElementInPane()
                                .plusDelayOf(Duration.standardSeconds(5))))
//                                        .withEarlyFirings(
//                                                AfterProcessingTime
//                                                        .pastFirstElementInPane()
//                                                        .plusDelayOf(Duration.standardSeconds(10)))
//                                        .withLateFirings(AfterPane.elementCountAtLeast(1)))
                        .accumulatingFiredPanes())
                .apply(TextIO
                        .write()
                        .withWindowedWrites()
                        .withNumShards(1)
                        .withSuffix(".txt")
                        .to("output/kafka/"));

        pipeline.run();
    }
}
