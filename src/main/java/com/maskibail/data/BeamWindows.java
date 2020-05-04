package com.maskibail.data;

import com.maskibail.data.io.CSVSink;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class BeamWindows {
    private static final Logger LOG = LoggerFactory.getLogger(BeamWindows.class);

    public static void main(String[] args) {
        PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        final List<String> LINES = Arrays.asList(
                "To be, or not to be: that is the question: ",
                "Whether 'tis nobler in the mind to suffer ",
                "The slings and arrows of outrageous fortune, ",
                "Or to take arms against a sea of troubles, ");

        PCollection<String> words = pipeline
                .apply("Read lines", Create.of(LINES)).setCoder(StringUtf8Coder.of())
                .apply("Flatten", FlatMapElements.into(TypeDescriptors.strings()).via((String s) -> Arrays.asList(s.split("[^\\p{L}]+"))))
                .apply("Filter empty words", Filter.by((String word) -> !word.isEmpty()));
//                .apply("Check Timestamp", ParDo.of(new DoFn<String, String>() {
//                    @ProcessElement
//                    public void processElement(@Element String word, OutputReceiver<String> outputReceiver, @Timestamp Instant timestamp) {
//                        LOG.info("element [{}] with timestamp [{}]", word, timestamp.toString());
//                        outputReceiver.output(word);
//                    }
//                }))
//                .apply("Check window", ParDo.of(new DoFn<String, String>() {
//                    @ProcessElement
//                    public void processElement(@Element String word, OutputReceiver<String> outputReceiver, PipelineOptions options) {
//                        LOG.info("element [{}] with timestamp [{}]", word, options.toString());
//                        outputReceiver.output(word);
//                    }
//                }));

        words.apply(
                Window.<String>into(FixedWindows.of(Duration.standardMinutes(1)))
                        .withAllowedLateness(Duration.standardSeconds(30))
                        .triggering(
                                AfterWatermark
                                        .pastEndOfWindow()
                                        .withEarlyFirings(
                                                AfterProcessingTime
                                                        .pastFirstElementInPane()
                                                        .plusDelayOf(Duration.standardSeconds(30)))
                                        .withLateFirings(AfterPane.elementCountAtLeast(1)))
                        .accumulatingFiredPanes());

        words
                .apply(MapElements
                        .into(TypeDescriptors.lists(TypeDescriptors.strings()))
                        .via(Arrays::asList))
                .apply(FileIO.<List<String>>write()
                        .via(new CSVSink(Arrays.asList("user", "amount")))
                        .to("output/csv/")
                        .withPrefix("words")
                        .withNumShards(1)
                        .withSuffix(".csv"));

        pipeline.run().waitUntilFinish();
    }
}
