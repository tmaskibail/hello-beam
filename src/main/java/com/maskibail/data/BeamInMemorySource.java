package com.maskibail.data;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;
import java.util.List;

//TODO: Fix this!
public class BeamInMemorySource {
    public static void main(String[] args) {

        final List<String> LINES = Arrays.asList(
                "To be, or not to be: that is the question: ",
                "Whether 'tis nobler in the mind to suffer ",
                "The slings and arrows of outrageous fortune, ",
                "Or to take arms against a sea of troubles, ");

        PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        pipeline.apply(Create.of(LINES)).setCoder(StringUtf8Coder.of())
                .apply(FlatMapElements.into(TypeDescriptors.strings())
                        .via((String s) -> Arrays.asList(s.split("[^\\\\p{L}]+"))))
                .apply(Filter.by((String s) -> !s.isEmpty()))
                .apply(Count.perElement())
                .apply(MapElements.into(TypeDescriptors.strings())
                        .via((KV<String, Long> kv) -> kv.getKey() + ":" + kv.getValue()))
                .apply(TextIO.write().to("output/inMemory/"));
        pipeline.run();
    }
}
