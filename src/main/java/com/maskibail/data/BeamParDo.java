package com.maskibail.data;

import com.maskibail.data.mapper.AvocadoMapper;
import com.maskibail.data.model.AvocadoSale;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class BeamParDo {
    private static final Logger LOG = LoggerFactory.getLogger(BeamParDo.class);

    public static void main(String[] args) {
        PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        PCollection<AvocadoSale> avocadoSalePCollection = pipeline
                .apply("Read lines", TextIO.read().from(new File("src/main/resources/avocado.csv").getAbsolutePath()))
                .apply("transform lines into AvocadoSale object collection", ParDo.of(new AvocadoMapper()));

        avocadoSalePCollection.apply(MapElements.into(TypeDescriptors.strings())
                .via(AvocadoSale::toString))
                .apply(TextIO.write().to("output/pardo/"));

        pipeline.run().waitUntilFinish();
    }
}
