package com.maskibail.data.io;

import avro.shaded.com.google.common.base.Joiner;
import org.apache.beam.sdk.io.FileIO;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.List;

public class CSVSink implements FileIO.Sink<List<String>> {
    private String header;
    private PrintWriter writer;

    public CSVSink(List<String> colNames) {
        this.header = Joiner.on(",").join(colNames);
    }

    public void open(WritableByteChannel channel) throws IOException {
        writer = new PrintWriter(Channels.newOutputStream(channel));
        writer.println(header);
    }

    public void write(List<String> element) throws IOException {
        writer.println(Joiner.on(",").join(element));
    }

    public void flush() throws IOException {
        writer.flush();
    }
}
