package com.redislabs.riot.cli.file;

import org.springframework.batch.item.file.FlatFileHeaderCallback;

import java.io.IOException;
import java.io.Writer;

public class HeaderCallback implements FlatFileHeaderCallback {

    private final String header;

    public HeaderCallback(String header) {
        this.header = header;
    }

    @Override
    public void writeHeader(Writer writer) throws IOException {
        writer.write(header);
    }
}
