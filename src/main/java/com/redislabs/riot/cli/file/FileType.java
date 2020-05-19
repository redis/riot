package com.redislabs.riot.cli.file;

import com.sun.tools.jdeprscan.CSV;

public enum FileType {

    DELIMITED(FileExtensions.CSV, FileExtensions.TSV), FIXED(FileExtensions.FW), JSON(FileExtensions.JSON), XML(FileExtensions.XML);

    private final String[] extensions;

    FileType(String... extensions) {
        this.extensions = extensions;
    }

    public String[] getExtensions() {
        return extensions;
    }
}
