package com.redislabs.riot.file;

public enum FileType {

    DELIMITED(FileOptions.CSV, FileOptions.TSV), FIXED(FileOptions.FW), JSON(FileOptions.JSON), XML(FileOptions.XML);

    private final String[] extensions;

    FileType(String... extensions) {
        this.extensions = extensions;
    }

    public String[] getExtensions() {
        return extensions;
    }
}
