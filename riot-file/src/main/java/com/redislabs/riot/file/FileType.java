package com.redislabs.riot.file;

public enum FileType {

    DELIMITED(FileOptions.EXT_CSV, FileOptions.EXT_TSV), FIXED(FileOptions.EXT_FW), JSON(FileOptions.EXT_JSON), XML(FileOptions.EXT_XML);

    private final String[] extensions;

    FileType(String... extensions) {
        this.extensions = extensions;
    }

    public String[] getExtensions() {
        return extensions;
    }
}