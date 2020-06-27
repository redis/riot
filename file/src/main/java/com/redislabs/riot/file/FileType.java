package com.redislabs.riot.file;

public enum FileType {

    DELIMITED(ResourceHelper.EXT_CSV, ResourceHelper.EXT_TSV), FIXED(ResourceHelper.EXT_FW), JSON(ResourceHelper.EXT_JSON), XML(ResourceHelper.EXT_XML);

    private final String[] extensions;

    FileType(String... extensions) {
        this.extensions = extensions;
    }

    public String[] getExtensions() {
        return extensions;
    }
}