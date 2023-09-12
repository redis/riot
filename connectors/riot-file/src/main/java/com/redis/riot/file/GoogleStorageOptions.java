package com.redis.riot.file;

import java.io.File;

public class GoogleStorageOptions {

    private File keyFile;

    private String projectId;

    private String encodedKey;

    public File getKeyFile() {
        return keyFile;
    }

    public void setKeyFile(File file) {
        this.keyFile = file;
    }

    public String getProjectId() {
        return projectId;
    }

    public void setProjectId(String id) {
        this.projectId = id;
    }

    public String getEncodedKey() {
        return encodedKey;
    }

    public void setEncodedKey(String key) {
        this.encodedKey = key;
    }

}
