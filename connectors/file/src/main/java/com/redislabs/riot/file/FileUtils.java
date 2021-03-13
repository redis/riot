package com.redislabs.riot.file;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.batch.item.resource.StandardInputResource;
import org.springframework.batch.item.resource.StandardOutputResource;
import org.springframework.batch.item.xml.XmlItemReader;
import org.springframework.batch.item.xml.XmlObjectReader;
import org.springframework.batch.item.xml.support.XmlItemReaderBuilder;
import org.springframework.cloud.aws.core.io.s3.SimpleStorageProtocolResolver;
import org.springframework.cloud.gcp.autoconfigure.storage.GcpStorageAutoConfiguration;
import org.springframework.cloud.gcp.core.GcpScope;
import org.springframework.cloud.gcp.core.UserAgentHeaderProvider;
import org.springframework.cloud.gcp.storage.GoogleStorageResource;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;
import org.springframework.util.Assert;
import org.springframework.util.ResourceUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FileUtils {

    public final static String GS_URI_PREFIX = "gs://";
    public final static String S3_URI_PREFIX = "s3://";

    private final static Pattern EXTENSION_PATTERN = Pattern.compile("(?i)\\.(?<extension>\\w+)(?<gz>\\.gz)?$");

    public static String filename(Resource resource) throws IOException {
        if (resource instanceof StandardInputResource) {
            return "stdin";
        }
        if (resource instanceof StandardOutputResource) {
            return "stdout";
        }
        if (resource.isFile()) {
            return resource.getFilename();
        }
        String path = resource.getURI().getPath();
        int cut = path.lastIndexOf('/');
        if (cut == -1) {
            return path;
        }
        return path.substring(cut + 1);
    }

    public static boolean isGzip(String file) {
        return extensionGroup(file, "gz") != null;
    }

    public static String extension(String file) {
        return extensionGroup(file, "extension");
    }

    public static String extensionGroup(String file, String group) {
        Matcher matcher = EXTENSION_PATTERN.matcher(file);
        if (matcher.find()) {
            return matcher.group(group);
        }
        return null;
    }

    public static boolean isFile(String file) {
        return !(isGcs(file) || isS3(file) || ResourceUtils.isUrl(file) || isConsole(file));
    }

    public static boolean isConsole(String file) {
        return "-".equals(file);
    }

    private static boolean isS3(String file) {
        return file.startsWith(S3_URI_PREFIX);
    }

    private static boolean isGcs(String file) {
        return file.startsWith(GS_URI_PREFIX);
    }

    public static Resource inputResource(String file, FileOptions options) throws IOException {
        if (isConsole(file)) {
            return new StandardInputResource();
        }
        Resource resource = resource(file, options, true);
        if (options.isGzip()) {
            return new GZIPInputStreamResource(resource.getInputStream(), resource.getDescription());
        }
        return resource;
    }

    public static WritableResource outputResource(String location, FileOptions options) throws IOException {
        if (isConsole(location)) {
            return new StandardOutputResource();
        }
        Resource resource = resource(location, options, false);
        Assert.isInstanceOf(WritableResource.class, resource);
        WritableResource writable = (WritableResource) resource;
        if (options.isGzip() || isGzip(location)) {
            return new GZIPOutputStreamResource(writable.getOutputStream(), writable.getDescription());
        }
        return writable;
    }

    public static Resource s3Resource(String location, S3Options options) {
        AmazonS3ClientBuilder clientBuilder = AmazonS3Client.builder();
        if (options != null) {
            if (options.getRegion() != null) {
                clientBuilder.withRegion(options.getRegion());
            }
            if (options.getAccessKey() != null) {
                clientBuilder.withCredentials(new SimpleAWSCredentialsProvider(options.getAccessKey(), options.getSecretKey()));
            }
        }
        SimpleStorageProtocolResolver resolver = new SimpleStorageProtocolResolver() {
            @Override
            public AmazonS3 getAmazonS3() {
                return clientBuilder.build();
            }
        };
        resolver.afterPropertiesSet();
        return resolver.resolve(location, new DefaultResourceLoader());
    }

    private static class SimpleAWSCredentialsProvider implements AWSCredentialsProvider {

        private final String accessKey;
        private final String secretKey;

        private SimpleAWSCredentialsProvider(String accessKey, String secretKey) {
            this.accessKey = accessKey;
            this.secretKey = secretKey;
        }

        @Override
        public AWSCredentials getCredentials() {
            return new BasicAWSCredentials(accessKey, secretKey);
        }

        @Override
        public void refresh() {
            // do nothing
        }

    }

    private static Resource resource(String location, FileOptions options, boolean readOnly) throws IOException {
        if (isS3(location)) {
            return s3Resource(location, options.getS3());
        }
        if (isGcs(location)) {
            return gcsResource(location, options.getGcs(), readOnly);
        }
        if (ResourceUtils.isUrl(location)) {
            return new UncustomizedUrlResource(location);
        }
        return new FileSystemResource(location);
    }

    private static GoogleStorageResource gcsResource(String locationUri, GcsOptions options, boolean readOnly) throws IOException {
        StorageOptions.Builder builder = StorageOptions.newBuilder();
        if (options.getCredentials() != null) {
            builder.setCredentials(GoogleCredentials.fromStream(Files.newInputStream(options.getCredentials().toPath())).createScoped((readOnly ? GcpScope.STORAGE_READ_ONLY : GcpScope.STORAGE_READ_WRITE).getUrl()));
        }
        if (options.getEncodedKey() != null) {
            builder.setCredentials(GoogleCredentials.fromStream(new ByteArrayInputStream(Base64.getDecoder().decode(options.getEncodedKey()))));
        }
        builder.setHeaderProvider(new UserAgentHeaderProvider(GcpStorageAutoConfiguration.class));
        builder.setProjectId(options.getProjectId() == null ? ServiceOptions.getDefaultProjectId() : options.getProjectId());
        Storage storage = builder.build().getService();
        return new GoogleStorageResource(storage, locationUri);
    }


    public static <T> JsonItemReader<T> jsonReader(Resource resource, Class<T> clazz) {
        JsonItemReaderBuilder<T> jsonReaderBuilder = new JsonItemReaderBuilder<>();
        jsonReaderBuilder.name("json-file-reader");
        jsonReaderBuilder.resource(resource);
        JacksonJsonObjectReader<T> jsonObjectReader = new JacksonJsonObjectReader<>(clazz);
        jsonObjectReader.setMapper(new ObjectMapper());
        jsonReaderBuilder.jsonObjectReader(jsonObjectReader);
        return jsonReaderBuilder.build();
    }

    public static <T> XmlItemReader<T> xmlReader(Resource resource, Class<T> clazz) {
        XmlItemReaderBuilder<T> xmlReaderBuilder = new XmlItemReaderBuilder<>();
        xmlReaderBuilder.name("xml-file-reader");
        xmlReaderBuilder.resource(resource);
        XmlObjectReader<T> xmlObjectReader = new XmlObjectReader<>(clazz);
        xmlObjectReader.setMapper(new XmlMapper());
        xmlReaderBuilder.xmlObjectReader(xmlObjectReader);
        return xmlReaderBuilder.build();
    }

    public static List<String> expand(String... files) throws IOException {
        if (files == null) {
            return null;
        }
        List<String> expandedFiles = new ArrayList<>();
        for (String file : files) {
            if (isFile(file)) {
                Path path = Paths.get(file);
                if (Files.exists(path)) {
                    expandedFiles.add(file);
                } else {
                    // Path might be glob pattern
                    Path parent = path.getParent();
                    if (parent != null) {
                        try (DirectoryStream<Path> stream = Files.newDirectoryStream(parent, path.getFileName().toString())) {
                            stream.forEach(p -> expandedFiles.add(p.toString()));
                        }
                    }
                }
            } else {
                expandedFiles.add(file);
            }
        }
        return expandedFiles;
    }

}
