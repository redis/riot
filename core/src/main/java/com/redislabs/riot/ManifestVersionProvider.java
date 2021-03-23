package com.redislabs.riot;

import picocli.CommandLine;

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

/**
 * {@link picocli.CommandLine.IVersionProvider} implementation that returns version information from
 * the jar file's {@code /META-INF/MANIFEST.MF} file.
 */
public class ManifestVersionProvider implements CommandLine.IVersionProvider {

    @Override
    public String[] getVersion() {
        return new String[]{
                // @formatter:off
                "",
                "      ▀        █     @|fg(4;1;1) ██████████████████████████|@",
                " █ ██ █  ███  ████   @|fg(4;2;1) ██████████████████████████|@",
                " ██   █ █   █  █     @|fg(5;4;1) ██████████████████████████|@",
                " █    █ █   █  █     @|fg(1;4;1) ██████████████████████████|@",
                " █    █  ███    ██   @|fg(0;3;4) ██████████████████████████|@"+ "  v" + getVersionString(),
                ""};
                // @formatter:on
    }

    private String getVersionString() {
        try {
            Enumeration<URL> resources = getClass().getClassLoader().getResources("META-INF/MANIFEST.MF");
            while (resources.hasMoreElements()) {
                URL url = resources.nextElement();
                Manifest manifest = new Manifest(url.openStream());
                if (isApplicableManifest(manifest)) {
                    Attributes attr = manifest.getMainAttributes();
                    return String.valueOf(get(attr, "Implementation-Version"));
                }
            }
        } catch (IOException ex) {
            // ignore
        }
        return "N/A";
    }

    private boolean isApplicableManifest(Manifest manifest) {
        Attributes attributes = manifest.getMainAttributes();
        return "RIOT".equals(get(attributes, "Implementation-Title"));
    }

    private static Object get(Attributes attributes, String key) {
        return attributes.get(new Attributes.Name(key));
    }
}