package com.redis.riot;

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import picocli.CommandLine;

/**
 * {@link picocli.CommandLine.IVersionProvider} implementation that returns
 * version information from the jar file's {@code /META-INF/MANIFEST.MF} file.
 */
public class ManifestVersionProvider implements CommandLine.IVersionProvider {

	@Override
	public String[] getVersion() throws Exception {
		return new String[] {
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

	private String getVersionString() throws IOException {
		Enumeration<URL> resources = ManifestVersionProvider.class.getClassLoader()
				.getResources("META-INF/MANIFEST.MF");
		while (resources.hasMoreElements()) {
			URL url = resources.nextElement();
			try {
				Manifest manifest = new Manifest(url.openStream());
				if (isApplicableManifest(manifest)) {
					Attributes attr = manifest.getMainAttributes();
					return String.valueOf(get(attr, "Implementation-Version"));
				}
			} catch (IOException ex) {
				return "Unable to read from " + url + ": " + ex;
			}
		}
		return "N/A";
	}

	private boolean isApplicableManifest(Manifest manifest) {
		Attributes attributes = manifest.getMainAttributes();
		return "riot".equals(get(attributes, "Implementation-Title"));
	}

	private static Object get(Attributes attributes, String key) {
		return attributes.get(new Attributes.Name(key));
	}
}