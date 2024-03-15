package com.redis.riot.cli;

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;
import java.util.Optional;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import picocli.CommandLine.IVersionProvider;

/**
 * {@link picocli.CommandLine.IVersionProvider} implementation that returns
 * version information from the jar file's {@code /META-INF/MANIFEST.MF} file.
 */
public abstract class AbstractManifestVersionProvider implements IVersionProvider {

	public String getVersionString() {
		try {
			Enumeration<URL> resources = AbstractManifestVersionProvider.class.getClassLoader()
					.getResources("META-INF/MANIFEST.MF");
			while (resources.hasMoreElements()) {
				Optional<String> version = version(resources.nextElement());
				if (version.isPresent()) {
					return version.get();
				}
			}
		} catch (IOException e) {
			// Fail silently
		}
		return "N/A";
	}

	private Optional<String> version(URL url) {
		try {
			Manifest manifest = new Manifest(url.openStream());
			if (isApplicableManifest(manifest)) {
				Attributes attr = manifest.getMainAttributes();
				return Optional.of(String.valueOf(get(attr, "Implementation-Version")));
			}
			return Optional.empty();
		} catch (IOException e) {
			Logger log = LoggerFactory.getLogger(AbstractManifestVersionProvider.class);
			log.warn("Unable to read from {}", url, e);
			return Optional.of("N/A");
		}
	}

	private boolean isApplicableManifest(Manifest manifest) {
		Attributes attributes = manifest.getMainAttributes();
		return getImplementationTitle().equals(get(attributes, "Implementation-Title"));
	}

	protected abstract String getImplementationTitle();

	private static Object get(Attributes attributes, String key) {
		return attributes.get(new Attributes.Name(key));
	}

}
