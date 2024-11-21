package com.redis.riot.file;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.FileUrlResource;
import org.springframework.core.io.ProtocolResolver;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ResourceUtils;

public class RiotResourceLoader implements ResourceLoader {

	private GoogleStorageProtocolResolver googleStorageProtocolResolver = new GoogleStorageProtocolResolver();
	private S3ProtocolResolver s3ProtocolResolver = new S3ProtocolResolver();

	private Set<ProtocolResolver> protocolResolvers = new LinkedHashSet<>(4);

	/**
	 * Register the given resolver with this resource loader, allowing for
	 * additional protocols to be handled.
	 * <p>
	 * Any such resolver will be invoked ahead of this loader's standard resolution
	 * rules. It may therefore also override any default rules.
	 * 
	 * @see #getProtocolResolvers()
	 */
	public void addProtocolResolver(ProtocolResolver resolver) {
		Assert.notNull(resolver, "ProtocolResolver must not be null");
		this.protocolResolvers.add(resolver);
	}

	public GoogleStorageProtocolResolver getGoogleStorageProtocolResolver() {
		return googleStorageProtocolResolver;
	}

	public void setGoogleStorageProtocolResolver(GoogleStorageProtocolResolver resolver) {
		this.googleStorageProtocolResolver = resolver;
	}

	public S3ProtocolResolver getS3ProtocolResolver() {
		return s3ProtocolResolver;
	}

	public void setS3ProtocolResolver(S3ProtocolResolver resolver) {
		this.s3ProtocolResolver = resolver;
	}

	/**
	 * Return the collection of currently registered protocol resolvers, allowing
	 * for introspection as well as modification.
	 * 
	 * @see #addProtocolResolver(ProtocolResolver)
	 */
	public Collection<ProtocolResolver> getProtocolResolvers() {
		return this.protocolResolvers;
	}

	@Override
	public ClassLoader getClassLoader() {
		return ClassUtils.getDefaultClassLoader();
	}

	@Override
	public Resource getResource(String location) {
		Assert.notNull(location, "Location must not be null");
		for (ProtocolResolver protocolResolver : allProtocolResolvers()) {
			Resource resource = protocolResolver.resolve(location, this);
			if (resource != null) {
				return resource;
			}
		}
		try {
			// Try to parse the location as a URL...
			URL url = ResourceUtils.toURL(location);
			return (ResourceUtils.isFileURL(url) ? new FileUrlResource(url) : new UncustomizedUrlResource(url));
		} catch (MalformedURLException ex) {
			// No URL -> resolve as resource path.
			return new FileSystemResource(location);
		}
	}

	private Iterable<ProtocolResolver> allProtocolResolvers() {
		List<ProtocolResolver> resolvers = new ArrayList<>();
		resolvers.add(s3ProtocolResolver);
		resolvers.add(googleStorageProtocolResolver);
		resolvers.addAll(protocolResolvers);
		return resolvers;
	}

}
