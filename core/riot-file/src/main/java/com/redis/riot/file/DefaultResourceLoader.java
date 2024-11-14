package com.redis.riot.file;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.FileUrlResource;
import org.springframework.core.io.ProtocolResolver;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.io.UrlResource;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ResourceUtils;

/**
 * Implementation of the {@link ResourceLoader} interface.
 *
 *
 * <p>
 * Will return a {@link UrlResource} if the location value is a URL, and a
 * {@link ClassPathResource} if it is a non-URL path or a "classpath:"
 * pseudo-URL.
 *
 */
public class DefaultResourceLoader implements ResourceLoader {

	private final Set<ProtocolResolver> protocolResolvers = new LinkedHashSet<>(4);
	private final Map<Class<?>, Map<Resource, ?>> resourceCaches = new ConcurrentHashMap<>(4);

	@Override
	@Nullable
	public ClassLoader getClassLoader() {
		return ClassUtils.getDefaultClassLoader();
	}

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

	/**
	 * Return the collection of currently registered protocol resolvers, allowing
	 * for introspection as well as modification.
	 * 
	 * @see #addProtocolResolver(ProtocolResolver)
	 */
	public Collection<ProtocolResolver> getProtocolResolvers() {
		return this.protocolResolvers;
	}

	/**
	 * Obtain a cache for the given value type, keyed by {@link Resource}.
	 * 
	 * @param valueType the value type, e.g. an ASM {@code MetadataReader}
	 * @return the cache {@link Map}, shared at the {@code ResourceLoader} level
	 */
	@SuppressWarnings("unchecked")
	public <T> Map<Resource, T> getResourceCache(Class<T> valueType) {
		return (Map<Resource, T>) this.resourceCaches.computeIfAbsent(valueType, key -> new ConcurrentHashMap<>());
	}

	/**
	 * Clear all resource caches in this resource loader.
	 * 
	 * @since 5.0
	 * @see #getResourceCache
	 */
	public void clearResourceCaches() {
		this.resourceCaches.clear();
	}

	@Override
	public Resource getResource(String location) {
		Assert.notNull(location, "Location must not be null");
		for (ProtocolResolver protocolResolver : getProtocolResolvers()) {
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

}
