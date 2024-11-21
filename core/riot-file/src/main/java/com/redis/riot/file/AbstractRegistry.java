package com.redis.riot.file;

import java.util.HashSet;
import java.util.Set;

import org.springframework.core.io.ProtocolResolver;
import org.springframework.core.io.Resource;
import org.springframework.util.MimeType;

public class AbstractRegistry {

	private ResourceMap resourceMap = new RiotResourceMap();
	private Set<ProtocolResolver> protocolResolvers = new HashSet<>();

	protected MimeType type(Resource resource, FileOptions options) {
		if (options.getContentType() == null) {
			return MimeType.valueOf(resourceMap.getContentTypeFor(resource));
		}
		return options.getContentType();
	}

	public void addProtocolResolver(ProtocolResolver protocolResolver) {
		protocolResolvers.add(protocolResolver);
	}

	protected Resource resource(String location, FileOptions options) {
		RiotResourceLoader resourceLoader = new RiotResourceLoader();
		protocolResolvers.forEach(resourceLoader::addProtocolResolver);
		resourceLoader.getS3ProtocolResolver().setClientSupplier(options.getS3Options()::client);
		resourceLoader.getGoogleStorageProtocolResolver()
				.setStorageSupplier(options.getGoogleStorageOptions()::storage);
		return resourceLoader.getResource(location);
	}

	public ResourceMap getResourceMap() {
		return resourceMap;
	}

	public void setResourceMap(ResourceMap resourceMap) {
		this.resourceMap = resourceMap;
	}

}
