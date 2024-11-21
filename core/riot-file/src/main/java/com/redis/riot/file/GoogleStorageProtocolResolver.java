package com.redis.riot.file;

import java.util.function.Supplier;

import org.springframework.core.io.ProtocolResolver;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

import com.google.cloud.spring.storage.GoogleStorageResource;
import com.google.cloud.storage.Storage;

public class GoogleStorageProtocolResolver implements ProtocolResolver {

	private Supplier<Storage> storageSupplier;
	private Storage storage;

	@Override
	public Resource resolve(String location, ResourceLoader resourceLoader) {
		if (location.startsWith(com.google.cloud.spring.storage.GoogleStorageProtocolResolver.PROTOCOL)) {
			return new GoogleStorageResource(storage(), location, true);
		}
		return null;
	}

	private Storage storage() {
		if (storage == null) {
			storage = storageSupplier.get();
		}
		return storage;
	}

	public void setStorage(Storage storage) {
		this.storage = storage;
	}

	public void setStorageSupplier(Supplier<Storage> storageSupplier) {
		this.storageSupplier = storageSupplier;
	}

}
