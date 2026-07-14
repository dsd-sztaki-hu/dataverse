package edu.harvard.iq.dataverse.arp.rocrate;

import jakarta.ejb.Singleton;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Stores transient RO-Crate upload import mappings for async export.
 *
 * The mapping is needed to reconcile uploaded RO-Crate file entity ids ("@id")
 * with the newly created Dataverse files (storageIdentifier / datafile id).
 *
 * This cannot be kept in a CDI {@code @SessionScoped} bean because RO-Crate export
 * runs asynchronously without a web session context.
 */
@Singleton
public class RoCrateImportMappingStoreBean {

    private static final Duration DEFAULT_TTL = Duration.ofMinutes(10);

    private static final class Entry {
        private final Map<String, String> mapping;
        private final Instant expiresAt;

        private Entry(Map<String, String> mapping, Instant expiresAt) {
            this.mapping = mapping;
            this.expiresAt = expiresAt;
        }
    }

    private final ConcurrentHashMap<Long, Entry> byDatasetVersionId = new ConcurrentHashMap<>();

    public void put(Long datasetVersionId, Map<String, String> importMapping) {
        put(datasetVersionId, importMapping, DEFAULT_TTL);
    }

    public void put(Long datasetVersionId, Map<String, String> importMapping, Duration ttl) {
        if (datasetVersionId == null || importMapping == null || importMapping.isEmpty()) {
            return;
        }
        Duration effectiveTtl = ttl == null ? DEFAULT_TTL : ttl;
        byDatasetVersionId.put(datasetVersionId, new Entry(Map.copyOf(importMapping), Instant.now().plus(effectiveTtl)));
    }

    /**
     * Consume-and-remove.
     */
    public Map<String, String> take(Long datasetVersionId) {
        if (datasetVersionId == null) {
            return null;
        }

        Entry entry = byDatasetVersionId.remove(datasetVersionId);
        if (entry == null) {
            return null;
        }
        if (Instant.now().isAfter(entry.expiresAt)) {
            return null;
        }
        return entry.mapping;
    }

    public void remove(Long datasetVersionId) {
        if (datasetVersionId == null) {
            return;
        }
        byDatasetVersionId.remove(datasetVersionId);
    }

    public void pruneExpired() {
        Instant now = Instant.now();
        byDatasetVersionId.entrySet().removeIf(e -> e.getValue() == null || now.isAfter(e.getValue().expiresAt));
    }
}

