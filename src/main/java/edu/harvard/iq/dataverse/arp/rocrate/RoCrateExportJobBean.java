package edu.harvard.iq.dataverse.arp.rocrate;

import edu.harvard.iq.dataverse.DatasetVersion;
import edu.harvard.iq.dataverse.DatasetVersionServiceBean;
import jakarta.ejb.Asynchronous;
import jakarta.ejb.EJB;
import jakarta.ejb.Stateless;
import jakarta.ejb.TransactionAttribute;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import static jakarta.ejb.TransactionAttributeType.REQUIRES_NEW;

/**
 * Runs RO-Crate export work asynchronously in a new transaction.
 *
 * Important: this must be a separate EJB from {@link RoCrateExportManager} to avoid
 * EJB self-invocation, which would bypass {@link Asynchronous}.
 */
@Stateless
public class RoCrateExportJobBean {

    private static final Logger logger = Logger.getLogger(RoCrateExportJobBean.class.getCanonicalName());

    /**
     * RO-Crate generation can be triggered repeatedly and quickly (e.g. validate+upload loops).
     * Serialize export per dataset to avoid overlapping persistence work.
     */
    private static final ConcurrentHashMap<Long, ReentrantLock> DATASET_EXPORT_LOCKS = new ConcurrentHashMap<>();

    private static ReentrantLock getDatasetExportLock(Long datasetId) {
        return DATASET_EXPORT_LOCKS.computeIfAbsent(datasetId, __ -> new ReentrantLock(true));
    }

    private static void maybeRemoveDatasetExportLock(Long datasetId, ReentrantLock lock) {
        if (datasetId == null || lock == null) {
            return;
        }
        if (!lock.hasQueuedThreads()) {
            DATASET_EXPORT_LOCKS.remove(datasetId, lock);
        }
    }

    @EJB
    DatasetVersionServiceBean datasetVersionServiceBean;

    @EJB
    RoCrateExportManager roCrateExportManager;

    @EJB
    RoCrateImportMappingStoreBean roCrateImportMappingStore;

    @Asynchronous
    @TransactionAttribute(REQUIRES_NEW)
    public void createOrUpdateRoCrateAsync(Long datasetVersionId) {
        if (datasetVersionId == null) {
            return;
        }

        // The async job may start before the creating transaction commits; retry briefly.
        // Use em.find()-backed lookup first to avoid noisy NoResultException logs from findDeep().
        DatasetVersion version = null;
        for (int attempt = 0; attempt < 10 && version == null; attempt++) {
            try {
                DatasetVersion shallow = datasetVersionServiceBean.find(datasetVersionId);
                if (shallow != null) {
                    // Now that the row exists, do the deep fetch.
                    version = datasetVersionServiceBean.findDeep(datasetVersionId);
                }
            } catch (RuntimeException ignored) {
                // Treat as "not visible yet" and retry.
                version = null;
            }
            if (version == null) {
                try {
                    Thread.sleep(200L);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
        if (version == null) {
            logger.warning("createOrUpdateRoCrateAsync: DatasetVersion not found for id=" + datasetVersionId);
            return;
        }

        Long datasetId = null;
        try {
            datasetId = version.getDataset() == null ? null : version.getDataset().getId();
        } catch (RuntimeException ignored) {
        }

        ReentrantLock lock = null;
        if (datasetId != null) {
            lock = getDatasetExportLock(datasetId);
            lock.lock();
        }
        try {
            Map<String, String> importMapping = roCrateImportMappingStore.take(datasetVersionId);
            roCrateExportManager.doCreateOrUpdateRoCrate(version, importMapping);
        } catch (Exception e) {
            logger.warning("createOrUpdateRoCrateAsync failed for datasetVersionId=" + datasetVersionId + ": " + e.getMessage());
        } finally {
            if (lock != null) {
                try {
                    lock.unlock();
                } finally {
                    maybeRemoveDatasetExportLock(datasetId, lock);
                }
            }
        }
    }
}

