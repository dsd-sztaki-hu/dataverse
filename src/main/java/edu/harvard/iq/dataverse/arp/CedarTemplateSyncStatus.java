package edu.harvard.iq.dataverse.arp;

import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;

/**
 * Whether a metadata block matches the CEDAR template it was imported from.
 */
public enum CedarTemplateSyncStatus {
    NONE,
    IN_SYNC,
    UPDATE,
    ERROR;

    /**
     * CEDAR {@code /templates/{id}/details} can report {@code pav:lastUpdatedOn}
     * a few seconds later than the template document. Treat that as in sync.
     */
    static final Duration CEDAR_DETAILS_TIMESTAMP_TOLERANCE = Duration.ofSeconds(5);

    /**
     * Compare stored vs CEDAR {@code pav:lastUpdatedOn} timestamps.
     * CEDAR is out of date from Dataverse's perspective only when its timestamp
     * is more than {@link #CEDAR_DETAILS_TIMESTAMP_TOLERANCE} later.
     */
    static CedarTemplateSyncStatus compareLastUpdatedOn(String storedLastUpdatedOn, String cedarLastUpdatedOn) {
        if (cedarLastUpdatedOn == null || cedarLastUpdatedOn.isBlank()) {
            return ERROR;
        }
        if (storedLastUpdatedOn == null || storedLastUpdatedOn.isBlank()) {
            return UPDATE;
        }
        try {
            Instant stored = Instant.parse(storedLastUpdatedOn);
            Instant cedar = Instant.parse(cedarLastUpdatedOn);
            if (!cedar.isAfter(stored)) {
                return IN_SYNC;
            }
            if (Duration.between(stored, cedar).compareTo(CEDAR_DETAILS_TIMESTAMP_TOLERANCE) <= 0) {
                return IN_SYNC;
            }
            return UPDATE;
        } catch (DateTimeParseException e) {
            return ERROR;
        }
    }
}
