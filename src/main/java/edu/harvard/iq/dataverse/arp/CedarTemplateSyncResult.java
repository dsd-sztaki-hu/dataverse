package edu.harvard.iq.dataverse.arp;

import edu.harvard.iq.dataverse.util.BundleUtil;

import java.net.http.HttpTimeoutException;
import java.util.List;

/**
 * Result of comparing a metadata block with its CEDAR template.
 */
public final class CedarTemplateSyncResult {

    private final CedarTemplateSyncStatus status;
    private final String errorDetail;

    private CedarTemplateSyncResult(CedarTemplateSyncStatus status, String errorDetail) {
        this.status = status;
        this.errorDetail = errorDetail;
    }

    public static CedarTemplateSyncResult of(CedarTemplateSyncStatus status) {
        return new CedarTemplateSyncResult(status, null);
    }

    public static CedarTemplateSyncResult none() {
        return of(CedarTemplateSyncStatus.NONE);
    }

    public static CedarTemplateSyncResult error(String errorDetail) {
        return new CedarTemplateSyncResult(CedarTemplateSyncStatus.ERROR, errorDetail);
    }

    public CedarTemplateSyncStatus getStatus() {
        return status;
    }

    public String getErrorDetail() {
        return errorDetail;
    }

    static String messageForHttpStatus(int statusCode) {
        if (statusCode == 401 || statusCode == 403) {
            return BundleUtil.getStringFromBundle("dataverse.arpSync.error.unauthorized",
                    List.of(String.valueOf(statusCode)));
        }
        if (statusCode == 404) {
            return BundleUtil.getStringFromBundle("dataverse.arpSync.error.notFound",
                    List.of(String.valueOf(statusCode)));
        }
        return BundleUtil.getStringFromBundle("dataverse.arpSync.error.http",
                List.of(String.valueOf(statusCode)));
    }

    static String describeException(Exception e) {
        if (e == null) {
            return BundleUtil.getStringFromBundle("dataverse.arpSync.error.generic", List.of("unknown error"));
        }
        if (e instanceof HttpTimeoutException) {
            return BundleUtil.getStringFromBundle("dataverse.arpSync.error.timeout");
        }
        String msg = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
        return BundleUtil.getStringFromBundle("dataverse.arpSync.error.generic", List.of(msg));
    }
}
