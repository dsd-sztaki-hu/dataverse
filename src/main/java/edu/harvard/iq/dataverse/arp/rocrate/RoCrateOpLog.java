package edu.harvard.iq.dataverse.arp.rocrate;

import edu.harvard.iq.dataverse.Dataset;
import edu.harvard.iq.dataverse.DatasetVersion;
import edu.harvard.iq.dataverse.arp.ArpConfig;
import edu.kit.datamanager.ro_crate.RoCrate;
import edu.kit.datamanager.ro_crate.reader.Readers;
import edu.kit.datamanager.ro_crate.writer.Writers;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Timing logs for RO-Crate work. Operation steps use {@link #start(String)};
 * disk/network I/O uses {@link #startIo(String)}. Both are gated by
 * {@code arp.rocrate.timing.log} ({@code all}, {@code io}, {@code ops}, {@code off}).
 *
 * <pre>
 * try (var t = RoCrateOpLog.start("open", dataset).extra("version", versionNumber)) {
 *     // operation work
 * }
 * try (var t = RoCrateOpLog.startIo("read.json").extra("path", path)) {
 *     // disk read
 * }
 * </pre>
 */
public final class RoCrateOpLog {

    private static final Logger logger = Logger.getLogger(RoCrateOpLog.class.getName());
    private static final int MAX_ERROR_LENGTH = 200;
    static final String TIMING_LOG_KEY = "arp.rocrate.timing.log";

    enum Category {
        IO,
        OPS
    }

    private RoCrateOpLog() {
    }

    public static Timer start(String op) {
        return new Timer(op, Category.OPS);
    }

    public static Timer start(String op, Dataset dataset) {
        return new Timer(op, Category.OPS).dataset(dataset);
    }

    public static Timer start(String op, DatasetVersion version) {
        return new Timer(op, Category.OPS).dataset(version);
    }

    public static Timer startIo(String op) {
        return new Timer(op, Category.IO);
    }

    public static Timer startIo(String op, Dataset dataset) {
        return new Timer(op, Category.IO).dataset(dataset);
    }

    public static Timer startIo(String op, DatasetVersion version) {
        return new Timer(op, Category.IO).dataset(version);
    }

    /**
     * Writes the crate to disk (including preview generation) and logs {@code io=write}.
     */
    public static void saveCrate(RoCrate crate, String folderPath) throws IOException {
        try (Timer t = startIo("write").extra("path", folderPath)) {
            try {
                Writers.newFolderWriter().withAutomaticProvenance(null).save(crate, folderPath);
            } catch (IOException | RuntimeException e) {
                t.fail(e);
                throw e;
            }
        }
    }

    /**
     * Reads a crate folder from disk and logs {@code io=read.folder}.
     */
    public static RoCrate readCrateFolder(String folderPath) throws IOException {
        try (Timer t = startIo("read.folder").extra("path", folderPath)) {
            try {
                return Readers.newFolderReader().readCrate(folderPath);
            } catch (IOException | RuntimeException e) {
                t.fail(e);
                throw e;
            }
        }
    }

    public static final class Timer implements AutoCloseable {
        private final String op;
        private final Category category;
        private final boolean enabled;
        private final long startMs = System.currentTimeMillis();
        private final Map<String, Object> extras = new LinkedHashMap<>();
        private String datasetId;
        private boolean failed;
        private String error;
        private boolean closed;

        Timer(String op, Category category) {
            this.op = op;
            this.category = category;
            this.enabled = isEnabled(category);
            if (enabled && category == Category.IO) {
                logger.info(formatLine("start", null));
            }
        }

        public Timer dataset(Dataset dataset) {
            if (dataset != null) {
                this.datasetId = datasetId(dataset);
            }
            return this;
        }

        public Timer dataset(DatasetVersion version) {
            if (version != null) {
                if (version.getDataset() != null) {
                    this.datasetId = datasetId(version.getDataset());
                }
                if (version.getFriendlyVersionNumber() != null) {
                    extras.put("version", version.getFriendlyVersionNumber());
                }
            }
            return this;
        }

        public Timer extra(String key, Object value) {
            if (key != null && value != null) {
                extras.put(key, value);
            }
            return this;
        }

        public Timer fail(Throwable t) {
            this.failed = true;
            if (t != null) {
                this.error = sanitizeError(t.getMessage());
            }
            return this;
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            if (!enabled) {
                return;
            }
            long elapsedMs = System.currentTimeMillis() - startMs;
            logger.info(formatLine(failed ? "failed" : "ok", elapsedMs));
        }

        private String formatLine(String status, Long elapsedMs) {
            StringBuilder sb = new StringBuilder(128);
            if (category == Category.IO) {
                sb.append("RO-Crate io=");
            } else {
                sb.append("RO-Crate op=");
            }
            sb.append(op);
            sb.append(" status=").append(status);
            if (datasetId != null) {
                sb.append(" dataset=").append(datasetId);
            }
            extras.forEach((key, value) -> sb.append(' ').append(key).append('=').append(value));
            if (elapsedMs != null) {
                sb.append(" elapsedMs=").append(elapsedMs);
            }
            if (error != null && elapsedMs != null) {
                sb.append(" error=").append(error);
            }
            return sb.toString();
        }
    }

    static boolean isEnabled(Category category) {
        String mode = timingLogMode();
        return switch (mode) {
            case "off" -> false;
            case "io" -> category == Category.IO;
            case "ops" -> category == Category.OPS;
            default -> true;
        };
    }

    static String timingLogMode() {
        try {
            if (ArpConfig.instance != null) {
                String value = ArpConfig.instance.get(TIMING_LOG_KEY);
                if (value != null && !value.isBlank()) {
                    return value.trim().toLowerCase();
                }
            }
        } catch (RuntimeException ignored) {
            // Fall back to all when config is not ready.
        }
        return "all";
    }

    static String datasetId(Dataset dataset) {
        try {
            if (dataset.getGlobalId() != null) {
                return dataset.getGlobalId().asString();
            }
            String storageId = dataset.getIdentifierForFileStorage();
            if (storageId != null && !storageId.isBlank()) {
                return storageId;
            }
        } catch (RuntimeException ignored) {
            // Fall through to numeric id.
        }
        if (dataset.getId() != null) {
            return "id:" + dataset.getId();
        }
        return "unknown";
    }

    static String sanitizeError(String message) {
        if (message == null || message.isBlank()) {
            return null;
        }
        String collapsed = message.replaceAll("\\s+", " ").trim();
        if (collapsed.length() > MAX_ERROR_LENGTH) {
            return collapsed.substring(0, MAX_ERROR_LENGTH);
        }
        return collapsed;
    }
}
