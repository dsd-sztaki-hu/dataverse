package edu.harvard.iq.dataverse.arp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Picks the CEDAR template whose {@code pav:version} matches this Dataverse
 * installation. Starts from the latest document in the public folder and
 * follows {@code pav:derivedFrom} until that version is found.
 * <p>
 * ARP workflows (copy, archive and draft, merge) always set
 * {@code pav:derivedFrom} on the new resource. That is the only link this
 * selector uses. It does not follow {@code pav:previousVersion}.
 */
public final class CedarTemplateVersionSelector {

    static final int MAX_HOPS = 50;

    private CedarTemplateVersionSelector() {
    }

    /**
     * Loads a template document by its CEDAR {@code @id}.
     */
    @FunctionalInterface
    public interface TemplateFetcher {
        String fetch(String templateId) throws Exception;
    }

    /**
     * The template document that matched the installation version.
     */
    public static final class Selection {
        private final String templateJson;
        private final String selectedId;
        private final String version;

        Selection(String templateJson, String selectedId, String version) {
            this.templateJson = templateJson;
            this.selectedId = selectedId;
            this.version = version;
        }

        public String templateJson() {
            return templateJson;
        }

        public String selectedId() {
            return selectedId;
        }

        public String version() {
            return version;
        }
    }

    /**
     * @param startJson latest template document from the public folder
     * @param targetVersion Dataverse installation version, without a build number
     * @param fetch loads the document of the older version by its id
     */
    public static Selection select(String startJson, String targetVersion, TemplateFetcher fetch) throws Exception {
        if (targetVersion == null || targetVersion.isBlank()) {
            throw new IllegalArgumentException("Installation version is required");
        }
        if (fetch == null) {
            throw new IllegalArgumentException("Template fetcher is required");
        }
        ObjectMapper mapper = new ObjectMapper();
        Set<String> seenIds = new HashSet<>();
        List<String> seenVersions = new ArrayList<>();
        String json = startJson;
        for (int hop = 0; hop < MAX_HOPS; hop++) {
            if (json == null || json.isBlank()) {
                throw new IllegalStateException("CEDAR template is empty (" + versionsSeen(seenVersions) + ")");
            }
            JsonNode root;
            try {
                root = mapper.readTree(json);
            } catch (JsonProcessingException e) {
                throw new IllegalStateException("CEDAR template is not valid JSON", e);
            }
            String id = text(root, "@id");
            if (id.isEmpty()) {
                throw new IllegalStateException("CEDAR template has no @id (" + versionsSeen(seenVersions) + ")");
            }
            if (!seenIds.add(id)) {
                throw new IllegalStateException("CEDAR version chain cycled at " + id + " (" + versionsSeen(seenVersions) + ")");
            }
            String version = text(root, "pav:version");
            seenVersions.add(version.isEmpty() ? "(none)" : version);
            if (versionsMatch(version, targetVersion)) {
                return new Selection(json, id, version);
            }
            String olderId = derivedFromId(root);
            if (olderId.isEmpty()) {
                throw new IllegalStateException("No CEDAR resource matches installation version "
                        + targetVersion.trim() + " (" + versionsSeen(seenVersions) + ")");
            }
            json = fetch.fetch(olderId);
        }
        throw new IllegalStateException("CEDAR version chain exceeded " + MAX_HOPS + " hops (" + versionsSeen(seenVersions) + ")");
    }

    static boolean versionsMatch(String resourceVersion, String installationVersion) {
        return normalizeVersion(resourceVersion).equals(normalizeVersion(installationVersion));
    }

    /**
     * Trims the version, drops one leading {@code v} and drops trailing {@code .0} parts.
     * So {@code v6.9} matches {@code 6.9}, and {@code 6.10.0} matches {@code 6.10}.
     * CEDAR only stores versions with three parts, Dataverse reports {@code 6.10}.
     */
    static String normalizeVersion(String version) {
        if (version == null) {
            return "";
        }
        String trimmed = version.trim();
        if (trimmed.length() > 1 && (trimmed.charAt(0) == 'v' || trimmed.charAt(0) == 'V')) {
            trimmed = trimmed.substring(1);
        }
        return trimmed.replaceAll("(\\.0+)+$", "");
    }

    /**
     * The id of the older ARP version, from {@code pav:derivedFrom}.
     */
    static String derivedFromId(JsonNode root) {
        JsonNode derived = root.get("pav:derivedFrom");
        if (derived == null || derived.isNull()) {
            return "";
        }
        if (derived.isObject()) {
            return text(derived, "@id");
        }
        if (derived.isTextual()) {
            return derived.asText("").trim();
        }
        return "";
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            return "";
        }
        return value.asText("").trim();
    }

    private static String versionsSeen(List<String> seenVersions) {
        if (seenVersions.isEmpty()) {
            return "saw no pav:version";
        }
        return "saw pav:version " + String.join(", ", seenVersions);
    }
}
