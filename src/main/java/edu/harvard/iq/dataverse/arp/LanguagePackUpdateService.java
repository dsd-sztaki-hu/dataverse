package edu.harvard.iq.dataverse.arp;

import jakarta.ejb.EJB;
import jakarta.ejb.Stateless;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ResourceBundle;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Overlay English and Hungarian UI translations from a GitHub language-packs
 * repository into {@code dataverse.lang.directory}. Extra files already there
 * (CEDAR UUID bundles) are left in place.
 */
@Stateless
public class LanguagePackUpdateService {

    private static final Logger logger = Logger.getLogger(LanguagePackUpdateService.class.getCanonicalName());

    static final String CONFIG_REPO_URL = "arp.lang.packs.repoUrl";
    static final String CONFIG_REF = "arp.lang.packs.ref";
    static final String CONFIG_UPDATE_ON_START = "arp.lang.packs.updateOnStart";

    private static final Duration HTTP_TIMEOUT = Duration.ofSeconds(60);
    private static final String USER_AGENT = "Hunverse-Dataverse-lang-packs";

    @EJB
    ArpConfig arpConfig;

    public boolean isUpdateOnStartEnabled() {
        return HarvestRegistryCheckService.isTruthy(arpConfig.get(CONFIG_UPDATE_ON_START));
    }

    public UpdateResult updateFromConfig() throws IOException, InterruptedException {
        return update(null, null);
    }

    /**
     * @param repoUrlOverride optional GitHub repo URL; falls back to config
     * @param refOverride optional branch, tag, or SHA; falls back to config
     */
    public UpdateResult update(String repoUrlOverride, String refOverride) throws IOException, InterruptedException {
        String repoUrl = firstNonBlank(repoUrlOverride, arpConfig.get(CONFIG_REPO_URL));
        String ref = firstNonBlank(refOverride, arpConfig.get(CONFIG_REF));
        if (repoUrl == null || repoUrl.isBlank()) {
            throw new IllegalArgumentException("Language pack repo URL is not set (" + CONFIG_REPO_URL + ")");
        }
        if (ref == null || ref.isBlank()) {
            throw new IllegalArgumentException("Language pack ref is not set (" + CONFIG_REF + ")");
        }

        Path langDir = langDirectory();
        Files.createDirectories(langDir);

        URI zipball = zipballUri(repoUrl, ref);
        logger.info("Downloading language packs from " + zipball);
        HttpClient client = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .connectTimeout(HTTP_TIMEOUT)
                .build();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(zipball)
                .timeout(HTTP_TIMEOUT)
                .header("User-Agent", USER_AGENT)
                .header("Accept", "application/vnd.github+json")
                .GET()
                .build();
        HttpResponse<InputStream> response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IOException("Language pack download failed (HTTP " + response.statusCode() + ") from " + zipball);
        }

        List<String> written;
        try (InputStream body = response.body()) {
            written = extractPacks(body, langDir);
        }
        ResourceBundle.clearCache();
        logger.info("Wrote " + written.size() + " language pack files to " + langDir);
        return new UpdateResult(repoUrl, ref, written);
    }

    static Path langDirectory() {
        String dir = System.getProperty("dataverse.lang.directory");
        if (dir == null || dir.isBlank()) {
            throw new IllegalStateException("dataverse.lang.directory is not set");
        }
        return Paths.get(dir);
    }

    static URI zipballUri(String repoUrl, String ref) {
        String[] ownerRepo = parseGitHubOwnerRepo(repoUrl);
        return URI.create("https://api.github.com/repos/" + ownerRepo[0] + "/" + ownerRepo[1]
                + "/zipball/" + encodePathSegment(ref));
    }

    /**
     * Accepts {@code https://github.com/owner/repo} with optional {@code .git}
     * or {@code /tree/...} suffix.
     */
    static String[] parseGitHubOwnerRepo(String repoUrl) {
        if (repoUrl == null || repoUrl.isBlank()) {
            throw new IllegalArgumentException("repoUrl is required");
        }
        String trimmed = repoUrl.trim();
        URI uri = URI.create(trimmed);
        String host = uri.getHost();
        if (host == null || !host.equalsIgnoreCase("github.com")) {
            throw new IllegalArgumentException("Language pack repoUrl must be a github.com URL: " + repoUrl);
        }
        String path = uri.getPath();
        if (path == null) {
            throw new IllegalArgumentException("Language pack repoUrl is missing owner/repo: " + repoUrl);
        }
        String[] parts = path.replaceFirst("^/", "").split("/");
        if (parts.length < 2 || parts[0].isBlank() || parts[1].isBlank()) {
            throw new IllegalArgumentException("Language pack repoUrl is missing owner/repo: " + repoUrl);
        }
        String repo = parts[1];
        if (repo.endsWith(".git")) {
            repo = repo.substring(0, repo.length() - 4);
        }
        return new String[] { parts[0], repo };
    }

    /**
     * Writes {@code * /en_US/*.properties} and {@code * /hu_HU/*.properties}
     * into {@code langDir} using the zip entry basename only.
     */
    static List<String> extractPacks(InputStream zipStream, Path langDir) throws IOException {
        Path langDirReal = langDir.toAbsolutePath().normalize();
        Files.createDirectories(langDirReal);
        List<String> written = new ArrayList<>();
        try (ZipInputStream zis = new ZipInputStream(zipStream)) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                try {
                    if (entry.isDirectory()) {
                        continue;
                    }
                    String name = entry.getName().replace('\\', '/');
                    if (hasDotDotSegment(name) || !isHunverseLocaleProperties(name)) {
                        continue;
                    }
                    String base = name.substring(name.lastIndexOf('/') + 1);
                    if (base.isBlank() || base.contains("..")) {
                        continue;
                    }
                    Path dest = langDirReal.resolve(base).normalize();
                    if (!dest.startsWith(langDirReal)) {
                        logger.warning("Skipped zip-slip path: " + name);
                        continue;
                    }
                    Files.copy(zis, dest, StandardCopyOption.REPLACE_EXISTING);
                    written.add(base);
                } finally {
                    zis.closeEntry();
                }
            }
        }
        Collections.sort(written);
        return written;
    }

    static boolean hasDotDotSegment(String zipPath) {
        for (String segment : zipPath.split("/")) {
            if ("..".equals(segment)) {
                return true;
            }
        }
        return false;
    }

    static boolean isHunverseLocaleProperties(String zipPath) {
        if (zipPath == null || !zipPath.endsWith(".properties")) {
            return false;
        }
        return zipPath.contains("/en_US/") || zipPath.startsWith("en_US/")
                || zipPath.contains("/hu_HU/") || zipPath.startsWith("hu_HU/");
    }

    private static String encodePathSegment(String ref) {
        return ref.replace(" ", "%20");
    }

    private static String firstNonBlank(String preferred, String fallback) {
        if (preferred != null && !preferred.isBlank()) {
            return preferred.trim();
        }
        if (fallback != null && !fallback.isBlank()) {
            return fallback.trim();
        }
        return null;
    }

    public static class UpdateResult {
        public final String repoUrl;
        public final String ref;
        public final List<String> writtenFiles;

        public UpdateResult(String repoUrl, String ref, List<String> writtenFiles) {
            this.repoUrl = repoUrl;
            this.ref = ref;
            this.writtenFiles = writtenFiles;
        }
    }
}
