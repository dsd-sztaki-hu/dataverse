package edu.harvard.iq.dataverse.arp;

import edu.harvard.iq.dataverse.UserNotification;
import edu.harvard.iq.dataverse.UserNotification.Type;
import edu.harvard.iq.dataverse.UserNotificationServiceBean;
import edu.harvard.iq.dataverse.authorization.AuthenticationServiceBean;
import edu.harvard.iq.dataverse.authorization.users.AuthenticatedUser;
import edu.harvard.iq.dataverse.util.SystemConfig;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import jakarta.ejb.EJB;
import jakarta.ejb.Singleton;
import jakarta.ejb.Startup;
import jakarta.ejb.Timeout;
import jakarta.ejb.Timer;
import jakarta.ejb.TimerConfig;
import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonObject;
import jakarta.json.JsonReader;
import jakarta.json.JsonValue;

import java.io.StringReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Optional startup check: if this installation's public site URL is not among
 * the harvest-registry {@code link} values, notify superusers in-app only.
 */
@Startup
@Singleton
public class HarvestRegistryCheckService {

    private static final Logger logger = Logger.getLogger(HarvestRegistryCheckService.class.getCanonicalName());

    static final String STATS_URL = "https://search.researchdata.hu/stats";
    static final String CONFIG_KEY_ENABLED = "arp.harvest.registry.check.enabled";

    private static final long INITIAL_DELAY_MS = 120_000L;
    private static final long RETRY_DELAY_MS = 60_000L;
    private static final int MAX_RETRIES = 5;
    private static final Duration HTTP_TIMEOUT = Duration.ofSeconds(15);

    @Resource
    jakarta.ejb.TimerService timerService;

    @EJB
    ArpConfig arpConfig;

    @EJB
    AuthenticationServiceBean authenticationService;

    @EJB
    UserNotificationServiceBean userNotificationService;

    @PostConstruct
    public void init() {
        if (!isEnabled()) {
            logger.info("Skipping harvest registry check (disabled)");
            return;
        }
        logger.info("Scheduling harvest registry check");
        timerService.createSingleActionTimer(INITIAL_DELAY_MS, new TimerConfig(Integer.valueOf(0), false));
    }

    @Timeout
    public void handleTimeout(Timer timer) {
        int attempt = 0;
        Object info = timer.getInfo();
        if (info instanceof Integer) {
            attempt = (Integer) info;
        }
        try {
            runCheck(attempt);
        } catch (Exception e) {
            logger.log(Level.WARNING, "Harvest registry check failed", e);
        }
    }

    void runCheck(int attempt) {
        String siteUrl = SystemConfig.getDataverseSiteUrlStatic();
        if (siteUrl == null || siteUrl.isBlank()) {
            logger.warning("Harvest registry check skipped: site URL could not be determined");
            return;
        }

        String body;
        try {
            body = fetchStatsJson();
        } catch (Exception e) {
            logger.log(Level.WARNING, "Harvest registry check skipped: could not fetch " + STATS_URL, e);
            return;
        }

        List<String> links;
        try {
            links = extractLinks(body);
        } catch (Exception e) {
            logger.log(Level.WARNING, "Harvest registry check skipped: could not parse stats JSON", e);
            return;
        }

        if (urlsMatch(siteUrl, links)) {
            logger.info("Harvest registry lists this installation: " + siteUrl);
            return;
        }

        List<AuthenticatedUser> superUsers = authenticationService.findSuperUsers();
        if (superUsers == null || superUsers.isEmpty()) {
            if (attempt < MAX_RETRIES) {
                logger.info("Harvest registry check: no superusers yet, retrying");
                timerService.createSingleActionTimer(RETRY_DELAY_MS, new TimerConfig(Integer.valueOf(attempt + 1), false));
            } else {
                logger.warning("Harvest registry check: site URL is not registered (" + siteUrl
                        + ") but no superusers were found to notify");
            }
            return;
        }

        logger.warning("Harvest registry does not list this installation: " + siteUrl);
        notifySuperusers(superUsers, siteUrl);
    }

    private void notifySuperusers(List<AuthenticatedUser> superUsers, String siteUrl) {
        Timestamp now = Timestamp.from(Instant.now());
        for (AuthenticatedUser user : superUsers) {
            if (user == null || user.getId() == null) {
                continue;
            }
            if (userNotificationService.hasUnreadOfType(user.getId(), Type.HARVESTREGISTRYMISSING)) {
                continue;
            }
            UserNotification notification = new UserNotification();
            notification.setUser(user);
            notification.setSendDate(now);
            notification.setType(Type.HARVESTREGISTRYMISSING);
            notification.setObjectId(null);
            notification.setAdditionalInfo(siteUrl);
            notification.setEmailed(false);
            if (userNotificationService.isNotificationMuted(notification)) {
                continue;
            }
            userNotificationService.save(notification);
        }
    }

    private boolean isEnabled() {
        return isTruthy(arpConfig.get(CONFIG_KEY_ENABLED));
    }

    String fetchStatsJson() throws Exception {
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(HTTP_TIMEOUT)
                .build();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(STATS_URL))
                .timeout(HTTP_TIMEOUT)
                .header("Accept", "application/json")
                .GET()
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IllegalStateException("Harvest stats HTTP " + response.statusCode());
        }
        return response.body();
    }

    static boolean isTruthy(String value) {
        if (value == null) {
            return false;
        }
        String trimmed = value.trim();
        return "true".equalsIgnoreCase(trimmed)
                || "1".equals(trimmed)
                || "yes".equalsIgnoreCase(trimmed);
    }

    /**
     * Parse harvest-stats JSON array and collect {@code link} values.
     */
    public static List<String> extractLinks(String json) {
        List<String> links = new ArrayList<>();
        if (json == null || json.isBlank()) {
            return links;
        }
        try (JsonReader reader = Json.createReader(new StringReader(json))) {
            JsonArray array = reader.readArray();
            for (JsonValue value : array) {
                if (value.getValueType() != JsonValue.ValueType.OBJECT) {
                    continue;
                }
                JsonObject object = value.asJsonObject();
                if (object.containsKey("link") && !object.isNull("link")) {
                    links.add(object.getString("link"));
                }
            }
        }
        return links;
    }

    public static boolean urlsMatch(String siteUrl, Collection<String> links) {
        String normalizedSite = normalizeRepoUrl(siteUrl);
        if (normalizedSite.isEmpty() || links == null) {
            return false;
        }
        for (String link : links) {
            if (normalizedSite.equals(normalizeRepoUrl(link))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Host + non-default port + path, ignoring scheme and trailing slash.
     */
    public static String normalizeRepoUrl(String url) {
        if (url == null) {
            return "";
        }
        String trimmed = url.trim();
        if (trimmed.isEmpty()) {
            return "";
        }
        try {
            URI uri = URI.create(trimmed);
            String host = uri.getHost();
            if (host == null) {
                return stripTrailingSlash(trimmed).toLowerCase();
            }
            StringBuilder normalized = new StringBuilder(host.toLowerCase());
            int port = uri.getPort();
            if (port != -1 && port != 80 && port != 443) {
                normalized.append(':').append(port);
            }
            String path = uri.getPath();
            if (path != null && !path.isEmpty() && !"/".equals(path)) {
                normalized.append(stripTrailingSlash(path));
            }
            return normalized.toString();
        } catch (IllegalArgumentException e) {
            return stripTrailingSlash(trimmed).toLowerCase();
        }
    }

    private static String stripTrailingSlash(String value) {
        if (value.endsWith("/") && value.length() > 1) {
            return value.substring(0, value.length() - 1);
        }
        return value;
    }
}
