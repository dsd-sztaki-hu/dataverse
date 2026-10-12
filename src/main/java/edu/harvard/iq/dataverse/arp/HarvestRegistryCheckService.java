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
 * If this installation's public site URL is not among the harvest-registry
 * {@code link} values, keep a warning for superusers and notify them in-app
 * once per unregistered gap. The check repeats about once an hour so the
 * warning goes away after the site is listed.
 */
@Startup
@Singleton
public class HarvestRegistryCheckService {

    private static final Logger logger = Logger.getLogger(HarvestRegistryCheckService.class.getCanonicalName());

    static final String STATS_URL = "https://search.researchdata.hu/stats";

    private static final long INITIAL_DELAY_MS = 120_000L;
    private static final long RECHECK_DELAY_MS = 3_600_000L;
    private static final long RETRY_DELAY_MS = 60_000L;
    private static final int MAX_RETRIES = 5;
    private static final Duration HTTP_TIMEOUT = Duration.ofSeconds(15);

    private boolean harvestRegistryMissing;
    private String missingSiteUrl;
    /** True after superusers were notified for the current unregistered gap. */
    private boolean notifiedThisGap;

    @Resource
    jakarta.ejb.TimerService timerService;

    @EJB
    AuthenticationServiceBean authenticationService;

    @EJB
    UserNotificationServiceBean userNotificationService;

    @PostConstruct
    public void init() {
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
        boolean scheduled = false;
        try {
            scheduled = runCheck(attempt);
        } catch (Exception e) {
            logger.log(Level.WARNING, "Harvest registry check failed", e);
        }
        if (!scheduled) {
            scheduleRecheck();
        }
    }

    /**
     * @return true when this attempt scheduled its own follow-up timer
     */
    boolean runCheck(int attempt) {
        String siteUrl = SystemConfig.getDataverseSiteUrlStatic();
        if (siteUrl == null || siteUrl.isBlank()) {
            logger.warning("Harvest registry check skipped: site URL could not be determined");
            applyRegistryResult(null, null);
            scheduleRecheck();
            return true;
        }

        String body;
        try {
            body = fetchStatsJson();
        } catch (Exception e) {
            logger.log(Level.WARNING, "Harvest registry check skipped: could not fetch " + STATS_URL, e);
            applyRegistryResult(null, null);
            scheduleRecheck();
            return true;
        }

        List<String> links;
        try {
            links = extractLinks(body);
        } catch (Exception e) {
            logger.log(Level.WARNING, "Harvest registry check skipped: could not parse stats JSON", e);
            applyRegistryResult(null, null);
            scheduleRecheck();
            return true;
        }

        boolean listed = urlsMatch(siteUrl, links);
        boolean notify = applyRegistryResult(listed, siteUrl);
        if (listed) {
            logger.info("Harvest registry lists this installation: " + siteUrl);
            scheduleRecheck();
            return true;
        }

        logger.warning("Harvest registry does not list this installation: " + siteUrl);
        if (!notify) {
            scheduleRecheck();
            return true;
        }

        List<AuthenticatedUser> superUsers = authenticationService.findSuperUsers();
        if (superUsers == null || superUsers.isEmpty()) {
            // The miss was recorded, but nobody could be told yet. Leave the latch
            // open so a later attempt can still create the inbox notice.
            notifiedThisGap = false;
            if (attempt < MAX_RETRIES) {
                logger.info("Harvest registry check: no superusers yet, retrying");
                timerService.createSingleActionTimer(RETRY_DELAY_MS, new TimerConfig(Integer.valueOf(attempt + 1), false));
                return true;
            }
            logger.warning("Harvest registry check: site URL is not registered (" + siteUrl
                    + ") but no superusers were found to notify");
            scheduleRecheck();
            return true;
        }

        notifySuperusers(superUsers, siteUrl);
        scheduleRecheck();
        return true;
    }

    /**
     * Records one concluded check.
     *
     * @param listed {@code null} when the fetch or parse produced no result.
     *               That leaves the previous banner state in place.
     * @param siteUrl installation URL to show while the site is missing
     * @return {@code true} when this is a new unregistered gap and superusers
     *         should receive an inbox notice
     */
    boolean applyRegistryResult(Boolean listed, String siteUrl) {
        if (listed == null) {
            return false;
        }
        if (listed) {
            harvestRegistryMissing = false;
            missingSiteUrl = null;
            notifiedThisGap = false;
            return false;
        }
        missingSiteUrl = siteUrl;
        harvestRegistryMissing = true;
        if (notifiedThisGap) {
            return false;
        }
        notifiedThisGap = true;
        return true;
    }

    public boolean isHarvestRegistryMissing() {
        return harvestRegistryMissing;
    }

    public String getMissingSiteUrl() {
        return missingSiteUrl;
    }

    private void scheduleRecheck() {
        timerService.createSingleActionTimer(RECHECK_DELAY_MS, new TimerConfig(Integer.valueOf(0), false));
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
