package edu.harvard.iq.dataverse.arp;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/*
 * Replaces the four client values in {@code globalThis.__AROMA_CONFIG__}.
 * Aroma reads that object at runtime, so each value appears once as a property string.
 * 
 * This class has been extracted from the ARP project (https://science-research-data.hu/en) in the frame of
 * FAIR-IMPACT's 1st Open Call "Enabling FAIR Signposting and RO-Crate for content/metadata discovery and consumption".
 *
 * @author Balázs E. Pataki <balazs.pataki@sztaki.hu>, SZTAKI, Department of Distributed Systems, https://dsd.sztaki.hu
 * @author Norbert Finta <norbert.finta@sztaki.hu>, SZTAKI, Department of Distributed Systems, https://dsd.sztaki.hu
 * @version 1.0
 */
public final class AromaBundleRewriter {

    static final String KEY_DV_HOST = "VITE_REACT_APP_DV_HOST";
    static final String KEY_CEDAR_DOMAIN = "VITE_REACT_APP_CEDAR_DOMAIN";
    static final String KEY_W3ID_BASE = "VITE_REACT_APP_W3ID_BASE";
    static final String KEY_CEDAR_PROXY = "VITE_REACT_APP_CEDAR_PROXY";

    private AromaBundleRewriter() {
    }

    /**
     * @param siteUrl Dataverse site URL. A trailing slash is removed. The CEDAR proxy is this URL plus {@code /api/arp/cedarResourceProxy/}.
     * @param cedarDomain {@code arp.cedar.domain}. Null or unsafe values keep the baked literal.
     * @param w3idBase {@code arp.w3id.base}. A trailing slash is removed. Null or unsafe values keep the baked literal.
     */
    public static String rewrite(String javascript, String siteUrl, String cedarDomain, String w3idBase) {
        String host = stripTrailingSlash(siteUrl);
        String w3id = stripTrailingSlash(w3idBase);
        String proxy = isSafe(host) ? host + "/api/arp/cedarResourceProxy/" : null;

        String result = javascript;
        result = replaceProperty(result, KEY_DV_HOST, host);
        result = replaceProperty(result, KEY_CEDAR_DOMAIN, cedarDomain);
        result = replaceProperty(result, KEY_W3ID_BASE, w3id);
        result = replaceProperty(result, KEY_CEDAR_PROXY, proxy);
        return result;
    }

    private static String replaceProperty(String javascript, String key, String value) {
        if (!isSafe(value)) {
            return javascript;
        }
        Pattern pattern = Pattern.compile(Pattern.quote(key) + ":\"[^\"]*\"");
        Matcher matcher = pattern.matcher(javascript);
        return matcher.replaceAll(Matcher.quoteReplacement(key + ":\"" + value + "\""));
    }

    static String stripTrailingSlash(String value) {
        if (value == null) {
            return null;
        }
        while (value.endsWith("/")) {
            value = value.substring(0, value.length() - 1);
        }
        return value;
    }

    /**
     * Values are inserted into double-quoted strings. Reject anything that could break that form.
     */
    static boolean isSafe(String value) {
        if (value == null || value.isBlank()) {
            return false;
        }
        if (value.indexOf('"') >= 0 || value.indexOf('`') >= 0 || value.indexOf('\\') >= 0 || value.contains("${")) {
            return false;
        }
        for (int i = 0; i < value.length(); i++) {
            if (Character.isWhitespace(value.charAt(i))) {
                return false;
            }
        }
        return true;
    }
}
