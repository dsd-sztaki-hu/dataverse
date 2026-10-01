package edu.harvard.iq.dataverse.util;

import java.io.File;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.List;
import java.util.Locale;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Map;
import java.util.HashMap;
import jakarta.faces.context.FacesContext;
import jakarta.servlet.http.HttpServletRequest;

public class BundleUtil {

    private static final Logger logger = Logger.getLogger(BundleUtil.class.getCanonicalName());

    private static final String defaultBundleFile = "Bundle";

    private static final Map<String, ClassLoader> classLoaderCache = new HashMap<String, ClassLoader>();

    /** Locale for the current API request, when the UI language is not a JSF view. */
    private static final ThreadLocal<Locale> requestLocale = new ThreadLocal<>();

    public static void setRequestLocale(Locale locale) {
        if (locale == null) {
            requestLocale.remove();
        } else {
            requestLocale.set(locale);
        }
    }

    public static void clearRequestLocale() {
        requestLocale.remove();
    }

    public static Locale getRequestLocale() {
        return requestLocale.get();
    }

    /**
     * Uses {@code lang} on this HTTP request, or on the page URL that sent it.
     * @return true when this call set the locale and the caller must clear it
     */
    public static boolean applyLocaleFrom(HttpServletRequest request) {
        if (requestLocale.get() != null || request == null) {
            return false;
        }
        String lang = request.getParameter("lang");
        if (lang == null || lang.isBlank()) {
            lang = langFromUrl(request.getHeader("Referer"));
        }
        Locale locale = localeFromCode(lang);
        if (locale == null) {
            return false;
        }
        requestLocale.set(locale);
        return true;
    }

    private static String langFromUrl(String url) {
        if (url == null) {
            return null;
        }
        int queryStart = url.indexOf('?');
        if (queryStart < 0 || queryStart == url.length() - 1) {
            return null;
        }
        String query = url.substring(queryStart + 1);
        int hash = query.indexOf('#');
        if (hash >= 0) {
            query = query.substring(0, hash);
        }
        for (String part : query.split("&")) {
            int eq = part.indexOf('=');
            if (eq <= 0) {
                continue;
            }
            if ("lang".equals(part.substring(0, eq))) {
                try {
                    return URLDecoder.decode(part.substring(eq + 1), StandardCharsets.UTF_8);
                } catch (IllegalArgumentException ex) {
                    return null;
                }
            }
        }
        return null;
    }

    /**
     * Turns a UI language code such as {@code hu}, {@code en}, or {@code hu-HU} into a locale
     * that matches the language bundle file names.
     */
    public static Locale localeFromCode(String code) {
        if (code == null || code.isBlank()) {
            return null;
        }
        String trimmed = code.trim();
        int comma = trimmed.indexOf(',');
        if (comma > 0) {
            trimmed = trimmed.substring(0, comma);
        }
        int semi = trimmed.indexOf(';');
        if (semi > 0) {
            trimmed = trimmed.substring(0, semi);
        }
        trimmed = trimmed.trim();
        if (trimmed.isEmpty()) {
            return null;
        }
        if ("en_US".equalsIgnoreCase(trimmed) || "en-US".equalsIgnoreCase(trimmed)) {
            return new Locale("en");
        }
        Locale locale = Locale.forLanguageTag(trimmed.replace('_', '-'));
        if (locale.getLanguage() == null || locale.getLanguage().isEmpty()) {
            return null;
        }
        return locale;
    }

    public static String getStringFromBundle(String key) {
        return getStringFromBundle(key, (List<String>)null);
    }
    
    public static String getStringFromBundle(String key, Locale locale) {
        return getStringFromBundle(key, null, locale);
    }

    private static String getStringFromBundle(String key, List<String> arguments, Locale locale) {
        ResourceBundle bundle = getResourceBundle(defaultBundleFile, locale);
        if (bundle == null) {
            return null;
        }
        return getStringFromBundle(key, arguments, bundle);
    }

    public static String getStringFromBundle(String key, List<String> arguments) {
        ResourceBundle bundle = getResourceBundle(defaultBundleFile );
        if (bundle == null) {
            return null;
        }
        return getStringFromBundle(key, arguments, bundle);
    }

    public static String getStringFromBundle(String key, List<String> arguments, ResourceBundle bundle) {
        try {
            return getStringFromBundleNoMissingCheck(key, arguments, bundle);
        } catch (MissingResourceException ex) {
            logger.warning("Could not find key \"" + key + "\" in bundle file: ");
            logger.log(Level.CONFIG, ex.getMessage(), ex);
            return null;
        }
    }


    /**
     * This call was added to allow bypassing the exception catch, for filetype indexing needs the exception to bubble up
     * --MAD 4.9.4
     */
    private static String getStringFromBundleNoMissingCheck(String key, List<String> arguments, ResourceBundle bundle) throws MissingResourceException {
        if (key == null || key.isEmpty()) {
            return null;
        }
        String stringFromBundle = null;

        stringFromBundle = bundle.getString(key);
        logger.fine("string found: " + stringFromBundle);

        if (arguments != null) {
            Object[] argArray = new String[arguments.size()];
            argArray = arguments.toArray(argArray);
            return MessageFormat.format(stringFromBundle, argArray);
        } else {
            return stringFromBundle;
        }
    }

    public static String getStringFromPropertyFile(String key, String propertyFileName) throws MissingResourceException {
        return getStringFromPropertyFile(key, propertyFileName, null);
    }
    
    public static String getStringFromPropertyFile(String key, String propertyFileName, Locale locale) throws MissingResourceException {
        ResourceBundle bundle = getResourceBundle(propertyFileName,locale);
        if (bundle == null) {
            return null;
        }
        return getStringFromBundleNoMissingCheck(key, null, bundle);
    }

    public static ResourceBundle getResourceBundle(String propertyFileName ) {
        return getResourceBundle(propertyFileName, null);
    }

    public static ResourceBundle getResourceBundle(String propertyFileName, Locale currentLocale) {
        ResourceBundle bundle;

        String filesRootDirectory = System.getProperty("dataverse.lang.directory");

        if(currentLocale == null) {
            currentLocale = getCurrentLocale();
        }

        if (filesRootDirectory == null || filesRootDirectory.isEmpty()) {
            bundle = ResourceBundle.getBundle("propertyFiles/" + propertyFileName, currentLocale);
        } else {
            try {
                ClassLoader loader = getClassLoader(filesRootDirectory);
                bundle = ResourceBundle.getBundle(propertyFileName, currentLocale, loader);
            } catch (MissingResourceException mre) {
                logger.fine("No property file named " + propertyFileName + "_" + currentLocale.getLanguage()
                        + " found in " + filesRootDirectory + ", using untranslated values");
                bundle = ResourceBundle.getBundle("propertyFiles/" + propertyFileName, currentLocale);
            }
        }

        return bundle ;
    }

    private static ClassLoader getClassLoader(String filesRootDirectory) {
        if (classLoaderCache.containsKey(filesRootDirectory)){
            return classLoaderCache.get(filesRootDirectory);
        }

        File bundleFileDir  = new File(filesRootDirectory);
        URL[] urls = null;
        try {
            urls = new URL[]{bundleFileDir.toURI().toURL()};
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

        ClassLoader loader = new URLClassLoader(urls);
        classLoaderCache.put(filesRootDirectory, loader);
        return loader;
    }

    public static Locale getCurrentLocale() {
        Locale override = requestLocale.get();
        if (override != null) {
            return override;
        }
        if (FacesContext.getCurrentInstance() == null) {
            String localeEnvVar = System.getenv().get("LANG");
            if (localeEnvVar != null) {
                if (localeEnvVar.indexOf('.') > 0) {
                    localeEnvVar = localeEnvVar.substring(0, localeEnvVar.indexOf('.'));
                }
                if (!"en_US".equals(localeEnvVar)) {
                    logger.fine("BundleUtil: LOCALE code from the environmental variable is "+localeEnvVar);
                    return new Locale(localeEnvVar);
                }
            }

            return new Locale("en");
        } else if (FacesContext.getCurrentInstance().getViewRoot() == null) {
            return FacesContext.getCurrentInstance().getExternalContext().getRequestLocale();
        } else if (FacesContext.getCurrentInstance().getViewRoot().getLocale().getLanguage().equals("en_US")) {
            return new Locale("en");
        }

        return FacesContext.getCurrentInstance().getViewRoot().getLocale();

    }


    public static String getStringFromDefaultBundle(String key) {
        try {
            return getStringFromBundleNoMissingCheck(key, null, getResourceBundle(defaultBundleFile , getDefaultLocale() ));
        } catch (MissingResourceException ex) {
            logger.warning("Could not find key \"" + key + "\" in bundle file: ");
            logger.log(Level.CONFIG, ex.getMessage(), ex);
            return null;
        }
    }

    public static String getStringFromDefaultPropertyFile(String key, String propertyFileName  ) throws MissingResourceException {
        ResourceBundle bundle = getResourceBundle(propertyFileName, getDefaultLocale());
        if (bundle == null) {
            return null;
        }
        return getStringFromBundleNoMissingCheck(key, null, bundle);
    }
    
    /**
     * Return JVM default locale.
     *
     * For now, this simply forwards default system behaviour.
     * That means on JDK8 the system property user.language will be set on startup
     * from environment variables like LANG or via Maven arguments (which is important for testing).
     * (See also pom.xml for an example how we pinpoint this for reproducible tests!)
     * (You should also be aware that good IDEs are honoring settings from pom.xml.)
     *
     * Nonetheless, someday we might want to have more influence on how this is determined, thus this wrapper.
     * @return Dataverse default locale
     */
    public static Locale getDefaultLocale() {
        return Locale.getDefault();
    }

}
