package edu.harvard.iq.dataverse.arp;

import edu.harvard.iq.dataverse.settings.SettingsServiceBean;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.inject.Named;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

@Stateless
@Named
public class ArpConfig
{
    @EJB
    SettingsServiceBean settingsService;

    private static final Logger logger = Logger.getLogger(ArpConfig.class.getCanonicalName());

    private static final Properties defaultProperties = new Properties();

    static {
        try (InputStream input = ArpConfig.class.getClassLoader().getResourceAsStream("arp/default.properties")) {
            if (input == null) {
                logger.log(Level.SEVERE, "ArpConfigBean was unable to load config.properties");
            }
            defaultProperties.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Returns the configuration value for the given key.
     *
     * The property value is read in the following order from various sources:
     * 1. admin config properties (http://localhost:8080/api/admin/settings)
     * 2. System.getProperty - command line props via -D or ./asadmin create-jvm-options "-Ddataverse.fqdn=dataverse.example.com"
     * 3. OS environment variables
     * 4. default.properties from the classpath
     *
     * The key should have a dot notation starting with "arp", for example "arp.solr.updater.address". If the value is
     * set via admin config it should be the same string, but in uppercase, for example
     * arp.solr.updater.address --> ARP_SOLR_UPDATER_ADDRESS
     *
     * To override te default value at runtime, eg:
     *     curl -X PUT -d http://some.other.host/aroma http://localhost:8080/api/admin/settings/arp.aroma.address
     * Remove runtime setting and fall back to default:
     *     curl -X DELETE http://localhost:8080/api/admin/settings/arp.aroma.address
     *
     * @param key
     * @return value for key, or null if nothing is set
     */
    public String get(String key) {
        // 1. admin config properties ( http://localhost:8080/api/admin/settings)
        var value = settingsService.get(key);

        // 2. System.getProperty - command line props via -D or ./asadmin create-jvm-options "-Ddataverse.fqdn=dataverse.example.com"
        if (value == null) {
            value = System.getProperty(key);
        }
        // 3. OS environment variables.
        if (value == null) {
            // arp.solr.updater.address --> ARP_SOLR_UPDATER_ADDRESS
            value = System.getenv(dotNotationToEnvVar(key));
        }
        // 4. defaultProperties
        if (value == null) {
            value = defaultProperties.getProperty(key);
        }

        return value;
    }

    private  String dotNotationToEnvVar(String input) {
        if (input == null || input.isEmpty()) {
            return input;
        }

        String[] words = input.split("\\.");
        StringBuilder result = new StringBuilder();

        for (String word : words) {
            if (result.length() > 0) {
                result.append("_");
            }
            result.append(word.toUpperCase());
        }

        return result.toString();
    }

}
