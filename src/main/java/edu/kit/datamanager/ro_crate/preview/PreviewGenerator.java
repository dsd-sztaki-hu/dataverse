package edu.kit.datamanager.ro_crate.preview;

import edu.harvard.iq.dataverse.arp.ArpConfig;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

// Note: this class have been copied from the original ro_crate project and updated to call our preview generation
// service running as a standalone service.

/**
 * Class responsible for the generation of the human-readable representation of the metadata.
 */
public class PreviewGenerator {

    private static final Logger logger = Logger.getLogger(ArpConfig.class.getCanonicalName());


    /**
     * The method that from the location of the crate generates the html file.
     *
     * @param location the location of the crate in the filesystem.
     */
    public static void generatePreview(String location) throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        String[] parts = location.split("/");
        int numParts = parts.length;
        String roCratePath = String.join("/", parts[numParts - 4], parts[numParts - 3], parts[numParts - 2], parts[numParts - 1]);
        String previewGeneratorAddress = ArpConfig.instance.get("arp.rocrate.previewgenerator.address");
        String uriString = previewGeneratorAddress + "/" + roCratePath;
        HttpRequest request = HttpRequest.newBuilder()
                .uri(new URI(uriString))
                .GET()
                .build();
        HttpResponse<String> previewGeneratorResponse = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (previewGeneratorResponse.statusCode() != 200) {
            throw new Exception("Failed to generate HTML preview for the roCrate with path: " + location + "\n" + previewGeneratorResponse.body());
        }
    }
}
