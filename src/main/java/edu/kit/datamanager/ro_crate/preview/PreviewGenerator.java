package edu.kit.datamanager.ro_crate.preview;

import edu.harvard.iq.dataverse.arp.ArpConfig;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
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

// This is a solution for the following edge case: a dataset is open in AROMA and the Dataverse is restarted. Then if
// we try to edit and save the dataset in AROMA the preview generation will fail because  ArpConfig.instance is not
// initialized until any Dataverse UI operation is initiated. This should however not really happen in real life so
// no need for this forced ArpConfig initialization. And this is also quite fragile because the EJB name is bound to
// the current dataverse varsion (java:global/dataverse-5.13) and sowith every DV update we also have to take care
// to update this._
//
//    static {
//        try {
//            InitialContext ic = new InitialContext();
//            ArpConfig arpConfig = (ArpConfig) ic.lookup("java:global/dataverse-5.13/ArpConfig");
//        } catch (NamingException e) {
//            logger.severe("java:global/dataverse-5.13/ArpConfig is not available. Make sure the version used for lookup matches the curreent Dataverse version" );
//            e.printStackTrace();
//        }
//    }

    /**
     * The method that from the location of the crate generates the html file.
     *
     * @param location the location of the crate in the filesystem.
     */
    public static void generatePreview(String location) throws Exception {
        String roCrateMetadataJson = new String(Files.readAllBytes(Paths.get(location+"/ro-crate-metadata.json")), java.nio.charset.StandardCharsets.UTF_8);
        HttpClient client = HttpClient.newHttpClient();
        String previewGeneratorAddress = ArpConfig.instance.get("arp.rocrate.previewgenerator.address");
        HttpRequest request = HttpRequest.newBuilder()
                .uri(new URI(previewGeneratorAddress))
                .POST(HttpRequest.BodyPublishers.ofString(roCrateMetadataJson, StandardCharsets.UTF_8))
                .build();
        HttpResponse<String> resp = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            throw new Exception("Failed to generate HTML preview for the roCrate with path: " + location + "\n" + resp.body());
        }
        Files.writeString(Paths.get(location+"/ro-crate-preview.html"), resp.body(), StandardCharsets.UTF_8);
    }
}
