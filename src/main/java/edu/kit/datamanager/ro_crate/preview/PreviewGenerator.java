package edu.kit.datamanager.ro_crate.preview;

import edu.harvard.iq.dataverse.arp.ArpConfig;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

// Note: this class have been copied from the original ro_crate project and updated to call our preview generation
// service running as a standalone service.

/**
 * Class responsible for the generation of the human-readable representation of the metadata.
 */
public class PreviewGenerator {

    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

    // This is a solution for the following edge case: a dataset is open in AROMA and the Dataverse is restarted. Then if
    // we try to edit and save the dataset in AROMA the preview generation will fail because  ArpConfig.instance is not
    // initialized until any Dataverse UI operation is initiated. This should however not really happen in real life so
    // no need for this forced ArpConfig initialization. And this is also quite fragile because the EJB name is bound to
    // the current dataverse varsion (java:global/dataverse-5.13) and sowith every DV update we also have to take care
    // to update this._
    //
    static {
        ArpConfig.ensureStaticInstance();
    }

    // There is no need to check if the rocrate preview generator is available,
    // we use our hosted generator, if it is not available we can not generate the preview
    public static boolean isRochtmlAvailable() {
        return true;
    }
    
    /**
     * The method that from the location of the crate generates the html file.
     *
     * @param location the location of the crate in the filesystem.
     */
    public static void generatePreview(String location) throws Exception {
        String roCrateMetadataJson = Files.readString(Paths.get(location, "ro-crate-metadata.json"), StandardCharsets.UTF_8);
        generatePreview(location, roCrateMetadataJson);
    }

    /**
     * Generates the HTML preview from in-memory metadata JSON so the file is not re-read from disk.
     */
    public static void generatePreview(String location, String roCrateMetadataJson) throws Exception {
        String previewGeneratorAddress = ArpConfig.instance.get("arp.rocrate.previewgenerator.address");
        HttpRequest request = HttpRequest.newBuilder()
                .uri(new URI(previewGeneratorAddress))
                .POST(HttpRequest.BodyPublishers.ofString(roCrateMetadataJson, StandardCharsets.UTF_8))
                .build();
        HttpResponse<String> resp = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            throw new Exception("Failed to generate HTML preview for the roCrate with path: " + location + "\n" + resp.body());
        }
        Files.writeString(Paths.get(location, "ro-crate-preview.html"), resp.body(), StandardCharsets.UTF_8);
    }
}
