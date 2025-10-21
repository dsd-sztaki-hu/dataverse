package edu.harvard.iq.dataverse.mocks;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import edu.harvard.iq.dataverse.arp.ArpServiceBean;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/**
 * Mock implementation of ArpServiceBean for testing purposes.
 * This class provides consistent mocking behavior across all tests.
 */
public class MockArpService extends ArpServiceBean {

    private static final Gson gson = new Gson();
    private JsonObject fileClassEnJson;

    public MockArpService() {
        super();
        try {
            // Load the actual fileClass.en.json file
            String fileClassEnContent = Files.readString(Paths.get("src/main/resources/arp/fileClass.en.json"));
            fileClassEnJson = gson.fromJson(fileClassEnContent, JsonObject.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load fileClass.en.json", e);
        }
    }

    @Override
    public JsonObject getFileClassEn() {
        return fileClassEnJson;
    }

    @Override
    public boolean hasExternalValues(JsonObject cedarFieldTemplate) {
        if (cedarFieldTemplate == null || !cedarFieldTemplate.has("_valueConstraints")) {
            return false;
        }

        JsonObject valueConstraints = cedarFieldTemplate.getAsJsonObject("_valueConstraints");
        return valueConstraints.has("branches") ||
                valueConstraints.has("valueSets") ||
                valueConstraints.has("ontologies");
    }

    @Override
    public List<String> collectExternalVocabStrings(List<String> urls, JsonObject cedarFieldTemplate)
            throws IOException, InterruptedException, NoSuchAlgorithmException, KeyManagementException, URISyntaxException {
        // Return empty list for external vocab strings to avoid actual HTTP calls
        return new ArrayList<>();
    }

    @Override
    public List<String> getExternalVocabValuesUrls(JsonObject cedarFieldTemplate) {
        // Return empty list to avoid actual HTTP calls
        return new ArrayList<>();
    }
}
