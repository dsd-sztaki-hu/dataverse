package edu.harvard.iq.dataverse.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.*;
import io.restassured.RestAssured;
import io.restassured.response.Response;
import edu.harvard.iq.dataverse.arp.ExportToCedarParams;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.Customization;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.skyscreamer.jsonassert.comparator.CustomComparator;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import static io.restassured.RestAssured.given;
import static java.net.http.HttpResponse.BodyHandlers.ofString;
import static jakarta.ws.rs.core.Response.Status.OK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ArpCedarIT {
    
    private static final Logger logger = Logger.getLogger(ArpCedarIT.class.getCanonicalName());

    public static final String API_TOKEN_HTTP_HEADER = "X-Dataverse-key";
    
    private static final String TEST_FOLDER_NAME = "arpCedarITFolder";
    
    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
    
    private static ExportToCedarParams cedarParams;
    
    private static String cedarUUID;
    
    @BeforeEach
    public void initCEDAR() {
        try {
            HttpClient httpClient = getUnsafeHttpClient();
            String params = Files.readString(Paths.get("src/test/resources/arp/cedarParams.json"));
            JsonObject testParams = gson.fromJson(params, JsonObject.class);
            String parentIdFromEnv = System.getenv("CEDAR_PARENT_FOLDER_ID_FOR_THE_TESTS");
            String apiKeyFromEnv = System.getenv("CEDAR_API_KEY");
            String domainFromEnv = System.getenv("CEDAR_DOMAIN");
            String cedarUUIDFromEnv = System.getenv("CEDAR_UUID");
            if (parentIdFromEnv != null) {
                testParams.addProperty("folderId", parentIdFromEnv);
            }
            if (apiKeyFromEnv != null) {
                testParams.addProperty("apiKey", apiKeyFromEnv);
            }
            if (domainFromEnv != null) {
                testParams.addProperty("cedarDomain", domainFromEnv);
            }
            if (cedarUUIDFromEnv != null) {
                testParams.addProperty("cedarUUID", cedarUUIDFromEnv);
            }
            
            String parentFolderId = testParams.get("folderId").getAsString();
            String apiKey = testParams.get("apiKey").getAsString();
            String cedarDomain = testParams.get("cedarDomain").getAsString();
            cedarUUID = testParams.get("cedarUUID").getAsString();
            
            //In case the test folder is present before running the tests, remove it
            checkAndDeleteTestFolderIfExists(httpClient, parentFolderId, apiKey, cedarDomain);
            
            String testFolderId = createFolder(parentFolderId, cedarDomain, apiKey, httpClient);
            testParams.addProperty("folderId", testFolderId);
            cedarParams = new ExportToCedarParams();
            cedarParams.cedarDomain = cedarDomain;
            cedarParams.folderId = testFolderId;
            cedarParams.apiKey = apiKey;
        } catch (Exception e) {
            e.printStackTrace();
            assertEquals(0,1);
        }
    }

    @BeforeEach
    public void setUpClass() {
        RestAssured.baseURI = UtilIT.getRestAssuredBaseUri();
    }
    
    @AfterEach
    public void removeTestDataFromCEDAR() throws Exception {
        String encodedFolderId = URLEncoder.encode(cedarParams.folderId, StandardCharsets.UTF_8);
        deleteFolderAndContents(encodedFolderId, cedarParams.apiKey, cedarParams.cedarDomain);
    }
    
    @Test
    public void uploadCitation() {
        try {
            HttpClient httpClient = getUnsafeHttpClient();
            String apiToken = createRandomSuperUser();
            exportAndCheckTemplate("citation", "Citation Metadata", "src/test/resources/arp/citation.json", httpClient, apiToken);    
        } catch (Exception e) {
            e.printStackTrace();
            assertEquals(0,1);
        }
    }

    @Test
    public void uploadGeospatial() {
        try {
            HttpClient httpClient = getUnsafeHttpClient();
            String apiToken = createRandomSuperUser();
            exportAndCheckTemplate("geospatial", "Geospatial Metadata", "src/test/resources/arp/geospatial.json", httpClient, apiToken);
        } catch (Exception e) {
            e.printStackTrace();
            assertEquals(0,1);
        }
    }

    @Test
    public void uploadSocialScience() {
        try {
            HttpClient httpClient = getUnsafeHttpClient();
            String apiToken = createRandomSuperUser();
            exportAndCheckTemplate("socialscience", "Social Science and Humanities Metadata", "src/test/resources/arp/socialscience.json", httpClient, apiToken);
        } catch (Exception e) {
            e.printStackTrace();
            assertEquals(0,1);
        }
    }

    @Test
    public void uploadAstrophysics() {
        try {
            HttpClient httpClient = getUnsafeHttpClient();
            String apiToken = createRandomSuperUser();
            exportAndCheckTemplate("astrophysics", "Astronomy and Astrophysics Metadata", "src/test/resources/arp/astrophysics.json", httpClient, apiToken);
        } catch (Exception e) {
            e.printStackTrace();
            assertEquals(0,1);
        }
    }

    @Test
    public void uploadBiomedical() {
        try {
            HttpClient httpClient = getUnsafeHttpClient();
            String apiToken = createRandomSuperUser();
            exportAndCheckTemplate("biomedical", "Life Sciences Metadata", "src/test/resources/arp/biomedical.json", httpClient, apiToken);
        } catch (Exception e) {
            e.printStackTrace();
            assertEquals(0,1);
        }
    }

    @Test
    public void uploadJournal() {
        try {
            HttpClient httpClient = getUnsafeHttpClient();
            String apiToken = createRandomSuperUser();
            exportAndCheckTemplate("journal", "Journal Metadata", "src/test/resources/arp/journal.json", httpClient, apiToken);
        } catch (Exception e) {
            e.printStackTrace();
            assertEquals(0,1);
        }
    }
    
    private static void exportAndCheckTemplate(String mdbIdtf, String templateName, String templatePath, HttpClient httpClient, String apiToken) {
        Response exportMdbResponse = exportMdb(apiToken, mdbIdtf, gson.toJson(cedarParams));
        assertEquals(200, exportMdbResponse.getStatusCode());
        exportMdbResponse.then().assertThat().statusCode(OK.getStatusCode());
        
        JsonArray folderContent = listFolderContent(httpClient, cedarParams.folderId, cedarParams.apiKey, cedarParams.cedarDomain);
        boolean containsTemplate = false;
        String templateId = null;
        for (JsonElement entry : folderContent) {
            JsonObject jsonObject = entry.getAsJsonObject();
            if (jsonObject.get("schema:name").getAsString().equals(templateName) && jsonObject.get("resourceType").getAsString().equals("folder")) {
                JsonArray templateFolderContent = listFolderContent(httpClient, jsonObject.get("@id").getAsString(), cedarParams.apiKey, cedarParams.cedarDomain);
                for (JsonElement templateFolderEntry : templateFolderContent) {
                    JsonObject templateObject = templateFolderEntry.getAsJsonObject();
                    if (templateObject.get("schema:name").getAsString().equals(templateName) && templateObject.get("resourceType").getAsString().equals("template")) {
                        containsTemplate = true;
                        templateId = templateObject.get("@id").getAsString();
                        break;
                    }
                }
            }
        }
        assertTrue(containsTemplate);

        String templateContent = null;
        try {
            templateContent = Files.readString(Paths.get(templatePath));
        } catch (IOException e) {
            logger.warning(e.getMessage());
            assertEquals(0,1);
        }
        
        String uploadedTemplateContent =  getTemplate(httpClient, templateId);
        logger.info("Expected template: \n" + templateContent);
        logger.info("Actual template: \n" + gson.toJson(JsonParser.parseString(uploadedTemplateContent)));
        
        JSONAssert.assertEquals(templateContent,  uploadedTemplateContent, new CustomComparator(
                JSONCompareMode.LENIENT,
                new Customization("**.@id", (t1, t2) -> true),
                new Customization("**:createdOn", (t1, t2) -> true),
                new Customization("**:lastUpdatedOn", (t1, t2) -> true),
                new Customization("**:createdBy", (t1, t2) -> true),
                new Customization("**:modifiedBy", (t1, t2) -> true)
        ));
    }

    static Response exportMdb(String apiToken, String mdbIdtf, String body) {
        return given()
                .header(API_TOKEN_HTTP_HEADER, apiToken)
                .queryParam("uuid", cedarUUID)
                .body(body)
                .contentType("application/json; charset=utf-8")
                .post("/api/arp/exportMdbToCedar/" + mdbIdtf);
    }

    private static JsonArray listFolderContent(HttpClient client, String folderId, String apiKey, String cedarDomain) {
        JsonArray resources = null;
        try {
            String encodedFolderId = URLEncoder.encode(folderId, StandardCharsets.UTF_8);
            String url = String.format("https://resource.%s/folders/%s/contents", cedarDomain, encodedFolderId);
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Authorization", "apiKey " + apiKey)
                    .header("Content-Type", "application/json")
                    .build();
            HttpResponse<String> listFolderResponse = client.send(request, ofString());
            if (listFolderResponse.statusCode() != 200) {
                throw new Exception("An error occurred during listing conten of folder: " + TEST_FOLDER_NAME);
            }
            resources = gson.fromJson(listFolderResponse.body(), JsonObject.class).getAsJsonArray("resources");
        } catch (Exception e) {
            e.printStackTrace();
            assertEquals(0,1);
        }
        return resources;
    }

    private static String getTemplate(HttpClient client, String templateId) {
        String template = null;
        String encodedTemplateId = URLEncoder.encode(templateId, StandardCharsets.UTF_8);
        
        try {
            String url = String.format("https://resource.%s/templates/", cedarParams.cedarDomain) + encodedTemplateId;
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Authorization", "apiKey " + cedarParams.apiKey)
                    .header("Content-Type", "application/json")
                    .build();
            HttpResponse<String> getTemplateResponse = client.send(request, ofString());
            if (getTemplateResponse.statusCode() != 200) {
                throw new Exception("An error occurred during getting template: " + templateId);
            }
            template = getTemplateResponse.body();
        } catch (Exception e) {
            e.printStackTrace();
            assertEquals(0,1);
        }
        return template;
    }
    
    private static HttpClient getUnsafeHttpClient() throws NoSuchAlgorithmException, KeyManagementException {
        TrustManager[] trustAllCerts = new TrustManager[] { new X509TrustManager() {
            public X509Certificate[] getAcceptedIssuers() {
                return new X509Certificate[0];
            }
            public void checkClientTrusted(X509Certificate[] certs, String authType) {}
            public void checkServerTrusted(X509Certificate[] certs, String authType) {}
        } };
        SSLContext sslContext = SSLContext.getInstance("SSL");
        sslContext.init(null, trustAllCerts, new SecureRandom());
        return HttpClient.newBuilder()
                .sslContext(sslContext)
                .build();
    }

    private static String createFolder(String parentFolderId, String cedarDomain, String apiKey, HttpClient client) throws Exception {
        String url = "https://resource." + cedarDomain + "/folders";
        Map<String, Object> data = new HashMap<>();
        data.put("folderId", parentFolderId);
        data.put("name", TEST_FOLDER_NAME);
        data.put("description", TEST_FOLDER_NAME + " description");
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .POST(HttpRequest.BodyPublishers.ofString(new ObjectMapper().writeValueAsString(data)))
                .header("Authorization", "apiKey " + apiKey)
                .header("Content-Type", "application/json")
                .build();
        HttpResponse<String> createFolderResponse = client.send(request, ofString());
        if (createFolderResponse.statusCode() != 201) {
            throw new Exception("An error occurred during creating new folder: arpCedarITtestFolder: "+createFolderResponse.body());
        }
        return new ObjectMapper().readTree(createFolderResponse.body()).get("@id").textValue();
    }

    private static void deleteFolderAndContents(String folderId, String apiKey, String baseDomain) throws Exception {
        String url = "https://resource." + baseDomain + "/folders/" + folderId + "/contents";
        HttpClient httpClient = getUnsafeHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Authorization", "apiKey " + apiKey)
                .GET()
                .build();
        
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            logger.severe("Error: Could not get contents for folder " + folderId);
            return;
        }
        
        JsonObject contents = gson.fromJson(response.body(), JsonObject.class);
        
        for (JsonElement element : contents.getAsJsonArray("resources")) {
            JsonObject item = element.getAsJsonObject();
            String itemId = URLEncoder.encode(item.get("@id").getAsString(), StandardCharsets.UTF_8);

            switch (item.get("resourceType").getAsString()) {
                case "folder":
                    deleteFolderAndContents(itemId, apiKey, baseDomain);
                    break;
                case "element":
                    response = httpClient.send(HttpRequest.newBuilder()
                            .uri(URI.create(String.format("https://resource.%s/template-elements/%s", baseDomain, itemId)))
                            .header("Authorization", "apiKey " + apiKey)
                            .DELETE()
                            .build(), HttpResponse.BodyHandlers.ofString());
                    if (response.statusCode() != 204) {
                        System.out.println(response.body());
                        System.out.println("Error: Could not delete template element " + itemId);
                    }
                    break;
                case "template":
                    response = httpClient.send(HttpRequest.newBuilder()
                            .uri(URI.create(String.format("https://resource.%s/templates/%s", baseDomain, itemId)))
                            .header("Authorization", "apiKey " + apiKey)
                            .DELETE()
                            .build(), HttpResponse.BodyHandlers.ofString());
                    if (response.statusCode() != 204) {
                        System.out.println(response.body());
                        System.out.println("Error: Could not delete template " + itemId);
                    }
                    break;
                case "field":
                    response = httpClient.send(HttpRequest.newBuilder()
                            .uri(URI.create(String.format("https://resource.%s/template-fields/%s", baseDomain, itemId)))
                            .header("Authorization", "apiKey " + apiKey)
                            .DELETE()
                            .build(), HttpResponse.BodyHandlers.ofString());
                    if (response.statusCode() != 204) {
                        System.out.println(response.body());
                        System.out.println("Error: Could not delete template field " + itemId);
                    }
                    break;
            }
        }
        
        response = httpClient.send(HttpRequest.newBuilder()
                .uri(URI.create(String.format("https://resource.%s/folders/%s", baseDomain, folderId)))
                .header("Authorization", "apiKey " + apiKey)
                .DELETE()
                .build(), HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 204) {
            System.out.println("Error: Could not delete folder " + folderId);
        }
    }
    
    private static void checkAndDeleteTestFolderIfExists(HttpClient httpClient, String folderId, String apiKey, String cedarDomain) throws Exception {
        JsonArray folderContent = listFolderContent(httpClient, folderId, apiKey, cedarDomain);
        for (JsonElement entry : folderContent) {
            JsonObject jsonObject = entry.getAsJsonObject();
            if (jsonObject.get("resourceType").getAsString().equals("folder") && jsonObject.get("schema:name").getAsString().equals(TEST_FOLDER_NAME)) {
                String encodedFolderId = URLEncoder.encode(jsonObject.get("@id").getAsString(), StandardCharsets.UTF_8);
                deleteFolderAndContents(encodedFolderId, apiKey, cedarDomain);
            }
        }
    }

    public String createRandomSuperUser() {
        Response createUser = UtilIT.createRandomUser();
        createUser.prettyPrint();
        assertEquals(200, createUser.getStatusCode());
        String username = UtilIT.getUsernameFromResponse(createUser);
        String apiToken = UtilIT.getApiTokenFromResponse(createUser);
        Response makeSuperUser = UtilIT.makeSuperUser(username);
        assertEquals(200, makeSuperUser.getStatusCode());
        return apiToken;
    }
    
}
