package edu.harvard.iq.dataverse.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.*;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.response.Response;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
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

import static com.jayway.restassured.RestAssured.given;
import static java.net.http.HttpResponse.BodyHandlers.ofString;
import static javax.ws.rs.core.Response.Status.OK;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

public class ArpCedarIT {
    
    private static final Logger logger = Logger.getLogger(ArpCedarIT.class.getCanonicalName());

    public static final String API_TOKEN_HTTP_HEADER = "X-Dataverse-key";
    
    private static final String TEST_FOLDER_NAME = "arpCedarITFolder";
    
    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
    
    private static JsonObject cedarParams;
    
    @BeforeClass
    public static void initCEDAR() {
        try {
            HttpClient httpClient = getUnsafeHttpClient();
            String params = Files.readString(Paths.get("src/test/resources/arp/cedarParams.json"));
            JsonObject originalParams = gson.fromJson(params, JsonObject.class);
            JsonObject testParams = originalParams.getAsJsonObject("cedarParams");
            String parentIdFromEnv = System.getenv("CEDAR_PARENT_FOLDER_ID_FOR_THE_TESTS");
            String apiKeyFromEnv = System.getenv("CEDAR_API_KEY");
            String domainFromEnv = System.getenv("CEDAR_DOMAIN");
            if (parentIdFromEnv != null) {
                testParams.addProperty("parentFolderIdForTheTests", parentIdFromEnv);
            }
            if (apiKeyFromEnv != null) {
                testParams.addProperty("apiKey", apiKeyFromEnv);
            }
            if (domainFromEnv != null) {
                testParams.addProperty("cedarDomain", domainFromEnv);
            }
            
            String parentFolderId = testParams.get("parentFolderIdForTheTests").getAsString();
            String apiKey = testParams.get("apiKey").getAsString();
            String cedarDomain = testParams.get("cedarDomain").getAsString();
            
            //In case the test folder is present before running the tests, remove it
            checkAndDeleteTestFolderIfExists(httpClient, parentFolderId, apiKey, cedarDomain);
            
            String testFolderId = createFolder(parentFolderId, cedarDomain, apiKey, httpClient);
            testParams.addProperty("folderId", testFolderId);
            originalParams.add("cedarParams", testParams);
            cedarParams = originalParams;
        } catch (Exception e) {
            e.printStackTrace();
            assertEquals(0,1);
        }
    }

    @BeforeClass
    public static void setUpClass() {
        RestAssured.baseURI = UtilIT.getRestAssuredBaseUri();
    }
    
    @AfterClass
    public static void removeTestDataFromCEDAR() throws Exception {
        JsonObject params = cedarParams.getAsJsonObject("cedarParams");
        String encodedFolderId = URLEncoder.encode(params.get("folderId").getAsString(), StandardCharsets.UTF_8);
        deleteFolderAndContents(encodedFolderId, params.get("apiKey").getAsString(), params.get("cedarDomain").getAsString());
    }
    
    @Test
    public void uploadCitation() {
        try {
            HttpClient httpClient = getUnsafeHttpClient();
            Response createUser = UtilIT.createRandomUser();
            String apiToken = UtilIT.getApiTokenFromResponse(createUser);
            exportAndCheckTemplate("1", "citation", "src/test/resources/arp/citation.json", httpClient, apiToken);    
        } catch (Exception e) {
            e.printStackTrace();
            assertEquals(0,1);
        }
    }

    @Test
    public void uploadGeospatial() {
        try {
            HttpClient httpClient = getUnsafeHttpClient();
            Response createUser = UtilIT.createRandomUser();
            String apiToken = UtilIT.getApiTokenFromResponse(createUser);
            exportAndCheckTemplate("2", "geospatial", "src/test/resources/arp/geospatial.json", httpClient, apiToken);
        } catch (Exception e) {
            e.printStackTrace();
            assertEquals(0,1);
        }
    }

    @Test
    public void uploadSocialScience() {
        try {
            HttpClient httpClient = getUnsafeHttpClient();
            Response createUser = UtilIT.createRandomUser();
            String apiToken = UtilIT.getApiTokenFromResponse(createUser);
            exportAndCheckTemplate("3", "socialscience", "src/test/resources/arp/socialscience.json", httpClient, apiToken);
        } catch (Exception e) {
            e.printStackTrace();
            assertEquals(0,1);
        }
    }

    @Test
    public void uploadAstrophysics() {
        try {
            HttpClient httpClient = getUnsafeHttpClient();
            Response createUser = UtilIT.createRandomUser();
            String apiToken = UtilIT.getApiTokenFromResponse(createUser);
            exportAndCheckTemplate("4", "astrophysics", "src/test/resources/arp/astrophysics.json", httpClient, apiToken);
        } catch (Exception e) {
            e.printStackTrace();
            assertEquals(0,1);
        }
    }

    @Test
    public void uploadBiomedical() {
        try {
            HttpClient httpClient = getUnsafeHttpClient();
            Response createUser = UtilIT.createRandomUser();
            String apiToken = UtilIT.getApiTokenFromResponse(createUser);
            exportAndCheckTemplate("5", "biomedical", "src/test/resources/arp/biomedical.json", httpClient, apiToken);
        } catch (Exception e) {
            e.printStackTrace();
            assertEquals(0,1);
        }
    }

    @Test
    public void uploadJournal() {
        try {
            HttpClient httpClient = getUnsafeHttpClient();
            Response createUser = UtilIT.createRandomUser();
            String apiToken = UtilIT.getApiTokenFromResponse(createUser);
            exportAndCheckTemplate("6", "journal", "src/test/resources/arp/journal.json", httpClient, apiToken);
        } catch (Exception e) {
            e.printStackTrace();
            assertEquals(0,1);
        }
    }
    
    private static void exportAndCheckTemplate(String mdbIdtf, String templateName, String templatePath, HttpClient httpClient, String apiToken) {
        Response exportMdbResponse = exportMdb(apiToken, mdbIdtf, gson.toJson(cedarParams).getBytes());
        assertEquals(200, exportMdbResponse.getStatusCode());
        exportMdbResponse.then().assertThat().statusCode(OK.getStatusCode());
        
        JsonObject params = cedarParams.getAsJsonObject("cedarParams");

        JsonArray folderContent = listFolderContent(httpClient, params.get("folderId").getAsString(), params.get("apiKey").getAsString(), params.get("cedarDomain").getAsString());
        boolean containsTemplate = false;
        String templateId = null;
        for (JsonElement entry : folderContent) {
            JsonObject jsonObject = entry.getAsJsonObject();
            if (jsonObject.get("schema:name").getAsString().equals(templateName) && jsonObject.get("resourceType").getAsString().equals("folder")) {
                JsonArray templateFolderContent = listFolderContent(httpClient, jsonObject.get("@id").getAsString(), params.get("apiKey").getAsString(), params.get("cedarDomain").getAsString());
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
        JSONAssert.assertEquals(templateContent,  uploadedTemplateContent, new CustomComparator(
                JSONCompareMode.LENIENT,
                new Customization("**.@id", (t1, t2) -> true),
                new Customization("**:createdOn", (t1, t2) -> true),
                new Customization("**:lastUpdatedOn", (t1, t2) -> true),
                new Customization("**::createdBy", (t1, t2) -> true),
                new Customization("**:modifiedBy", (t1, t2) -> true)
        ));
    }

    static Response exportMdb(String apiToken, String mdbIdtf, byte[] body) {
        return given()
                .header(API_TOKEN_HTTP_HEADER, apiToken)
                .body(body)
                .contentType("application/json; charset=utf-8")
                .post("/api/arp/exportTsvToCedar/" + mdbIdtf);
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
        JsonObject params = cedarParams.getAsJsonObject("cedarParams");
        
        try {
            String url = String.format("https://resource.%s/templates/", params.get("cedarDomain").getAsString()) + encodedTemplateId;
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Authorization", "apiKey " + params.get("apiKey").getAsString())
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
            throw new Exception("An error occurred during creating new folder: arpCedarITtestFolder");
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
    
}
