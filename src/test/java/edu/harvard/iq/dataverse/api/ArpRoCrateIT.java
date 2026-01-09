package edu.harvard.iq.dataverse.api;

import com.google.gson.*;
import edu.harvard.iq.dataverse.settings.SettingsServiceBean;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.restassured.path.json.JsonPath;
import io.restassured.response.Response;
import io.smallrye.common.constraint.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.logging.Logger;

import static edu.harvard.iq.dataverse.api.ApiConstants.*;
import static edu.harvard.iq.dataverse.api.UtilIT.API_TOKEN_HTTP_HEADER;
import static io.restassured.RestAssured.given;
import static jakarta.ws.rs.core.Response.Status.*;
import static org.hamcrest.CoreMatchers.*;

/**
 * Test RoCrate editing an RoCrate-Dataset synchronisation. Based on DatasetsIT.java.
 */
public class ArpRoCrateIT {

    private static final Logger logger = Logger.getLogger(ArpRoCrateIT.class.getCanonicalName());

    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    @BeforeAll
    public static void setUpClass() {

        RestAssured.registerParser("text/plain", io.restassured.parsing.Parser.JSON);

        RestAssured.baseURI = UtilIT.getRestAssuredBaseUri();

        Response removeIdentifierGenerationStyle = UtilIT.deleteSetting(SettingsServiceBean.Key.IdentifierGenerationStyle);
        removeIdentifierGenerationStyle.then().assertThat()
                .statusCode(200);

        Response removeExcludeEmail = UtilIT.deleteSetting(SettingsServiceBean.Key.ExcludeEmailFromExport);
        removeExcludeEmail.then().assertThat()
                .statusCode(200);

        Response removeAnonymizedFieldTypeNames = UtilIT.deleteSetting(SettingsServiceBean.Key.AnonymizedFieldTypeNames);
        removeAnonymizedFieldTypeNames.then().assertThat()
                .statusCode(200);

        UtilIT.deleteSetting(SettingsServiceBean.Key.MaxEmbargoDurationInMonths);

        /* With Dual mode, we can no longer mess with upload methods since native is now required for anything to work
               
        Response removeDcmUrl = UtilIT.deleteSetting(SettingsServiceBean.Key.DataCaptureModuleUrl);
        removeDcmUrl.then().assertThat()
                .statusCode(200);

        Response removeUploadMethods = UtilIT.deleteSetting(SettingsServiceBean.Key.UploadMethods);
        removeUploadMethods.then().assertThat()
                .statusCode(200);
         */
    }


    @AfterAll
    public static void afterClass() {
        RestAssured.unregisterParser("text/plain");

        Response removeIdentifierGenerationStyle = UtilIT.deleteSetting(SettingsServiceBean.Key.IdentifierGenerationStyle);
        removeIdentifierGenerationStyle.then().assertThat()
                .statusCode(200);

        Response removeExcludeEmail = UtilIT.deleteSetting(SettingsServiceBean.Key.ExcludeEmailFromExport);
        removeExcludeEmail.then().assertThat()
                .statusCode(200);

        Response removeAnonymizedFieldTypeNames = UtilIT.deleteSetting(SettingsServiceBean.Key.AnonymizedFieldTypeNames);
        removeAnonymizedFieldTypeNames.then().assertThat()
                .statusCode(200);

        UtilIT.deleteSetting(SettingsServiceBean.Key.MaxEmbargoDurationInMonths);

        /* See above
        Response removeDcmUrl = UtilIT.deleteSetting(SettingsServiceBean.Key.DataCaptureModuleUrl);
        removeDcmUrl.then().assertThat()
                .statusCode(200);

        Response removeUploadMethods = UtilIT.deleteSetting(SettingsServiceBean.Key.UploadMethods);
        removeUploadMethods.then().assertThat()
                .statusCode(200);
         */
    }

    static Response updateDatasetMetadataJsonViaNative(String persistentId, String json, String apiToken) {
        Response response = given()
                .header(API_TOKEN_HTTP_HEADER, apiToken)
                .body(json)
                .contentType("application/json")
                .put("/api/datasets/:persistentId/versions/" + DS_VERSION_DRAFT + "?persistentId=" + persistentId);
        return response;
    }

    public static class TestSetup {
        public String username;
        public String apiToken;
        public String dataverseAlias;
        public Integer datasetId;
        public String datasetPersistentId;
    }

    /**
     * Helper method to create a random user, dataverse, and dataset for testing purposes.
     * This method sets up a complete environment for running tests, including:
     * - Creating a random user with an API token
     * - Creating a random dataverse and publishing it
     * - Creating a random dataset within the dataverse
     * - Retrieving the dataset's persistent ID
     *
     * @return TestSetup object containing all the necessary information for the test setup
     */
    public static TestSetup createRandomUserDataverseAndDataset() {
        TestSetup setup = new TestSetup();

        // Create random user
        Response createUserResponse = UtilIT.createRandomUser();
        createUserResponse.then().assertThat().statusCode(OK.getStatusCode());
        setup.username = UtilIT.getUsernameFromResponse(createUserResponse);
        setup.apiToken = UtilIT.getApiTokenFromResponse(createUserResponse);
    
        // Create random dataverse
        Response createDataverseResponse = UtilIT.createRandomDataverse(setup.apiToken);
        createDataverseResponse.then().assertThat().statusCode(CREATED.getStatusCode());
        setup.dataverseAlias = UtilIT.getAliasFromResponse(createDataverseResponse);

        // Publish dataverse
        Response publishDataverseResponse = UtilIT.publishDataverseViaSword(setup.dataverseAlias, setup.apiToken);
        publishDataverseResponse.then().assertThat().statusCode(OK.getStatusCode());

        // Create random dataset
        Response createDatasetResponse = UtilIT.createRandomDatasetViaNativeApi(setup.dataverseAlias, setup.apiToken);
        createDatasetResponse.then().assertThat().statusCode(CREATED.getStatusCode());
        setup.datasetId = UtilIT.getDatasetIdFromResponse(createDatasetResponse);

        // Get dataset persistent ID
        Response datasetAsJson = UtilIT.nativeGet(setup.datasetId, setup.apiToken);
        datasetAsJson.then().assertThat().statusCode(OK.getStatusCode());
        String protocol = JsonPath.from(datasetAsJson.getBody().asString()).getString("data.protocol");
        String authority = JsonPath.from(datasetAsJson.getBody().asString()).getString("data.authority");
        String identifier = JsonPath.from(datasetAsJson.getBody().asString()).getString("data.identifier");
        setup.datasetPersistentId = protocol + ":" + authority + "/" + identifier;

        return setup;
    }

    /**
     * Helper method to clean up the test environment created by createRandomUserDataverseAndDataset.
     * This method performs the following cleanup operations:
     * - Deletes the dataset
     * - Deletes the dataverse
     * - Deletes the user
     *
     * @param setup TestSetup object containing the information about the environment to be cleaned up
     */
    public static void cleanupUserDataverseAndDataset(TestSetup setup) {
        // Delete dataset
        Response deleteDatasetResponse = UtilIT.deleteDatasetViaNativeApi(setup.datasetId, setup.apiToken);
        deleteDatasetResponse.then().assertThat().statusCode(OK.getStatusCode());
    
        // Delete dataverse
        Response deleteDataverseResponse = UtilIT.deleteDataverse(setup.dataverseAlias, setup.apiToken);
        deleteDataverseResponse.then().assertThat().statusCode(OK.getStatusCode());
    
        // Delete user
        Response deleteUserResponse = UtilIT.deleteUser(setup.username);
        deleteUserResponse.then().assertThat().statusCode(OK.getStatusCode());
    }

    @Test
    public void testSomething() {
        TestSetup setup = createRandomUserDataverseAndDataset();

        // Upload a zip file to the dataset using SWORD
        String pathToFile = "scripts/search/data/binary/arp-test-dataset-files.zip";
        Response uploadResponse = UtilIT.uploadZipFileViaSword(setup.datasetPersistentId, pathToFile, setup.apiToken);
        uploadResponse.then().assertThat()
                .statusCode(CREATED.getStatusCode());

        Response datasetResponse = UtilIT.nativeGet(setup.datasetId, setup.apiToken);
        datasetResponse.prettyPrint();
        String originalJson = datasetResponse.getBody().asString();

        // Create an ArpDatasetMetadataEditor instance
        ArpDatasetMetadataEditor editor = new ArpDatasetMetadataEditor(originalJson);

        // Edit the title
        editor.editFieldLevelMetadata("citation", "title", "New Dataset Title");

        // Edit the author (complex field)
        JsonObject newAuthor = new JsonObject();
        JsonObject authorName = new JsonObject();
        authorName.addProperty("typeName", "authorName");
        authorName.addProperty("multiple", false);
        authorName.addProperty("typeClass", "primitive");
        authorName.addProperty("value", "New, Test User");
        newAuthor.add("authorName", authorName);

        JsonObject authorAffiliation = new JsonObject();
        authorAffiliation.addProperty("typeName", "authorAffiliation");
        authorAffiliation.addProperty("multiple", false);
        authorAffiliation.addProperty("typeClass", "primitive");
        authorAffiliation.addProperty("value", "New Affiliation");
        newAuthor.add("authorAffiliation", authorAffiliation);
        JsonArray jsonArray = new JsonArray();
        jsonArray.add(newAuthor);
        editor.editFieldLevelMetadata("citation", "author", jsonArray);

        // Get the updated JSON
        String updatedJson = editor.getCurrentJsonStateForUpdate();

        // Use the updated JSON to update the dataset
        Response updateResponse = updateDatasetMetadataJsonViaNative(setup.datasetPersistentId, updatedJson, setup.apiToken);
        updateResponse.then().assertThat().statusCode(OK.getStatusCode());

        // Verify the changes
        Response verifyResponse = UtilIT.nativeGet(setup.datasetId, setup.apiToken);
        verifyResponse.then().assertThat()
                .statusCode(OK.getStatusCode())
                .body("data.latestVersion.metadataBlocks.citation.fields.find { it.typeName == 'title' }.value", equalTo("New Dataset Title"))
                .body("data.latestVersion.metadataBlocks.citation.fields.find { it.typeName == 'author' }.value[0].authorName.value", equalTo("New, Test User"))
                .body("data.latestVersion.metadataBlocks.citation.fields.find { it.typeName == 'author' }.value[0].authorAffiliation.value", equalTo("New Affiliation"));

        //
        // Edit a file
        //

        // Get the file's ID
        Long origFileId = updateResponse.jsonPath().getLong("data.files.find { it.label == '1200px-Pushkin_population_history.svg.png' }.dataFile.id ");
        String origFileName = updateResponse.jsonPath().getString("data.files.find { it.label == '1200px-Pushkin_population_history.svg.png' }.dataFile.filename ");

        // Note: getting the draft (getDataFileMetadataDraft) and published (getDataFileMetadata) metadata are separate calls
        Response getMetadataResponse = UtilIT.getDataFileMetadataDraft(origFileId, setup.apiToken);
        getMetadataResponse.prettyPrint();
        getMetadataResponse.then().assertThat()
                .statusCode(OK.getStatusCode())
                .body("label", equalTo("1200px-Pushkin_population_history.svg.png"))
                .body("description", equalTo(""));

        // Update label and description
        JsonObject jsonObject = gson.fromJson(getMetadataResponse.body().asString(), JsonObject.class);
        jsonObject.addProperty("label", "Nice filename.png");
        jsonObject.addProperty("description", "This is a nice file");
        Response updateFileMetadataResponse = UtilIT.updateFileMetadata(origFileId.toString(), gson.toJson(jsonObject), setup.apiToken);
        updateFileMetadataResponse.prettyPrint();
        updateFileMetadataResponse.then().assertThat()
                .statusCode(OK.getStatusCode());

        // Verify the changes
        Response getMetadataResponse2 = UtilIT.getDataFileMetadataDraft(origFileId, setup.apiToken);
        getMetadataResponse2.prettyPrint();
        getMetadataResponse2.then().assertThat()
                .statusCode(OK.getStatusCode())
                .body("label", equalTo("Nice filename.png"))
                .body("description", equalTo("This is a nice file"));

        cleanupUserDataverseAndDataset(setup);
    }


    @Test
    public void createDatasetAndEditInAroma() throws IOException
    {
        TestSetup setup = createRandomUserDataverseAndDataset();
    
        // Upload a zip file to the dataset using SWORD
        String pathToFile = "scripts/search/data/binary/arp-test-dataset-files.zip";
        Response uploadResponse = UtilIT.uploadZipFileViaSword(setup.datasetPersistentId, pathToFile, setup.apiToken);
        uploadResponse.then().assertThat()
                .statusCode(CREATED.getStatusCode());

        System.out.println("Initial Dataverse JSON");
        Response datasetResponse = UtilIT.nativeGet(setup.datasetId, setup.apiToken);
        datasetResponse.prettyPrint();

        System.out.println("Initial Ro-Crate metadata JSON to be comapred with rocrate1.json");
        Response roCrateResponse = getRoCrate(setup.datasetPersistentId, "DRAFT", setup.apiToken);
        String roCrateJson = roCrateResponse.prettyPrint();

        // Commpare with snapshot. Ignore ID and date related fields from comparison
        String rocrate1Json = Files.readString(Paths.get("src/test/resources/arp/rocrate-tests/rocrate1.json"));
        Assert.assertTrue(ArpJsonStructureComparator.compareJsonStructures(roCrateJson, rocrate1Json));


        String originalJson = datasetResponse.getBody().asString();
        // Create an ArpDatasetMetadataEditor instance
        ArpDatasetMetadataEditor editor = new ArpDatasetMetadataEditor(originalJson);

        // Edit the title
        editor.editFieldLevelMetadata("citation", "title", "Updated Darwin's Finches Study");

        // Edit the author (complex field)
        JsonObject newAuthor = new JsonObject();
        JsonObject authorName = new JsonObject();
        authorName.addProperty("typeName", "authorName");
        authorName.addProperty("multiple", false);
        authorName.addProperty("typeClass", "primitive");
        authorName.addProperty("value", "Darwin, Charles");
        newAuthor.add("authorName", authorName);
    
        JsonObject authorAffiliation = new JsonObject();
        authorAffiliation.addProperty("typeName", "authorAffiliation");
        authorAffiliation.addProperty("multiple", false);
        authorAffiliation.addProperty("typeClass", "primitive");
        authorAffiliation.addProperty("value", "HMS Beagle");
        newAuthor.add("authorAffiliation", authorAffiliation);
        JsonArray jsonArray = new JsonArray();
        jsonArray.add(newAuthor);
        editor.editFieldLevelMetadata("citation", "author", jsonArray);
    
        // Get the updated JSON
        String updatedJson = editor.getCurrentJsonStateForUpdate();
        System.out.println("updatedJson:\n" + updatedJson);
        // Use the updated JSON to update the dataset
        Response updateResponse = updateDatasetMetadataJsonViaNative(setup.datasetPersistentId, updatedJson, setup.apiToken);
        updateResponse.then().assertThat().statusCode(OK.getStatusCode());
    
        // Download and verify the updated RO-Crate metadata JSON
        System.out.println("Updated Ro-Crate metadata JSON to be comapred with rocrate2.json");
        Response roCrateResponse2 = getRoCrate(setup.datasetPersistentId, "DRAFT", setup.apiToken);
        String updatedRoCratejson = roCrateResponse2.prettyPrint();
        roCrateResponse2.then()
            .assertThat()
            .statusCode(OK.getStatusCode())
            .body("'@graph'.find { it.'@type' == 'Dataset' }.title", equalTo("Updated Darwin's Finches Study"))
            .body("'@graph'.find { it.'@type' == 'author' }.authorName", equalTo("Darwin, Charles"))
            .body("'@graph'.find { it.'@type' == 'author' }.authorAffiliation", equalTo("HMS Beagle"));

        String rocrate2Json = Files.readString(Paths.get("src/test/resources/arp/rocrate-tests/rocrate2.json"));
        //Assert.assertTrue(ArpJsonStructureComparator.compareJsonStructures(updatedRoCratejson, rocrate2Json));

        //
        // Now edit the dataset via RO-Crate and check if Dataverse JSON is updated
        //


        System.out.println("Updating RO-Crate JSON");
        JsonObject updatedRoCrate = new JsonParser().parse(updatedRoCratejson).getAsJsonObject();
        JsonArray graph = updatedRoCrate.getAsJsonArray("@graph");

        // Update the title
        for (JsonElement element : graph) {
            JsonObject obj = element.getAsJsonObject();
            if (obj.has("@type") && obj.get("@type").getAsString().equals("Dataset")) {
                obj.addProperty("title", "Updated Darwin's Finches Study in AROMA");
                break;
            }
        }

        // Add a new author
        JsonObject newAuthor2 = new JsonObject();
        newAuthor2.addProperty("@id", "#hooker-joseph"); // Add an @id for the new author
        newAuthor2.addProperty("@type", "author");
        newAuthor2.addProperty("authorName", "Hooker, Joseph");
        newAuthor2.addProperty("authorAffiliation", "Royal Botanical Gardens, Kew");
        newAuthor2.addProperty("name", "Hooker, Joseph; (Royal Botanical Gardens, Kew)");
        graph.add(newAuthor2);

        // Add reference to root dataset
        JsonObject newAuthor2Ref = new JsonObject();
        newAuthor2Ref.addProperty("@id", "#hooker-joseph");
        JsonObject rootDataset = updatedRoCrate.get("@graph").getAsJsonArray().get(0).getAsJsonObject();
        // At this poiunt we have a single author, so author is an object
        JsonObject author1 = rootDataset.getAsJsonObject().get("author").getAsJsonObject();
        // We crate an object and ad the original author as a reference as well as the new author
        JsonArray authors = new JsonArray();
        authors.add(author1);
        authors.add(newAuthor2Ref);
        rootDataset.getAsJsonObject().add("author", authors);


        // Send updated RO-Crate back to Dataverse
        Response updateRoCrateResponse = updateRoCrate(setup.datasetPersistentId, updatedRoCrate.toString(), setup.apiToken);
        updateRoCrateResponse.then().assertThat().statusCode(OK.getStatusCode());

        System.out.println("Verifying updates in Dataverse JSON");
        Response updatedDatasetResponse = UtilIT.nativeGet(setup.datasetId, setup.apiToken);
        updatedDatasetResponse.prettyPrint();

        // Verify changes in the Dataverse JSON
        updatedDatasetResponse.then().assertThat()
                .statusCode(OK.getStatusCode())
                .body("data.latestVersion.metadataBlocks.citation.fields.find { it.typeName == 'title' }.value", equalTo("Updated Darwin's Finches Study in AROMA"))
                .body("data.latestVersion.metadataBlocks.citation.fields.find { it.typeName == 'author' }.value.size()", equalTo(2))
                .body("data.latestVersion.metadataBlocks.citation.fields.find { it.typeName == 'author' }.value.find { it.authorName.value == 'Hooker, Joseph' }.authorAffiliation.value", equalTo("Royal Botanical Gardens, Kew"));

        System.out.println("Verifying updates in RO-Crate JSON");
        Response updatedRoCrateResponse = getRoCrate(setup.datasetPersistentId, "DRAFT", setup.apiToken);
        String updatedRoCrateJson = updatedRoCrateResponse.prettyPrint();

        // Verify changes in the RO-Crate JSON
        updatedRoCrateResponse.then().assertThat()
                .statusCode(OK.getStatusCode())
                .body("'@graph'.find { it.'@type' == 'Dataset' }.title", equalTo("Updated Darwin's Finches Study in AROMA"))
                .body("'@graph'.findAll { it.'@type' == 'author' }.size()", equalTo(2))
                .body("'@graph'.find { it.'@type' == 'author' && it.authorName == 'Hooker, Joseph' }.authorAffiliation", equalTo("Royal Botanical Gardens, Kew"));


//        cleanupUserDataverseAndDataset(setup);
    }

    @Test
    public void testUploadRoCrateJson() throws IOException {
        TestSetup setup = createRandomUserDataverseAndDataset();

        // Read the RO-Crate JSON file
        String roCrateJson = Files.readString(Paths.get("src/test/resources/arp/rocrate-api-tests/ro-crate-metadata.json"));

        // Upload the RO-Crate JSON
        Response uploadResponse = uploadRoCrateJson(roCrateJson, setup.apiToken, setup.dataverseAlias);

        // Assert that the response code is 200 and the status is OK
        uploadResponse.then().assertThat()
                .statusCode(OK.getStatusCode())
                .body("status", equalTo("OK"));

        String arpPid = ((LinkedHashMap<String, Object>) ((ArrayList<Object>) uploadResponse.getBody().jsonPath().getMap("data").get("@graph")).get(0)).get("@arpPid").toString();
        UtilIT.destroyDataset(arpPid, setup.apiToken);

        cleanupUserDataverseAndDataset(setup);
    }
    
    public static Response getRoCrate(String persistentId, String version, String apiToken) {
        String path = String.format("/api/arp/rocrate/%s", persistentId);
        if (version != null) {
            path += "?version=" + version;
        }
        return given()
                .header(API_TOKEN_HTTP_HEADER, apiToken)
                .get(path);
    }

    public static Response updateRoCrate(String persistentId, String roCrateJson, String apiToken) {
        String path = String.format("/api/arp/rocrate/%s", persistentId);
        return given()
                .header(API_TOKEN_HTTP_HEADER, apiToken)
                .contentType(ContentType.JSON)
                .body(roCrateJson)
                .post(path);
    }

    /**
     * Helper method to upload RO-Crate JSON using the uploadRoCrateJson API
     */
    public static Response uploadRoCrateJson(String roCrateJson, String apiToken, String ownerId) {
        return given()
                .header(API_TOKEN_HTTP_HEADER, apiToken)
                .contentType(ContentType.JSON)
                .body(roCrateJson)
                .post("/api/arp/uploadRoCrateJson?ownerId=" + ownerId);
    }
}
