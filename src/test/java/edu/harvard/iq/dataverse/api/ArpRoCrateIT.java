package edu.harvard.iq.dataverse.api;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import edu.harvard.iq.dataverse.DataFile;
import edu.harvard.iq.dataverse.DatasetVersionFilesServiceBean;
import edu.harvard.iq.dataverse.FileSearchCriteria;
import edu.harvard.iq.dataverse.authorization.DataverseRole;
import edu.harvard.iq.dataverse.authorization.groups.impl.builtin.AuthenticatedUsers;
import edu.harvard.iq.dataverse.authorization.users.PrivateUrlUser;
import edu.harvard.iq.dataverse.dataaccess.AbstractRemoteOverlayAccessIO;
import edu.harvard.iq.dataverse.dataaccess.GlobusOverlayAccessIOTest;
import edu.harvard.iq.dataverse.datavariable.VarGroup;
import edu.harvard.iq.dataverse.datavariable.VariableMetadata;
import edu.harvard.iq.dataverse.datavariable.VariableMetadataDDIParser;
import edu.harvard.iq.dataverse.settings.SettingsServiceBean;
import edu.harvard.iq.dataverse.util.BundleUtil;
import edu.harvard.iq.dataverse.util.SystemConfig;
import edu.harvard.iq.dataverse.util.json.JSONLDUtil;
import edu.harvard.iq.dataverse.util.json.JsonUtil;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.restassured.parsing.Parser;
import io.restassured.path.json.JsonPath;
import io.restassured.path.xml.XmlPath;
import io.restassured.response.Response;
import jakarta.ws.rs.core.Response.Status;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.logging.Logger;

import static edu.harvard.iq.dataverse.DatasetVersion.ARCHIVE_NOTE_MAX_LENGTH;
import static edu.harvard.iq.dataverse.api.ApiConstants.*;
import static edu.harvard.iq.dataverse.api.UtilIT.API_TOKEN_HTTP_HEADER;
import static edu.harvard.iq.dataverse.api.UtilIT.equalToCI;
import static io.restassured.RestAssured.given;
import static io.restassured.path.json.JsonPath.with;
import static jakarta.ws.rs.core.Response.Status.*;
import static java.lang.Thread.sleep;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.Matchers.contains;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test RoCrate editing an RoCrate-Dataset synchronisation. Based on DatasetsIT.java.
 */
public class ArpRoCrateIT
{

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
    public void testCreateDataset() {

        Response createUser = UtilIT.createRandomUser();
        createUser.prettyPrint();
        String username = UtilIT.getUsernameFromResponse(createUser);
        String apiToken = UtilIT.getApiTokenFromResponse(createUser);

        Response createDataverseResponse = UtilIT.createRandomDataverse(apiToken);
        createDataverseResponse.prettyPrint();
        String dataverseAlias = UtilIT.getAliasFromResponse(createDataverseResponse);

        // Now, let's allow anyone with a Dataverse account (any "random user")
        // to create datasets in this dataverse:

        Response grantRole = UtilIT.grantRoleOnDataverse(dataverseAlias, DataverseRole.DS_CONTRIBUTOR, AuthenticatedUsers.get().getIdentifier(), apiToken);
        grantRole.prettyPrint();
        assertEquals(OK.getStatusCode(), grantRole.getStatusCode());

        Response publishDataverse = UtilIT.publishDataverseViaSword(dataverseAlias, apiToken);
        assertEquals(OK.getStatusCode(), publishDataverse.getStatusCode());

        // Return a short sleep here
        //without it we have seen some database deadlocks SEK 09/13/2019

        try {
            Thread.sleep(1000l);
        } catch (InterruptedException iex) {}

        // ... And now that it's published, try to create a dataset again,
        // as the "random", not specifically authorized user:
        // (this time around, it should work!)

        Response createDatasetResponse = UtilIT.createRandomDatasetViaNativeApi(dataverseAlias, apiToken);
        createDatasetResponse.prettyPrint();
        Integer datasetId = UtilIT.getDatasetIdFromResponse(createDatasetResponse);

        Response datasetAsJson = UtilIT.nativeGet(datasetId, apiToken);
        datasetAsJson.then().assertThat()
                .statusCode(OK.getStatusCode());

        // OK, let's delete this dataset as well, and then delete the dataverse...

        Response deleteDatasetResponse = UtilIT.deleteDatasetViaNativeApi(datasetId, apiToken);
        deleteDatasetResponse.prettyPrint();
        assertEquals(200, deleteDatasetResponse.getStatusCode());

        Response deleteDataverseResponse = UtilIT.deleteDataverse(dataverseAlias, apiToken);
        deleteDataverseResponse.prettyPrint();
        assertEquals(200, deleteDataverseResponse.getStatusCode());

        Response deleteUserResponse = UtilIT.deleteUser(username);
        deleteUserResponse.prettyPrint();
        assertEquals(200, deleteUserResponse.getStatusCode());
    }
}
