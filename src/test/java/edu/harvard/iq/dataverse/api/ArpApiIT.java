package edu.harvard.iq.dataverse.api;

import com.jayway.restassured.RestAssured;
import com.jayway.restassured.path.json.JsonPath;
import com.jayway.restassured.response.Response;
import edu.harvard.iq.dataverse.MetadataBlockServiceBean;
import org.apache.commons.math3.util.Pair;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.ejb.EJB;
import javax.inject.Inject;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import static com.jayway.restassured.RestAssured.given;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.OK;
import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ArpApiIT {

    private static final Logger logger = Logger.getLogger(ArpApiIT.class.getCanonicalName());

    public static final String API_TOKEN_HTTP_HEADER = "X-Dataverse-key";


    @BeforeClass
    public static void setUpClass() {
        RestAssured.baseURI = UtilIT.getRestAssuredBaseUri();
    }

    @Test
    public void checkTemplate_NoError() {
        Response createUser = UtilIT.createRandomUser();
        String apiToken = UtilIT.getApiTokenFromResponse(createUser);

        byte[] templateContent = null;
        try {
            templateContent = Files.readAllBytes(Paths.get("src/test/resources/arp/no-error-template.json"));
        } catch (IOException e) {
            logger.warning(e.getMessage());
            assertEquals(0,1);
        }

        Response response = checkTemplate(apiToken, templateContent);
        assertEquals(200, response.getStatusCode());
        response.then().assertThat().statusCode(OK.getStatusCode());

        String body = response.getBody().asString();
        String status = JsonPath.from(body).getString("status");
        assertEquals("OK", status);

        Map<String, String> data = JsonPath.from(body).getMap("data");
        assertEquals(1, data.size());
        String message = data.get("message");
        assertEquals("Valid Template", message);
    }

    @Test
    public void checkTemplate_unprocessable() {
        Response createUser = UtilIT.createRandomUser();
        String apiToken = UtilIT.getApiTokenFromResponse(createUser);

        byte[] templateContent = null;
        try {
            templateContent = Files.readAllBytes(Paths.get("src/test/resources/arp/unprocessable-template.json"));
        } catch (IOException e) {
            logger.warning(e.getMessage());
            assertEquals(0,1);
        }

        Response response = checkTemplate(apiToken, templateContent);
        assertEquals(500, response.getStatusCode());
        response.then().assertThat().statusCode(INTERNAL_SERVER_ERROR.getStatusCode());

        String body = response.getBody().asString();
        String status = JsonPath.from(body).getString("status");
        assertEquals("ERROR", status);

        Map<String, List<String>> data = JsonPath.from(body).getMap("message");
        assertEquals(1, data.size());
        String message = data.get("unprocessableElements").get(0);
        assertEquals("/properties/lvl_1_element_test/lvl_2_element_test", message);
    }

    @Test
    public void checkTemplate_invalidNames() {
        Response createUser = UtilIT.createRandomUser();
        String apiToken = UtilIT.getApiTokenFromResponse(createUser);

        byte[] templateContent = null;
        try {
            templateContent = Files.readAllBytes(Paths.get("src/test/resources/arp/invalid-names-template.json"));
        } catch (IOException e) {
            logger.warning(e.getMessage());
            assertEquals(0,1);
        }

        Response response = checkTemplate(apiToken, templateContent);
        assertEquals(500, response.getStatusCode());
        response.then().assertThat().statusCode(INTERNAL_SERVER_ERROR.getStatusCode());

        String body = response.getBody().asString();
        String status = JsonPath.from(body).getString("status");
        assertEquals("ERROR", status);

        Map<String, List<String>> data = JsonPath.from(body).getMap("message");
        List<String> invalidNames = data.get("invalidNames");
        assertEquals(6, invalidNames.size());
        assertTrue(invalidNames.contains("_invalid1_test_"));
        assertTrue(invalidNames.contains("2invalid_test"));
        assertTrue(invalidNames.contains("invalid3_test_"));
        assertTrue(invalidNames.contains("invalid 4_test"));
        assertTrue(invalidNames.contains("invalid#_test"));
        assertTrue(invalidNames.contains("name"));
    }

    @Test
    public void checkTemplate_incompatiblePairs_afterOverrideUpdate() {
        Response createUser = UtilIT.createRandomUser();
        String apiToken = UtilIT.getApiTokenFromResponse(createUser);

        byte[] templateContent = null;
        try {
            templateContent = Files.readAllBytes(Paths.get("src/test/resources/arp/incompatible-pairs-template.json"));
        } catch (IOException e) {
            logger.warning(e.getMessage());
            assertEquals(0,1);
        }

        Response response = checkTemplate(apiToken, templateContent);
        assertEquals(200, response.getStatusCode());
        response.then().assertThat().statusCode(OK.getStatusCode());

        String body = response.getBody().asString();
        String status = JsonPath.from(body).getString("status");
        assertEquals("OK", status);

        Map<String, String> data = JsonPath.from(body).getMap("data");
        assertEquals(1, data.size());
        String message = data.get("message");
        assertEquals("Valid Template", message);

        /*
        We used to have the response below, but after adding the override generation this template no longer contains errors,
        the required fields will be overridden during the mdb creation

        Response response = checkTemplate(apiToken, templateContent);
        assertEquals(500, response.getStatusCode());
        response.then().assertThat().statusCode(INTERNAL_SERVER_ERROR.getStatusCode());

        String body = response.getBody().asString();
        String status = JsonPath.from(body).getString("status");
        assertEquals("ERROR", status);

        Map<String, Map<Pair<String, String>, HashMap<String, String>>> data = JsonPath.from(body).getMap("message");
        Map<Pair<String, String>, HashMap<String, String>> incompatiblePairs = data.get("incompatiblePairs");
        assertEquals(4, incompatiblePairs.size());
        assertTrue(incompatiblePairs.containsValue(Map.of("authorIdentifierScheme", "http://purl.org/spar/datacite/AgentIdentifierScheme")));
        assertTrue(incompatiblePairs.containsValue(Map.of("software", "https://www.w3.org/TR/prov-o/#wasGeneratedBy")));
        assertTrue(incompatiblePairs.containsValue(Map.of("publicationIDType", "http://purl.org/spar/datacite/ResourceIdentifierScheme")));
        assertTrue(incompatiblePairs.containsValue(Map.of("alternativeURL", "https://schema.org/distribution")));
        */
    }

    @Test
    public void generateTsv_noError() {
        Response createUser = UtilIT.createRandomUser();
        String apiToken = UtilIT.getApiTokenFromResponse(createUser);

        byte[] templateContent = null;
        try {
            templateContent = Files.readAllBytes(Paths.get("src/test/resources/arp/no-error-template.json"));
        } catch (IOException e) {
            logger.warning(e.getMessage());
            assertEquals(0,1);
        }

        Response response = cedarToMdb(apiToken, templateContent);
        assertEquals(200, response.getStatusCode());
        response.then().assertThat().statusCode(OK.getStatusCode());

        String body = response.getBody().asString();

        String tsvContent = null;
        try {
            tsvContent = Files.readString(Paths.get("src/test/resources/arp/generated-no-error.tsv"));
        } catch (IOException e) {
            logger.warning(e.getMessage());
            assertEquals(0,1);
        }

        assertEquals(tsvContent, body);
    }

    @Test
    public void checkMdbOverrideCreation() throws Exception {
        int metadataBlockMaxId = getMaxIdFromTable("metadatablock");
        int datasetFieldTypeMaxId = getMaxIdFromTable("datasetfieldtype");
        int datasetFieldTypeOverrideMaxId = getMaxIdFromTable("datasetfieldtypeoverride");
        logger.info("metadataBlockIdMax " + metadataBlockMaxId);
        logger.info("datasetFieldTypeMaxId " + datasetFieldTypeMaxId);
        logger.info("datasetFieldTypeOverrideMaxId " + datasetFieldTypeOverrideMaxId);

        Response createSuperuser = UtilIT.createRandomUser();
        String superuserUsername = UtilIT.getUsernameFromResponse(createSuperuser);
        String superuserApiToken = UtilIT.getApiTokenFromResponse(createSuperuser);
        Response toggleSuperuser = UtilIT.makeSuperUser(superuserUsername);
        toggleSuperuser.then().assertThat()
                .statusCode(OK.getStatusCode());

        byte[] mdbToOverride = Files.readAllBytes(Paths.get("src/test/resources/arp/mdb-to-override.json"));
        Response response = cedarToMdbWithUpload(superuserApiToken, mdbToOverride);
        assertEquals(200, response.getStatusCode());
        response.then().assertThat().statusCode(OK.getStatusCode());

//        byte[] mdbContainingOverride = Files.readAllBytes(Paths.get("src/test/resources/arp/mdb-containing-override.json"));
        byte[] mdbContainingOverride = Files.readAllBytes(Paths.get("src/test/resources/arp/mdb-containing-override-and-onemore.json"));
        Response response2 = cedarToMdbWithUpload(superuserApiToken, mdbContainingOverride);
        assertEquals(200, response2.getStatusCode());
        response2.then().assertThat().statusCode(OK.getStatusCode());

        Connection connection = connectToDatabase();
        if (connection == null) {
            throw new Exception("Can not connect to database");
        }
        connection.setAutoCommit(false);
        Statement stmt = connection.createStatement();

        ResultSet metadatablockTable = stmt.executeQuery( "SELECT * from metadatablock where id > " + metadataBlockMaxId);
        metadatablockTable.next();
        assertEquals("MDB to override", metadatablockTable.getString("displayname"));
        int mdbToOverrideId = metadatablockTable.getInt("id");
        metadatablockTable.next();
        assertEquals("MDB containing override", metadatablockTable.getString("displayname"));
        int mdbContainingOverrideId = metadatablockTable.getInt("id");
        metadatablockTable.close();

        ResultSet datasetfieldtypeTable = stmt.executeQuery("SELECT * from datasetfieldtype where id > " + datasetFieldTypeMaxId);
        datasetfieldtypeTable.next();
        assertEquals("datasetfieldtype_to_override", datasetfieldtypeTable.getString("name"));
        assertEquals("Datasetfieldtype that will be overridden", datasetfieldtypeTable.getString("title"));
        assertEquals(mdbToOverrideId, datasetfieldtypeTable.getInt("metadatablock_id"));
        datasetfieldtypeTable.close();

        ResultSet datasetfieldtypeoverrideTable = stmt.executeQuery("SELECT * from datasetfieldtypeoverride where id > " + datasetFieldTypeOverrideMaxId);
        datasetfieldtypeoverrideTable.next();
        assertEquals("Datasetfieldtype that will overridde", datasetfieldtypeoverrideTable.getString("title"));
        assertEquals("datasetfieldtype_that_override", datasetfieldtypeoverrideTable.getString("localname"));
        assertEquals(mdbContainingOverrideId, datasetfieldtypeoverrideTable.getInt("metadatablock_id"));
        datasetfieldtypeoverrideTable.close();
        stmt.close();
        connection.close();

        deleteTestDataFromTable(datasetFieldTypeOverrideMaxId, "datasetfieldtypeoverride", "id");
        deleteTestDataFromTable(datasetFieldTypeMaxId, "datasetfieldtype", "id");
        deleteTestDataFromTable(metadataBlockMaxId, "dataverse_metadatablock", "metadatablocks_id");
        deleteTestDataFromTable(metadataBlockMaxId, "metadatablock", "id");
    }

    @Test
    public void generateTsv_unprocessable() {
        Response createUser = UtilIT.createRandomUser();
        String apiToken = UtilIT.getApiTokenFromResponse(createUser);

        byte[] templateContent = null;
        try {
            templateContent = Files.readAllBytes(Paths.get("src/test/resources/arp/unprocessable-template.json"));
        } catch (IOException e) {
            logger.warning(e.getMessage());
            assertEquals(0,1);
        }

        Response response = cedarToMdb(apiToken, templateContent);
        assertEquals(500, response.getStatusCode());
        response.then().assertThat().statusCode(INTERNAL_SERVER_ERROR.getStatusCode());

        String body = response.getBody().asString();
        String status = JsonPath.from(body).getString("status");
        assertEquals("ERROR", status);

        Map<String, List<String>> data = JsonPath.from(body).getMap("message");
        assertEquals(1, data.size());
        String message = data.get("unprocessableElements").get(0);
        assertEquals("/properties/lvl_1_element_test/lvl_2_element_test", message);
    }

    @Test
    public void generateDescriboProfile_noError() {
        Response createUser = UtilIT.createRandomUser();
        String apiToken = UtilIT.getApiTokenFromResponse(createUser);

        byte[] templateContent = null;
        try {
            templateContent = Files.readAllBytes(Paths.get("src/test/resources/arp/no-error-template.json"));
        } catch (IOException e) {
            logger.warning(e.getMessage());
            assertEquals(0,1);
        }

        Response response = cedarToDescribo(apiToken, templateContent);
        assertEquals(200, response.getStatusCode());
        response.then().assertThat().statusCode(OK.getStatusCode());

        String body = response.getBody().asString();

        String profileContent = null;
        try {
            profileContent = Files.readString(Paths.get("src/test/resources/arp/generated-describo-profile.json"));
        } catch (IOException e) {
            logger.warning(e.getMessage());
            assertEquals(0,1);
        }

        assertEquals(profileContent, body + "\n");
    }

    @Test
    public void generateDescriboProfile_invalidNames() {
        Response createUser = UtilIT.createRandomUser();
        String apiToken = UtilIT.getApiTokenFromResponse(createUser);

        byte[] templateContent = null;
        try {
            templateContent = Files.readAllBytes(Paths.get("src/test/resources/arp/invalid-names-template.json"));
        } catch (IOException e) {
            logger.warning(e.getMessage());
            assertEquals(0,1);
        }

        Response response = cedarToDescribo(apiToken, templateContent);
        assertEquals(500, response.getStatusCode());
        response.then().assertThat().statusCode(INTERNAL_SERVER_ERROR.getStatusCode());

        String body = response.getBody().asString();
        String status = JsonPath.from(body).getString("status");
        assertEquals("ERROR", status);

        Map<String, List<String>> data = JsonPath.from(body).getMap("message");
        List<String> invalidNames = data.get("invalidNames");
        assertEquals(6, invalidNames.size());
        assertTrue(invalidNames.contains("_invalid1_test_"));
        assertTrue(invalidNames.contains("2invalid_test"));
        assertTrue(invalidNames.contains("invalid3_test_"));
        assertTrue(invalidNames.contains("invalid 4_test"));
        assertTrue(invalidNames.contains("invalid#_test"));
        assertTrue(invalidNames.contains("name"));
    }

    @Test
    public void citationToTsv() {
        checkGeneratedTsv("1", "src/test/resources/arp/citation.tsv");
    }

    @Test
    public void geospatialToTsv() {
        checkGeneratedTsv("2", "src/test/resources/arp/geospatial.tsv");
    }

    @Test
    public void socialscienceToTsv() {
        checkGeneratedTsv("3", "src/test/resources/arp/socialscience.tsv");
    }

    @Test
    public void astrophysicsToTsv() {
        checkGeneratedTsv("4", "src/test/resources/arp/astrophysics.tsv");
    }

    @Test
    public void biomedicalToTsv() {
        checkGeneratedTsv("5", "src/test/resources/arp/biomedical.tsv");
    }

    @Test
    public void journalToTsv() {
        checkGeneratedTsv("6", "src/test/resources/arp/journal.tsv");
    }

    static Response checkTemplate(String apiToken, byte[] body) {
        return given()
                .header(API_TOKEN_HTTP_HEADER, apiToken)
                .contentType("application/json; charset=utf-8")
                .body(body)
                .post("/api/arp/checkCedarTemplate");
    }

    static Response cedarToMdb(String apiToken, byte[] body) {
        return given()
                .header(API_TOKEN_HTTP_HEADER, apiToken)
                .contentType("application/json; charset=utf-8")
                .body(body)
                .queryParam("skipUpload", "true")
                .post("/api/arp/cedarToMdb/root");
    }

    static Response exportMdbAsTsv(String apiToken, String mdbIdtf) {
        return given()
                .header(API_TOKEN_HTTP_HEADER, apiToken)
                .contentType("application/json; charset=utf-8")
                .get("/api/arp/exportMdbAsTsv/" + mdbIdtf);
    }

    static void checkGeneratedTsv(String mdbIdtf, String tsvName) {
        Response createUser = UtilIT.createRandomUser();
        String apiToken = UtilIT.getApiTokenFromResponse(createUser);

        Response response = exportMdbAsTsv(apiToken, mdbIdtf);
        assertEquals(200, response.getStatusCode());
        response.then().assertThat().statusCode(OK.getStatusCode());

        String body = response.getBody().asString();

        String tsvContent = null;
        try {
            tsvContent = Files.readString(Paths.get(tsvName));
        } catch (IOException e) {
            logger.warning(e.getMessage());
            assertEquals(0,1);
        }

        assertEquals(tsvContent, body);
    }

    static Response cedarToMdbWithUpload(String apiToken, byte[] body) {
        return given()
                .header(API_TOKEN_HTTP_HEADER, apiToken)
                .contentType("application/json; charset=utf-8")
                .body(body)
                .post("/api/arp/cedarToMdb/root");
    }

    static Response cedarToDescribo(String apiToken, byte[] body) {
        return given()
                .header(API_TOKEN_HTTP_HEADER, apiToken)
                .contentType("application/json; charset=utf-8")
                .body(body)
                .post("/api/arp/cedarToDescribo");
    }

    // Opens the connection to the database.
    // Uses the credentials supplied via JVM options
    private static Connection connectToDatabase() {
        Connection c;

        String host = System.getProperty("db.serverName") != null ? System.getProperty("db.serverName") : "localhost";
        String port = System.getProperty("db.portNumber") != null ? System.getProperty("db.portNumber") : "5432";
        String database = System.getProperty("db.databaseName") != null ? System.getProperty("db.databaseName") : "dvndb";
        String pguser = System.getProperty("db.user") != null ? System.getProperty("db.user") : "dvnapp";
        String pgpasswd = System.getProperty("db.password") != null ? System.getProperty("db.password") : "secret";

        try {
            Class.forName("org.postgresql.Driver");
            c = DriverManager
                    .getConnection("jdbc:postgresql://" + host + ":" + port + "/" + database,
                            pguser,
                            pgpasswd);
        } catch (Exception e) {
            return null;
        }
        return c;
    }

    public static int getMaxIdFromTable(String tableName) throws Exception {
        int maxId;

        Connection connection = connectToDatabase();
        if (connection == null) {
            throw new Exception("Can not connect to database");
        }
        connection.setAutoCommit(false);
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery( "SELECT MAX(id) as max_id from " + tableName);
        rs.next();
        maxId = rs.getInt("max_id");
        rs.close();
        stmt.close();
        connection.close();

        return maxId;
    }

    public static void deleteTestDataFromTable(int originalMaxId, String tableName, String idName) throws Exception {
        Connection connection = connectToDatabase();
        if (connection == null) {
            throw new Exception("Can not connect to database");
        }
        connection.setAutoCommit(false);
        Statement stmt = connection.createStatement();
        stmt.execute( "DELETE FROM " + tableName + " WHERE " + idName + " > " + originalMaxId + ";");
        connection.commit();
        stmt.close();
        connection.close();
    }
}
