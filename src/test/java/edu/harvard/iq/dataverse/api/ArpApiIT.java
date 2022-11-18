package edu.harvard.iq.dataverse.api;

import com.jayway.restassured.RestAssured;
import com.jayway.restassured.path.json.JsonPath;
import com.jayway.restassured.response.Response;
import org.apache.commons.math3.util.Pair;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
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
    public void checkTemplate_incompatiblePairs() {
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

    static Response checkTemplate(String apiToken, byte[] body) {
        return given()
                .header(API_TOKEN_HTTP_HEADER, apiToken)
                .contentType("application/json; charset=utf-8")
                .body(body)
                .post("/api/admin/arp/checkCedarTemplate");
    }

    static Response cedarToMdb(String apiToken, byte[] body) {
        return given()
                .header(API_TOKEN_HTTP_HEADER, apiToken)
                .contentType("application/json; charset=utf-8")
                .body(body)
                .queryParam("skipUpload", "true")
                .post("/api/admin/arp/cedarToMdb/root");
    }

    static Response cedarToDescribo(String apiToken, byte[] body) {
        return given()
                .header(API_TOKEN_HTTP_HEADER, apiToken)
                .contentType("application/json; charset=utf-8")
                .body(body)
                .post("/api/admin/arp/cedarToDescribo");
    }
}
