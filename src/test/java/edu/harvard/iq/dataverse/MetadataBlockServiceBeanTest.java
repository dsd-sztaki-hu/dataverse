package edu.harvard.iq.dataverse;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import javax.json.Json;
import javax.json.JsonObject;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;

@RunWith(MockitoJUnitRunner.class)
public class MetadataBlockServiceBeanTest {
    private static final Logger logger = Logger.getLogger(MetadataBlockServiceBeanTest.class.getCanonicalName());
    @Mock
    private MetadataBlockServiceBean metadataBlockSvc;
    
    @Test
    public void testCitationTsvGeneration() throws IOException {
        String generatedTsv = generateTestTsv("json/citationMdb.json");
        String tsvContent = Files.readString(Paths.get("src/test/resources/json/citation.tsv"));
        logTsvContents("citation", tsvContent, generatedTsv);
        assertEquals(tsvContent, generatedTsv);
    }

    @Test
    public void testGeospatialTsvGeneration() throws IOException {
        String generatedTsv = generateTestTsv("json/geospatialMdb.json");
        String tsvContent = Files.readString(Paths.get("src/test/resources/json/geospatial.tsv"));
        logTsvContents("geospatial", tsvContent, generatedTsv);
        assertEquals(tsvContent, generatedTsv);
    }

    @Test
    public void testSocialScienceTsvGeneration() throws IOException {
        String generatedTsv = generateTestTsv("json/socialScienceMdb.json");
        String tsvContent = Files.readString(Paths.get("src/test/resources/json/socialScience.tsv"));
        logTsvContents("socialScience", tsvContent, generatedTsv);
        assertEquals(tsvContent, generatedTsv);
    }

    @Test
    public void testAstrophysicsTsvGeneration() throws IOException {
        String generatedTsv = generateTestTsv("json/astrophysicsMdb.json");
        String tsvContent = Files.readString(Paths.get("src/test/resources/json/astrophysics.tsv"));
        logTsvContents("astrophysics", tsvContent, generatedTsv);
        assertEquals(tsvContent, generatedTsv);
    }

    @Test
    public void testBiomedicalTsvGeneration() throws IOException {
        String generatedTsv = generateTestTsv("json/biomedicalMdb.json");
        String tsvContent = Files.readString(Paths.get("src/test/resources/json/biomedical.tsv"));
        logTsvContents("biomedical", tsvContent, generatedTsv);
        assertEquals(tsvContent, generatedTsv);
    }

    @Test
    public void testJournalTsvGeneration() throws IOException {
        String generatedTsv = generateTestTsv("json/journalMdb.json");
        String tsvContent = Files.readString(Paths.get("src/test/resources/json/journal.tsv"));
        logTsvContents("journal", tsvContent, generatedTsv);
        assertEquals(tsvContent, generatedTsv);
    }
    
    private String generateTestTsv(String testJsonPath) throws JsonProcessingException, UnsupportedEncodingException {
        var mdb = new MetadataBlock();
        JsonObject testJson;
        InputStream jsonFile = ClassLoader.getSystemResourceAsStream(testJsonPath);
        InputStreamReader reader = new InputStreamReader(jsonFile, "UTF-8");
        testJson = Json.createReader(reader).readObject();
        mdb.setName(testJson.getString("name"));
        mdb.setDisplayName(testJson.getString("displayName"));
        mdb.setId(Long.valueOf(testJson.get("id").toString()));
        mdb.setNamespaceUri(testJson.get("namespaceUri").toString().replace("\"", ""));
        List<DatasetFieldType> datasetFieldTypes = new ObjectMapper().readValue(testJson.getJsonArray("datasetFieldTypes").toString(), new TypeReference<>() {});
        mdb.setDatasetFieldTypes(datasetFieldTypes);
        
        Mockito.when(metadataBlockSvc.exportMdbAsTsv(anyString())).thenCallRealMethod();
        Mockito.lenient().when(metadataBlockSvc.findById(anyLong())).thenReturn(mdb);
        return metadataBlockSvc.exportMdbAsTsv("1");
    }
    
    private void logTsvContents(String mdbName, String expected, String generated) {
        logger.info("Expected " + mdbName + " tsv: \n" + expected);
        logger.info("Generated " + mdbName + " tsv: \n" + generated);
    }
}
