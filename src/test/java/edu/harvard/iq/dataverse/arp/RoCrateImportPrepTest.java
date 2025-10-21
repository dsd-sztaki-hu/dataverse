package edu.harvard.iq.dataverse.arp;

import com.fasterxml.jackson.core.JsonProcessingException;
import edu.harvard.iq.dataverse.*;
import edu.harvard.iq.dataverse.arp.rocrate.RoCrateConformsToIdProvider;
import edu.harvard.iq.dataverse.arp.rocrate.RoCrateImportManager;
import edu.harvard.iq.dataverse.arp.rocrate.RoCrateImportPrepResult;
import edu.harvard.iq.dataverse.mocks.MockArpService;
import edu.harvard.iq.dataverse.mocks.MockDatasetFieldSvc;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.logging.Logger;
import org.mockito.Mockito;
import static org.mockito.Mockito.when;
import java.lang.reflect.Field;
import com.google.gson.JsonObject;
import com.google.gson.Gson;
import edu.harvard.iq.dataverse.arp.rocrate.RoCrateServiceBean;
import static org.mockito.ArgumentMatchers.any;
import com.google.gson.JsonArray;


/*
* In this class the preparation of an RO-Crate will be tested, before it is imported to Dataverse.
* The original RO-Crate is a valid crate, that contains values only that is accepted by the schema (an example citation mdb).
* The other tests will check whether the pre-checking process can detect the invalid values.
* The differences from the original RO-Crate will be stated above every test case, you can always check the schema
* in the mockDatasetFieldSvc() function.
* */

@ExtendWith(MockitoExtension.class)
public class RoCrateImportPrepTest {
    
    private static final Logger logger = Logger.getLogger(RoCrateImportPrepTest.class.getCanonicalName());
    private static final MockDatasetFieldSvc datasetFieldTypeSvc = new MockDatasetFieldSvc();
    private static RoCrateImportManager roCrateImportManager;

    @BeforeAll
    public static void setUpClass() {
        mockDatasetFieldSvc();

        // Create mock ArpServiceBean as a local variable
        ArpServiceBean mockArpService = new MockArpService();

        // Create mock DataverseServiceBean as a local variable
        DataverseServiceBean mockDataverseServiceBean = Mockito.mock(DataverseServiceBean.class);

        // Create mock ArpMetadataBlockServiceBean as a local variable
        ArpMetadataBlockServiceBean mockArpMetadataBlockServiceBean = Mockito.mock(ArpMetadataBlockServiceBean.class);

        // Create mock ControlledVocabularyValueServiceBean as a local variable
        ControlledVocabularyValueServiceBean mockControlledVocabularyValueService = Mockito.mock(ControlledVocabularyValueServiceBean.class);

        // Create a real RoCrateServiceBean instance
        RoCrateServiceBean roCrateServiceBean = new RoCrateServiceBean();

        // Create RoCrateImportManager instance
        roCrateImportManager = new RoCrateImportManager(datasetFieldTypeSvc);

        // Use reflection to set the required fields in RoCrateImportManager
        try {
            Field arpServiceField = RoCrateImportManager.class.getDeclaredField("arpService");
            arpServiceField.setAccessible(true);
            arpServiceField.set(roCrateImportManager, mockArpService);

            // Use reflection to set the dataverseServiceBean field
            Field dataverseServiceBeanField = RoCrateImportManager.class.getDeclaredField("dataverseServiceBean");
            dataverseServiceBeanField.setAccessible(true);
            dataverseServiceBeanField.set(roCrateImportManager, mockDataverseServiceBean);

            // Use reflection to set the roCrateServiceBean field to use the real instance
            Field roCrateServiceBeanField = RoCrateImportManager.class.getDeclaredField("roCrateServiceBean");
            roCrateServiceBeanField.setAccessible(true);
            roCrateServiceBeanField.set(roCrateImportManager, roCrateServiceBean);

            // Use reflection to set the fieldService field to use our mock
            Field fieldServiceField = RoCrateImportManager.class.getDeclaredField("fieldService");
            fieldServiceField.setAccessible(true);
            fieldServiceField.set(roCrateImportManager, datasetFieldTypeSvc);

            // Use reflection to set the arpMetadataBlockServiceBean field
            Field arpMetadataBlockServiceBeanField = RoCrateImportManager.class.getDeclaredField("arpMetadataBlockServiceBean");
            arpMetadataBlockServiceBeanField.setAccessible(true);
            arpMetadataBlockServiceBeanField.set(roCrateImportManager, mockArpMetadataBlockServiceBean);

            // Use reflection to set the controlledVocabularyValueService field
            Field controlledVocabularyValueServiceField = RoCrateImportManager.class.getDeclaredField("controlledVocabularyValueService");
            controlledVocabularyValueServiceField.setAccessible(true);
            controlledVocabularyValueServiceField.set(roCrateImportManager, mockControlledVocabularyValueService);
        } catch (Exception e) {
            throw new RuntimeException("Failed to inject mock dependencies", e);
        }

        // Use reflection to set the required fields in RoCrateServiceBean
        try {
            // Set the fieldService in RoCrateServiceBean
            Field roCrateFieldServiceField = RoCrateServiceBean.class.getDeclaredField("fieldService");
            roCrateFieldServiceField.setAccessible(true);
            roCrateFieldServiceField.set(roCrateServiceBean, datasetFieldTypeSvc);

            // Create mocks for other dependencies of RoCrateServiceBean
            RoCrateConformsToIdProvider mockConformsToProvider = Mockito.mock(RoCrateConformsToIdProvider.class);
            ArpConfig mockArpConfig = Mockito.mock(ArpConfig.class);

            // Set the other dependencies
            Field conformsToProviderField = RoCrateServiceBean.class.getDeclaredField("roCrateConformsToProvider");
            conformsToProviderField.setAccessible(true);
            conformsToProviderField.set(roCrateServiceBean, mockConformsToProvider);

            Field arpConfigField = RoCrateServiceBean.class.getDeclaredField("arpConfig");
            arpConfigField.setAccessible(true);
            arpConfigField.set(roCrateServiceBean, mockArpConfig);

            // Mock the findMetadataBlockForConformsToIds method
            when(mockConformsToProvider.findMetadataBlockForConformsToIds(any())).thenReturn(new ArrayList<>());

            // Mock the arpConfig get method
            when(mockArpConfig.get(any())).thenReturn("");
        } catch (Exception e) {
            throw new RuntimeException("Failed to inject RoCrateServiceBean dependencies", e);
        }

        // Create a mock MetadataBlock for the citation MDB
        MetadataBlock mockCitationMdb = new MetadataBlock();
        mockCitationMdb.setName("citation");

        // Initialize the dataset field types with the ones from our mock service
        mockCitationMdb.setDatasetFieldTypes(datasetFieldTypeSvc.findAllOrderedById());

        // Mock the findMDBByName method to return our mock
        when(mockDataverseServiceBean.findMDBByName("citation")).thenReturn(mockCitationMdb);

        // Mock the findDatasetFieldTypeArpForFieldType method
        when(mockArpMetadataBlockServiceBean.findDatasetFieldTypeArpForFieldType(any(DatasetFieldType.class))).thenAnswer(invocation -> {
            DatasetFieldType fieldType = invocation.getArgument(0);
            DatasetFieldTypeArp datasetFieldTypeArp = new DatasetFieldTypeArp();
            datasetFieldTypeArp.setFieldType(fieldType);

            // Create a mock Cedar definition without external values for most fields
            JsonObject cedarDefinition = new JsonObject();

            // Only add external values for fields that have controlled vocabulary
            if (fieldType.isAllowControlledVocabulary() && fieldType.getControlledVocabularyValues() != null
                    && !fieldType.getControlledVocabularyValues().isEmpty()) {
                JsonObject valueConstraints = new JsonObject();
                JsonArray branches = new JsonArray();
                JsonObject branch = new JsonObject();
                branch.addProperty("uri", "http://example.org/vocab");
                branch.addProperty("acronym", "example");
                branches.add(branch);
                valueConstraints.add("branches", branches);
                cedarDefinition.add("_valueConstraints", valueConstraints);
            }

            datasetFieldTypeArp.setCedarDefinition(cedarDefinition.toString());
            return datasetFieldTypeArp;
        });

        // Mock the findByDatasetFieldTypeId method to return controlled vocabulary values
        when(mockControlledVocabularyValueService.findByDatasetFieldTypeId(any(Long.class))).thenAnswer(invocation -> {
            Long fieldTypeId = invocation.getArgument(0);
            DatasetFieldType fieldType = datasetFieldTypeSvc.find(fieldTypeId);

            if (fieldType != null && fieldType.isAllowControlledVocabulary()) {
                return fieldType.getControlledVocabularyValues();
            }

            return new ArrayList<>();
        });
    }



    /*
    * This is the only test using the original and valid RO-Crate. This should not contain any errors.
    * */
    @Test
    public void testOriginalRoCrate() throws JsonProcessingException {
        String roCrateJsonString = null;
        try {
            roCrateJsonString = Files.readString(Paths.get("src/test/resources/arp/roCrateImportPrep/initialRoCrate/ro-crate-metadata.json"));
        } catch (IOException e) {
            logger.warning(e.getMessage());
            Assertions.assertEquals(0, 1);
        }
        var processResult = roCrateImportManager.prepareRoCrateForDataverseImport(roCrateJsonString, true);
        Assertions.assertTrue(processResult.getErrors().isEmpty());
        Assertions.assertTrue(processResult.getWarnings().isEmpty());
    }
    
    /*
    * Test that multiple values are only allowed where they are allowed by the scheme.
    * Processing an array of one value should be allowed for single values too.
    * 
    * Diff:
    *  + @graph[0].datePublished
        "2024-02-28T11:34:41.522162203Z"
      + @graph[0].datasetContact[1].@id
        "https://w3id.org/arp/dev/ro-id/doi:10.5072/FK2/UWPDNR/datasetContact/102"
      + @graph[0].datasetContact[1].datasetContactName
        "Second PointOfContact"
      + @graph[0].datasetContact[1].datasetContactEmail
        "pointof@mail.com"
      + @graph[0].datasetContact[1].datasetContactAffiliation
        "Dataverse.org"
      + @graph[0].author[1].@id
        "https://w3id.org/arp/dev/ro-id/doi:10.5072/FK2/UWPDNR/author/106"
      + @graph[0].author[1].authorName
        "Second Author"
      + @graph[0].author[1].authorAffiliation
        "SecondAuthAffiliation"
      + @graph[0].dsDescription[1].@id
        "https://w3id.org/arp/dev/ro-id/doi:10.5072/FK2/UWPDNR/dsDescription/113"
      + @graph[0].dsDescription[1].dsDescriptionValue
        "Descr_2"
      + @graph[0].dsDescription[1].dsDescriptionDate
        "1111-12-12"
      + @graph[0].publication[1].@id
        "https://w3id.org/arp/dev/ro-id/doi:10.5072/FK2/UWPDNR/publication/103"
      + @graph[0].publication[1].publicationCitation
        "RelatedPubl_2"
      + @graph[0].publication[1].publicationIDType
        "cstr"
      + @graph[0].publication[1].publicationIDNumber
        "456"
      + @graph[0].publication[1].publicationURL
        "https://relpubtwo.com"
      + @graph[0].producer[1].@id
        "https://w3id.org/arp/dev/ro-id/doi:10.5072/FK2/UWPDNR/producer/104"
      + @graph[0].producer[1].producerName
        "ProdTwo"
      + @graph[0].producer[1].producerAffiliation
        "ProdTwoAff"
      + @graph[0].producer[1].producerAbbreviation
        "ProdTwoAN"
      + @graph[0].producer[1].producerURL
        "https://prodtwo.com"
      + @graph[0].producer[1].producerLogoURL
        "https://prodtwologo.com"
      + @graph[0].keyword[1].@id
        "https://w3id.org/arp/dev/ro-id/doi:10.5072/FK2/UWPDNR/keyword/109"
      + @graph[0].keyword[1].keywordValue
        "SecondKwTerm"
      + @graph[0].keyword[1].keywordVocabulary
        "SecondCvn"
      + @graph[0].keyword[1].keywordVocabularyURI
        "https://secondcvn.com"
       
    * */
    @Test
    public void testMultipleValuesForAllowedFields() throws JsonProcessingException {
        String roCrateJsonString = null;
        try {
            roCrateJsonString = Files.readString(Paths.get("src/test/resources/arp/roCrateImportPrep/ro-crate-metadata-valid-multiple-values.json"));
        } catch (IOException e) {
            logger.warning(e.getMessage());
            Assertions.assertEquals(0, 1);
        }
        var processResult = roCrateImportManager.prepareRoCrateForDataverseImport(roCrateJsonString, true);
        logger.info(processResult.toJson().toString());
        Assertions.assertTrue(processResult.getErrors().isEmpty());
        Assertions.assertTrue(processResult.getWarnings().isEmpty());
    }

    /*
    Neither the subject nor the dsDescription fields allow multiple values.
    
    Diff:
        + @graph[0].dsDescription[1].@id
            "https://w3id.org/arp/dev/ro-id/doi:10.5072/FK2/UWPDNR/dsDescription/113"
        + @graph[0].dsDescription[1].dsDescriptionDate
            "1111-12-12"
        + @graph[0].subject
            [ "Engineering", "Law" ]
    * */
    @Test
    public void testMultipleValuesForNotAllowedFields() throws JsonProcessingException {
        String roCrateJsonString = null;
        try {
            roCrateJsonString = Files.readString(Paths.get("src/test/resources/arp/roCrateImportPrep/ro-crate-metadata-invalid-multiple-values.json"));
        } catch (IOException e) {
            logger.warning(e.getMessage());
            Assertions.assertEquals(0, 1);
        }
        var processResult = roCrateImportManager.prepareRoCrateForDataverseImport(roCrateJsonString, true);
        logger.info(processResult.toJson().toString());

        // Check that there are errors
        Assertions.assertFalse(processResult.getErrors().isEmpty());

        // Get the errors map
        Map<String, Map<String, Set<RoCrateImportPrepResult.IssueDetail>>> errors = processResult.getErrors();

        // Check that there's an error for the root entity ("./")
        Assertions.assertTrue(errors.containsKey("./"));
        Map<String, Set<RoCrateImportPrepResult.IssueDetail>> rootErrors = errors.get("./");

        // Verify the subject field error
        Assertions.assertTrue(rootErrors.containsKey("subject"));
        Set<RoCrateImportPrepResult.IssueDetail> subjectErrors = rootErrors.get("subject");
        Assertions.assertEquals(1, subjectErrors.size());

        RoCrateImportPrepResult.IssueDetail subjectError = subjectErrors.iterator().next();
        Assertions.assertEquals("subject", subjectError.fieldName());
        Assertions.assertEquals("The field does not allow multiple values, but got: [\"Engineering\",\"Law\"]",
                subjectError.message());
        Assertions.assertEquals("Provide only a single value for 'subject', e.g. pick one from: [\"Engineering\",\"Law\"]",
                subjectError.suggestion());

        // Verify the dsDescription field error
        Assertions.assertTrue(rootErrors.containsKey("dsDescription"));
        Set<RoCrateImportPrepResult.IssueDetail> dsDescErrors = rootErrors.get("dsDescription");
        Assertions.assertEquals(1, dsDescErrors.size());

        RoCrateImportPrepResult.IssueDetail dsDescError = dsDescErrors.iterator().next();
        Assertions.assertEquals("dsDescription", dsDescError.fieldName());
        Assertions.assertEquals("The field does not allow multiple values, but got: [{\"@id\":\"https://w3id.org/arp/dev/ro-id/doi:10.5072/FK2/UWPDNR/dsDescription/99\"},{\"@id\":\"https://w3id.org/arp/dev/ro-id/doi:10.5072/FK2/UWPDNR/dsDescription/113\"}]",
                dsDescError.message());
        Assertions.assertEquals("Provide only a single value for 'dsDescription', e.g. pick one from: [{\"@id\":\"https://w3id.org/arp/dev/ro-id/doi:10.5072/FK2/UWPDNR/dsDescription/99\"},{\"@id\":\"https://w3id.org/arp/dev/ro-id/doi:10.5072/FK2/UWPDNR/dsDescription/113\"}]",
                dsDescError.suggestion());

        // Check that there are exactly 2 fields with errors for the root entity
        Assertions.assertEquals(2, rootErrors.size());

        // Check that there are no other entities with errors
        Assertions.assertEquals(1, errors.size());
    }

    /*
    Test that parent-child entity pairs are present for every field, even if the field does not exist in dv.
    
    Diff:
        - @graph[0].author
        {
          "@id" : "https://w3id.org/arp/dev/ro-id/doi:10.5072/FK2/UWPDNR/author/98"
        }
        + @graph[0].producer
        {
          "@id": "https://w3id.org/arp/dev/ro-id/doi:10.5072/FK2/UWPDNR/producer/108"
        }
        - {
            "dsDescriptionValue" : "Descr_1",
            "name" : "Descr_1",
            "@id" : "https://w3id.org/arp/dev/ro-id/doi:10.5072/FK2/UWPDNR/dsDescription/99",
            "@type" : "dsDescription"
          }
          + {
            "producerName" : "ProdTwo",
            "producerAffiliation" : "ProdTwoAff",
            "producerAbbreviation" : "ProdTwoAN",
            "producerURL" : "https://prodtwo.com",
            "producerLogoURL" : "https://prodtwologo.com",
            "name" : "ProdTwo; (ProdTwoAff); (ProdTwoAN); <a href=\"https://prodtwo.com\" target=\"_blank\" rel=\"noopener\">https://prodtwo.com</a>; <img src=\"https://prodtwologo.com\" alt=\"Logo URL\" class=\"metadata-logo\"/><br/>",
            "@id" : "https://w3id.org/arp/dev/ro-id/doi:10.5072/FK2/UWPDNR/producer/104",
            "@type" : "producer"
          }
    
    */
    @Test
    public void testMissingChildOrParenEntities() throws JsonProcessingException {
        String roCrateJsonString = null;
        try {
            roCrateJsonString = Files.readString(Paths.get("src/test/resources/arp/roCrateImportPrep/ro-crate-metadata-missing-entities.json"));
        } catch (IOException e) {
            logger.warning(e.getMessage());
            Assertions.assertEquals(0, 1);
        }
        var processResult = roCrateImportManager.prepareRoCrateForDataverseImport(roCrateJsonString, true);
        logger.info(processResult.toJson().toString());

        // Check that there are errors
        Assertions.assertFalse(processResult.getErrors().isEmpty());

        // Get the errors map
        Map<String, Map<String, Set<RoCrateImportPrepResult.IssueDetail>>> errors = processResult.getErrors();

        // Check that there's an error for the root entity ("./")
        Assertions.assertTrue(errors.containsKey("./"));
        Map<String, Set<RoCrateImportPrepResult.IssueDetail>> rootErrors = errors.get("./");

        // Verify the dsDescription field error
        Assertions.assertTrue(rootErrors.containsKey("dsDescription"));
        Set<RoCrateImportPrepResult.IssueDetail> dsDescErrors = rootErrors.get("dsDescription");
        Assertions.assertEquals(1, dsDescErrors.size());

        RoCrateImportPrepResult.IssueDetail dsDescError = dsDescErrors.iterator().next();
        Assertions.assertEquals("dsDescription", dsDescError.fieldName());
        Assertions.assertEquals("No child entity found for the parent entity with id: https://w3id.org/arp/dev/ro-id/doi:10.5072/FK2/UWPDNR/dsDescription/99",
                dsDescError.message());
        Assertions.assertEquals("Ensure that an entity with id 'https://w3id.org/arp/dev/ro-id/doi:10.5072/FK2/UWPDNR/dsDescription/99' exists, or update the parent to reference a valid child entity.",
                dsDescError.suggestion());

        // Verify the producer field error
        Assertions.assertTrue(rootErrors.containsKey("producer"));
        Set<RoCrateImportPrepResult.IssueDetail> producerErrors = rootErrors.get("producer");
        Assertions.assertEquals(1, producerErrors.size());

        RoCrateImportPrepResult.IssueDetail producerError = producerErrors.iterator().next();
        Assertions.assertEquals("producer", producerError.fieldName());
        Assertions.assertEquals("No child entity found for the parent entity with id: 'https://w3id.org/arp/dev/ro-id/doi:10.5072/FK2/UWPDNR/producer/108'",
                producerError.message());
        Assertions.assertEquals("Ensure that an entity with id 'https://w3id.org/arp/dev/ro-id/doi:10.5072/FK2/UWPDNR/producer/108' exists, or update the parent to reference a valid child entity.",
                producerError.suggestion());

        // Check that there are exactly 2 fields with errors for the root entity
        Assertions.assertEquals(2, rootErrors.size());

        // Check that there are no other entities with errors
        Assertions.assertEquals(1, errors.size());
    }

    /*
    Test that serious errors are found at pre-checking the entities.
    
    Diff:
        - @graph[2].@id
          "https://w3id.org/arp/dev/ro-id/doi:10.5072/FK2/UWPDNR/datasetContact/97"
        - @graph[2].@type
          "datasetContact"
        - @graph[3].@type
          "author"
        - @graph[4].@type
          "dsDescription"
        + @graph[5]
          {
            "authorName": "Admin, Dataverse DUPLICATED",
            "authorAffiliation": "Dataverse.org DUPLICATED",
            "name": "Admin, Dataverse; (Dataverse.org) DUPLICATED",
            "@id": "https://w3id.org/arp/dev/ro-id/doi:10.5072/FK2/UWPDNR/author/98"
          }
    */
    @Test
    public void testFailingAtPreCheck() throws JsonProcessingException {
        String roCrateJsonString = null;
        try {
            roCrateJsonString = Files.readString(Paths.get("src/test/resources/arp/roCrateImportPrep/ro-crate-metadata-fail-at-pre-check.json"));
        } catch (IOException e) {
            logger.warning(e.getMessage());
            Assertions.assertEquals(0, 1);
        }
        var processResult = roCrateImportManager.prepareRoCrateForDataverseImport(roCrateJsonString, true);
        logger.info(processResult.toJson().toString());

        // Check that there are errors
        Assertions.assertFalse(processResult.getErrors().isEmpty());

        // Get the errors map
        Map<String, Map<String, Set<RoCrateImportPrepResult.IssueDetail>>> errors = processResult.getErrors();

        // Check that there are errors for two entities: the author entity and the RO-Crate entity
        Assertions.assertEquals(2, errors.size());

        // Check errors for the author entity
        String authorEntityId = "https://w3id.org/arp/dev/ro-id/doi:10.5072/FK2/UWPDNR/author/98";
        Assertions.assertTrue(errors.containsKey(authorEntityId));
        Map<String, Set<RoCrateImportPrepResult.IssueDetail>> authorErrors = errors.get(authorEntityId);

        // Verify the @type error for the author entity
        Assertions.assertTrue(authorErrors.containsKey("@type"));
        Set<RoCrateImportPrepResult.IssueDetail> typeErrors = authorErrors.get("@type");
        Assertions.assertEquals(1, typeErrors.size());

        RoCrateImportPrepResult.IssueDetail typeError = typeErrors.iterator().next();
        Assertions.assertEquals("@type", typeError.fieldName());
        Assertions.assertEquals("The entity with id: 'https://w3id.org/arp/dev/ro-id/doi:10.5072/FK2/UWPDNR/author/98' does not have a '@type'.",
                typeError.message());
        Assertions.assertEquals("Provide a supported '@type' property.",
                typeError.suggestion());

        // Verify the @id error for the author entity (duplicate ID)
        Assertions.assertTrue(authorErrors.containsKey("@id"));
        Set<RoCrateImportPrepResult.IssueDetail> idErrors = authorErrors.get("@id");
        Assertions.assertEquals(1, idErrors.size());

        RoCrateImportPrepResult.IssueDetail idError = idErrors.iterator().next();
        Assertions.assertEquals("@id", idError.fieldName());
        Assertions.assertEquals("The RO-Crate contains the following '@id' multiple times: https://w3id.org/arp/dev/ro-id/doi:10.5072/FK2/UWPDNR/author/98",
                idError.message());
        Assertions.assertEquals("Modify or remove the duplicated '@id'-s.",
                idError.suggestion());

        // Check that there are exactly 2 fields with errors for the author entity
        Assertions.assertEquals(2, authorErrors.size());

        // Check errors for the RO-Crate entity
        Assertions.assertTrue(errors.containsKey("RO-Crate"));
        Map<String, Set<RoCrateImportPrepResult.IssueDetail>> roCrateErrors = errors.get("RO-Crate");

        // Verify the @id errors for missing IDs
        Assertions.assertTrue(roCrateErrors.containsKey("@id"));
        Set<RoCrateImportPrepResult.IssueDetail> missingIdErrors = roCrateErrors.get("@id");
        Assertions.assertEquals(2, missingIdErrors.size());

        // Check for the first missing @id error (datasetContact entity)
        boolean foundDatasetContactError = false;
        boolean foundDsDescriptionError = false;

        for (RoCrateImportPrepResult.IssueDetail error : missingIdErrors) {
            if (error.message().contains("datasetContactName")) {
                foundDatasetContactError = true;
                Assertions.assertEquals("@id", error.fieldName());
                Assertions.assertTrue(error.message().contains("Missing '@id' for entity"));
                Assertions.assertTrue(error.message().contains("datasetContactName"));
                Assertions.assertEquals("Provide a valid '@id' value for the entity.",
                        error.suggestion());
            } else if (error.message().contains("dsDescriptionValue")) {
                foundDsDescriptionError = true;
                Assertions.assertEquals("@id", error.fieldName());
                Assertions.assertTrue(error.message().contains("Missing '@id' for entity"));
                Assertions.assertTrue(error.message().contains("dsDescriptionValue"));
                Assertions.assertEquals("Provide a valid '@id' value for the entity.",
                        error.suggestion());
            }
        }

        Assertions.assertTrue(foundDatasetContactError, "Should have found missing @id error for datasetContact entity");
        Assertions.assertTrue(foundDsDescriptionError, "Should have found missing @id error for dsDescription entity");

        // Check that there is exactly 1 field with errors for the RO-Crate entity
        Assertions.assertEquals(1, roCrateErrors.size());
    }


    private static void mockDatasetFieldSvc() {
        datasetFieldTypeSvc.setMetadataBlock("citation");

        DatasetFieldType titleType = datasetFieldTypeSvc.add(new DatasetFieldType("title", DatasetFieldType.FieldType.TEXTBOX, false));
        DatasetFieldType depositorType = datasetFieldTypeSvc.add(new DatasetFieldType("depositor", DatasetFieldType.FieldType.TEXT, false));
        DatasetFieldType authorType = datasetFieldTypeSvc.add(new DatasetFieldType("author", DatasetFieldType.FieldType.TEXT, true));
        Set<DatasetFieldType> authorChildTypes = new HashSet<>();
        authorChildTypes.add(datasetFieldTypeSvc.add(new DatasetFieldType("authorName", DatasetFieldType.FieldType.TEXT, false)));
        authorChildTypes.add(datasetFieldTypeSvc.add(new DatasetFieldType("authorAffiliation", DatasetFieldType.FieldType.TEXT, false)));
        authorChildTypes.add(datasetFieldTypeSvc.add(new DatasetFieldType("authorIdentifier", DatasetFieldType.FieldType.TEXT, false)));
        DatasetFieldType authorIdentifierSchemeType = datasetFieldTypeSvc.add(new DatasetFieldType("authorIdentifierScheme", DatasetFieldType.FieldType.TEXT, false));
        authorIdentifierSchemeType.setAllowControlledVocabulary(true);
        authorIdentifierSchemeType.setControlledVocabularyValues(Arrays.asList(
                // Why aren't these enforced? Should be ORCID, etc.
                new ControlledVocabularyValue(1l, "ark", authorIdentifierSchemeType),
                new ControlledVocabularyValue(2l, "doi", authorIdentifierSchemeType),
                new ControlledVocabularyValue(3l, "url", authorIdentifierSchemeType)
        ));
        authorChildTypes.add(datasetFieldTypeSvc.add(authorIdentifierSchemeType));
        for (DatasetFieldType t : authorChildTypes) {
            t.setParentDatasetFieldType(authorType);
        }
        authorType.setChildDatasetFieldTypes(authorChildTypes);

        DatasetFieldType datasetContactType = datasetFieldTypeSvc.add(new DatasetFieldType("datasetContact", DatasetFieldType.FieldType.TEXT, true));
        Set<DatasetFieldType> datasetContactTypes = new HashSet<>();
        datasetContactTypes.add(datasetFieldTypeSvc.add(new DatasetFieldType("datasetContactEmail", DatasetFieldType.FieldType.TEXT, false)));
        datasetContactTypes.add(datasetFieldTypeSvc.add(new DatasetFieldType("datasetContactName", DatasetFieldType.FieldType.TEXT, false)));
        datasetContactTypes.add(datasetFieldTypeSvc.add(new DatasetFieldType("datasetContactAffiliation", DatasetFieldType.FieldType.TEXT, false)));
        for (DatasetFieldType t : datasetContactTypes) {
            t.setParentDatasetFieldType(datasetContactType);
        }
        datasetContactType.setChildDatasetFieldTypes(datasetContactTypes);

        DatasetFieldType dsDescriptionType = datasetFieldTypeSvc.add(new DatasetFieldType("dsDescription", DatasetFieldType.FieldType.TEXT, false));
        Set<DatasetFieldType> dsDescriptionTypes = new HashSet<>();
        dsDescriptionTypes.add(datasetFieldTypeSvc.add(new DatasetFieldType("dsDescriptionValue", DatasetFieldType.FieldType.TEXT, false)));
        for (DatasetFieldType t : dsDescriptionTypes) {
            t.setParentDatasetFieldType(dsDescriptionType);
        }
        dsDescriptionType.setChildDatasetFieldTypes(dsDescriptionTypes);

        DatasetFieldType keywordType = datasetFieldTypeSvc.add(new DatasetFieldType("keyword", DatasetFieldType.FieldType.TEXT, true));
        Set<DatasetFieldType> keywordChildTypes = new HashSet<>();
        keywordChildTypes.add(datasetFieldTypeSvc.add(new DatasetFieldType("keywordValue", DatasetFieldType.FieldType.TEXT, false)));
        keywordChildTypes.add(datasetFieldTypeSvc.add(new DatasetFieldType("keywordVocabulary", DatasetFieldType.FieldType.TEXT, false)));
        keywordChildTypes.add(datasetFieldTypeSvc.add(new DatasetFieldType("keywordVocabularyURI", DatasetFieldType.FieldType.TEXT, false)));
        keywordType.setChildDatasetFieldTypes(keywordChildTypes);

        DatasetFieldType topicClassificationType = datasetFieldTypeSvc.add(new DatasetFieldType("topicClassification", DatasetFieldType.FieldType.TEXT, true));
        Set<DatasetFieldType> topicClassificationTypes = new HashSet<>();
        topicClassificationTypes.add(datasetFieldTypeSvc.add(new DatasetFieldType("topicClassValue", DatasetFieldType.FieldType.TEXT, false)));
        topicClassificationTypes.add(datasetFieldTypeSvc.add(new DatasetFieldType("topicClassVocab", DatasetFieldType.FieldType.TEXT, false)));
        topicClassificationTypes.add(datasetFieldTypeSvc.add(new DatasetFieldType("topicClassVocabURI", DatasetFieldType.FieldType.TEXT, false)));
        topicClassificationType.setChildDatasetFieldTypes(topicClassificationTypes);

        DatasetFieldType descriptionType = datasetFieldTypeSvc.add(new DatasetFieldType("description", DatasetFieldType.FieldType.TEXTBOX, false));

        DatasetFieldType subjectType = datasetFieldTypeSvc.add(new DatasetFieldType("subject", DatasetFieldType.FieldType.TEXT, false));
        subjectType.setAllowControlledVocabulary(true);
        subjectType.setControlledVocabularyValues(Arrays.asList(
                new ControlledVocabularyValue(1l, "Engineering", subjectType),
                new ControlledVocabularyValue(2l, "Law", subjectType),
                new ControlledVocabularyValue(3l, "Computer and Information Science", subjectType)
        ));

        DatasetFieldType pubIdType = datasetFieldTypeSvc.add(new DatasetFieldType("publicationIdType", DatasetFieldType.FieldType.TEXT, false));
        pubIdType.setAllowControlledVocabulary(true);
        pubIdType.setControlledVocabularyValues(Arrays.asList(
                new ControlledVocabularyValue(1l, "ark", pubIdType),
                new ControlledVocabularyValue(2l, "doi", pubIdType),
                new ControlledVocabularyValue(3l, "url", pubIdType)
        ));

        DatasetFieldType compoundSingleType = datasetFieldTypeSvc.add(new DatasetFieldType("coordinate", DatasetFieldType.FieldType.TEXT, true));
        Set<DatasetFieldType> childTypes = new HashSet<>();
        childTypes.add(datasetFieldTypeSvc.add(new DatasetFieldType("lat", DatasetFieldType.FieldType.TEXT, false)));
        childTypes.add(datasetFieldTypeSvc.add(new DatasetFieldType("lon", DatasetFieldType.FieldType.TEXT, false)));

        for (DatasetFieldType t : childTypes) {
            t.setParentDatasetFieldType(compoundSingleType);
        }
        compoundSingleType.setChildDatasetFieldTypes(childTypes);

        DatasetFieldType contributorType = datasetFieldTypeSvc.add(new DatasetFieldType("contributor", DatasetFieldType.FieldType.TEXT, true));
        Set<DatasetFieldType> contributorChildTypes = new HashSet<>();
        contributorChildTypes.add(datasetFieldTypeSvc.add(new DatasetFieldType("contributorName", DatasetFieldType.FieldType.TEXT, false)));
        DatasetFieldType contributorTypes = datasetFieldTypeSvc.add(new DatasetFieldType("contributorType", DatasetFieldType.FieldType.TEXT, false));
        contributorTypes.setAllowControlledVocabulary(true);
        contributorTypes.setControlledVocabularyValues(Arrays.asList(
                // Why aren't these enforced?
                new ControlledVocabularyValue(1l, "Data Collector", contributorTypes),
                new ControlledVocabularyValue(2l, "Data Curator", contributorTypes),
                new ControlledVocabularyValue(3l, "Data Manager", contributorTypes),
                new ControlledVocabularyValue(3l, "Editor", contributorTypes),
                new ControlledVocabularyValue(3l, "Funder", contributorTypes),
                new ControlledVocabularyValue(3l, "Hosting Institution", contributorTypes)
                // Etc. There are more.
        ));
        contributorChildTypes.add(datasetFieldTypeSvc.add(contributorTypes));
        for (DatasetFieldType t : contributorChildTypes) {
            t.setParentDatasetFieldType(contributorType);
        }
        contributorType.setChildDatasetFieldTypes(contributorChildTypes);

        DatasetFieldType publicationType = datasetFieldTypeSvc.add(new DatasetFieldType("publication", DatasetFieldType.FieldType.TEXT, true));
        Set<DatasetFieldType> publicationChildTypes = new HashSet<>();
        publicationChildTypes.add(datasetFieldTypeSvc.add(new DatasetFieldType("publicationCitation", DatasetFieldType.FieldType.TEXT, false)));
        DatasetFieldType publicationIdTypes = datasetFieldTypeSvc.add(new DatasetFieldType("publicationIDType", DatasetFieldType.FieldType.TEXT, false));
        publicationIdTypes.setAllowControlledVocabulary(true);
        publicationIdTypes.setControlledVocabularyValues(Arrays.asList(
                // Why aren't these enforced?
                new ControlledVocabularyValue(1l, "ark", publicationIdTypes),
                new ControlledVocabularyValue(2l, "arXiv", publicationIdTypes),
                new ControlledVocabularyValue(3l, "bibcode", publicationIdTypes),
                new ControlledVocabularyValue(4l, "cstr", publicationIdTypes),
                new ControlledVocabularyValue(5l, "doi", publicationIdTypes),
                new ControlledVocabularyValue(6l, "ean13", publicationIdTypes),
                new ControlledVocabularyValue(7l, "handle", publicationIdTypes)
                // Etc. There are more.
        ));
        publicationChildTypes.add(datasetFieldTypeSvc.add(publicationIdTypes));
        publicationChildTypes.add(datasetFieldTypeSvc.add(new DatasetFieldType("publicationIDNumber", DatasetFieldType.FieldType.TEXT, false)));
        DatasetFieldType publicationURLType = new DatasetFieldType("publicationURL", DatasetFieldType.FieldType.URL, false);
        publicationURLType.setDisplayFormat("<a href=\"#VALUE\" target=\"_blank\">#VALUE</a>");
        publicationChildTypes.add(datasetFieldTypeSvc.add(publicationURLType));
        publicationType.setChildDatasetFieldTypes(publicationChildTypes);

        DatasetFieldType timePeriodCoveredType = datasetFieldTypeSvc.add(new DatasetFieldType("timePeriodCovered", DatasetFieldType.FieldType.NONE, true));
        Set<DatasetFieldType> timePeriodCoveredChildTypes = new HashSet<>();
        timePeriodCoveredChildTypes.add(datasetFieldTypeSvc.add(new DatasetFieldType("timePeriodCoveredStart", DatasetFieldType.FieldType.DATE, false)));
        timePeriodCoveredChildTypes.add(datasetFieldTypeSvc.add(new DatasetFieldType("timePeriodCoveredEnd", DatasetFieldType.FieldType.DATE, false)));
        timePeriodCoveredType.setChildDatasetFieldTypes(timePeriodCoveredChildTypes);

        DatasetFieldType geographicCoverageType = datasetFieldTypeSvc.add(new DatasetFieldType("geographicCoverage", DatasetFieldType.FieldType.TEXT, true));
        Set<DatasetFieldType> geographicCoverageChildTypes = new HashSet<>();
        DatasetFieldType countries = datasetFieldTypeSvc.add(new DatasetFieldType("country", DatasetFieldType.FieldType.TEXT, false));
        countries.setAllowControlledVocabulary(true);
        countries.setControlledVocabularyValues(Arrays.asList(
                // Why aren't these enforced?
                new ControlledVocabularyValue(1l, "Afghanistan", countries),
                new ControlledVocabularyValue(2l, "Albania", countries)
                // And many more countries.
        ));
        geographicCoverageChildTypes.add(datasetFieldTypeSvc.add(countries));
        geographicCoverageChildTypes.add(datasetFieldTypeSvc.add(new DatasetFieldType("state", DatasetFieldType.FieldType.TEXT, false)));
        geographicCoverageChildTypes.add(datasetFieldTypeSvc.add(new DatasetFieldType("city", DatasetFieldType.FieldType.TEXT, false)));
        geographicCoverageChildTypes.add(datasetFieldTypeSvc.add(new DatasetFieldType("otherGeographicCoverage", DatasetFieldType.FieldType.TEXT, false)));
        geographicCoverageChildTypes.add(datasetFieldTypeSvc.add(new DatasetFieldType("geographicUnit", DatasetFieldType.FieldType.TEXT, false)));
        for (DatasetFieldType t : geographicCoverageChildTypes) {
            t.setParentDatasetFieldType(geographicCoverageType);
        }
        geographicCoverageType.setChildDatasetFieldTypes(geographicCoverageChildTypes);
    }
}
