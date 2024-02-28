package edu.harvard.iq.dataverse.arp;

import com.fasterxml.jackson.core.JsonProcessingException;
import edu.harvard.iq.dataverse.ControlledVocabularyValue;
import edu.harvard.iq.dataverse.DatasetFieldType;
import edu.harvard.iq.dataverse.api.arp.RoCrateManager;
import edu.harvard.iq.dataverse.mocks.MockDatasetFieldSvc;
import edu.kit.datamanager.ro_crate.RoCrate;
import edu.kit.datamanager.ro_crate.reader.FolderReader;
import edu.kit.datamanager.ro_crate.reader.RoCrateReader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

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
    
    private static final RoCrateManager roCrateManager = new RoCrateManager(datasetFieldTypeSvc);

    @BeforeAll
    public static void setUpClass() {
        mockDatasetFieldSvc();
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
        var processResult = roCrateManager.prepareRoCrateForDataverseImport(roCrateJsonString);
        Assertions.assertTrue(processResult.errors.isEmpty());
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
        var processResult = roCrateManager.prepareRoCrateForDataverseImport(roCrateJsonString);
        processResult.errors.forEach(logger::info);
        Assertions.assertTrue(processResult.errors.isEmpty());
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
        var processResult = roCrateManager.prepareRoCrateForDataverseImport(roCrateJsonString);
        processResult.errors.forEach(logger::info);
        Assertions.assertFalse(processResult.errors.isEmpty());
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
        var processResult = roCrateManager.prepareRoCrateForDataverseImport(roCrateJsonString);
        processResult.errors.forEach(logger::info);
        Assertions.assertFalse(processResult.errors.isEmpty());
        // The child node for the producer was removed (field does not exist in dv)
        Assertions.assertTrue(processResult.errors.contains("No child entity found for the parent entity with id: 'https://w3id.org/arp/dev/ro-id/doi:10.5072/FK2/UWPDNR/producer/108'"));
        // The child node for the dsDescription was removed (field exists in dv)
        Assertions.assertTrue(processResult.errors.contains("No child entity found for the parent entity with id: 'https://w3id.org/arp/dev/ro-id/doi:10.5072/FK2/UWPDNR/dsDescription/99'"));
        // The parent node for the dsDescription was removed (field exists in dv), 
        // The producer node has no parent entity (field does not exist in dv)
        Assertions.assertTrue(processResult.errors.contains("Entities with the following '@id'-s could not be validated, check their relations in the RO-Crate: [https://w3id.org/arp/dev/ro-id/doi:10.5072/FK2/UWPDNR/author/98, https://w3id.org/arp/dev/ro-id/doi:10.5072/FK2/UWPDNR/producer/104]"));
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
        var processResult = roCrateManager.prepareRoCrateForDataverseImport(roCrateJsonString);
        processResult.errors.forEach(logger::info);
        Assertions.assertFalse(processResult.errors.isEmpty());
        Assertions.assertTrue(processResult.errors.contains("Missing '@id' for entity: {\"datasetContactName\":\"Admin, Dataverse\",\"datasetContactAffiliation\":\"Dataverse.org\",\"datasetContactEmail\":\"finta@sztaki.hu\",\"name\":\"Admin, Dataverse; (Dataverse.org); \"}"));
        Assertions.assertTrue(processResult.errors.contains("The entity with id: 'https://w3id.org/arp/dev/ro-id/doi:10.5072/FK2/UWPDNR/author/98' does not have a '@type'."));
        Assertions.assertTrue(processResult.errors.contains("Missing '@id' for entity: {\"dsDescriptionValue\":\"Descr_1\",\"name\":\"Descr_1\",\"@type\":\"dsDescription\"}"));
        Assertions.assertTrue(processResult.errors.contains("The RO-Crate contains the following '@id' multiple times: https://w3id.org/arp/dev/ro-id/doi:10.5072/FK2/UWPDNR/author/98"));
        
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
