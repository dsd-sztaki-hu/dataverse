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
//        RoCrateReader roCrateFolderReader = new RoCrateReader(new FolderReader());
//        RoCrate roCrate = roCrateFolderReader.readCrate("/Users/fintanorbert/Work/dataverse/payara5/glassfish/domains/domain1/files/10.5072/FK2/B5QZDJ/ro-crate-metadata");

        
        String roCrateJsonString = null;
        try {
            roCrateJsonString = Files.readString(Paths.get("/Users/fintanorbert/Work/dataverse/payara5/glassfish/domains/domain1/files/10.5072/FK2/B5QZDJ/ro-crate-metadata/ro-crate-metadata.json"));
        } catch (IOException e) {
            logger.warning(e.getMessage());
            Assertions.assertEquals(0, 1);
        }
        var processResult = roCrateManager.prepareRoCrateForDataverseImport(roCrateJsonString);
        Assertions.assertTrue(processResult.errors.isEmpty());
    }
    
    /*
    * Test that multiple values are only allowed where they are allowed by the scheme.
    * Processing an array of one value should be allowed.
    * 
    * Diff:
    *   - @graph[0].dsDescription
        + @graph[0].dsDescription[0]
          [
            {
              "@id" : "#11::fec52185-647e-470b-b2dd-c364462b82be",
              "@type" : "dsDescription"
            }
          ]
        
        + @graph[0].dsDescription[1].@id
          "#12::04fd9e9c-1ce6-4f0a-90cd-0153e44e8b1b"
        
        + @graph[0].dsDescription[1].dsDescriptionValue
          "Second desc"
          
        //this is still allowed, since the array contains a single value only
        - @graph[0].subject
        + @graph[0].subject[0]
          "Engineering"
        
        + @graph[0].datasetContact[1].@id
          "#13::e32f8827-2a7c-4dc2-8fb0-853d5bf53d1d"
        
        + @graph[0].datasetContact[1].datasetContactName
          "Second, Contact"
        
        + @graph[0].datasetContact[1].datasetContactAffiliation
          "Dataverse.org"
        
        + @graph[0].datasetContact[1].datasetContactEmail
          "dataverse2@mailinator.com"
       
    * */
    @Test
    public void testMultipleValuesForAllowedFields() throws JsonProcessingException {
        String roCrateJsonString = null;
        try {
            roCrateJsonString = Files.readString(Paths.get("/Users/fintanorbert/Work/dataverse/src/test/resources/arp/roCrateImportPrep/ro-crate-metadata-valid-multiple-values.json"));
        } catch (IOException e) {
            logger.warning(e.getMessage());
            Assertions.assertEquals(0, 1);
        }
        var processResult = roCrateManager.prepareRoCrateForDataverseImport(roCrateJsonString);
        processResult.errors.forEach(logger::info);
        Assertions.assertTrue(processResult.errors.isEmpty());
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

        DatasetFieldType dsDescriptionType = datasetFieldTypeSvc.add(new DatasetFieldType("dsDescription", DatasetFieldType.FieldType.TEXT, true));
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
                new ControlledVocabularyValue(2l, "law", subjectType),
                new ControlledVocabularyValue(3l, "cs", subjectType)
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
