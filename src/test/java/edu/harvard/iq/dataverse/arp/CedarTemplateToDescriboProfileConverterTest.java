package edu.harvard.iq.dataverse.arp;

import edu.harvard.iq.dataverse.api.arp.CedarTemplateToDescriboProfileConverter;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CedarTemplateToDescriboProfileConverterTest {
    CedarTemplateToDescriboProfileConverter cedarTemplateToDescriboProfileConverter;
    
    @Test
    public void testTextTypeHun() throws IOException {
        cedarTemplateToDescriboProfileConverter  = new CedarTemplateToDescriboProfileConverter("hu", null);
        String cedarTemplate = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/textTypeCedarTemplate.json"));
        String expectedProfile = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/textTypeDescriboProfile_hu.json"));
        String generatedProfile = cedarTemplateToDescriboProfileConverter.processCedarTemplate(cedarTemplate);
        assertEquals(expectedProfile, generatedProfile);
    }

    @Test
    public void testTextTypeEn() throws IOException {
        cedarTemplateToDescriboProfileConverter  = new CedarTemplateToDescriboProfileConverter("en", null);
        String cedarTemplate = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/textTypeCedarTemplate.json"));
        String expectedProfile = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/textTypeDescriboProfile_en.json"));
        String generatedProfile = cedarTemplateToDescriboProfileConverter.processCedarTemplate(cedarTemplate);
        assertEquals(expectedProfile, generatedProfile);
    }

    @Test
    public void testTextAreaTypeHun() throws IOException {
        cedarTemplateToDescriboProfileConverter  = new CedarTemplateToDescriboProfileConverter("hu", null);
        String cedarTemplate = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/textAreaTypeCedarTemplate.json"));
        String expectedProfile = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/textAreaTypeDescriboProfile_hu.json"));
        String generatedProfile = cedarTemplateToDescriboProfileConverter.processCedarTemplate(cedarTemplate);
        assertEquals(expectedProfile, generatedProfile);
    }

    @Test
    public void testTextAreaTypeEn() throws IOException {
        cedarTemplateToDescriboProfileConverter  = new CedarTemplateToDescriboProfileConverter("en", null);
        String cedarTemplate = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/textAreaTypeCedarTemplate.json"));
        String expectedProfile = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/textAreaTypeDescriboProfile_en.json"));
        String generatedProfile = cedarTemplateToDescriboProfileConverter.processCedarTemplate(cedarTemplate);
        assertEquals(expectedProfile, generatedProfile);
    }

    @Test
    public void testNumberTypeHun() throws IOException {
        cedarTemplateToDescriboProfileConverter  = new CedarTemplateToDescriboProfileConverter("hu", null);
        String cedarTemplate = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/numberTypeCedarTemplate.json"));
        String expectedProfile = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/numberTypeDescriboProfile_hu.json"));
        String generatedProfile = cedarTemplateToDescriboProfileConverter.processCedarTemplate(cedarTemplate);
        assertEquals(expectedProfile, generatedProfile);
    }

    @Test
    public void testNumberTypeEn() throws IOException {
        cedarTemplateToDescriboProfileConverter  = new CedarTemplateToDescriboProfileConverter("en", null);
        String cedarTemplate = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/numberTypeCedarTemplate.json"));
        String expectedProfile = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/numberTypeDescriboProfile_en.json"));
        String generatedProfile = cedarTemplateToDescriboProfileConverter.processCedarTemplate(cedarTemplate);
        assertEquals(expectedProfile, generatedProfile);
    }

    @Test
    public void testDateTypeHun() throws IOException {
        cedarTemplateToDescriboProfileConverter  = new CedarTemplateToDescriboProfileConverter("hu", null);
        String cedarTemplate = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/dateTypeCedarTemplate.json"));
        String expectedProfile = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/dateTypeDescriboProfile_hu.json"));
        String generatedProfile = cedarTemplateToDescriboProfileConverter.processCedarTemplate(cedarTemplate);
        assertEquals(expectedProfile, generatedProfile);
    }

    @Test
    public void testDateTypeEn() throws IOException {
        cedarTemplateToDescriboProfileConverter  = new CedarTemplateToDescriboProfileConverter("en", null);
        String cedarTemplate = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/dateTypeCedarTemplate.json"));
        String expectedProfile = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/dateTypeDescriboProfile_en.json"));
        String generatedProfile = cedarTemplateToDescriboProfileConverter.processCedarTemplate(cedarTemplate);
        assertEquals(expectedProfile, generatedProfile);
    }

    @Test
    public void testUrlTypeHun() throws IOException {
        cedarTemplateToDescriboProfileConverter  = new CedarTemplateToDescriboProfileConverter("hu", null);
        String cedarTemplate = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/urlTypeCedarTemplate.json"));
        String expectedProfile = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/urlTypeDescriboProfile_hu.json"));
        String generatedProfile = cedarTemplateToDescriboProfileConverter.processCedarTemplate(cedarTemplate);
        assertEquals(expectedProfile, generatedProfile);
    }

    @Test
    public void testUrlTypeEn() throws IOException {
        cedarTemplateToDescriboProfileConverter  = new CedarTemplateToDescriboProfileConverter("en", null);
        String cedarTemplate = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/urlTypeCedarTemplate.json"));
        String expectedProfile = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/urlTypeDescriboProfile_en.json"));
        String generatedProfile = cedarTemplateToDescriboProfileConverter.processCedarTemplate(cedarTemplate);
        assertEquals(expectedProfile, generatedProfile);
    }

    @Test
    public void testListTypeHun() throws IOException {
        cedarTemplateToDescriboProfileConverter  = new CedarTemplateToDescriboProfileConverter("hu", null);
        String cedarTemplate = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/listTypeCedarTemplate.json"));
        String expectedProfile = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/listTypeDescriboProfile_hu.json"));
        String generatedProfile = cedarTemplateToDescriboProfileConverter.processCedarTemplate(cedarTemplate);
        assertEquals(expectedProfile, generatedProfile);
    }

    @Test
    public void testListTypeEn() throws IOException {
        cedarTemplateToDescriboProfileConverter  = new CedarTemplateToDescriboProfileConverter("en", null);
        String cedarTemplate = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/listTypeCedarTemplate.json"));
        String expectedProfile = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/listTypeDescriboProfile_en.json"));
        String generatedProfile = cedarTemplateToDescriboProfileConverter.processCedarTemplate(cedarTemplate);
        assertEquals(expectedProfile, generatedProfile);
    }
}
