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
        String describoProfileHun = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/textTypeDescriboProfile_hu.json"));
        String result = cedarTemplateToDescriboProfileConverter.processCedarTemplate(cedarTemplate);
        assertEquals(result, describoProfileHun);
    }

    @Test
    public void testTextTypeEn() throws IOException {
        cedarTemplateToDescriboProfileConverter  = new CedarTemplateToDescriboProfileConverter("en", null);
        String cedarTemplate = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/textTypeCedarTemplate.json"));
        String describoProfileHun = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/textTypeDescriboProfile_en.json"));
        String result = cedarTemplateToDescriboProfileConverter.processCedarTemplate(cedarTemplate);
        assertEquals(result, describoProfileHun);
    }

    @Test
    public void testTextAreaTypeHun() throws IOException {
        cedarTemplateToDescriboProfileConverter  = new CedarTemplateToDescriboProfileConverter("hu", null);
        String cedarTemplate = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/textAreaTypeCedarTemplate.json"));
        String describoProfileHun = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/textAreaTypeDescriboProfile_hu.json"));
        String result = cedarTemplateToDescriboProfileConverter.processCedarTemplate(cedarTemplate);
        assertEquals(result, describoProfileHun);
    }

    @Test
    public void testTextAreaTypeEn() throws IOException {
        cedarTemplateToDescriboProfileConverter  = new CedarTemplateToDescriboProfileConverter("en", null);
        String cedarTemplate = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/textAreaTypeCedarTemplate.json"));
        String describoProfileHun = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/textAreaTypeDescriboProfile_en.json"));
        String result = cedarTemplateToDescriboProfileConverter.processCedarTemplate(cedarTemplate);
        assertEquals(result, describoProfileHun);
    }

    @Test
    public void testNumberTypeHun() throws IOException {
        cedarTemplateToDescriboProfileConverter  = new CedarTemplateToDescriboProfileConverter("hu", null);
        String cedarTemplate = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/numberTypeCedarTemplate.json"));
        String describoProfileHun = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/numberTypeDescriboProfile_hu.json"));
        String result = cedarTemplateToDescriboProfileConverter.processCedarTemplate(cedarTemplate);
        assertEquals(result, describoProfileHun);
    }

    @Test
    public void testNumberTypeEn() throws IOException {
        cedarTemplateToDescriboProfileConverter  = new CedarTemplateToDescriboProfileConverter("en", null);
        String cedarTemplate = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/numberTypeCedarTemplate.json"));
        String describoProfileHun = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/numberTypeDescriboProfile_en.json"));
        String result = cedarTemplateToDescriboProfileConverter.processCedarTemplate(cedarTemplate);
        assertEquals(result, describoProfileHun);
    }

    @Test
    public void testDateTypeHun() throws IOException {
        cedarTemplateToDescriboProfileConverter  = new CedarTemplateToDescriboProfileConverter("hu", null);
        String cedarTemplate = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/dateTypeCedarTemplate.json"));
        String describoProfileHun = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/dateTypeDescriboProfile_hu.json"));
        String result = cedarTemplateToDescriboProfileConverter.processCedarTemplate(cedarTemplate);
        assertEquals(result, describoProfileHun);
    }

    @Test
    public void testDateTypeEn() throws IOException {
        cedarTemplateToDescriboProfileConverter  = new CedarTemplateToDescriboProfileConverter("en", null);
        String cedarTemplate = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/dateTypeCedarTemplate.json"));
        String describoProfileHun = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/dateTypeDescriboProfile_en.json"));
        String result = cedarTemplateToDescriboProfileConverter.processCedarTemplate(cedarTemplate);
        assertEquals(result, describoProfileHun);
    }

    @Test
    public void testUrlTypeHun() throws IOException {
        cedarTemplateToDescriboProfileConverter  = new CedarTemplateToDescriboProfileConverter("hu", null);
        String cedarTemplate = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/urlTypeCedarTemplate.json"));
        String describoProfileHun = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/urlTypeDescriboProfile_hu.json"));
        String result = cedarTemplateToDescriboProfileConverter.processCedarTemplate(cedarTemplate);
        assertEquals(result, describoProfileHun);
    }

    @Test
    public void testUrlTypeEn() throws IOException {
        cedarTemplateToDescriboProfileConverter  = new CedarTemplateToDescriboProfileConverter("en", null);
        String cedarTemplate = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/urlTypeCedarTemplate.json"));
        String describoProfileHun = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/urlTypeDescriboProfile_en.json"));
        String result = cedarTemplateToDescriboProfileConverter.processCedarTemplate(cedarTemplate);
        assertEquals(result, describoProfileHun);
    }

    @Test
    public void testListTypeHun() throws IOException {
        cedarTemplateToDescriboProfileConverter  = new CedarTemplateToDescriboProfileConverter("hu", null);
        String cedarTemplate = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/listTypeCedarTemplate.json"));
        String describoProfileHun = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/listTypeDescriboProfile_hu.json"));
        String result = cedarTemplateToDescriboProfileConverter.processCedarTemplate(cedarTemplate);
        assertEquals(result, describoProfileHun);
    }

    @Test
    public void testListTypeEn() throws IOException {
        cedarTemplateToDescriboProfileConverter  = new CedarTemplateToDescriboProfileConverter("en", null);
        String cedarTemplate = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/listTypeCedarTemplate.json"));
        String describoProfileHun = Files.readString(Paths.get("src/test/resources/arp/cedarTemplateToDescriboProfile/listTypeDescriboProfile_en.json"));
        String result = cedarTemplateToDescriboProfileConverter.processCedarTemplate(cedarTemplate);
        assertEquals(result, describoProfileHun);
    }
}
