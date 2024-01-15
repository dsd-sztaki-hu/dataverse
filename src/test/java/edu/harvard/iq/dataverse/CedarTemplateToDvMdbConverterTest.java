package edu.harvard.iq.dataverse;

import edu.harvard.iq.dataverse.api.arp.CedarTemplateToDvMdbConverter;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CedarTemplateToDvMdbConverterTest {
    
    CedarTemplateToDvMdbConverter cedarTemplateToDvMdbConverter;
    
    @Test
    public void testCitationModifiedArpValuesForAuthorName() throws IOException {
        cedarTemplateToDvMdbConverter = new CedarTemplateToDvMdbConverter();
        String originalSchema = Files.readString(Paths.get("src/test/resources/arp/citation.json"));
        String originalTsv = Files.readString(Paths.get("src/test/resources/arp/citation.tsv"));
        String generatedMdbTsv = cedarTemplateToDvMdbConverter.processCedarTemplate(originalSchema, new HashSet<>());
        assertEquals(originalTsv.toLowerCase(), generatedMdbTsv.toLowerCase().trim());
        
        // Modify the "_arp" and "_valueConstraints" values of the authorName and datasetContactEmail properties as if 
        // these values were edited in CEDAR
        String modifiedSchema = Files.readString(Paths.get("src/test/resources/arp/citation_modified_arp_values.json"));
        String modifiedTsv = Files.readString(Paths.get("src/test/resources/arp/citation_modified_arp_values.tsv"));
        String generatedModifiedMdbTsv = cedarTemplateToDvMdbConverter.processCedarTemplate(modifiedSchema, new HashSet<>());
        assertEquals(modifiedTsv.toLowerCase(), generatedModifiedMdbTsv.toLowerCase().trim());
        
    }
    
}
