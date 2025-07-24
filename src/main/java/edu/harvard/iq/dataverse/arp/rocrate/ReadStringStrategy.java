package edu.harvard.iq.dataverse.arp.rocrate;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.kit.datamanager.ro_crate.objectmapper.MyObjectMapper;
import edu.kit.datamanager.ro_crate.reader.GenericReaderStrategy;
import org.apache.commons.lang3.NotImplementedException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

public class ReadStringStrategy implements GenericReaderStrategy<String> {
    @Override
    public ObjectNode readMetadataJson(String roCrateJsonString) {
        ObjectMapper objectMapper = MyObjectMapper.getMapper();
        ObjectNode objectNode = objectMapper.createObjectNode();
        try {
            objectNode = objectMapper.readTree(roCrateJsonString).deepCopy();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return objectNode;
    }

    @Override
    public File readContent(String location) {
        return new File(location);
    }
}

