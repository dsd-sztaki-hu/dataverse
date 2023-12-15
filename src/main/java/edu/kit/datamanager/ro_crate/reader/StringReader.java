package edu.kit.datamanager.ro_crate.reader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import edu.kit.datamanager.ro_crate.objectmapper.MyObjectMapper;
import org.apache.commons.lang3.NotImplementedException;

import java.io.File;
import java.io.IOException;

// Note: this class have been copied from the original ro_crate project and updated to parse an RO-Crate object
// from a string (instead of saving that string with the FolderWriter and re-read its content with the FolderReader)
// maybe we should ask for this functionality or create a pr

/**
 * A class for reading a crate from a folder.
 *
 * @author Nikola Tzotchev on 9.2.2022 г.
 * @version 1
 */
public class StringReader implements ReaderStrategy {

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
        throw new NotImplementedException();
    }
}