package edu.harvard.iq.dataverse.arp.rocrate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import edu.harvard.iq.dataverse.DataFile;
import jakarta.ejb.Stateless;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@Stateless
public class RoCrateImportMappingServiceBean {

    /**
     * Mapping between the fileEntity ids from the uploaded RO-Crate ("@id") and the storageIdentifier
     * of their newly created Dataverse DataFile.
     */
    public Map<String, String> createImportMapping(ArrayNode roCrateGraph, List<DataFile> importedFiles) {
        if (roCrateGraph == null || importedFiles == null || importedFiles.isEmpty()) {
            return Map.of();
        }

        HashMap<String, String> idAndStorageIdentifierMapping = new HashMap<>();

        // Collect the RO-Crate fileEntities for easier processing
        ArrayList<JsonNode> roCrateFiles = new ArrayList<>();
        roCrateGraph.forEach(jsonNode -> {
            if (jsonNode.has("@type") && hasType(jsonNode, "File")) {
                roCrateFiles.add(jsonNode);
            }
        });

        for (var importedFile : importedFiles) {
            String name = importedFile.getDisplayName();
            String directoryLabel = importedFile.getDirectoryLabel();
            String storageIdentifier = importedFile.getStorageIdentifier();

            Optional<JsonNode> correspondingFileEntity = findFileEntity(roCrateFiles, name, directoryLabel);
            correspondingFileEntity.ifPresent(fileEntity ->
                    idAndStorageIdentifierMapping.put(fileEntity.get("@id").textValue(), storageIdentifier)
            );
        }

        return idAndStorageIdentifierMapping;
    }

    private boolean hasType(JsonNode jsonNode, String type) {
        JsonNode typeNode = jsonNode.get("@type");
        if (typeNode == null) {
            return false;
        }
        if (typeNode instanceof ArrayNode) {
            for (int i = 0; i < typeNode.size(); i++) {
                var t = typeNode.get(i);
                if (t != null && Objects.equals(t.textValue(), type)) {
                    return true;
                }
            }
            return false;
        }
        return Objects.equals(typeNode.textValue(), type);
    }

    // find the corresponding file in the RO-CRATE for the uploaded datasetFile
    private Optional<JsonNode> findFileEntity(ArrayList<JsonNode> roCrateFiles, String name, String directoryLabel) {
        for (JsonNode node : roCrateFiles) {
            String nodeName = node.has("name") ? node.get("name").textValue() : null;
            String nodeDirectoryLabel = node.has("directoryLabel") ? node.get("directoryLabel").textValue() : null;

            if (Objects.equals(name, nodeName) &&
                    (directoryLabel == null || Objects.equals(directoryLabel, nodeDirectoryLabel))) {
                return Optional.of(node);
            }
        }

        return Optional.empty();
    }
}

