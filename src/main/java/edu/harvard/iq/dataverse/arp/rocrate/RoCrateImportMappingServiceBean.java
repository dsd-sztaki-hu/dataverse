package edu.harvard.iq.dataverse.arp.rocrate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import edu.harvard.iq.dataverse.DataFile;
import edu.harvard.iq.dataverse.DatasetVersion;
import edu.harvard.iq.dataverse.FileMetadata;
import edu.harvard.iq.dataverse.util.StringUtil;
import jakarta.ejb.Stateless;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

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

    /**
     * Removes zip entries for files that already exist in the dataset.
     * Used when re-uploading an RO-Crate zip to an existing dataset so files are not duplicated
     * in the dataset or in the exported RO-Crate.
     *
     * @return filtered zip bytes, or {@code null} if every entry was removed
     */
    public byte[] filterExistingFilesFromZip(byte[] zipBytes, ArrayNode roCrateGraph, DatasetVersion version) throws IOException {
        if (zipBytes == null || zipBytes.length == 0) {
            return zipBytes;
        }

        Set<String> existingChecksums = new HashSet<>();
        Set<String> existingNamePaths = new HashSet<>();
        collectExistingFileKeys(version, existingChecksums, existingNamePaths);
        if (existingChecksums.isEmpty() && existingNamePaths.isEmpty()) {
            return zipBytes;
        }

        Set<String> zipPathsToSkip = new HashSet<>();
        if (roCrateGraph != null) {
            roCrateGraph.forEach(node -> {
                if (node.has("@type") && hasType(node, "File") && fileEntityMatchesExisting(node, existingChecksums, existingNamePaths)) {
                    zipPathsToSkip.add(normalizeZipPath(buildZipEntryPath(
                            node.has("directoryLabel") ? node.get("directoryLabel").textValue() : null,
                            node.has("name") ? node.get("name").textValue() : null
                    )));
                }
            });
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        boolean wroteAnyEntry = false;
        try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zipBytes));
             ZipOutputStream zos = new ZipOutputStream(baos)) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                if (entry.isDirectory()) {
                    continue;
                }
                String entryName = entry.getName();
                if (isMacOsMetadataEntry(entryName)) {
                    continue;
                }
                if (zipPathsToSkip.contains(normalizeZipPath(entryName))
                        || zipEntryMatchesExisting(entryName, existingNamePaths)) {
                    continue;
                }
                zos.putNextEntry(new ZipEntry(entryName));
                zis.transferTo(zos);
                zos.closeEntry();
                wroteAnyEntry = true;
            }
        }

        return wroteAnyEntry ? baos.toByteArray() : null;
    }

    private void collectExistingFileKeys(DatasetVersion version, Set<String> existingChecksums, Set<String> existingNamePaths) {
        for (FileMetadata fm : version.getFileMetadatas()) {
            if (fm.getDataFile() == null || fm.getDataFile().getId() == null) {
                continue;
            }
            String checksum = fm.getDataFile().getChecksumValue();
            if (checksum != null) {
                existingChecksums.add(checksum);
            }
            existingNamePaths.add(buildNamePathKey(fm.getLabel(), fm.getDirectoryLabel()));
        }
    }

    private boolean fileEntityMatchesExisting(JsonNode fileNode, Set<String> existingChecksums, Set<String> existingNamePaths) {
        String hash = fileNode.has("hash") ? fileNode.get("hash").textValue() : null;
        if (hash != null && existingChecksums.contains(hash)) {
            return true;
        }
        String name = fileNode.has("name") ? fileNode.get("name").textValue() : null;
        String directoryLabel = fileNode.has("directoryLabel") ? fileNode.get("directoryLabel").textValue() : null;
        return existingNamePaths.contains(buildNamePathKey(name, directoryLabel));
    }

    private boolean zipEntryMatchesExisting(String entryName, Set<String> existingNamePaths) {
        String shortName = entryName.replaceFirst("^.*[\\\\/]", "");
        if (shortName.startsWith("._") || shortName.startsWith(".DS_Store") || shortName.isEmpty()) {
            return false;
        }
        String directoryName = entryName.replaceFirst("[\\\\/][\\\\/]*[^\\\\/]*$", "");
        if (StringUtil.isEmpty(directoryName)) {
            directoryName = null;
        } else {
            directoryName = StringUtil.sanitizeFileDirectory(directoryName, true);
        }
        return existingNamePaths.contains(buildNamePathKey(shortName, directoryName));
    }

    private String buildNamePathKey(String name, String directoryLabel) {
        String normalizedDirectoryLabel = RoCrateServiceBean.normalizeDirectoryLabel(directoryLabel);
        if (normalizedDirectoryLabel == null) {
            return name;
        }
        return normalizedDirectoryLabel + "/" + name;
    }

    private String buildZipEntryPath(String directoryLabel, String name) {
        String normalizedDirectoryLabel = RoCrateServiceBean.normalizeDirectoryLabel(directoryLabel);
        if (normalizedDirectoryLabel == null) {
            return name;
        }
        return normalizedDirectoryLabel + "/" + name;
    }

    private String normalizeZipPath(String zipPath) {
        return zipPath.replace('\\', '/');
    }

    private boolean isMacOsMetadataEntry(String entryName) {
        String shortName = entryName.replaceFirst("^.*[\\\\/]", "");
        return shortName.startsWith("._") || shortName.startsWith(".DS_Store");
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
        String normalizedDirectoryLabel = RoCrateServiceBean.normalizeDirectoryLabel(directoryLabel);
        for (JsonNode node : roCrateFiles) {
            String nodeName = node.has("name") ? node.get("name").textValue() : null;
            String nodeDirectoryLabel = node.has("directoryLabel") ? node.get("directoryLabel").textValue() : null;

            if (Objects.equals(name, nodeName) &&
                    directoryLabelsMatch(normalizedDirectoryLabel, nodeDirectoryLabel)) {
                return Optional.of(node);
            }
        }

        return Optional.empty();
    }

    private boolean directoryLabelsMatch(String directoryLabel, String nodeDirectoryLabel) {
        if (directoryLabel == null) {
            return RoCrateServiceBean.normalizeDirectoryLabel(nodeDirectoryLabel) == null;
        }
        return Objects.equals(directoryLabel, RoCrateServiceBean.normalizeDirectoryLabel(nodeDirectoryLabel));
    }
}

