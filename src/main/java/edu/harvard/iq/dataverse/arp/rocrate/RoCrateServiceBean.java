package edu.harvard.iq.dataverse.arp.rocrate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.harvard.iq.dataverse.*;
import edu.harvard.iq.dataverse.api.arp.util.StorageUtils;
import edu.harvard.iq.dataverse.arp.ArpConfig;
import edu.harvard.iq.dataverse.arp.ArpServiceBean;
import edu.kit.datamanager.ro_crate.Crate;
import edu.kit.datamanager.ro_crate.RoCrate;
import edu.kit.datamanager.ro_crate.entities.AbstractEntity;
import edu.kit.datamanager.ro_crate.entities.data.RootDataEntity;
import edu.kit.datamanager.ro_crate.reader.Readers;
import jakarta.ejb.EJB;
import jakarta.ejb.Stateless;
import jakarta.inject.Named;

import java.io.File;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.logging.Logger;

@Stateless
@Named
public class RoCrateServiceBean {
    private static final Logger logger = Logger.getLogger(RoCrateServiceBean.class.getCanonicalName());

    @EJB
    DatasetFieldServiceBean fieldService;

    @EJB
    RoCrateConformsToIdProvider roCrateConformsToProvider;

    @EJB
    ArpConfig arpConfig;

    public final List<String> propsToIgnore = List.of("conformsTo", "name", "hasPart", "license");

    public Map<String, DatasetFieldType> getDatasetFieldTypeMapByConformsTo(RoCrate roCrate) {
        ArrayList<String> conformsToIds = new ArrayList<>();
        var conformsTo = roCrate.getRootDataEntity().getProperties().get("conformsTo");

        if (conformsTo != null) {
            if (conformsTo.isArray()) {
                conformsTo.elements().forEachRemaining(conformsToObj -> conformsToIds.add(conformsToObj.get("@id").textValue()));
            } else {
                conformsToIds.add(conformsTo.get("@id").textValue());
            }
        }

        List<String> mdbIds = roCrateConformsToProvider.findMetadataBlockForConformsToIds(conformsToIds).stream()
                .map(MetadataBlock::getIdString)
                .toList();

        return fieldService.findAllOrderedById().stream().filter(datasetFieldType -> mdbIds.contains(datasetFieldType.getMetadataBlock().getIdString())).collect(Collectors.toMap(DatasetFieldType::getName, Function.identity()));
    }

    public boolean hasType(JsonNode jsonNode, String typeString) {
        JsonNode typeProp = jsonNode.get("@type");
        boolean hasType = false;

        if (typeProp.isTextual()) {
            hasType = typeProp.textValue().equals(typeString);
        } else if (typeProp.isArray()) {
            for (var type : typeProp) {
                if (type.textValue().equals(typeString)) {
                    hasType = true;
                }
            }
        }

        return hasType;
    }

    public boolean isVirtualFile(ObjectNode file) {
        String pattern = ".*/([A-Za-z0-9]+)/file/([0-9]+)$";
        return !file.get("@id").textValue().matches(pattern);
    }

    public String getTypeAsString(JsonNode jsonNode) {
        JsonNode typeProp = jsonNode.get("@type");
        String typeString;

        if (typeProp.isArray()) {
            typeString = typeProp.get(0).textValue();
        } else {
            typeString = typeProp.textValue();
        }

        return typeString;
    }

    public void collectConformsToIds(Dataset dataset, RootDataEntity rootDataEntity) {
        collectConformsToIds(rootDataEntity, dataset, new ObjectMapper());
    }

    public void collectConformsToIds(RootDataEntity rootDataEntity, Dataset dataset, ObjectMapper mapper) {
        var conformsToArray = mapper.createArrayNode();
        final List<String> conformsToIdsFromMdbs;
        try {
            conformsToIdsFromMdbs = roCrateConformsToProvider.generateConformsToIds(dataset, rootDataEntity);
        } catch (RuntimeException e) {
            logger.warning("Failed to generate RO-Crate conformsTo ids: " + e.getMessage());
            return;
        }

        Set<String> existingConformsToIds = new HashSet<>();
        if (rootDataEntity.getProperties().has("conformsTo")) {
            JsonNode conformsToNode = rootDataEntity.getProperties().get("conformsTo");
            // conformsTo maybe an array or an object
            if (conformsToNode.isArray()) {
                conformsToNode.elements().forEachRemaining(jsonNode -> {
                    existingConformsToIds.add(((ObjectNode)jsonNode).get("@id").textValue());
                    conformsToArray.add(jsonNode);
                });
            }
            else {
                existingConformsToIds.add(((ObjectNode)conformsToNode).get("@id").textValue());
                conformsToArray.add(conformsToNode);
            }
        }

        // Add those ID-s that are not already in conformsToArray
        conformsToIdsFromMdbs.forEach(id -> {
            if (!existingConformsToIds.contains(id)) {
                conformsToArray.add(mapper.createObjectNode().put("@id", id));
            }
        });

        if (rootDataEntity.getProperties().has("conformsTo")) {
            rootDataEntity.getProperties().set("conformsTo", conformsToArray);
        } else {
            rootDataEntity.addProperty("conformsTo", conformsToArray);
        }
    }

    public String getRoCrateFolder(DatasetVersion version) {
        String localDir = StorageUtils.getLocalRoCrateDir(version.getDataset());
        var baseName = String.join(File.separator, localDir, "ro-crate-metadata");
        if (!version.isDraft()) {
            baseName += "_v" + version.getFriendlyVersionNumber();
        }
        return baseName;
    }

    public String getRoCratePath(DatasetVersion version) {
        return String.join(File.separator, getRoCrateFolder(version), ArpServiceBean.RO_CRATE_METADATA_JSON_NAME);
    }

    public boolean hasJsonCrate(Dataset dataset, String versionString) {
        if (dataset == null || dataset.getStorageIdentifier() == null || dataset.getStorageIdentifier().isEmpty()) {
            return false;
        }
        return Files.exists(Paths.get(getRoCratePath(resolveVersion(dataset, versionString))));
    }

    private DatasetVersion resolveVersion(Dataset dataset, String versionString) {
        if (versionString == null || versionString.isBlank()) {
            return dataset.getLatestVersionForCopy();
        }
        return dataset.getVersions().stream()
                .filter(dsv -> dsv.getFriendlyVersionNumber().equals(versionString))
                .findFirst()
                .orElseGet(dataset::getLatestVersion);
    }
    
    public List<String> collectConformsTo(DatasetVersion version) throws IOException {
        String roCrateFolderPath = getRoCrateFolder(version);
        Crate crate = Readers.newFolderReader().readCrate(roCrateFolderPath);
        return extractConformsToIds(crate.getRootDataEntity());
    }

    public boolean hasAdditionalRoCrateMetadata(DatasetVersion version) {
        if (version == null || !hasJsonCrate(version.getDataset(), version.getFriendlyVersionNumber())) {
            return false;
        }
        if (!loadFilesWithRoCrateMetadata(version.getDataset()).isEmpty()) {
            return true;
        }
        try {
            String roCrateFolderPath = getRoCrateFolder(version);
            Crate crate = Readers.newFolderReader().readCrate(roCrateFolderPath);
            RootDataEntity rootDataEntity = crate.getRootDataEntity();
            List<String> conformsToInRoCrate = extractConformsToIds(rootDataEntity);
            if (conformsToInRoCrate.isEmpty()) {
                return false;
            }
            Set<String> mdbConformsToIds = new HashSet<>(roCrateConformsToProvider.generateConformsToIds(version.getDataset(), rootDataEntity));
            return conformsToInRoCrate.stream().anyMatch(id -> !mdbConformsToIds.contains(id));
        } catch (Exception e) {
            logger.warning("Failed to check for additional RO-Crate metadata: " + e.getMessage());
            return false;
        }
    }

    private List<String> extractConformsToIds(RootDataEntity rootDataEntity) {
        ArrayList<String> ids = new ArrayList<>();
        JsonNode conformsTo = rootDataEntity.getProperties().get("conformsTo");
        if (conformsTo == null) {
            return ids;
        }
        if (conformsTo.isArray()) {
            conformsTo.forEach(idObj -> ids.add(idObj.get("@id").textValue()));
        } else {
            ids.add(conformsTo.get("@id").textValue());
        }
        return ids;
    }

    public String getRoCrateHtmlPreviewPath(DatasetVersion version) {
        return String.join(File.separator, getRoCrateFolder(version), arpConfig.get("arp.rocrate.html.preview.name"));
    }

    public String getDraftRoCrateJson(Dataset dataset) {
        String localDir = StorageUtils.getLocalRoCrateDir(dataset);
        return String.join(File.separator, localDir, "ro-crate-metadata", ArpServiceBean.RO_CRATE_METADATA_JSON_NAME);
    }

    public String getDraftRoCrateFolder(Dataset dataset) {
        String localDir = StorageUtils.getLocalRoCrateDir(dataset);
        return String.join(File.separator, localDir, "ro-crate-metadata");
    }

    public String getRoCrateParentFolder(Dataset dataset) {
        return StorageUtils.getLocalRoCrateDir(dataset);
    }

    public Set<Long> loadFilesWithRoCrateMetadata(Dataset dataset) {
        Path extrasPath = Paths.get(getRoCrateParentFolder(dataset), ArpServiceBean.RO_CRATE_EXTRAS_JSON_NAME);
        if (!Files.exists(extrasPath)) {
            return Collections.emptySet();
        }
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode root = objectMapper.readTree(extrasPath.toFile());
            JsonNode fileIds = root.get(ArpServiceBean.ID_OF_FILES_WITH_METADATA);
            if (fileIds == null || !fileIds.isArray()) {
                return Collections.emptySet();
            }
            Set<Long> result = new HashSet<>();
            fileIds.forEach(idNode -> result.add(idNode.asLong()));
            return result;
        } catch (IOException e) {
            return Collections.emptySet();
        }
    }

    // We use this generation logic for datasets that are the part of an RO-Crate (folder paths in DV)
    // not for Datasets as DV Objects, this might change later when the w3id is implemented
    public String createRoIdForDataset(String folderName, JsonNode parentObj) {
        String parentId = parentObj.get("@id").textValue();
        if (parentId.equals("./")) {
            parentId = "";
        }
        return parentId + URLEncoder.encode(folderName.replaceAll("\\s", "_"), StandardCharsets.UTF_8) + "/";
    }

    public String createRoIdForDataFile(DataFile dataFile) {
        // https://w3id.org/arp/ro-id/doi:A10.5072/FK2/ZL0O25/file/123
        return createRoidWithFieldName(
                dataFile.getOwner(),
                "file",
                dataFile.getId()
        );
    }

    public String getDataFileIdFromRoId(String roId) {
        return getLastPathElementAsId(roId);
    }

    public String createRoIdForCompound(DatasetFieldCompoundValue compoundValue) {
        // https://w3id.org/arp/ro-id/doi:A10.5072/FK2/ZL0O25/author/2088
        return createRoidWithFieldName(
                compoundValue.getParentDatasetField().getDatasetVersion().getDataset(),
                compoundValue.getParentDatasetField().getDatasetFieldType().getName(),
                compoundValue.getId()
        );
    }

    public String getCompoundIdFromRoId(String roId) {
        return getLastPathElementAsId(roId);
    }

    public String getLastPathElementAsId(String roId) {
        String[] pathSegments = roId.split("/");
        try {
            return pathSegments[pathSegments.length - 1];
        }
        catch (NumberFormatException ex) {
            throw new RuntimeException("Invalid ro-id"+roId);
        }
    }

    public String createRoidWithFieldName(Dataset dataset, String fieldName, Long dvId) {
        // https://w3id.org/arp/ro-id/doi:A10.5072/FK2/ZL0O25/author/2088
        String w3IdBase = arpConfig.get("arp.w3id.base");
        String pid = dataset.getGlobalId().asString();
        var roid = w3IdBase + "/ro-id/" + pid
                + "/" + fieldName
                + "/" + dvId;
        return roid;

    }

    /**
     * Replaces every {@code @id} reference to {@code oldId} with {@code newId} across the whole RO-Crate,
     * including nested file and folder entities.
     */
    public void replaceEntityIdReferences(RoCrate roCrate, String oldId, String newId) {
        if (oldId == null || newId == null || oldId.equals(newId)) {
            return;
        }
        replaceIdInProperties(roCrate.getRootDataEntity().getProperties(), oldId, newId);
        roCrate.getAllContextualEntities().forEach(entity -> replaceIdInProperties(entity.getProperties(), oldId, newId));
        roCrate.getAllDataEntities().forEach(entity -> replaceIdInProperties(entity.getProperties(), oldId, newId));
        AbstractEntity entity = roCrate.getEntityById(oldId);
        if (entity != null) {
            entity.getProperties().put("@id", newId);
        }
    }

    private void replaceIdInProperties(ObjectNode properties, String oldId, String newId) {
        properties.fields().forEachRemaining(field -> {
            if (propsToIgnore.contains(field.getKey())) {
                return;
            }
            JsonNode value = field.getValue();
            if (isIdReferenceNode(value)) {
                replaceIdNode(value, oldId, newId);
            }
        });
    }

    private boolean isIdReferenceNode(JsonNode node) {
        if (node.isObject() && node.size() == 1 && node.has("@id")) {
            return true;
        }
        if (node.isArray()) {
            for (JsonNode element : node) {
                if (!element.isObject() || element.size() != 1 || !element.has("@id")) {
                    return false;
                }
            }
            return !node.isEmpty();
        }
        return false;
    }

    private void replaceIdNode(JsonNode node, String oldId, String newId) {
        if (node.isObject()) {
            if (node.has("@id") && oldId.equals(node.get("@id").textValue())) {
                ((ObjectNode) node).put("@id", newId);
            }
        } else if (node.isArray()) {
            node.forEach(idObj -> replaceIdNode(idObj, oldId, newId));
        }
    }

}
