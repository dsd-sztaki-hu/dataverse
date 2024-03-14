package edu.harvard.iq.dataverse.arp.rocrate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.harvard.iq.dataverse.*;
import edu.harvard.iq.dataverse.api.arp.util.StorageUtils;
import edu.harvard.iq.dataverse.arp.ArpConfig;
import edu.harvard.iq.dataverse.arp.ArpServiceBean;
import edu.kit.datamanager.ro_crate.RoCrate;
import edu.kit.datamanager.ro_crate.entities.data.RootDataEntity;
import jakarta.ejb.EJB;
import jakarta.ejb.Stateless;
import jakarta.inject.Named;

import java.io.File;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Stateless
@Named
public class RoCrateServiceBean {

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
        return !file.has("@arpPid");
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
        var conformsToIdsFromMdbs = roCrateConformsToProvider.generateConformsToIds(dataset, rootDataEntity);

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

    public String getRoCrateHtmlPreviewPath(DatasetVersion version) {
        return String.join(File.separator, getRoCrateFolder(version), arpConfig.get("arp.rocrate.html.preview.name"));
    }

    public String getDraftRoCrateFolder(Dataset dataset) {
        String localDir = StorageUtils.getLocalRoCrateDir(dataset);
        return String.join(File.separator, localDir, "ro-crate-metadata", ArpServiceBean.RO_CRATE_METADATA_JSON_NAME);
    }

    public String getRoCrateParentFolder(Dataset dataset) {
        return StorageUtils.getLocalRoCrateDir(dataset);
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
    
}
