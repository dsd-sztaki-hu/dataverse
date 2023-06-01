package edu.harvard.iq.dataverse.api.arp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import edu.harvard.iq.dataverse.*;
import edu.harvard.iq.dataverse.api.arp.util.JsonHelper;
import edu.harvard.iq.dataverse.arp.ArpMetadataBlockServiceBean;
import edu.harvard.iq.dataverse.arp.DatasetFieldTypeArp;
import edu.harvard.iq.dataverse.util.BundleUtil;
import edu.kit.datamanager.ro_crate.RoCrate;
import edu.kit.datamanager.ro_crate.entities.contextual.ContextualEntity;
import edu.kit.datamanager.ro_crate.entities.data.FileEntity;
import edu.kit.datamanager.ro_crate.entities.data.RootDataEntity;
import edu.kit.datamanager.ro_crate.reader.FolderReader;
import edu.kit.datamanager.ro_crate.reader.RoCrateReader;
import org.apache.commons.lang3.tuple.Pair;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.inject.Named;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

@Stateless
@Named
public class RoCrateManager {

    private final String compoundIdAndUuidSeparator = "::";
    
    private final List<String> propsToIgnore = List.of("conformsTo", "name", "hasPart");

    @EJB
    DatasetFieldServiceBean fieldService;
    
    @EJB
    ArpMetadataBlockServiceBean arpMetadataBlockServiceBean;

    @EJB
    ControlledVocabularyValueServiceBean controlledVocabularyValueService;

    //TODO: what should we do with the "name" property of the contextualEntities? 
    // now the "name" prop is added from AROMA and it's value is the same as the original id of the entity
    public void createOrUpdate(RoCrate roCrate, Dataset dataset, boolean isCreation, Map<String, DatasetFieldType> datasetFieldTypeMap) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        RootDataEntity rootDataEntity = roCrate.getRootDataEntity();
        List<ContextualEntity> contextualEntities = roCrate.getAllContextualEntities();
        RoCrate.RoCrateBuilder roCrateContextUpdater = new RoCrate.RoCrateBuilder(roCrate);
        List<DatasetField> datasetFields = dataset.getLatestVersion().getDatasetFields();
        Set<MetadataBlock> conformsToMdbs = new HashSet<>();
        
        // Remove the entities from the RO-Crate that had been deleted from DV
        if (!isCreation) {
            removeDeletedEntities(roCrate, dataset, datasetFields, datasetFieldTypeMap);
        }

        // Update the RO-Crate with the values from DV
        for (var datasetField : datasetFields) {
            List<DatasetFieldCompoundValue> compoundValues = datasetField.getDatasetFieldCompoundValues();
            List<DatasetFieldValue> fieldValues = datasetField.getDatasetFieldValues();
            List<ControlledVocabularyValue> controlledVocabValues = datasetField.getControlledVocabularyValues();
            DatasetFieldType fieldType = datasetField.getDatasetFieldType();
            String fieldName = fieldType.getName();
            String fieldUri = fieldType.getUri();
            
            conformsToMdbs.add(datasetField.getDatasetFieldType().getMetadataBlock());
            
            // Update the contextual entities with the new compound values
            if (fieldType.isCompound()) {
                if (!rootDataEntity.getProperties().has(fieldName)) {
                    //Add new contextual entity to the RO-Crate
                    for (var compoundValue : compoundValues) {
                        addNewContextualEntity(roCrate, roCrateContextUpdater, compoundValue, mapper, fieldName, fieldUri, isCreation, false);
                    }
                } else {
                    // First, if the property is an array, remove every entity from the RO-Crate that had been deleted in DV,
                    // then update the entities in the RO-Crate with the new values from DV
                    var entityToUpdate = rootDataEntity.getProperties().get(fieldName);
                    if (entityToUpdate.isArray()) {
                        List<String> compoundValueIds = compoundValues.stream().map(compoundValue -> compoundValue.getId().toString()).collect(Collectors.toList());
                        for (Iterator<JsonNode> it = entityToUpdate.elements(); it.hasNext();) {
                            JsonNode entity = it.next();
                            String rootEntityId = entity.get("@id").textValue();
                            if (!compoundValueIds.contains(rootEntityId.split(compoundIdAndUuidSeparator)[0].substring(1))) {
                                ContextualEntity contextualEntityToDelete = contextualEntities.stream().filter(contextualEntity -> contextualEntity.getId().equals(rootEntityId)).findFirst().get();
                                List<String> compoundValueProps = datasetField.getDatasetFieldType().getChildDatasetFieldTypes().stream().map(DatasetFieldType::getName).collect(Collectors.toList());
                                // Delete the properties from the contextual entity that was removed from DV,
                                // if the contextual entity does not contain any other props than @id and @type, just delete it
                                // else, the contextual entity still contains properties and those props are from AROMA, we can not delete the contextual entity
                                for (var prop : compoundValueProps) {
                                    contextualEntityToDelete.getProperties().remove(prop);
                                }
                                var actProperties = contextualEntityToDelete.getProperties();
                                if (actProperties.size() == 2 && actProperties.has("@id") && actProperties.has("@type")) {
                                    it.remove();
                                    roCrate.deleteEntityById(contextualEntityToDelete.getId());
                                }
                            }
                        }
                    }

                    // The entityToUpdate value has to be updated after every modification, in case its value is changed in the RO-Crate,
                    // eg: from object to array
                    for (var compoundValue : compoundValues) {
                        String entityToUpdateId;
                        entityToUpdate = rootDataEntity.getProperties().get(fieldName);
                        if (entityToUpdate.isObject() && compoundValue.getDisplayOrder() == 0) {
                            entityToUpdateId = entityToUpdate.get("@id").textValue();
                        } else if (entityToUpdate.isArray()) {
                            String matchingId = "";
                            for (var idObj : entityToUpdate) {
                                if (idObj.get("@id").textValue().split(compoundIdAndUuidSeparator)[0].substring(1).equals(compoundValue.getId().toString())) {
                                    matchingId = idObj.get("@id").textValue();
                                    break;
                                }
                            }
                            if (matchingId.isBlank()) {
                                // There was no matching compoundValueId, this is a new entity
                                addNewContextualEntity(roCrate, roCrateContextUpdater, compoundValue, mapper, fieldName, fieldUri, isCreation, true);
                                continue;
                            } else {
                                entityToUpdateId = matchingId;
                            }
                        } else {
                            addNewContextualEntity(roCrate, roCrateContextUpdater, compoundValue, mapper, fieldName, fieldUri, isCreation, false);
                            continue;
                        }

                        //Update the fields of the entity with the values from DV
                        var actEntityToUpdate = contextualEntities.stream().filter(contextualEntity -> contextualEntity.getId().equals(entityToUpdateId)).findFirst().get();
                        //Loop through the properties of the compoundValue (the entity) and update the values
                        for (var childDatasetFieldType : datasetField.getDatasetFieldType().getChildDatasetFieldTypes()) {
                            String childFieldName = childDatasetFieldType.getName();
                            String childFieldUri = childDatasetFieldType.getUri();
                            Optional<DatasetField> optChildDatasetField = compoundValue.getChildDatasetFields().stream().filter(childDsf -> childDsf.getDatasetFieldType().getName().equals(childFieldName)).findFirst();
                            if (optChildDatasetField.isPresent()) {
                                DatasetField childDatasetField = optChildDatasetField.get();
                                List<DatasetFieldValue> childFieldValues = childDatasetField.getDatasetFieldValues();
                                List<ControlledVocabularyValue> childControlledVocabValues = childDatasetField.getControlledVocabularyValues();
                                DatasetFieldType childFieldType = childDatasetField.getDatasetFieldType();
                                if (!childFieldValues.isEmpty() || !childControlledVocabValues.isEmpty()) {
                                    var roCrateContext = mapper.readTree(roCrate.getJsonMetadata()).get("@context").get(1);
                                    //Update the property with the new value(s)
                                    if (isCreation || !roCrateContext.has(fieldName)) {
                                        roCrateContextUpdater.addValuePairToContext(fieldName, fieldUri);
                                    }
                                    if (isCreation || !roCrateContext.has(childFieldName)) {
                                        roCrateContextUpdater.addValuePairToContext(childFieldName, childFieldUri);
                                    }
                                    if (!childFieldValues.isEmpty()) {
                                        for (var childFieldValue : childFieldValues) {
                                            actEntityToUpdate.addProperty(childFieldName, childFieldValue.getValue());
                                        }
                                    }
                                    if (!childControlledVocabValues.isEmpty()) {
                                        if (childControlledVocabValues.size() == 1) {
                                            actEntityToUpdate.addProperty(childFieldName, childControlledVocabValues.get(0).getStrValue());
                                        } else {
                                            ArrayNode cvvs = mapper.createArrayNode();
                                            for (var controlledVocabValue : childControlledVocabValues) {
                                                cvvs.add(controlledVocabValue.getStrValue());
                                            }
                                            actEntityToUpdate.addProperty(childFieldName, cvvs);
                                            
                                        }
                                    }
                                }
                            } else {
                                //Remove the property, because the value was deleted in DV
                                actEntityToUpdate.getProperties().remove(childFieldName);
                                roCrate.deleteValuePairFromContext(childFieldName);
                            }
                        }
                    }
                }

            } else {
                var roCrateContext = mapper.readTree(roCrate.getJsonMetadata()).get("@context").get(1);
                if (isCreation || !roCrateContext.has(fieldName)) {
                    roCrateContextUpdater.addValuePairToContext(fieldName, fieldUri);
                }
                if (!fieldValues.isEmpty()) {
                    if (fieldValues.size() == 1) {
                        rootDataEntity.addProperty(fieldName, fieldValues.get(0).getValue());
                    } else {
                        ArrayNode valuesNode = mapper.createArrayNode();
                        for (var fieldValue : fieldValues) {
                            valuesNode.add(fieldValue.getValue());
                        }
                        rootDataEntity.addProperty(fieldName, valuesNode);
                    }
                }
                if (!controlledVocabValues.isEmpty()) {
                    if (controlledVocabValues.size() == 1) {
                        rootDataEntity.addProperty(fieldName, controlledVocabValues.get(0).getStrValue());
                    } else {
                        ArrayNode strValuesNode = mapper.createArrayNode();
                        for (var controlledVocabValue : controlledVocabValues) {
                            strValuesNode.add(controlledVocabValue.getStrValue());
                        }
                        rootDataEntity.addProperty(fieldName, strValuesNode);
                    }
                }

            }
        }

        var conformsToArray = mapper.createArrayNode();
        var conformsToIds = conformsToMdbs.stream().map(mdb -> {
            var mdbArp = arpMetadataBlockServiceBean.findMetadataBlockArpForMetadataBlock(mdb);
            if (mdbArp == null) {
                throw new Error("No ARP metadatablock found for metadatablock '"+mdb.getName()+
                        "'. You need to upload the metadatablock to CEDAR and back to Dataverse to connect it with its CEDAR template representation");
            }
            return mdbArp.getRoCrateConformsToId();
        }).collect(Collectors.toList());

        conformsToIds.forEach(id -> {
            conformsToArray.add(mapper.createObjectNode().put("@id", id));
        });
        
        rootDataEntity.addProperty("conformsTo", conformsToArray);
        
    }
    
    private void removeDeletedEntities(RoCrate roCrate, Dataset dataset, List<DatasetField> datasetFields, Map<String, DatasetFieldType> datasetFieldTypeMap) {
        List<String> fieldNames = datasetFields.stream().map(dsf -> dsf.getDatasetFieldType().getName()).collect(Collectors.toList());
        List<String> removedEntityNames = new ArrayList<>();
        RootDataEntity rootDataEntity = roCrate.getRootDataEntity();
        

        // First, remove the values from the RO-Crate that had been removed from DV and do not contain any additional props from AROMA,
        // even if the entity was removed in DV, if it contains any other props from AROMA, it will not be removed from the RO-Crate,
        // but it will not be displayed in DV
        rootDataEntity.getProperties().fieldNames().forEachRemaining(entityName -> {
            if (!entityName.startsWith("@") && !propsToIgnore.contains(entityName) && !fieldNames.contains(entityName)) {
                removedEntityNames.add(entityName);
            }
        });

        for (var removedEntityName : removedEntityNames) {
            var datasetFieldType = datasetFieldTypeMap.get(removedEntityName);
            if (datasetFieldType.isCompound()) {
                JsonNode rootEntity = rootDataEntity.getProperties().get(removedEntityName);
                if (rootEntity.isArray()) {
                    List<String> entityIdsToKeep = new ArrayList<>();
                    rootEntity.forEach(entity -> {
                        String entityId = entity.get("@id").textValue();
                        boolean removeFromRoot = deleteCompoundValue(roCrate, entityId, datasetFieldType);
                        if (!removeFromRoot) {
                            entityIdsToKeep.add(entityId);
                        }
                    });
                    if (entityIdsToKeep.isEmpty()) {
                        rootDataEntity.getProperties().remove(removedEntityName);
                        roCrate.deleteValuePairFromContext(removedEntityName);
                    } else {
                        for (Iterator<JsonNode> it = rootDataEntity.getProperties().withArray(removedEntityName).elements(); it.hasNext();) {
                            JsonNode entity = it.next();
                            if (!entityIdsToKeep.contains(entity.get("@id").textValue())) {
                                it.remove();
                            }
                        }
                    }
                } else {
                    boolean removeFromRoot = deleteCompoundValue(roCrate, rootEntity.get("@id").textValue(), datasetFieldType);
                    if (removeFromRoot) {
                        rootDataEntity.getProperties().remove(removedEntityName);
                        roCrate.deleteValuePairFromContext(removedEntityName);
                    }
                }
                // Remove the compoundFieldValue URIs from the context
                datasetFieldType.getChildDatasetFieldTypes().forEach(fieldType -> roCrate.deleteValuePairFromContext(fieldType.getName()));
            } else {
                rootDataEntity.getProperties().remove(removedEntityName);
                roCrate.deleteValuePairFromContext(removedEntityName);
            }
        }

        // Check and remove deleted files
        var fileNames = dataset.getLatestVersion().getFileMetadatas().stream().map(FileMetadata::getLabel).collect(Collectors.toList());
        var dataEntityNameAndIdPairs = roCrate.getAllDataEntities().stream().map(dataEntity -> Pair.of(dataEntity.getProperties().get("name").textValue(), dataEntity.getId())).collect(Collectors.toList());

        dataEntityNameAndIdPairs.forEach(entityNameAndIdPair -> {
            if (!fileNames.contains(entityNameAndIdPair.getKey())) {
                roCrate.deleteEntityById(entityNameAndIdPair.getValue());
            }
        });

    }

    private boolean deleteCompoundValue(RoCrate roCrate, String entityId, DatasetFieldType datasetFieldType) {
        ContextualEntity contextualEntity = roCrate.getContextualEntityById(entityId);
        datasetFieldType.getChildDatasetFieldTypes().forEach(fieldType -> {
            contextualEntity.getProperties().remove(fieldType.getName());
        });

        var actProperties = contextualEntity.getProperties();

        if (actProperties.size() == 2 && actProperties.has("@id") && actProperties.has("@type")) {
            //the entity is basically "empty" and needs to be removed
            roCrate.deleteEntityById(entityId);
            return true;
        }
        return false;
    }

    private void buildNewContextualEntity(RoCrate roCrate, RoCrate.RoCrateBuilder roCrateContextUpdater, ContextualEntity.ContextualEntityBuilder contextualEntityBuilder, DatasetFieldCompoundValue compoundValue, ObjectMapper mapper, String parentFieldName, String parentFieldUri, boolean isCreation) throws JsonProcessingException {
        for (var childDatasetField : compoundValue.getChildDatasetFields()) {
            List<DatasetFieldValue> childFieldValues = childDatasetField.getDatasetFieldValues();
            List<ControlledVocabularyValue> childControlledVocabValues = childDatasetField.getControlledVocabularyValues();
            if (!childFieldValues.isEmpty() || !childControlledVocabValues.isEmpty()) {
                var roCrateContext = mapper.readTree(roCrate.getJsonMetadata()).get("@context").get(1);
                if (isCreation || !roCrateContext.has(parentFieldName)) {
                    roCrateContextUpdater.addValuePairToContext(parentFieldName, parentFieldUri);
                }
                DatasetFieldType childFieldType = childDatasetField.getDatasetFieldType();
                String childFieldName = childFieldType.getName();
                String childFieldUri = childFieldType.getUri();
                if (isCreation || !roCrateContext.has(childFieldName)) {
                    roCrateContextUpdater.addValuePairToContext(childFieldName, childFieldUri);
                }
                if (!childFieldValues.isEmpty()) {
                    for (var childFieldValue : childFieldValues) {
                        contextualEntityBuilder.addProperty(childFieldName, childFieldValue.getValue());
                    }
                }
                if (!childControlledVocabValues.isEmpty()) {
                    for (var controlledVocabValue : childControlledVocabValues) {
                        contextualEntityBuilder.addProperty(childFieldName, controlledVocabValue.getStrValue());
                    }
                }
            }
        }
    }

    public void addNewContextualEntity(RoCrate roCrate, RoCrate.RoCrateBuilder roCrateContextUpdater, DatasetFieldCompoundValue compoundValue, ObjectMapper mapper, String parentFieldName, String parentFieldUri, boolean isCreation, boolean reorderCompoundValues) throws JsonProcessingException {
        ContextualEntity.ContextualEntityBuilder contextualEntityBuilder = new ContextualEntity.ContextualEntityBuilder();
        buildNewContextualEntity(roCrate, roCrateContextUpdater, contextualEntityBuilder, compoundValue, mapper, parentFieldName, parentFieldUri, isCreation);
        // The hashmark before the uuid is required in AROMA
        // "@id's starting with # as these signify the reference is internal to the crate"
        // the compoundIdAndUuidSeparator separates the id of the parent compound value from the uuid
        // the parent compound id is used to get the correct values upon modifying the RO-Crate with data from AROMA
        contextualEntityBuilder.setId("#" + compoundValue.getId() + compoundIdAndUuidSeparator + UUID.randomUUID());
        ContextualEntity contextualEntity = contextualEntityBuilder.build();
        // The "@id" is always a prop in a contextualEntity
        if (contextualEntity.getProperties().size() > 1) {
            contextualEntity.addType(parentFieldName);
            // To keep the order of the compound field values synchronised with their corresponding root data entity values
            // the new compound field values need to be inserted to the same position
            // in the RO-Crate as their displayPosition in DV, since the order of the values are displayed in AROMA 
            // based on the order of the values in the RO-Crate
            if (reorderCompoundValues) {
                roCrate.getRootDataEntity().getProperties().withArray(parentFieldName).insert(compoundValue.getDisplayOrder(), mapper.createObjectNode().put("@id", contextualEntity.getId()));
            } else {
                roCrate.getRootDataEntity().addIdProperty(parentFieldName, contextualEntity.getId());
            }
            roCrate.addContextualEntity(contextualEntity);
        }
    }

    public void generateRoCrateFiles(RoCrate roCrate, List<FileMetadata> fileMetadatas) {
        for (var fileMetadata : fileMetadatas) {
            FileEntity.FileEntityBuilder fileEntityBuilder = new FileEntity.FileEntityBuilder();
            String fileName = fileMetadata.getLabel();
            var dataEntities = roCrate.getAllDataEntities();
            if (dataEntities.isEmpty() || dataEntities.stream().noneMatch(dataEntity -> dataEntity.getProperties().get("name").textValue().equals(fileName))) {
                DataFile dataFile = fileMetadata.getDataFile();
                fileEntityBuilder.setId("#" + UUID.randomUUID());
                fileEntityBuilder.addProperty("name", fileName);
                fileEntityBuilder.addProperty("contentSize", dataFile.getFilesize());
                fileEntityBuilder.setEncodingFormat(dataFile.getContentType());
                if (fileMetadata.getDescription() != null) {
                    fileEntityBuilder.addProperty("description", fileMetadata.getDescription());
                }
                roCrate.addDataEntity(fileEntityBuilder.build(), true);
            }
        }
    }

    public String getRoCratePath(Dataset dataset) {
        return String.join(File.separator, getRoCrateFolder(dataset), BundleUtil.getStringFromBundle("arp.rocrate.metadata.name"));
    }

    public String getRoCrateFolder(Dataset dataset) {
        String filesRootDirectory = System.getProperty("dataverse.files.directory");
        if (filesRootDirectory == null || filesRootDirectory.isEmpty()) {
            filesRootDirectory = "/tmp/files";
        }

        return String.join(File.separator, filesRootDirectory, dataset.getAuthorityForFileStorage(), dataset.getIdentifierForFileStorage());
    }

    public void createOrUpdateRoCrate(Dataset dataset) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        var roCratePath = Paths.get(getRoCratePath(dataset));
        RoCrate roCrate;

        if (!Files.exists(roCratePath)) {
            roCrate = new RoCrate();
            createOrUpdate(roCrate, dataset, true, null);
            if (!Files.exists(Paths.get(getRoCrateFolder(dataset)))) {
                Files.createDirectories(Path.of(getRoCrateFolder(dataset)));
            }
        } else {
            RoCrateReader roCrateFolderReader = new RoCrateReader(new FolderReader());
            roCrate = roCrateFolderReader.readCrate(getRoCrateFolder(dataset));
            Map<String, DatasetFieldType> datasetFieldTypeMap = getDatasetFieldTypeMapByConformsTo(roCrate);
            createOrUpdate(roCrate, dataset, false, datasetFieldTypeMap);
        }
        generateRoCrateFiles(roCrate, dataset.getLatestVersion().getFileMetadatas());
        Files.writeString(roCratePath, objectMapper.readTree(roCrate.getJsonMetadata()).toPrettyString());
    }

    public String importRoCrate(Dataset dataset) {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode importFormatMetadataBlocks = mapper.createObjectNode();

        RoCrateReader roCrateFolderReader = new RoCrateReader(new FolderReader());
        RoCrate roCrate = roCrateFolderReader.readCrate(getRoCrateFolder(dataset));
        Map<String, DatasetFieldType> datasetFieldTypeMap = getDatasetFieldTypeMapByConformsTo(roCrate);
        RootDataEntity rootDataEntity = roCrate.getRootDataEntity();
        Map<String, ContextualEntity> contextualEntityHashMap = roCrate.getAllContextualEntities().stream().collect(Collectors.toMap(ContextualEntity::getId, Function.identity()));

        rootDataEntity.getProperties().fields().forEachRemaining(field -> {
            String fieldName = field.getKey();
            if (!fieldName.startsWith("@") && !propsToIgnore.contains(fieldName)) {
                DatasetFieldType datasetFieldType = datasetFieldTypeMap.get(fieldName);
                // RO-Crate spec: name: SHOULD identify the dataset to humans well enough to disambiguate it from other RO-Crates
                // In our case if the MDB has a "name" field, then we use it and store the value we get, otherwise
                // if there's no "name" field, we ignore it. Still, we should check for datasetFieldType == null,
                // wich must be an error.`
                if (fieldName.equals("name") && datasetFieldType == null) {
                    return;
                }
                MetadataBlock metadataBlock = datasetFieldType.getMetadataBlock();
                // Check if the import format already contains the field's parent metadata block
                if (!importFormatMetadataBlocks.has(metadataBlock.getName())) {
                    createMetadataBlock(importFormatMetadataBlocks, metadataBlock);
                }

                // Process the values depending on the field's type
                if (datasetFieldType.isCompound()) {
                    processCompoundField(importFormatMetadataBlocks, field.getValue(), datasetFieldType, datasetFieldTypeMap, contextualEntityHashMap, mapper);
                } else {
                    ArrayNode container = importFormatMetadataBlocks.get(metadataBlock.getName()).withArray("fields");
                    boolean ontoPortalBasedVocab = false;
                    if (datasetFieldType.isAllowControlledVocabulary()) {
                        // TODO: this needs to be revised. Dataverse creates controlledvocabularyvalue values for
                        // our fake controlled vocab based fields.
                        // ALSO: datasetfieldtype has allowcontrolledvocabulare=true. We want these fake fields to
                        // be handled internally as ormal text fields, only on UI show a selection box. The same in
                        // AROMA
//                        // If special controlled vocabulary based on ontoportal, then handle it as a normal field.
//                        DatasetFieldTypeArp dftArp = arpMetadataBlockServiceBean.findDatasetFieldTypeArpForFieldType(datasetFieldType);
//                        if (dftArp != null && dftArp.getCedarDefinition() != null) {
//                            String cedarDef = dftArp.getCedarDefinition();
//                            JsonObject templateFieldJson = new Gson().fromJson(cedarDef, JsonObject.class);
//                            if (JsonHelper.getJsonObject(templateFieldJson, "_valueConstraints.branches[0]") != null) {
//                                ontoPortalBasedVocab = true;
//                            }
//                        }
//                        if (ontoPortalBasedVocab) {
//                            processPrimitiveField(fieldName, field.getValue().textValue(), container, datasetFieldTypeMap, mapper);
//                        }
//                        else {
//                            processControlledVocabFields(fieldName, field.getValue(), container, datasetFieldTypeMap, mapper);
//                        }
                        processControlledVocabFields(fieldName, field.getValue(), container, datasetFieldTypeMap, mapper);
                    } else {
                        processPrimitiveField(fieldName, field.getValue().textValue(), container, datasetFieldTypeMap, mapper);
                    }
                }
            }
        });

        ObjectNode importFormatJson = mapper.createObjectNode();
        importFormatJson.set("metadataBlocks", importFormatMetadataBlocks);
        return importFormatJson.toPrettyString();
    }
    
    private Map<String, DatasetFieldType> getDatasetFieldTypeMapByConformsTo(RoCrate roCrate) {
        ArrayList<String> conformsToIds = new ArrayList<>();
        var conformsTo = roCrate.getRootDataEntity().getProperties().get("conformsTo");
        
        if (conformsTo.isArray()) {
            conformsTo.elements().forEachRemaining(conformsToObj -> conformsToIds.add(conformsToObj.get("@id").textValue()));            
        } else {
            conformsToIds.add(conformsTo.get("@id").textValue());
        }

        List<String> mdbIds = conformsToIds.stream().map(id -> arpMetadataBlockServiceBean.findByRoCrateConformsToId(id).getMetadataBlock().getIdString()).collect(Collectors.toList());
        
        return fieldService.findAllOrderedById().stream().filter(datasetFieldType -> mdbIds.contains(datasetFieldType.getMetadataBlock().getIdString())).collect(Collectors.toMap(DatasetFieldType::getName, Function.identity()));
    }

    public String preProcessRoCrateFromAroma(String roCrateJson) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode rootNode = mapper.readTree(roCrateJson);
        removeReverseProperties(rootNode);
        return rootNode.toPrettyString();
    }

    public RoCrate postProcessRoCrateFromAroma(Dataset dataset) throws JsonProcessingException {
        RoCrateReader roCrateFolderReader = new RoCrateReader(new FolderReader());
        RoCrate roCrate = roCrateFolderReader.readCrate(getRoCrateFolder(dataset));
        ObjectNode rootDataEntityProperties = roCrate.getRootDataEntity().getProperties();
        Map<String, DatasetFieldType> compoundFields = dataset.getLatestVersion().getDatasetFields().stream().map(DatasetField::getDatasetFieldType).filter(DatasetFieldType::isCompound).collect(Collectors.toMap(DatasetFieldType::getName, Function.identity()));
        // Updates the id of the entities in the RO-Crate with their new ids from DV
        rootDataEntityProperties.fields().forEachRemaining(prop -> {
            String propName = prop.getKey();
            JsonNode propVal = prop.getValue();
            if (propName.equals("hasPart")) {
                if (propVal.isObject()) {
                    updateIdForFileEntity(roCrate, propVal);
                } else {
                    for (var arrayVal : propVal) {
                        updateIdForFileEntity(roCrate, arrayVal);
                    }
                }
            } else if (!propName.startsWith("@") && !propsToIgnore.contains(propName) && compoundFields.containsKey(propName)) {
                List<DatasetFieldCompoundValue> compoundValueToObtainIdFrom = dataset.getLatestVersion().getDatasetFields().stream().filter(dsf -> dsf.getDatasetFieldType().equals(compoundFields.get(propName))).findFirst().get().getDatasetFieldCompoundValues();
                int positionOfProp = 0;
                if (propVal.isObject()) {
                    String oldId = propVal.get("@id").textValue();
                    updateIdForProperty(roCrate, compoundValueToObtainIdFrom, oldId, propName, positionOfProp);
                } else if (propVal.isArray()) {
                    for (var arrayVal : propVal) {
                        String oldId = arrayVal.get("@id").textValue();
                        if (positionOfProp < compoundValueToObtainIdFrom.size()) {
                            // If updateIdForProperty returns true, that means the processed entity contained at least one property from DV, 
                            // so the position where the next value should be among the compound values needs to be increased 
                            // else, the entity contained properties only from AROMA, the id of the entity can not be updated, and the position should not change
                            // this position means the index of the compound value that contains the values for the RO-Crate entity that is being processed
                            // the position (index), and the order of the values are in the right order, since the compound values are built from the preprocessed RO-Crate
                            // that comes from AROMA, and the new values are always placed to the end of the compound field values array
                            // if positionOfProp not smaller than compoundValueToObtainIdFrom.size(), the entity contains no props from DV, as a result, no compound value will be present for the entity
                            if (updateIdForProperty(roCrate, compoundValueToObtainIdFrom, oldId, propName, positionOfProp)) {
                                positionOfProp++;
                            }
                        } // else, the entity contains properties only from AROMA and none from DV, there's nothing to do 
                    }
                }
            } // else it's a string property, there's nothing to update
        });
        // Delete properties from the @context, because AROMA only sets them to "null" if the entity is deleted
        var roCrateContext = new ObjectMapper().readTree(roCrate.getJsonMetadata()).get("@context").get(1);
        roCrateContext.fields().forEachRemaining(entry -> {
            if (entry.getValue().textValue().equals("null")) {
                roCrate.deleteValuePairFromContext(entry.getKey());
            }
        });


        return roCrate;
    }

    private void updateIdForFileEntity(RoCrate roCrate, JsonNode rootDataEntity) {
        String oldId = rootDataEntity.get("@id").textValue();
        boolean idNeedsToBeUpdated = false;
        try {
            UUID.fromString(oldId);
        } catch (IllegalArgumentException ex) {
            idNeedsToBeUpdated = true;
        }

        if (idNeedsToBeUpdated) {
            String newId  = UUID.randomUUID().toString();
            ((ObjectNode) rootDataEntity).put("@id", newId);
            roCrate.getAllContextualEntities().stream().filter(contextualEntity -> contextualEntity.getId().equals(oldId)).findFirst().get().getProperties().put("@id", newId);
        }
    }

    private boolean updateIdForProperty(RoCrate roCrate, List<DatasetFieldCompoundValue> compoundValues, String oldId, String propName, int positionOfProp) {
        // Upon creating a new compound field for a dataset field type in AROMA, that allows multiple values, 
        // the initial roCrateJson that is sent by aroma can contain id-s in its root data entity that have no contextual entities
        // need to check for these scenarios too
        var contextualEntityToUpdateId = roCrate.getAllContextualEntities().stream().filter(contextualEntity -> contextualEntity.getId().equals(oldId)).findFirst();
        if (contextualEntityToUpdateId.isPresent()) {
            Set<String> compoundFieldProps = compoundValues.get(positionOfProp).getChildDatasetFields().stream().map(child -> child.getDatasetFieldType().getName()).collect(Collectors.toSet());
            Set<String> entityProps = new HashSet<>();
            contextualEntityToUpdateId.get().getProperties().fieldNames().forEachRemaining(fieldName -> {
                if (!fieldName.startsWith("@") && !propsToIgnore.contains(fieldName)) {
                    entityProps.add(fieldName);
                }
            });
            compoundFieldProps.retainAll(entityProps);
            // Take the intersection of the property names of the compoundFieldProps and entityProps, if the result is an empty set, the entity contained props only from AROMA, and none from DV
            // the id of this entity can not be updated (since there's no compound value for the entity)
            if (!compoundFieldProps.isEmpty()) {
                DatasetFieldCompoundValue valueToObtainIdFrom = compoundValues.get(positionOfProp);
                String newId = "#" + valueToObtainIdFrom.getId() + compoundIdAndUuidSeparator + UUID.randomUUID();
                var rootDataEntity = roCrate.getRootDataEntity().getProperties().get(propName);
                if (rootDataEntity.isObject()) {
                    ((ObjectNode)rootDataEntity).put("@id", newId);
                } else {
                    ((ObjectNode)rootDataEntity.get(positionOfProp)).put("@id", newId);
                }

                contextualEntityToUpdateId.get().getProperties().put("@id", newId);
                return true;
            }
        }
        return false;
    }

    private void removeReverseProperties(JsonNode node) {
        if (node.isObject()) {
            ObjectNode objectNode = (ObjectNode) node;
            objectNode.remove("@reverse");
            objectNode.fields().forEachRemaining(entry -> removeReverseProperties(entry.getValue()));
        } else if (node.isArray()) {
            node.forEach(this::removeReverseProperties);
        }
    }

    private void createMetadataBlock(ObjectNode jsonObject, MetadataBlock metadataBlock) {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode mdb = mapper.createObjectNode();
        mdb.put("displayName", metadataBlock.getDisplayName());
        mdb.put("name", metadataBlock.getName());
        mdb.putArray("fields");
        jsonObject.set(metadataBlock.getName(), mdb);
    }

    private void processCompoundField(JsonNode metadataBlocks, JsonNode roCrateValues, DatasetFieldType datasetField, Map<String, DatasetFieldType> datasetFieldTypeMap, Map<String, ContextualEntity> contextualEntityHashMap, ObjectMapper mapper) {
        ObjectNode compoundField = mapper.createObjectNode();
        compoundField.put("typeName", datasetField.getName());
        compoundField.put("multiple", datasetField.isAllowMultiples());
        compoundField.put("typeClass", "compound");

        if (roCrateValues.isArray()) {
            ArrayNode compoundFieldValues = mapper.createArrayNode();
            roCrateValues.forEach(value -> {
                //The compoundFieldValue can be empty, unfortunately, because creating a new compound field is in AROMA,
                //first generates a dummy object like: {"@id":"a", "@type":"w/e", "name":"same as the id of this object"}
                //and these values can not be a part of any DatasetFieldType, for better understanding check the comments in the processCompoundFieldValue function
                ObjectNode compoundFieldValue = processCompoundFieldValue(value, datasetFieldTypeMap, contextualEntityHashMap, mapper);
                if (!compoundFieldValue.isEmpty()) {
                    compoundFieldValues.add(compoundFieldValue);
                }
            });
            compoundField.set("value", compoundFieldValues);
        } else {
            ObjectNode compoundFieldValue = processCompoundFieldValue(roCrateValues, datasetFieldTypeMap, contextualEntityHashMap, mapper);
            if (!compoundFieldValue.isEmpty()) {
                if (datasetField.isAllowMultiples()) {
                    ArrayNode valueArray = mapper.createArrayNode();
                    valueArray.add(compoundFieldValue);
                    compoundField.set("value", valueArray);
                } else {
                    compoundField.set("value", compoundFieldValue);
                }
            }
        }

        ((ArrayNode) metadataBlocks.get(datasetField.getMetadataBlock().getName()).withArray("fields")).add(compoundField);
    }

    private ObjectNode processCompoundFieldValue(JsonNode roCrateValue, Map<String, DatasetFieldType> datasetFieldTypeMap, Map<String, ContextualEntity> contextualEntityHashMap, ObjectMapper mapper) {
        ObjectNode compoundFieldValue = mapper.createObjectNode();
        ContextualEntity contextualEntity = contextualEntityHashMap.get(roCrateValue.get("@id").textValue());
        // Upon creating a new compound field for a dataset field type in AROMA, that allows multiple values, 
        // the initial roCrateJson that is sent by aroma can contain entities (only id-s really) in its root data entity that have no contextual entities
        // as a result, the contextualEntity can be null
        if (contextualEntity != null) {
            contextualEntity.getProperties().fields().forEachRemaining(roCrateField -> {
                String fieldName = roCrateField.getKey();
                if (!fieldName.startsWith("@") && !propsToIgnore.contains(fieldName)) {
                    DatasetFieldType datasetFieldType = datasetFieldTypeMap.get(fieldName);
                    // RO-Crate spec: name: SHOULD identify the dataset to humans well enough to disambiguate it from other RO-Crates
                    // In our case if the MDB has a "name" field, then we use it and store the value we get, otherwise
                    // if there's no "name" field, we ignore it. Still, we should check for datasetFieldType == null,
                    // wich must be an error.
                    if (fieldName.equals("name") && datasetFieldType == null) {
                        return;
                    }
                    if (datasetFieldType.isAllowControlledVocabulary()) {
                        if (roCrateField.getValue().isArray()) {
                            List<String> controlledVocabValues = new ArrayList<>();
                            roCrateField.getValue().forEach(controlledVocabValue -> controlledVocabValues.add(controlledVocabValue.textValue()));
                            processControlledVocabFields(fieldName, controlledVocabValues, compoundFieldValue, datasetFieldTypeMap, mapper);
                        } else {
                            processControlledVocabFields(fieldName, roCrateField.getValue().textValue(), compoundFieldValue, datasetFieldTypeMap, mapper);
                        }
                    } else {
                        processPrimitiveField(fieldName, roCrateField.getValue().textValue(), compoundFieldValue, datasetFieldTypeMap, mapper);
                    }
                }
            });
        }

        return compoundFieldValue;
    }

    public void processPrimitiveField(String fieldName, String fieldValue, JsonNode container, Map<String, DatasetFieldType> datasetFieldTypeMap, ObjectMapper mapper) {
        DatasetFieldType datasetFieldType = datasetFieldTypeMap.get(fieldName);
        ObjectNode primitiveField = mapper.createObjectNode();
        primitiveField.put("typeName", datasetFieldType.getName());
        primitiveField.put("multiple", datasetFieldType.isAllowMultiples());
        primitiveField.put("typeClass", "primitive");
        if (datasetFieldType.isAllowMultiples()) {
            primitiveField.set("value", TextNode.valueOf(fieldValue));
        } else {
            primitiveField.put("value", fieldValue);
        }

        if (container.isObject()) {
            ((ObjectNode) container).set(datasetFieldType.getName(), primitiveField);
        } else if (container.isArray()) {
            ((ArrayNode) container).add(primitiveField);
        }
    }

    public void processControlledVocabFields(String fieldName, Object fieldValue, JsonNode container, Map<String, DatasetFieldType> datasetFieldTypeMap, ObjectMapper mapper) {
        DatasetFieldType datasetFieldType = datasetFieldTypeMap.get(fieldName);
        ObjectNode field = mapper.createObjectNode();

        if (fieldValue instanceof ArrayNode) {
            ArrayNode controlledVocabValues = mapper.createArrayNode();
            AtomicInteger i = new AtomicInteger();
            ((ArrayNode) fieldValue).forEach(controlledVocabValue -> {
                createControlledVocabularyValueForDatasetFieldType(controlledVocabValue.textValue(), datasetFieldType, i.getAndIncrement());
                controlledVocabValues.add(controlledVocabValue.textValue());
            });
            field.set("value", controlledVocabValues);
        } else {
            String controlledVocabValue;
            if (fieldValue instanceof TextNode) {
                controlledVocabValue = ((TextNode) fieldValue).textValue();
            } else {
                controlledVocabValue = fieldValue.toString();
            }
            if (datasetFieldType.isAllowMultiples()) {
                field.set("value", TextNode.valueOf(controlledVocabValue));
            } else {
                field.put("value", controlledVocabValue);
            }
            createControlledVocabularyValueForDatasetFieldType(controlledVocabValue, datasetFieldType, 0);
        }

        field.put("typeName", datasetFieldType.getName());
        field.put("multiple", datasetFieldType.isAllowMultiples());
        field.put("typeClass", "controlledVocabulary");


        if (container instanceof ObjectNode) {
            ((ObjectNode) container).set(datasetFieldType.getName(), field);
        } else if (container instanceof ArrayNode) {
            ((ArrayNode) container).add(field);
        }
    }
    
    public void createControlledVocabularyValueForDatasetFieldType(String strValue, DatasetFieldType datasetFieldType, int displayOrder) {
        DatasetFieldTypeArp dftArp = arpMetadataBlockServiceBean.findDatasetFieldTypeArpForFieldType(datasetFieldType);
        if (dftArp != null && dftArp.getCedarDefinition() != null) {
            String cedarDef = dftArp.getCedarDefinition();
            JsonObject templateFieldJson = new Gson().fromJson(cedarDef, JsonObject.class);
            if (JsonHelper.getJsonObject(templateFieldJson, "_valueConstraints.branches[0]") != null) {
                List<ControlledVocabularyValue> controlledVocabValues = controlledVocabularyValueService.findByDatasetFieldTypeId(datasetFieldType.getId());
                boolean externalValueNotYetExists = controlledVocabValues.stream().noneMatch(cvv -> cvv.getStrValue().equals(strValue));
                if (externalValueNotYetExists) {
                    var cvv = new ControlledVocabularyValue();
                    cvv.setStrValue(strValue);
                    cvv.setDatasetFieldType(datasetFieldType);
                    cvv.setDisplayOrder(displayOrder);
                    cvv.setIdentifier("");
                    fieldService.save(cvv);
                }
            }
        }
    }

}
