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
import edu.harvard.iq.dataverse.arp.ArpConfig;
import edu.harvard.iq.dataverse.arp.ArpMetadataBlockServiceBean;
import edu.harvard.iq.dataverse.arp.ArpServiceBean;
import edu.harvard.iq.dataverse.arp.DatasetFieldTypeArp;
import edu.kit.datamanager.ro_crate.RoCrate;
import edu.kit.datamanager.ro_crate.entities.AbstractEntity;
import edu.kit.datamanager.ro_crate.entities.contextual.ContextualEntity;
import edu.kit.datamanager.ro_crate.entities.data.FileEntity;
import edu.kit.datamanager.ro_crate.entities.data.RootDataEntity;
import edu.kit.datamanager.ro_crate.preview.AutomaticPreview;
import edu.kit.datamanager.ro_crate.reader.FolderReader;
import edu.kit.datamanager.ro_crate.reader.RoCrateReader;
import edu.kit.datamanager.ro_crate.writer.FolderWriter;
import edu.kit.datamanager.ro_crate.writer.RoCrateWriter;
import org.apache.commons.io.FileUtils;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.inject.Named;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static edu.harvard.iq.dataverse.arp.ArpServiceBean.RO_CRATE_EXTRAS_JSON_NAME;

@Stateless
@Named
public class RoCrateManager {

    private static final Logger logger = Logger.getLogger(RoCrateManager.class.getCanonicalName());

    private final String compoundIdAndUuidSeparator = "::";
    
    private final List<String> propsToIgnore = List.of("conformsTo", "name", "hasPart");
    private final List<String> dataverseFileProps = List.of("@id", "@type", "name", "contentSize", "encodingFormat", "directoryLabel", "description", "identifier", "@arpPid");
    private final List<String> dataverseDatasetProps = List.of("@id", "@type", "name", "hasPart");
    @EJB
    DatasetFieldServiceBean fieldService;
    
    @EJB
    ArpMetadataBlockServiceBean arpMetadataBlockServiceBean;

    @EJB
    ControlledVocabularyValueServiceBean controlledVocabularyValueService;
    
    @EJB
    MetadataBlockServiceBean metadataBlockServiceBean;

    @EJB
    ArpConfig arpConfig;

    //TODO: what should we do with the "name" property of the contextualEntities? 
    // now the "name" prop is added from AROMA and it's value is the same as the original id of the entity
    public void createOrUpdate(RoCrate roCrate, Dataset dataset, boolean isCreation, Map<String, DatasetFieldType> datasetFieldTypeMap) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        RootDataEntity rootDataEntity = roCrate.getRootDataEntity();
        RoCrate.RoCrateBuilder roCrateContextUpdater = new RoCrate.RoCrateBuilder(roCrate);
        List<DatasetField> datasetFields = dataset.getLatestVersion().getDatasetFields();
        Set<MetadataBlock> conformsToMdbs = new HashSet<>();
        
        // Add the persistentId of the dataset to the RootDataEntity
        rootDataEntity.addProperty("@arpPid", dataset.getGlobalId().toString());
        
        // Remove the entities from the RO-Crate that had been deleted from DV
        if (!isCreation) {
            removeDeletedEntities(roCrate, dataset, datasetFields, datasetFieldTypeMap);
        }

        // Update the RO-Crate with the values from DV
        for (var datasetField : datasetFields) {
            DatasetFieldType fieldType = datasetField.getDatasetFieldType();
            String fieldName = fieldType.getName();
            String fieldUri = fieldType.getUri();
            
            if (!datasetField.isEmpty()) {
                conformsToMdbs.add(datasetField.getDatasetFieldType().getMetadataBlock());
            }
            
            // Update the contextual entities with the new compound values
            if (fieldType.isCompound()) {
                processCompoundFieldType(roCrate, roCrateContextUpdater, rootDataEntity, datasetField, fieldName, fieldUri, mapper, isCreation);
            } else {
                processPrimitiveFieldType(roCrate, roCrateContextUpdater, rootDataEntity, datasetField, fieldName, fieldUri, mapper, isCreation);
            }
        }
        
        collectConformsToIds(dataset, rootDataEntity);
    }
    
    private void processPrimitiveFieldType(RoCrate roCrate, RoCrate.RoCrateBuilder roCrateContextUpdater, RootDataEntity rootDataEntity, DatasetField datasetField, String fieldName, String fieldUri, ObjectMapper mapper, boolean isCreation) throws JsonProcessingException {
        List<DatasetFieldValue> fieldValues = datasetField.getDatasetFieldValues();
        List<ControlledVocabularyValue> controlledVocabValues = datasetField.getControlledVocabularyValues();
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
        processControlledVocabularyValues(controlledVocabValues, rootDataEntity, fieldName, mapper);
    }
    
    private void processCompoundFieldType(RoCrate roCrate, RoCrate.RoCrateBuilder roCrateContextUpdater, RootDataEntity rootDataEntity, DatasetField datasetField, String fieldName, String fieldUri, ObjectMapper mapper, boolean isCreation) throws JsonProcessingException {
        List<DatasetFieldCompoundValue> compoundValues = datasetField.getDatasetFieldCompoundValues();
        List<ContextualEntity> contextualEntities = roCrate.getAllContextualEntities();
        
        if (!rootDataEntity.getProperties().has(fieldName)) {
            //Add new contextual entity to the RO-Crate
            for (var compoundValue : compoundValues) {
                addNewContextualEntity(roCrate, roCrateContextUpdater, compoundValue, mapper, datasetField, isCreation, false);
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

            processCompoundValues(roCrate, roCrateContextUpdater, compoundValues, contextualEntities, entityToUpdate, rootDataEntity, datasetField, fieldName, fieldUri, mapper, isCreation);
        }
    }
    
    private void processCompoundValues(RoCrate roCrate, RoCrate.RoCrateBuilder roCrateContextUpdater, List<DatasetFieldCompoundValue> compoundValues, List<ContextualEntity> contextualEntities, JsonNode entityToUpdate, RootDataEntity rootDataEntity, DatasetField datasetField, String fieldName, String fieldUri, ObjectMapper mapper, boolean isCreation) throws JsonProcessingException {
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
                    addNewContextualEntity(roCrate, roCrateContextUpdater, compoundValue, mapper, datasetField, isCreation, true);
                    continue;
                } else {
                    entityToUpdateId = matchingId;
                }
            } else {
                addNewContextualEntity(roCrate, roCrateContextUpdater, compoundValue, mapper, datasetField, isCreation, false);
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
                        processControlledVocabularyValues(childControlledVocabValues, actEntityToUpdate, childFieldName, mapper);
                    }
                } else {
                    //Remove the property, because the value was deleted in DV
                    actEntityToUpdate.getProperties().remove(childFieldName);
                    roCrate.deleteValuePairFromContext(childFieldName);
                }
            }
        }
    }
    
    private void processControlledVocabularyValues(List<ControlledVocabularyValue> controlledVocabValues, AbstractEntity parentEntity, String fieldName, ObjectMapper mapper) {
        if (!controlledVocabValues.isEmpty()) {
            if (controlledVocabValues.size() == 1) {
                parentEntity.addProperty(fieldName, controlledVocabValues.get(0).getStrValue());
            } else {
                ArrayNode strValuesNode = mapper.createArrayNode();
                for (var controlledVocabValue : controlledVocabValues) {
                    strValuesNode.add(controlledVocabValue.getStrValue());
                }
                parentEntity.addProperty(fieldName, strValuesNode);
            }
        }
    }

    private void collectConformsToIds(Dataset dataset, RootDataEntity rootDataEntity)
    {
        collectConformsToIds(rootDataEntity, dataset.getOwner().getMetadataBlocks(), new ObjectMapper());
    }

    private void collectConformsToIds(RootDataEntity rootDataEntity, Collection<MetadataBlock> conformsToMdbs, ObjectMapper mapper) {
        var conformsToArray = mapper.createArrayNode();
        var conformsToIdsFromMdbs = conformsToMdbs.stream().map(mdb -> {
            var mdbArp = arpMetadataBlockServiceBean.findMetadataBlockArpForMetadataBlock(mdb);
            if (mdbArp == null) {
                throw new Error("No ARP metadatablock found for metadatablock '"+mdb.getName()+
                        "'. You need to upload the metadatablock to CEDAR and back to Dataverse to connect it with its CEDAR template representation");
            }
            return mdbArp.getRoCrateConformsToId();
        }).collect(Collectors.toList());

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
    
    private void removeDeletedEntities(RoCrate roCrate, Dataset dataset, List<DatasetField> datasetFields, Map<String, DatasetFieldType> datasetFieldTypeMap) {
        List<String> fieldNames = datasetFields.stream().map(dsf -> dsf.getDatasetFieldType().getName()).collect(Collectors.toList());
        List<String> removedEntityNames = new ArrayList<>();
        RootDataEntity rootDataEntity = roCrate.getRootDataEntity();
        

        // First, remove the values from the RO-Crate that had been removed from DV and do not contain any additional props from AROMA,
        // even if the entity was removed in DV, if it contains any other props from AROMA, it will not be removed from the RO-Crate,
        // but it will not be displayed in DV
        rootDataEntity.getProperties().fieldNames().forEachRemaining(entityName -> {
            if (!entityName.startsWith("@") && !propsToIgnore.contains(entityName) && !fieldNames.contains(entityName)) {
                // If the datasetFieldTypeMap does not contain the entityName, that means the entity has AROMA specific props only
                if (datasetFieldTypeMap.containsKey(entityName)) {
                    removedEntityNames.add(entityName);
                }
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
    }
    
    private String getTypeAsString(JsonNode jsonNode) {
        JsonNode typeProp = jsonNode.get("@type");
        String typeString;
        
        if (typeProp.isArray()) {
            typeString = typeProp.get(0).textValue();
        } else {
            typeString = typeProp.textValue();
        }
        
        return typeString;
    }
    
    private boolean hasType(JsonNode jsonNode, String typeString) {
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

    private boolean deleteCompoundValue(RoCrate roCrate, String entityId, DatasetFieldType datasetFieldType) {
        AbstractEntity contextualEntity = roCrate.getEntityById(entityId);
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

    public void addNewContextualEntity(
            RoCrate roCrate,
            RoCrate.RoCrateBuilder roCrateContextUpdater,
            DatasetFieldCompoundValue compoundValue,
            ObjectMapper mapper,
            DatasetField parentField,
            boolean isCreation,
            boolean reorderCompoundValues
    ) throws JsonProcessingException
    {
        DatasetFieldType parentFieldType = parentField.getDatasetFieldType();
        String parentFieldName = parentFieldType.getName();
        String parentFieldUri = parentFieldType.getUri();
        DatasetFieldTypeArp dsfArp = arpMetadataBlockServiceBean.findDatasetFieldTypeArpForFieldType(parentFieldType);

        ContextualEntity.ContextualEntityBuilder contextualEntityBuilder = new ContextualEntity.ContextualEntityBuilder();
        buildNewContextualEntity(roCrate, roCrateContextUpdater, contextualEntityBuilder, compoundValue, mapper, parentFieldName, parentFieldUri, isCreation);

        // The hashmark before the uuid is required in AROMA
        // "@id's starting with # as these signify the reference is internal to the crate"
        // the compoundIdAndUuidSeparator separates the id of the parent compound value from the uuid
        // the parent compound id is used to get the correct values upon modifying the RO-Crate with data from AROMA
        contextualEntityBuilder.setId("#" + compoundValue.getId() + compoundIdAndUuidSeparator + UUID.randomUUID());

        String nameFieldValue = null;

        // If compound value has a "name" field, use its value by default
        var nameField = compoundValue.getChildDatasetFields().stream()
                .filter(datasetField -> datasetField.getDatasetFieldType().getName().equals("name"))
                .findFirst();
        if (nameField.isPresent()) {
            // No need to set anything, since the context entity will already have a "name" field with a value
        }
        // Find an explicit displayNameField set in dsfArp
        else if (dsfArp.getDisplayNameField() != null) {
            var displayNameField = dsfArp.getDisplayNameField();
            var displayNameFieldValue = compoundValue.getChildDatasetFields().stream()
                    .filter(datasetField -> datasetField.getDatasetFieldType().getName().equals(displayNameField))
                    .findFirst();
            if (displayNameFieldValue.isPresent()) {
                nameFieldValue = displayNameFieldValue.get().getDisplayValue();
            }
        }
        // Fall back to DV's own display value
        else {
            // Similar to metadataFragment.xhtml, but we separate using ';' instead of space
            // <ui:repeat value="#{compoundValue.displayValueMap.entrySet().toArray()}" var="cvPart" varStatus="partStatus">
            nameFieldValue = compoundValue.getDisplayValueMap().entrySet().stream()
                    .map(o -> o.getValue())
                    .collect(Collectors.joining("; "));
        }

        // if we have set any value for nameFieldValue use that
        if (nameFieldValue != null) {
            contextualEntityBuilder.addProperty("name", nameFieldValue);
        }

        //contextualEntity.addProperty("name", "displayNameField");
        ContextualEntity contextualEntity = contextualEntityBuilder.build();
        // The "@id" and "name" are always props in a contextualEntity
        if (contextualEntity.getProperties().size() > 2) {
            contextualEntity.addType(parentFieldName);
            // To keep the order of the compound field values synchronised with their corresponding root data entity values
            // the new compound field values need to be inserted to the same position
            // in the RO-Crate as their displayPosition in DV, since the order of the values are displayed in AROMA 
            // based on the order of the values in the RO-Crate
            if (reorderCompoundValues) {
                roCrate.getRootDataEntity().getProperties().withArray(parentFieldName).insert(
                        compoundValue.getDisplayOrder(),
                        mapper.createObjectNode().put("@id", contextualEntity.getId())
                );
            } else {
                roCrate.getRootDataEntity().addIdProperty(parentFieldName, contextualEntity.getId());
            }
            roCrate.addContextualEntity(contextualEntity);
        }
    }
    
    private String addFileEntity(RoCrate roCrate, FileMetadata fileMetadata, boolean toHasPart) {
        String fileName = fileMetadata.getLabel();
        String fileId;
        
        FileEntity.FileEntityBuilder fileEntityBuilder = new FileEntity.FileEntityBuilder();
        DataFile dataFile = fileMetadata.getDataFile();
        fileId = "#" + dataFile.getId() + "::" + UUID.randomUUID();
        fileEntityBuilder.setId(fileId);
        fileEntityBuilder.addProperty("@arpPid", dataFile.getGlobalId().toString());
        fileEntityBuilder.addProperty("name", fileName);
        fileEntityBuilder.addProperty("contentSize", dataFile.getFilesize());
        fileEntityBuilder.setEncodingFormat(dataFile.getContentType());
        if (fileMetadata.getDescription() != null) {
            fileEntityBuilder.addProperty("description", fileMetadata.getDescription());
        }
        if (fileMetadata.getDirectoryLabel() != null) {
            fileEntityBuilder.addProperty("directoryLabel", dataFile.getDirectoryLabel());
        }
        var file = fileEntityBuilder.build();
        roCrate.addDataEntity(file, toHasPart);
        
        return fileId;
    }
    
    public void processRoCrateFiles(RoCrate roCrate, List<FileMetadata> fileMetadatas) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        List<FileMetadata> datasetFiles = fileMetadatas.stream().map(FileMetadata::createCopy).collect(Collectors.toList());
        List<ObjectNode> roCrateFileEntities = Stream.concat(
                roCrate.getAllContextualEntities().stream().map(AbstractEntity::getProperties).filter(ce -> getTypeAsString(ce).equals("File")), 
                roCrate.getAllDataEntities().stream().map(AbstractEntity::getProperties).filter(de -> getTypeAsString(de).equals("File"))
        ).collect(Collectors.toList());
        
        // Delete the entities from the RO-CRATE that have been removed from DV
        roCrateFileEntities.forEach(fe -> {
            String fileEntityName = fe.has("name") ? fe.get("name").textValue() : null;
            String fileEntityDirectoryLabel = fe.has("directoryLabel") ? fe.get("directoryLabel").textValue() : null;
            Optional<FileMetadata> datasetFile = datasetFiles.stream().filter(fileMetadata -> Objects.equals(fileEntityName, fileMetadata.getLabel()) && Objects.equals(fileEntityDirectoryLabel, fileMetadata.getDirectoryLabel())).findFirst();
            if (datasetFile.isPresent()) {
                datasetFiles.removeIf(fileMetadata -> Objects.equals(fileEntityName, fileMetadata.getLabel()) && Objects.equals(fileEntityDirectoryLabel, fileMetadata.getDirectoryLabel()));
            } else {
                roCrate.deleteEntityById(fe.get("@id").textValue());
            }
        });
        
        // Add the new files to the RO-CRATE as well
        for (FileMetadata df : datasetFiles) {
            ArrayList<String> folderNames = df.getDirectoryLabel() != null ? new ArrayList<>(Arrays.asList(df.getDirectoryLabel().split("/"))) : new ArrayList<>();
            if (folderNames.isEmpty()) {
                //add file
                addFileEntity(roCrate, df, true);
            } else {
                //process folder path and add files to the new datasets
                //check the rootDataset first
                JsonNode rootDataset = findRootDatasetAsJsonNode(roCrate, mapper);
                String parentDatasetId = createDatasetsFromFoldersAndReturnParentId(roCrate, rootDataset, folderNames, mapper, true);
                // childDataset means that the dataset is not top level dataset
                // top level datasets that are child of the rootDataset, those datasets needs to be handled differently
                String newFiledId = addFileEntity(roCrate, df, false);
                AbstractEntity newChildDataset = roCrate.getEntityById(parentDatasetId);
                ObjectNode parentDataset = Objects.requireNonNullElseGet(newChildDataset, () -> roCrate.getAllDataEntities().stream().filter(dataEntity -> dataEntity.getId().equals(parentDatasetId)).findFirst().get()).getProperties();
                addIdToHasPart(parentDataset, newFiledId, mapper);
            }
        }

        // Delete the empty Datasets from the RO-CRATE, this has to be done after adding the new files
        // this way we can keep the original Dataset (folder) ID-s in the RO-CRATE
        deleteEmptyDatasets(roCrate);
    }
    
    public void deleteEmptyDatasets(RoCrate roCrate) {
        List<String> emptyDatasetIds = Stream.concat(
                roCrate.getAllContextualEntities().stream().map(AbstractEntity::getProperties).filter(ce -> 
                        getTypeAsString(ce).equals("Dataset") && hasPartIsEmpty(ce)),
                roCrate.getAllDataEntities().stream().map(AbstractEntity::getProperties).filter(de -> 
                        getTypeAsString(de).equals("Dataset") && hasPartIsEmpty(de))
        ).map(entity -> entity.get("@id").textValue()).collect(Collectors.toList());
        
        if (!emptyDatasetIds.isEmpty()) {
            emptyDatasetIds.forEach(roCrate::deleteEntityById);
            deleteEmptyDatasets(roCrate);
        }
    }


    public boolean hasPartIsEmpty(ObjectNode entity) {
        if (!entity.has("hasPart")) {
            return true;
        }
        
        var hasPart = entity.get("hasPart");
        if (!hasPart.isEmpty()) {
            if (hasPart.isObject()) {
                return false;
            } else {
                for (JsonNode e : hasPart) {
                    if (!e.isEmpty()) {
                        return false;
                    }
                }   
            }
        }

        return true;
    }
    
    private JsonNode findRootDatasetAsJsonNode(RoCrate roCrate, ObjectMapper mapper) throws JsonProcessingException {
        String rootEntityId = roCrate.getRootDataEntity().getId();
        var roGraph = mapper.readTree(roCrate.getJsonMetadata()).withArray("@graph");
        Iterator<JsonNode> it = roGraph.elements();
        while (it.hasNext()) {
            var jsonNode = it.next();
            if (jsonNode.get("@id").textValue().equals(rootEntityId)
                    && jsonNode.has("@type")
                    && getTypeAsString(jsonNode).equals("Dataset")) {
                return jsonNode;
            }
        }
        return null;
    }
    
    //Add new dataset for every folder in the given folderNames list, and returns the id of the folder that contains the file
    private String createDatasetsFromFoldersAndReturnParentId(RoCrate roCrate, JsonNode parentObj, List<String> folderNames, ObjectMapper mapper, boolean isRootDataset) {
        String folderName = folderNames.remove(0);
        String alreadyPresentDatasetId = null;
        JsonNode alreadyPresentParentDataset = null;
        if (isRootDataset) {
            for (String nodeId : roCrate.getRootDataEntity().hasPart) { 
                var dataEntity = roCrate.getAllDataEntities().stream().filter(de -> de.getProperties().get("@id").textValue().equals(nodeId)).findFirst();
                if (dataEntity.isPresent()) {
                    ObjectNode node = dataEntity.get().getProperties();
                    if (node.has("name") && node.get("name").textValue().equals(folderName)
                            && node.has("@type") && getTypeAsString(node).equals("Dataset")) {
                        alreadyPresentDatasetId = nodeId;
                        alreadyPresentParentDataset = node;
                    }
                } else {
                    var contextualEntity = roCrate.getEntityById(nodeId);
                    if (contextualEntity != null) {
                        ObjectNode node = contextualEntity.getProperties();
                        if (node.has("name") && node.get("name").textValue().equals(folderName)
                                && node.has("@type") && getTypeAsString(node).equals("Dataset")) {
                            alreadyPresentDatasetId = nodeId;
                            alreadyPresentParentDataset = node;
                        }
                    }
                    
                }
            }
        } else {
            alreadyPresentDatasetId = getDatasetIdIfAlreadyPresent(roCrate, parentObj, folderName);
        }
        String newDatasetId;
        if (alreadyPresentDatasetId == null) {
            //add new dataset
            newDatasetId = "#" + UUID.randomUUID();
            ContextualEntity newDataset = new ContextualEntity.ContextualEntityBuilder()
                    .addType("Dataset")
                    .setId(newDatasetId)
                    .addProperty("name", folderName)
                    .build();
            roCrate.addContextualEntity(newDataset);
            
            //add the id of the new dataset to the hasPart
            if (isRootDataset) {
                roCrate.getRootDataEntity().hasPart.add(newDatasetId);
            } else {
                ObjectNode roCrateParentObj = roCrate.getEntityById(parentObj.get("@id").textValue()).getProperties();
                addIdToHasPart(roCrateParentObj, newDatasetId, mapper);
            }
        } else {
            newDatasetId = alreadyPresentDatasetId; 
        }
        
        if (!folderNames.isEmpty()) {
            JsonNode newDataset = alreadyPresentParentDataset == null ? roCrate.getEntityById(newDatasetId).getProperties() : alreadyPresentParentDataset;
            return createDatasetsFromFoldersAndReturnParentId(roCrate, newDataset, folderNames, mapper, false);    
        } else {
            return newDatasetId;
        }
    }
    
    private String getDatasetIdIfAlreadyPresent(RoCrate roCrate, JsonNode parentObj, String folderName) {
        String datasetId = null;
        if (parentObj.has("hasPart")) {
            JsonNode hasPart = parentObj.get("hasPart");
            if (hasPart.isObject()) {
                String nodeId = hasPart.get("@id").textValue();
                AbstractEntity contextualEntity = roCrate.getEntityById(nodeId);
                if (contextualEntity != null) {
                    ObjectNode node = roCrate.getEntityById(nodeId).getProperties();
                    if (node.has("name") && node.get("name").textValue().equals(folderName)
                            && node.has("@type") && getTypeAsString(node).equals("Dataset")) {
                        datasetId = nodeId;
                    }
                }
            } else {
                for (var idNode : hasPart) {
                    if (!idNode.isEmpty()) {
                        String nodeId = idNode.get("@id").textValue();
                        AbstractEntity contextualEntity = roCrate.getEntityById(nodeId);
                        if (contextualEntity != null) {
                            ObjectNode node = contextualEntity.getProperties();
                            if(node.has("name") && node.get("name").textValue().equals(folderName)
                                    && node.has("@type") && getTypeAsString(node).equals("Dataset")) {
                                datasetId = nodeId;
                                break;
                            };
                        }   
                    }
                }
            }
        }
        return datasetId;
    }
    
    private void addIdToHasPart(JsonNode parentObj, String id, ObjectMapper mapper) {
        var newHasPart = mapper.createObjectNode();
        newHasPart.put("@id", id);
        if (parentObj.has("hasPart")) {
            JsonNode hasPart = parentObj.get("hasPart");
            if (hasPart.isArray()) {
                ((ArrayNode) hasPart).add(newHasPart);
            } else {
                ArrayNode newHasPartArray = mapper.createArrayNode();
                newHasPartArray.add(parentObj.get("hasPart"));
                newHasPartArray.add(newHasPart);
                ((ObjectNode)parentObj).set("hasPart", newHasPartArray);
            }
        } else {
            ((ObjectNode)parentObj).set("hasPart", newHasPart);
        }
    }

    public String getRoCratePath(Dataset dataset) {
        return String.join(File.separator, getRoCrateFolder(dataset), ArpServiceBean.RO_CRATE_METADATA_JSON_NAME);
    }

    public String getRoCrateHtmlPreviewPath(Dataset dataset) {
        return String.join(File.separator, getRoCrateFolder(dataset), arpConfig.get("arp.rocrate.html.preview.name"));
    }

    public String getRoCrateFolder(Dataset dataset) {
        String filesRootDirectory = System.getProperty("dataverse.files.directory");
        if (filesRootDirectory == null || filesRootDirectory.isEmpty()) {
            filesRootDirectory = "/tmp/files";
        }

        return String.join(File.separator, filesRootDirectory, dataset.getAuthorityForFileStorage(), dataset.getIdentifierForFileStorage(), "ro-crate-metadata");
    }

    public String getRoCratePath(Dataset dataset, String versionNumber) {
        return String.join(File.separator, getRoCrateFolder(dataset, versionNumber), ArpServiceBean.RO_CRATE_METADATA_JSON_NAME);
    }

    public String getRoCrateHtmlPreviewPath(Dataset dataset, String versionNumber) {
        return String.join(File.separator, getRoCrateFolder(dataset, versionNumber), arpConfig.get("arp.rocrate.html.preview.name"));
    }
    public String getRoCrateFolder(Dataset dataset, String versionNumber) {
        String filesRootDirectory = System.getProperty("dataverse.files.directory");
        if (filesRootDirectory == null || filesRootDirectory.isEmpty()) {
            filesRootDirectory = "/tmp/files";
        }

        return String.join(File.separator, filesRootDirectory, dataset.getAuthorityForFileStorage(), dataset.getIdentifierForFileStorage(), "ro-crate-metadata_v" + versionNumber);
    }
    
    public void saveRoCrateVersion(Dataset dataset, boolean isUpdate, boolean isMinor) throws IOException {
        String versionNumber = isUpdate ? dataset.getLatestVersionForCopy().getFriendlyVersionNumber() : isMinor ? dataset.getNextMinorVersionString() : dataset.getNextMajorVersionString();
        String roCrateFolderPath = getRoCrateFolder(dataset);
        FileUtils.copyDirectory(new File(roCrateFolderPath), new File(roCrateFolderPath + "_v" + versionNumber));
    }
    public void saveRoCrateVersion(Dataset dataset, String versionNumber) throws IOException
    {
        String roCrateFolderPath = getRoCrateFolder(dataset);
        FileUtils.copyDirectory(new File(roCrateFolderPath), new File(roCrateFolderPath + "_v" + versionNumber));
    }

    public void createOrUpdateRoCrate(Dataset dataset) throws Exception {
        logger.info("createOrUpdateRoCrate called for dataset " + dataset.getIdentifierForFileStorage());
        var roCratePath = Paths.get(getRoCratePath(dataset));
        RoCrate roCrate;
        String roCrateFolderPath = getRoCrateFolder(dataset);

        if (!Files.exists(roCratePath)) {
            roCrate = new RoCrate.RoCrateBuilder()
                    .setPreview(new AutomaticPreview())
                    .build();
            createOrUpdate(roCrate, dataset, true, null);
            Path roCrateFolder = Path.of(roCrateFolderPath);
            if (!Files.exists(roCrateFolder)) {
                Files.createDirectories(roCrateFolder);
            }
        } else {
            RoCrateReader roCrateFolderReader = new RoCrateReader(new FolderReader());
            var ro = roCrateFolderReader.readCrate(roCrateFolderPath);
            roCrate = new RoCrate.RoCrateBuilder(ro).setPreview(new AutomaticPreview()).build();
            Map<String, DatasetFieldType> datasetFieldTypeMap = getDatasetFieldTypeMapByConformsTo(roCrate);
            createOrUpdate(roCrate, dataset, false, datasetFieldTypeMap);
        }
        processRoCrateFiles(roCrate, dataset.getLatestVersion().getFileMetadatas());
        RoCrateWriter roCrateFolderWriter = new RoCrateWriter(new FolderWriter());
        roCrateFolderWriter.save(roCrate, roCrateFolderPath);

        // Make sure we have a released version rocrate even for older datasets where we didn't sync rocrate
        // from the beginning
        var released = dataset.getReleasedVersion();
        if (released != null) {
            var releasedVersion = released.getFriendlyVersionNumber();
            var releasedPath = getRoCratePath(dataset, releasedVersion);
            if (!Files.exists(Paths.get(releasedPath))) {
                logger.info("createOrUpdateRoCrate: copying draft as "+releasedVersion);
                saveRoCrateVersion(dataset, releasedVersion);
            }
        }
    }

    public String importRoCrate(RoCrate roCrate) {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode importFormatMetadataBlocks = mapper.createObjectNode();
        Map<String, DatasetFieldType> datasetFieldTypeMap = getDatasetFieldTypeMapByConformsTo(roCrate);
        RootDataEntity rootDataEntity = roCrate.getRootDataEntity();
        Map<String, ContextualEntity> contextualEntityHashMap = roCrate.getAllContextualEntities().stream().collect(Collectors.toMap(ContextualEntity::getId, Function.identity()));

        var fieldsIterator = rootDataEntity.getProperties().fields();
        while (fieldsIterator.hasNext()) {
            var field = fieldsIterator.next();
            String fieldName = field.getKey();
            if (!fieldName.startsWith("@") && !propsToIgnore.contains(fieldName)) {
                DatasetFieldType datasetFieldType = datasetFieldTypeMap.get(fieldName);
                // If not a field belonging to DV, ignore it
                if (datasetFieldType == null) {
                    continue;
                }
                // RO-Crate spec: name: SHOULD identify the dataset to humans well enough to disambiguate it from other RO-Crates
                // In our case if the MDB has a "name" field, then we use it and store the value we get, otherwise
                // if there's no "name" field, we ignore it. Still, we should check for datasetFieldType == null,
                // wich must be an error.`
                if (fieldName.equals("name") && datasetFieldType == null) {
                    break;
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
                    if (datasetFieldType.isAllowControlledVocabulary()) {
                        processControlledVocabFields(fieldName, field.getValue(), container, datasetFieldTypeMap, mapper);
                    } else {
                        processPrimitiveField(fieldName, field.getValue().textValue(), container, datasetFieldTypeMap, mapper);
                    }
                }
            }
        }

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

        List<String> mdbIds = conformsToIds.stream()
                .map(id -> {
                    // If not found, map to null, filter it later
                    var mdbArp = arpMetadataBlockServiceBean.findByRoCrateConformsToId(id);
                    if (mdbArp == null) {
                        return null;
                    }
                    return mdbArp.getMetadataBlock().getIdString();
                })
                .filter(s -> s != null)
                .collect(Collectors.toList());
        
        return fieldService.findAllOrderedById().stream().filter(datasetFieldType -> mdbIds.contains(datasetFieldType.getMetadataBlock().getIdString())).collect(Collectors.toMap(DatasetFieldType::getName, Function.identity()));
    }

    public RoCrate preProcessRoCrateFromAroma(Dataset dataset, String roCrateJson) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode rootNode = mapper.readTree(roCrateJson);

        JsonNode roCrateEntities = rootNode.withArray("@graph");
        Set<String> duplicatedIds = collectDuplicatedIds(roCrateEntities);
        if (!duplicatedIds.isEmpty()) {
            throw new RuntimeException("The provided RO-CRATE contained the following ids multiple times: " + duplicatedIds);
        }
        
        removeReverseProperties(rootNode);
        
        try (FileWriter writer = new FileWriter(getRoCratePath(dataset))) {
            writer.write(rootNode.toPrettyString());
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        List<MetadataBlock> mdbs = metadataBlockServiceBean.listMetadataBlocks();
        RoCrateReader roCrateFolderReader = new RoCrateReader(new FolderReader());
        RoCrate roCrate = roCrateFolderReader.readCrate(getRoCrateFolder(dataset));
        ObjectNode rootDataEntityProps = roCrate.getRootDataEntity().getProperties();
        List<String> rootDataEntityPropNames = new ArrayList<>();
        Set<MetadataBlock> conformsToMdbs = new HashSet<>();
        RoCrate.RoCrateBuilder roCrateContextUpdater = new RoCrate.RoCrateBuilder(roCrate);
        var roCrateContext = new ObjectMapper().readTree(roCrate.getJsonMetadata()).get("@context").get(1);
        
        rootDataEntityProps.fieldNames().forEachRemaining(fieldName -> {
            if (!fieldName.startsWith("@") && !propsToIgnore.contains(fieldName)) {
                rootDataEntityPropNames.add(fieldName);
                var fieldByName = fieldService.findByName(fieldName);
                // Only handle if field  belongs to DV
                if (!roCrateContext.has(fieldName) && fieldByName != null) {
                    roCrateContextUpdater.addValuePairToContext(fieldName, fieldByName.getUri());
                }
                for (var mdb : mdbs) {
                    if (mdb.getDatasetFieldTypes().stream().anyMatch(datasetFieldType -> datasetFieldType.getName().equals(fieldName))) {
                        conformsToMdbs.add(mdb);
                        break;
                    }
                }
            }
        });
        
        collectConformsToIds(dataset, roCrate.getRootDataEntity());
        
        // Delete properties from the @context, because AROMA only deletes them if they were added in aroma
        // props added in DV won't be removed from the @context if they were deleted in AROMA
        roCrateContext.fields().forEachRemaining(entry -> {
            if (!rootDataEntityPropNames.contains(entry.getKey()) && 
                    roCrate.getAllContextualEntities().stream().noneMatch(ce -> ce.getProperties().has(entry.getKey()))) {
                roCrate.deleteValuePairFromContext(entry.getKey());
            }
        });
        
        return roCrate;
    }

    public void postProcessRoCrateFromAroma(Dataset dataset, RoCrate roCrate) throws IOException {
        String roCrateFolderPath = getRoCrateFolder(dataset);
        ObjectNode rootDataEntityProperties = roCrate.getRootDataEntity().getProperties();
        Map<String, DatasetFieldType> compoundFields = dataset.getLatestVersion().getDatasetFields().stream().map(DatasetField::getDatasetFieldType).filter(DatasetFieldType::isCompound).collect(Collectors.toMap(DatasetFieldType::getName, Function.identity()));
        List<JsonNode> rootHasPartDatasets = new ArrayList<>();
        Map<String, ArrayList<String>> extraMetadata = Map.ofEntries(
                Map.entry("virtualDatasetAdded", new ArrayList<>()),
                Map.entry("virtualFileAdded", new ArrayList<>()),
                Map.entry("datasetWithMetadata", new ArrayList<>()),
                Map.entry("fileWithMetadata", new ArrayList<>())
        );
        
        // We must take the union of the dataEntities and the contextualEntities,
        // since a single file is considered a dataEntity
        // a file in a folder considered a contextualEntity and its parent folder considered a dataEntity
        Map<String, ObjectNode> datasetAndFileEntities = Stream.concat(
                roCrate.getAllContextualEntities().stream().map(AbstractEntity::getProperties), 
                roCrate.getAllDataEntities().stream().map(AbstractEntity::getProperties))
                .filter(entity -> hasType(entity, "File") || hasType(entity, "Dataset"))
                .collect(Collectors.toMap(entity -> entity.get("@id").textValue(), Function.identity()));


        // Updates the id of the entities in the RO-Crate with their new ids from DV
        rootDataEntityProperties.fields().forEachRemaining(prop -> {
            String propName = prop.getKey();
            JsonNode propVal = prop.getValue();
            if (propName.equals("hasPart")) {
                if (propVal.isObject()) {
                    rootHasPartDatasets.add(propVal);
                } else {
                    for (var arrayVal : propVal) {
                        rootHasPartDatasets.add(arrayVal);
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

        List<DataFile> dvDatasetFiles = dataset.getFiles();
        rootHasPartDatasets.forEach(ds -> processDatasetAndFileEntities(datasetAndFileEntities, ds, dvDatasetFiles, extraMetadata));
        
        roCrate.setRoCratePreview(new AutomaticPreview());

        RoCrateWriter roCrateFolderWriter = new RoCrateWriter(new FolderWriter());
        roCrateFolderWriter.save(roCrate, roCrateFolderPath);
        writeOutRoCrateExtras(extraMetadata, roCrateFolderPath);
    }
    
    private Set<String> collectDuplicatedIds(JsonNode entities) {
        Set<String> encounteredIds = new HashSet<>();
        Set<String> duplicateIds = new HashSet<>();

        entities.forEach(entity -> {
            var entityId = entity.get("@id").textValue();
            if (!encounteredIds.add(entityId)) {
                duplicateIds.add(entityId);
            }
        });

        return duplicateIds;
    }

    // Update the id of the dataset and file entities in case they are not following the format: #UUID for datasets and
    // #datafileId::UUID for files
    // add @arpPid to the file entities again, because this property is getting removed by AROMA
    // recursively process the child entities for datasets (folders)
    // during processing the entities this function collects the data to generate the rocrate-extras.json too
    private boolean processDatasetAndFileEntities(Map<String, ObjectNode> datasetAndFileEntities, JsonNode datasetEntity, List<DataFile> dvDatasetFiles, Map<String, ArrayList<String>> extraMetadata) {
        String oldId = datasetEntity.get("@id").textValue();
        boolean idNeedsToBeUpdated = false;
        try {
            UUID.fromString(oldId.startsWith("#") ? oldId.substring(1) : oldId);
        } catch (IllegalArgumentException ex) {
            idNeedsToBeUpdated = true;
        }

        ObjectNode entityNode = datasetAndFileEntities.get(oldId);

        if (hasType(entityNode, "File")) {
            // If the dataset contains a non-virtual file, that means the dataset is non-virtual too
            boolean isVirtualFile = isVirtualFile(entityNode);
            if (isVirtualFile) {
                extraMetadata.get("virtualFileAdded").add(entityNode.get("@id").textValue());
            } else {
                String fileName = entityNode.get("name").textValue();
                String directoryLabel = entityNode.has("directoryLabel") ? entityNode.get("directoryLabel").textValue() : null;
                DataFile dataFile = dvDatasetFiles.stream().filter(f ->
                        f.getDisplayName().equals(fileName) && (directoryLabel == null || directoryLabel.equals(f.getDirectoryLabel()))
                ).findFirst().get();
                String arpPid = dataFile.getGlobalId().toString();
                entityNode.put("@arpPid", arpPid);
            }

            if (fileHasExtraMetadata(entityNode)) {
                extraMetadata.get("fileWithMetadata").add(entityNode.get("@id").textValue());
            }
            return isVirtualFile;
        } else {
            boolean isVirtual;
            JsonNode hasPart = entityNode.get("hasPart");
            if (hasPart != null && !hasPart.isEmpty()) {
                if (hasPart.isObject()) {
                    isVirtual = processDatasetAndFileEntities(datasetAndFileEntities, hasPart, dvDatasetFiles, extraMetadata);
                    if (idNeedsToBeUpdated && !isVirtual) {
                        String newId  = "#" + UUID.randomUUID();
                        ((ObjectNode) datasetEntity).put("@id", newId);
                        entityNode.put("@id", newId);
                    }
                } else {
                    ArrayList<Boolean> isVirtualResults = new ArrayList<>();
                    for (var arrayVal : hasPart) {
                        isVirtualResults.add(processDatasetAndFileEntities(datasetAndFileEntities, arrayVal, dvDatasetFiles, extraMetadata));
                    }
                    isVirtual = !isVirtualResults.contains(false);
                }
            } else {
                // This is a virtual Dataset from AROMA
                isVirtual = true;
            }
            
            if (isVirtual) {
                extraMetadata.get("virtualDatasetAdded").add(entityNode.get("@id").textValue());
            }

            if (datasetHasExtraMetadata(entityNode)) {
                extraMetadata.get("datasetWithMetadata").add(entityNode.get("@id").textValue());
            }
            
            return isVirtual;
        }
    }

    private boolean isVirtualFile(ObjectNode file) {
        // regular expression for the id pattern of the files from dataverse
        Pattern idPattern = Pattern.compile("#\\d+::[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");
        return !idPattern.matcher(file.get("@id").textValue()).matches();
    }

    private boolean datasetHasExtraMetadata(ObjectNode dataset) {
        AtomicBoolean hasExtraMetadata = new AtomicBoolean();
        hasExtraMetadata.set(false);
        dataset.fieldNames().forEachRemaining(prop -> {
            if (!dataverseDatasetProps.contains(prop)) {
                hasExtraMetadata.set(true);
            }
        });
        return hasExtraMetadata.get();
    }

    private boolean fileHasExtraMetadata(ObjectNode file) {
        AtomicBoolean hasExtraMetadata = new AtomicBoolean();
        hasExtraMetadata.set(false);
        file.fieldNames().forEachRemaining(prop -> {
            if (!dataverseFileProps.contains(prop)) {
                hasExtraMetadata.set(true);
            }
        });
        return hasExtraMetadata.get();
    }
    
    private void writeOutRoCrateExtras(Map<String, ArrayList<String>> extraMetadata, String roCrateFolderPath) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode resultJsonNode = objectMapper.createObjectNode();

        for (Map.Entry<String, ArrayList<String>> entry : extraMetadata.entrySet()) {
            resultJsonNode.put(entry.getKey(), entry.getValue().size());
        }

        objectMapper.writerWithDefaultPrettyPrinter().writeValue(new File(roCrateFolderPath + "/" + RO_CRATE_EXTRAS_JSON_NAME), resultJsonNode);
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

        // If there are no values originating from AROMA for the compound field, it can't be added to Dataverse, and 
        // this compound field will be excluded from the imported fields (importFormatJson).
        // This can occur when a compound field is added within AROMA, but only its name field (which is specific to AROMA) receives values.
        if (compoundField.has("value")) {
            ((ArrayNode) metadataBlocks.get(datasetField.getMetadataBlock().getName()).withArray("fields")).add(compoundField);
        }
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
                    // If field doesn't belong to the DV, ignore it
                    if (datasetFieldType == null) {
                        return;
                    }
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
                field.set("value", mapper.createArrayNode().add(controlledVocabValue));
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
