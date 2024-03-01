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
import edu.harvard.iq.dataverse.api.arp.util.StorageUtils;
import edu.harvard.iq.dataverse.arp.*;
import edu.harvard.iq.dataverse.dataset.DatasetUtil;
import edu.harvard.iq.dataverse.util.JsfHelper;
import edu.kit.datamanager.ro_crate.RoCrate;
import edu.kit.datamanager.ro_crate.entities.AbstractEntity;
import edu.kit.datamanager.ro_crate.entities.contextual.ContextualEntity;
import edu.kit.datamanager.ro_crate.entities.data.FileEntity;
import edu.kit.datamanager.ro_crate.entities.data.RootDataEntity;
import edu.kit.datamanager.ro_crate.preview.AutomaticPreview;
import edu.kit.datamanager.ro_crate.reader.FolderReader;
import edu.kit.datamanager.ro_crate.reader.RoCrateReader;
import edu.kit.datamanager.ro_crate.reader.StringReader;
import edu.kit.datamanager.ro_crate.writer.FolderWriter;
import edu.kit.datamanager.ro_crate.writer.RoCrateWriter;
import org.apache.commons.io.FileUtils;

import jakarta.ejb.EJB;
import jakarta.ejb.Stateless;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static edu.harvard.iq.dataverse.DatasetField.createNewEmptyDatasetField;
import static edu.harvard.iq.dataverse.DatasetFieldCompoundValue.createNewEmptyDatasetFieldCompoundValue;
import static edu.harvard.iq.dataverse.arp.ArpServiceBean.RO_CRATE_EXTRAS_JSON_NAME;
import static edu.harvard.iq.dataverse.validation.EMailValidator.isEmailValid;
import static edu.harvard.iq.dataverse.validation.URLValidator.isURLValid;

@Stateless
@Named
public class RoCrateManager {

    private static final Logger logger = Logger.getLogger(RoCrateManager.class.getCanonicalName());

    public RoCrateManager(DatasetFieldServiceBean fieldService) {
        this.fieldService = fieldService;
    }

    public RoCrateManager() {
    }

    private final String compoundIdAndUuidSeparator = "::";
    
    private final List<String> propsToIgnore = List.of("conformsTo", "name", "hasPart", "license");
    private final List<String> dataverseFileProps = List.of("@id", "@type", "name", "contentSize", "encodingFormat", "directoryLabel", "description", "identifier", "@arpPid", "hash");
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

    @Inject
    RoCrateUploadServiceBean roCrateUploadServiceBean;

    @EJB 
    DatasetServiceBean datasetService;
    
    @EJB 
    DataFileServiceBean datafileService;

    //TODO: what should we do with the "name" property of the contextualEntities? 
    // now the "name" prop is added from AROMA and it's value is the same as the original id of the entity
    public void createOrUpdate(RoCrate roCrate, DatasetVersion version, boolean isCreation, Map<String, DatasetFieldType> datasetFieldTypeMap) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        RootDataEntity rootDataEntity = roCrate.getRootDataEntity();
        RoCrate.RoCrateBuilder roCrateContextUpdater = new RoCrate.RoCrateBuilder(roCrate);
        List<DatasetField> datasetFields = version.getDatasetFields();
        Set<MetadataBlock> conformsToMdbs = new HashSet<>();
        
        // Add the persistentId of the dataset to the RootDataEntity
        rootDataEntity.addProperty("@arpPid", version.getDataset().getGlobalId().toString());

        // Make sure license and datePublished is set
        var props = rootDataEntity.getProperties();
        props.set("license", mapper.createObjectNode().put("@id", DatasetUtil.getLicenseURI(version)));
        ZonedDateTime now = ZonedDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
        String formattedDate = now.format(formatter);
        props.put("datePublished", formattedDate);
        rootDataEntity.setProperties(props);

        // Remove the entities from the RO-Crate that had been deleted from DV
        if (!isCreation) {
            removeDeletedEntities(roCrate, version, datasetFields, datasetFieldTypeMap);
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

        // MDB-s can only be get via the Dataset and its Dataverse, so we need to pass version.getDataset()
        collectConformsToIds(version.getDataset(), rootDataEntity);
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
                if (datasetField.getDatasetFieldType().getFieldType().equals(DatasetFieldType.FieldType.URL)) {
                    String urlString = fieldValues.get(0).getValue();
                    var alreadyPresentUrlEntity = roCrate.getEntityById(urlString);
                    if (alreadyPresentUrlEntity == null) {
                        addUrlContextualEntity(roCrate, urlString);
                        var idObj = mapper.createObjectNode();
                        idObj.put("@id", urlString);
                        rootDataEntity.addProperty(fieldName, idObj);
                    }
                } else {
                    rootDataEntity.addProperty(fieldName, fieldValues.get(0).getValue());
                }
            } else {
                ArrayNode valuesNode = mapper.createArrayNode();
                if (datasetField.getDatasetFieldType().getFieldType().equals(DatasetFieldType.FieldType.URL)) {
                    for (var fieldValue : fieldValues) {
                        var alreadyPresentUrlEntity = roCrate.getEntityById(fieldValue.getValue());
                        if (alreadyPresentUrlEntity == null) {
                            addUrlContextualEntity(roCrate, fieldValue.getValue());
                        }
                        var idObj = mapper.createObjectNode();
                        idObj.put("@id", fieldValue.getValue());
                        valuesNode.add(idObj);
                    }
                } else {
                    for (var fieldValue : fieldValues) {
                        valuesNode.add(fieldValue.getValue());
                    }
                }
                rootDataEntity.addProperty(fieldName, valuesNode);
            }
        }
        processControlledVocabularyValues(controlledVocabValues, rootDataEntity, fieldName, mapper);
    }
    
    private void addUrlContextualEntity(RoCrate roCrate, String url) {
        var urlEntity = new ContextualEntity.ContextualEntityBuilder();
        urlEntity.addProperty("@type", "URL");
        urlEntity.addProperty("name", url);
        urlEntity.setId(url);
        roCrate.addContextualEntity(urlEntity.build());
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
                    if (!compoundValueIds.contains(getCompoundIdFromRoId(rootEntityId))) {
                        ContextualEntity contextualEntityToDelete = contextualEntities.stream().filter(contextualEntity -> contextualEntity.getId().equals(rootEntityId)).findFirst().get();
                        List<String> compoundValueProps = datasetField.getDatasetFieldType().getChildDatasetFieldTypes().stream().map(DatasetFieldType::getName).collect(Collectors.toList());
                        // Delete the properties from the contextual entity that was removed from DV,
                        // if the contextual entity does not contain any other props than @id, @type and name, just delete it
                        // else, the contextual entity still contains properties and those props are from AROMA, we can not delete the contextual entity
                        for (var prop : compoundValueProps) {
                            contextualEntityToDelete.getProperties().remove(prop);
                        }
                        var actProperties = contextualEntityToDelete.getProperties();
                        if (actProperties.size() == 3 && actProperties.has("@id") && actProperties.has("@type") && actProperties.has("name")) {
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
                    if (getCompoundIdFromRoId(idObj.get("@id").textValue()).equals(compoundValue.getId().toString())) {
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
    
    private void removeDeletedEntities(RoCrate roCrate, DatasetVersion version, List<DatasetField> datasetFields, Map<String, DatasetFieldType> datasetFieldTypeMap) {
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
        contextualEntityBuilder.setId(createRoIdForCompound(compoundValue));

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
        fileId = createRoIdForDataFile(dataFile);
        fileEntityBuilder.setId(fileId);
        var globalId = dataFile.getGlobalId();
        fileEntityBuilder.addProperty("@arpPid", (globalId != null ? globalId.toString() : ""));
        fileEntityBuilder.addProperty("hash", dataFile.getChecksumValue());
        fileEntityBuilder.addProperty("name", fileName);
        fileEntityBuilder.addProperty("contentSize", dataFile.getFilesize());
        fileEntityBuilder.setEncodingFormat(dataFile.getContentType());
        if (fileMetadata.getDescription() != null) {
            fileEntityBuilder.addProperty("description", fileMetadata.getDescription());
        }
        if (fileMetadata.getDirectoryLabel() != null) {
            fileEntityBuilder.addProperty("directoryLabel", dataFile.getDirectoryLabel());
        }
        if (!fileMetadata.getCategoriesByName().isEmpty()) {
            fileEntityBuilder.addProperty("tags", new ObjectMapper().valueToTree(fileMetadata.getCategoriesByName()));
        }
        var file = fileEntityBuilder.build();
        roCrate.addDataEntity(file, toHasPart);
        
        return fileId;
    }
    
    public void updateRoCrateFileMetadatas(Dataset dataset) throws JsonProcessingException {
        RoCrateReader roCrateFolderReader = new RoCrateReader(new FolderReader());
        var ro = roCrateFolderReader.readCrate(getRoCrateFolder(dataset.getLatestVersion()));
        RoCrate roCrate = new RoCrate.RoCrateBuilder(ro).setPreview(new AutomaticPreview()).build();
        processRoCrateFiles(roCrate, dataset.getLatestVersion().getFileMetadatas(), null);
        RoCrateWriter roCrateFolderWriter = new RoCrateWriter(new FolderWriter());
        roCrateFolderWriter.save(roCrate, getRoCrateFolder(dataset.getLatestVersion()));
    }
    
    public void updateRoCrateFileMetadataAfterIngest(List<Long> fileIds) {
        RoCrateReader roCrateFolderReader = new RoCrateReader(new FolderReader());
        HashSet<Long> parentDsIds = new HashSet<>();
        // Collect the dataset and the belonging file ids, so we no longer need to assume that every ingest message
        // contains files that belong to the same dataset as it is assumed here: edu/harvard/iq/dataverse/ingest/IngestMessageBean.java
        fileIds.forEach(fileId -> {
            var dsId = datafileService.findCheapAndEasy(fileId).getOwner().getId();
            parentDsIds.add(dsId);
        });
        parentDsIds.forEach(dsId -> {
            var datasetVersion = datasetService.find(dsId).getLatestVersion();
            var ro = roCrateFolderReader.readCrate(getRoCrateFolder(datasetVersion));
            RoCrate roCrate = new RoCrate.RoCrateBuilder(ro).setPreview(new AutomaticPreview()).build();
            updateFileMetadataAfterIngest(roCrate, datasetVersion);
            RoCrateWriter roCrateFolderWriter = new RoCrateWriter(new FolderWriter());
            roCrateFolderWriter.save(roCrate, getRoCrateFolder(datasetVersion));
        });
    }
    
    private void updateFileMetadataAfterIngest(RoCrate roCrate, DatasetVersion datasetVersion) {
        List<ObjectNode> roCrateFileEntities = Stream.concat(
                roCrate.getAllContextualEntities().stream().map(AbstractEntity::getProperties).filter(ce -> getTypeAsString(ce).equals("File")),
                roCrate.getAllDataEntities().stream().map(AbstractEntity::getProperties).filter(de -> getTypeAsString(de).equals("File"))
        ).collect(Collectors.toList());
        List<FileMetadata> datasetFiles = datasetVersion.getFileMetadatas();

        roCrateFileEntities.forEach(fe -> {
            String dataFileId = getDataFileIdFromRoId(fe.get("@id").textValue());
            Optional<FileMetadata> datasetFile = datasetFiles.stream().filter(fileMetadata -> Objects.equals(dataFileId, fileMetadata.getDataFile().getId().toString())).findFirst();
            if (datasetFile.isPresent()) {
                var fmd = datasetFile.get();
                fe.put("name", fmd.getLabel());
                fe.put("hash", fmd.getDataFile().getChecksumValue());
            } else {
                throw new RuntimeException("An error occurred during updating the RO-CRATE after the file(s) been ingested.");
            }
        });
        
    }
    
    public void processRoCrateFiles(RoCrate roCrate, List<FileMetadata> fileMetadatas, Map<String, String> importMapping) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        List<FileMetadata> datasetFiles = fileMetadatas.stream().map(FileMetadata::createCopy).collect(Collectors.toList());
        List<ObjectNode> roCrateFileEntities = Stream.concat(
                roCrate.getAllContextualEntities().stream().map(AbstractEntity::getProperties).filter(ce -> getTypeAsString(ce).equals("File")), 
                roCrate.getAllDataEntities().stream().map(AbstractEntity::getProperties).filter(de -> getTypeAsString(de).equals("File"))
        ).collect(Collectors.toList());

        List<ObjectNode> roCrateDatasetEntities = Stream.concat(
                roCrate.getAllContextualEntities().stream().map(AbstractEntity::getProperties).filter(ce -> getTypeAsString(ce).equals("Dataset")),
                roCrate.getAllDataEntities().stream().map(AbstractEntity::getProperties).filter(de -> getTypeAsString(de).equals("Dataset"))
        ).collect(Collectors.toList());
        
        // Delete the entities from the RO-CRATE that have been removed from DV
        roCrateFileEntities.forEach(fe -> {
            if (isVirtualFile(fe)) {
                return;
            }
            String dataFileId = getDataFileIdFromRoId(fe.get("@id").textValue());
            Optional<FileMetadata> datasetFile;
            if (importMapping == null) {
                datasetFile = datasetFiles.stream().filter(fileMetadata -> Objects.equals(dataFileId, fileMetadata.getDataFile().getId().toString())).findFirst();
            } else {
                datasetFile = datasetFiles.stream().filter(fileMetadata -> 
                        Objects.equals(fileMetadata.getDataFile().getStorageIdentifier().split("://")[1], importMapping.get(fe.get("@id").textValue()))
                ).findFirst();
            }
            if (datasetFile.isPresent()) {
                var fmd = datasetFile.get();
                if (importMapping != null) {
                    String oldFileId = fe.get("@id").textValue();
                    String newFileId = createRoIdForDataFile(fmd.getDataFile());
                    replaceFileIdInHasParts(roCrate, oldFileId, newFileId);
                    fe.put("@id", newFileId);
                }
                fe.put("name", fmd.getLabel());
                if (fmd.getDescription() != null) {
                    fe.put("description", fmd.getDescription());
                }
                var fmdDirLabel = fmd.getDirectoryLabel();
                var feDirLabel = fe.has("directoryLabel") ? fe.get("directoryLabel").textValue() : null;
                if (!Objects.equals(fmdDirLabel, feDirLabel)) {
                    fe.put("directoryLabel", fmd.getDirectoryLabel());
                    processModifiedPath(roCrate, feDirLabel, fmdDirLabel, fe.get("@id").textValue());
                }

                fe.set("tags", mapper.valueToTree(fmd.getCategoriesByName()));
                String actualDataFileId = importMapping == null ? dataFileId : fmd.getDataFile().getId().toString();
                datasetFiles.removeIf(fileMetadata -> Objects.equals(actualDataFileId, fileMetadata.getDataFile().getId().toString()));
            } else {
                String entityId = fe.get("@id").textValue();
                removeChildContextualEntities(roCrate, entityId);
                roCrate.deleteEntityById(entityId);
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
                String parentDatasetId = createDatasetsFromFoldersAndReturnParentId(roCrate, roCrate.getRootDataEntity().getProperties(), folderNames, mapper);
                // childDataset means that the dataset is not top level dataset
                // top level datasets that are child of the rootDataset, those datasets needs to be handled differently
                String newFiledId = addFileEntity(roCrate, df, false);
                AbstractEntity newChildDataset = roCrate.getEntityById(parentDatasetId);
                ObjectNode parentDataset = Objects.requireNonNullElseGet(newChildDataset.getProperties(), () -> roCrateDatasetEntities.stream().filter(dataEntity -> dataEntity.get("@id").textValue().equals(parentDatasetId)).findFirst().get());
                addIdToHasPart(parentDataset, newFiledId, mapper);
            }
        }

        // Delete the empty Datasets from the RO-CRATE, this has to be done after adding the new files
        // this way we can keep the original Dataset (folder) ID-s in the RO-CRATE
        deleteEmptyDatasets(roCrate);
    }
    
    private void removeChildContextualEntities(RoCrate roCrate, String entityId) {
        var propsToIgnore = List.of("conformsTo");
        AbstractEntity entity = roCrate.getEntityById(entityId);
        entity.getProperties().fields().forEachRemaining(field -> {
            if (isContextualProp(field.getValue()) && !propsToIgnore.contains(field.getKey())) {
                if (field.getValue().isObject()) {
                    String contextualEntityToDeleteId = field.getValue().get("@id").textValue();
                    removeChildContextualEntities(roCrate, contextualEntityToDeleteId);
                    roCrate.deleteEntityById(contextualEntityToDeleteId);
                } else if (field.getValue().isArray()) {
                    field.getValue().forEach(idObj -> {
                        String contextualEntityToDeleteId = idObj.get("@id").textValue();
                        removeChildContextualEntities(roCrate, contextualEntityToDeleteId);
                        roCrate.deleteEntityById(contextualEntityToDeleteId);
                    });
                }
            }
        });
    }
    
    private boolean isContextualProp(JsonNode prop) {
        if (prop.isObject()) {
            return prop.size() == 1 && prop.has("@id");
        } else if (prop.isArray()) {
            for (JsonNode idObj : prop) {
                if (idObj.size() != 1 || !idObj.has("@id")) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
    
    private void replaceFileIdInHasParts(RoCrate roCrate, String oldFileId, String newFileId) {
        if (roCrate.getRootDataEntity().hasPart.removeIf(id -> id.equals(oldFileId))) {
            roCrate.getRootDataEntity().hasPart.add(newFileId);
        }

        roCrate.getAllContextualEntities().stream().map(AbstractEntity::getProperties)
                .filter(ce -> getTypeAsString(ce).equals("Dataset"))
                .forEach(ds -> {
                    if (ds.has("hasPart")) {
                        replaceId(ds.get("hasPart"), oldFileId, newFileId);
                    }
                });
        // This part might be removed in the future, but as of now the lib sometimes mixes the contextual/file entities
        roCrate.getAllDataEntities().stream().map(AbstractEntity::getProperties)
                .filter(de -> getTypeAsString(de).equals("Dataset"))
                .forEach(ds -> {
                    if (ds.has("hasPart")) {
                        replaceId(ds.get("hasPart"), oldFileId, newFileId);
                    }
                });
    }
    
    private void replaceId(JsonNode parentNode, String oldId, String newId) {
        if (parentNode.isObject()) {
            if (parentNode.has("@id") && parentNode.get("@id").textValue().equals(oldId)) {
                ((ObjectNode) parentNode).put("@id", newId);
            }
        } else if (parentNode.isArray()) {
            parentNode.forEach(idObj -> {
                if (idObj.has("@id") && idObj.get("@id").textValue().equals(oldId)) {
                    ((ObjectNode) idObj).put("@id", newId);
                }
            });
        }
    }
    
    private String findFirstModifiedDatasetId(RoCrate roCrate, ObjectNode parentObj, List<String> originalDirNames, List<String> modifiedDirNames) {
        boolean dsIsPresent = false;
        String dsId = null;
        if (!modifiedDirNames.isEmpty() && !originalDirNames.isEmpty()) {
            String folderName = modifiedDirNames.get(0);
            dsId = getDatasetIdIfAlreadyPresent(roCrate, parentObj, folderName);
            if (dsId != null) {
                dsIsPresent = true;
            }
        }
        
        if (dsIsPresent) {
            originalDirNames.remove(0);
            modifiedDirNames.remove(0);
            return findFirstModifiedDatasetId(roCrate, roCrate.getEntityById(dsId).getProperties(), originalDirNames, modifiedDirNames);
        } else {
            return parentObj.get("@id").textValue();
        }
    }
    
    private void processModifiedPath(RoCrate roCrate, String originalPath, String modifiedPath, String fileId) {
        ObjectMapper mapper = new ObjectMapper();
        
        List<String> originalDirNames = originalPath != null ? Arrays.stream(originalPath.split("/")).collect(Collectors.toList()) : new ArrayList<>();
        List<String> modifiedDirNames = modifiedPath != null ? Arrays.stream(modifiedPath.split("/")).collect(Collectors.toList()) : new ArrayList<>();

        // find the first modified part of the path and modify the dirNames lists to keep the unprocessed ones only
        String modifiedDsId = findFirstModifiedDatasetId(roCrate, roCrate.getRootDataEntity().getProperties(), originalDirNames, modifiedDirNames);
        ObjectNode modifiedDs = modifiedDsId.equals("./") ? roCrate.getRootDataEntity().getProperties() : roCrate.getEntityById(modifiedDsId).getProperties();
        String newParentId = modifiedDsId;

        if (!modifiedDirNames.isEmpty()) {
            newParentId = createDatasetsFromFoldersAndReturnParentId(roCrate, modifiedDs, modifiedDirNames, mapper);
        }
        
        if (newParentId.equals("./")) {
            roCrate.getRootDataEntity().hasPart.add(fileId);
        } else {
            addIdToHasPart(roCrate.getEntityById(newParentId).getProperties(), fileId, mapper);
        }
        
        if (!originalDirNames.isEmpty()) {
            // collect the id-s of the datasets from the original path in reversed order for easier processing
            ArrayList<String> originalDatasetIds = collectDatasetIds(roCrate, modifiedDs, originalDirNames, new ArrayList<>());

            // remove the file from its original parent dataset
            var fileParentDsId = originalDatasetIds.get(0);
            removeEntityFromHasPart(roCrate, fileParentDsId, fileId);

            // go through the original path, and remove the datasets that became empty during the path modification
            for (var originalId : originalDatasetIds) {
                var dataset = roCrate.getEntityById(originalId).getProperties();
                if (hasPartIsEmpty(dataset)) {
                    roCrate.deleteEntityById(originalId);
                }
            }
        } else {
            removeEntityFromHasPart(roCrate, modifiedDsId, fileId);
//            if (originalPath == null) {
//                roCrate.getRootDataEntity().hasPart.add(newParentId);
//            }
        }
    }
    
    private void removeEntityFromHasPart(RoCrate roCrate, String parentId, String idToRemove) {
        if (parentId.equals("./")) {
            roCrate.getRootDataEntity().hasPart.removeIf(id -> id.equals(idToRemove));
        } else {
            AbstractEntity parentEntity = roCrate.getEntityById(parentId);
            var hasPart = parentEntity.getProperty("hasPart");
            if (hasPart != null) {
                if (hasPart.isObject()) {
                    var presentId = hasPart.get("@id").textValue();
                    if (!Objects.equals(presentId, idToRemove)) {
                        throw new RuntimeException("The Dataset with id: " + parentId + " should contain a child in its hasPart with id: " + idToRemove);
                    } else {
                        (parentEntity.getProperties()).remove("hasPart");
                    }
                } else {
                    for (Iterator<JsonNode> it = hasPart.elements(); it.hasNext();) {
                        JsonNode entity = it.next();
                        if (Objects.equals(entity.get("@id").textValue(), idToRemove)) {
                            it.remove();
                        }
                    }
                }
            }   
        }
    }
    
    private ArrayList<String> collectDatasetIds(RoCrate roCrate, JsonNode parentObj, List<String> folderNames, ArrayList<String> datasetIds) {
        String dsName = folderNames.remove(0);
        var dsId = getDatasetIdIfAlreadyPresent(roCrate, parentObj, dsName);
        if (dsId == null) {
            throw new RuntimeException("Dataset with name: " + dsName + " should be a part of the Dataset with id: " + parentObj.get("@id").textValue());
        }
        
        // collect the id-s in reversed order for easier processing
        datasetIds.add(0, dsId);
        
        if (!folderNames.isEmpty()) {
            collectDatasetIds(roCrate, roCrate.getEntityById(dsId).getProperties(), folderNames, datasetIds);
        }
        
        return datasetIds;
    }
    
    public void deleteEmptyDatasets(RoCrate roCrate) {
        List<String> emptyDatasetIds = Stream.concat(
                roCrate.getAllContextualEntities().stream().map(AbstractEntity::getProperties).filter(ce -> 
                        getTypeAsString(ce).equals("Dataset") && hasPartIsEmpty(ce)),
                roCrate.getAllDataEntities().stream().map(AbstractEntity::getProperties).filter(de -> 
                        getTypeAsString(de).equals("Dataset") && hasPartIsEmpty(de))
        ).map(entity -> entity.get("@id").textValue()).collect(Collectors.toList());
        
        if (!emptyDatasetIds.isEmpty()) {
            emptyDatasetIds.forEach(dsId -> {
                removeChildContextualEntities(roCrate, dsId);
                roCrate.deleteEntityById(dsId);
            });
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
    
    //Add new dataset for every folder in the given folderNames list, and returns the id of the folder that contains the file
    private String createDatasetsFromFoldersAndReturnParentId(RoCrate roCrate, JsonNode parentObj, List<String> folderNames, ObjectMapper mapper) {
        String folderName = folderNames.remove(0);
        String newDatasetId = getDatasetIdIfAlreadyPresent(roCrate, parentObj, folderName);
        
        if (newDatasetId == null) {
            //add new dataset
            // Make it end with "/" to conform to Describo, which requires Dataset id-s to end in '/'
            // although this is is just a SHOULD not a MUST by the spec.
            newDatasetId = createRoIdForDataset(folderName, parentObj);
            ContextualEntity newDataset = new ContextualEntity.ContextualEntityBuilder()
                    .addType("Dataset")
                    .setId(newDatasetId)
                    .addProperty("name", folderName)
                    .build();
            roCrate.addContextualEntity(newDataset);
            
            //add the id of the new dataset to the hasPart
            if (parentObj.get("@id").textValue().equals("./")) {
                roCrate.getRootDataEntity().hasPart.add(newDatasetId);
            } else {
                addIdToHasPart(parentObj, newDatasetId, mapper);
            }
        }
        
        if (!folderNames.isEmpty()) {
            JsonNode newDataset = roCrate.getEntityById(newDatasetId).getProperties();
            return createDatasetsFromFoldersAndReturnParentId(roCrate, newDataset, folderNames, mapper);    
        } else {
            return newDatasetId;
        }
    }
    
    private String getDatasetIdIfAlreadyPresent(RoCrate roCrate, JsonNode parentObj, String folderName) {
        String datasetId = null;
        if (parentObj.get("@id").textValue().equals("./")) {
            List<ObjectNode> roCrateDatasetEntities = Stream.concat(
                    roCrate.getAllContextualEntities().stream().map(AbstractEntity::getProperties).filter(ce -> getTypeAsString(ce).equals("Dataset")),
                    roCrate.getAllDataEntities().stream().map(AbstractEntity::getProperties).filter(de -> getTypeAsString(de).equals("Dataset"))
            ).collect(Collectors.toList());
            for (var id : roCrate.getRootDataEntity().hasPart) {
                var optDataset = roCrateDatasetEntities.stream().filter(ds -> ds.get("@id").textValue().equals(id)).findFirst();
                if (optDataset.isPresent()) {
                    ObjectNode dsEntity = optDataset.get();
                    if (dsEntity.has("name") && dsEntity.get("name").textValue().equals(folderName)) {
                        datasetId = id;
                    }
                }   
            }
        } else {
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

    public String getRoCrateHtmlPreviewPath(DatasetVersion version) {
        return String.join(File.separator, getRoCrateFolder(version), arpConfig.get("arp.rocrate.html.preview.name"));
    }

    public String getRoCrateFolder(DatasetVersion version) {
        String localDir = StorageUtils.getLocalRoCrateDir(version.getDataset());
        var baseName = String.join(File.separator, localDir, "ro-crate-metadata");
        if (!version.isDraft()) {
            baseName += "_v" + version.getFriendlyVersionNumber();
        }
        return baseName;
    }
    
    public String getRoCrateParentFolder(Dataset dataset) {
        return StorageUtils.getLocalRoCrateDir(dataset);
    }

    public String getRoCratePath(DatasetVersion version) {
        return String.join(File.separator, getRoCrateFolder(version), ArpServiceBean.RO_CRATE_METADATA_JSON_NAME);
    }

    // This function always returns the path of a "draft" RO-CRATE
    // This function should only be used in the pre-processing of the RO-CRATE-s coming from AROMA
    public String getRoCratePathForPreProcess(DatasetVersion version) {
        return String.join(File.separator, getRoCrateFolderForPreProcess(version), ArpServiceBean.RO_CRATE_METADATA_JSON_NAME);
    }

    // This function always returns the folder path of a "draft" RO-CRATE
    // This function should only be used in the pre-processing of the RO-CRATE-s coming from AROMA
    public String getRoCrateFolderForPreProcess(DatasetVersion version) {
        var dataset = version.getDataset();
        String localDir = StorageUtils.getLocalRoCrateDir(version.getDataset());
        return String.join(File.separator, localDir, "ro-crate-metadata");
    }
    
    public void deleteDraftVersion(DatasetVersion datasetVersion) throws IOException {
        String draftPath = getRoCrateFolder(datasetVersion);
        String latestPublishedPath = getRoCrateFolder(datasetVersion.getDataset().getLatestVersionForCopy());
        FileUtils.copyDirectory(new File(latestPublishedPath), new File(draftPath));
    }
    
    public void saveRoCrateVersion(Dataset dataset, boolean isUpdate, boolean isMinor) throws IOException {
        String versionNumber = isUpdate ? dataset.getLatestVersionForCopy().getFriendlyVersionNumber() : isMinor ? dataset.getNextMinorVersionString() : dataset.getNextMajorVersionString();
        String roCrateFolderPath = getRoCrateFolder(dataset.getLatestVersion());
        FileUtils.copyDirectory(new File(roCrateFolderPath), new File(roCrateFolderPath + "_v" + versionNumber));
    }
    public void saveRoCrateVersion(Dataset dataset, String versionNumber) throws IOException
    {
        String roCrateFolderPath = getRoCrateFolder(dataset.getLatestVersion());
        FileUtils.copyDirectory(new File(roCrateFolderPath), new File(roCrateFolderPath + "_v" + versionNumber));
    }
    
    public void saveUploadedRoCrate(Dataset dataset, String roCrateJsonString) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        String roCratePath = getRoCratePath(dataset.getLatestVersion());
        JsonNode parsedRoCrate = objectMapper.readTree(roCrateJsonString);
        File roCrate = new File(roCratePath);

        if (!roCrate.getParentFile().exists()) {
            if (!roCrate.getParentFile().mkdirs()) {
                throw new RuntimeException("Failed to save uploaded RO-CRATE for dataset: " + dataset.getIdentifierForFileStorage());
            }
        }
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(new File(roCratePath), parsedRoCrate);
        logger.info("saveUploadedRoCrate called for dataset " + dataset.getIdentifierForFileStorage());
    }

    public void createOrUpdateRoCrate(DatasetVersion version) throws Exception {
        var dataset = version.getDataset();
        logger.info("createOrUpdateRoCrate called for dataset " + dataset.getIdentifierForFileStorage());
        var roCratePath = Paths.get(getRoCratePath(version));
        RoCrate roCrate;
        String roCrateFolderPath = getRoCrateFolder(version);

        if (!Files.exists(roCratePath)) {
            roCrate = new RoCrate.RoCrateBuilder()
                    .setPreview(new AutomaticPreview())
                    .build();
            createOrUpdate(roCrate, version, true, null);
            Path roCrateFolder = Path.of(roCrateFolderPath);
            if (!Files.exists(roCrateFolder)) {
                Files.createDirectories(roCrateFolder);
            }
        } else {
            RoCrateReader roCrateFolderReader = new RoCrateReader(new FolderReader());
            var ro = roCrateFolderReader.readCrate(roCrateFolderPath);
            roCrate = new RoCrate.RoCrateBuilder(ro).setPreview(new AutomaticPreview()).build();
            Map<String, DatasetFieldType> datasetFieldTypeMap = getDatasetFieldTypeMapByConformsTo(roCrate);
            createOrUpdate(roCrate, version, false, datasetFieldTypeMap);
        }
        processRoCrateFiles(roCrate, version.getFileMetadatas(), roCrateUploadServiceBean.getImportMapping());
        // If the rocrate is generated right after an rocrate zip has been uploaded, make sure we put back the
        // file and sub-dataset related metadata to the generated metadata from the uploaded ro-crate-metadata.json.
        // roCrate = roCrateUploadServiceBean.addUploadedFileMetadata(roCrate);
        RoCrateWriter roCrateFolderWriter = new RoCrateWriter(new FolderWriter());
        roCrateFolderWriter.save(roCrate, roCrateFolderPath);

        // If rocrate is saved, then we can reset the upload state, so that subsequent calls to
        // addUploadedFileMetadata would do nothing.
        roCrateUploadServiceBean.reset();

        // Make sure we have a released version rocrate even for older datasets where we didn't sync rocrate
        // from the beginning
        var released = dataset.getReleasedVersion();
        if (released != null) {
            var releasedVersion = released.getFriendlyVersionNumber();
            var releasedPath = getRoCratePath(released);
            if (!Files.exists(Paths.get(releasedPath))) {
                logger.info("createOrUpdateRoCrate: copying draft as "+releasedVersion);
                saveRoCrateVersion(dataset, releasedVersion);
            }
        }

    }

    // Handles the importing of the RO-Crates (mainly sent by AROMA)
    public void importRoCrate(RoCrate roCrate, DatasetVersion updatedVersion) {
        Map<String, DatasetFieldType> datasetFieldTypeMap = getDatasetFieldTypeMapByConformsTo(roCrate);
        RootDataEntity rootDataEntity = roCrate.getRootDataEntity();
        Map<String, ContextualEntity> contextualEntityHashMap = roCrate.getAllContextualEntities().stream().collect(Collectors.toMap(ContextualEntity::getId, Function.identity()));
        
        var fieldsIterator = rootDataEntity.getProperties().fields();
        while (fieldsIterator.hasNext()) {
            var field = fieldsIterator.next();
            String fieldName = field.getKey();
            JsonNode filedValue = field.getValue();
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
                if (fieldName.equals("name")) {
                    break;
                }
                
                // Check if the datasetField is already present
                if (updatedVersion.getDatasetFields().stream().noneMatch(datasetField -> datasetField.getDatasetFieldType().equals(datasetFieldType))) {
                    if (datasetFieldType.isCompound()) {
                        // Only add if the compound field contains any value that can be saved to DV
                        if (containsDvProp(roCrate, filedValue, datasetFieldType)) {
                            updatedVersion.getDatasetFields().add(createNewEmptyDatasetField(datasetFieldType, updatedVersion));
                        }
                    } else {
                        updatedVersion.getDatasetFields().add(createNewEmptyDatasetField(datasetFieldType, updatedVersion));
                    }
                }
                
                // Process the values depending on the field's type
                if (datasetFieldType.isCompound()) {
                    if (containsDvProp(roCrate, filedValue, datasetFieldType)) {
                        processCompoundField(filedValue, updatedVersion, datasetFieldType, contextualEntityHashMap);
                    }
                } else {
                    if (datasetFieldType.isAllowControlledVocabulary()) {
                        processControlledVocabFields(filedValue, updatedVersion.getDatasetField(datasetFieldType), datasetFieldType);
                    } else {
                        processPrimitiveField(filedValue, updatedVersion, datasetFieldType, roCrate);
                    }
                }
            }
        }
    }
    
    // Checks whether a node contains any properties that can be saved to DV or not (that means it contains only AROMA related props)
    private boolean containsDvProp(RoCrate roCrate, JsonNode idNode, DatasetFieldType datasetFieldType) {
        boolean containsDvProp = false;
        if (idNode.isObject()) {
            idNode = new ObjectMapper().createArrayNode().add(idNode);
        }
        for (var idObj : idNode) {
            String id = idObj.get("@id").textValue();
            ObjectNode compoundProp = roCrate.getEntityById(id).getProperties();
            for (Iterator<String> it = compoundProp.fieldNames(); it.hasNext(); ) {
                String propName = it.next();
                if (datasetFieldType.getChildDatasetFieldTypes().stream().anyMatch(dsft -> dsft.getName().equals(propName))) {
                    containsDvProp = true;
                    break;
                }
            }
        }
        return containsDvProp;
    }
    

    public List<FileMetadata> updateFileMetadatas(Dataset dataset, RoCrate roCrate) {
        List<FileMetadata> filesToBeDeleted = new ArrayList<>();
        List<String> fileMetadataHashes = dataset.getLatestVersion().getFileMetadatas().stream().map(fmd -> fmd.getDataFile().getChecksumValue()).collect(Collectors.toList());
        List<ObjectNode> roCrateFileEntities = Stream.concat(
                roCrate.getAllContextualEntities().stream().map(AbstractEntity::getProperties).filter(ce -> getTypeAsString(ce).equals("File")),
                roCrate.getAllDataEntities().stream().map(AbstractEntity::getProperties).filter(de -> getTypeAsString(de).equals("File"))
        ).collect(Collectors.toList());

        // Update the metadata for the files in the dataset
        roCrateFileEntities.forEach(fileEntity -> {
            if (isVirtualFile(fileEntity)) {
                return;
            }
            String fileEntityHash = fileEntity.get("hash").textValue();
            var fmd = dataset.getFiles().stream().filter(dataFile -> dataFile.getChecksumValue().equals(fileEntityHash)).findFirst().get().getFileMetadata();
            fmd.setLabel(fileEntity.get("name").textValue());
            String dirLabel = fileEntity.has("directoryLabel") ? fileEntity.get("directoryLabel").textValue() : "";
            fmd.setDirectoryLabel(dirLabel);
            String description = fileEntity.has("description") ? fileEntity.get("description").textValue() : "";
            fmd.setDescription(description);

            List<String> tags = new ArrayList<>();
            JsonNode roCrateTags = fileEntity.get("tags");
            if (roCrateTags != null) {
                if (roCrateTags.isArray()) {
                    roCrateTags.forEach(tag -> tags.add(tag.textValue()));
                } else {
                    tags.add(roCrateTags.textValue());
                }
                fmd.setCategoriesByName(tags);
            }
            if(!fileMetadataHashes.removeIf(fmdHash -> fmdHash.equals(fileEntityHash))) {
                // collect the fmd for files that were deleted in AROMA
                filesToBeDeleted.add(fmd);
                // the hash can not be modified in AROMA, so any modified hash should be removed from the RO-Crate as well,
                // since the modification was done by editing the RO-Crate directly
                roCrate.deleteEntityById(fileEntity.get("@id").textValue());
            }

            // the "@type" and the "@id" can not be modified in AROMA, prevent any modifications sent by API calls
        });
        
        return filesToBeDeleted;
    }
    
    private Map<String, DatasetFieldType> getDatasetFieldTypeMapByConformsTo(RoCrate roCrate) {
        ArrayList<String> conformsToIds = new ArrayList<>();
        var conformsTo = roCrate.getRootDataEntity().getProperties().get("conformsTo");
        
        if (conformsTo != null) {
            if (conformsTo.isArray()) {
                conformsTo.elements().forEachRemaining(conformsToObj -> conformsToIds.add(conformsToObj.get("@id").textValue()));
            } else {
                conformsToIds.add(conformsTo.get("@id").textValue());
            }
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

    // Prepare the RO-Crate from AROMA to be imported into Dataverse
    public RoCrate preProcessRoCrateFromAroma(Dataset dataset, String roCrateJsonToImport) throws JsonProcessingException, ArpException {
        RoCrateReader roCrateFolderReader = new RoCrateReader(new FolderReader());
        RoCrate latestVersionRoCrate = roCrateFolderReader.readCrate(getRoCrateFolderForPreProcess(dataset.getLatestVersion()));
        RoCrateImportPrepResult roCrateImportPrepResult = prepareRoCrateForDataverseImport(roCrateJsonToImport, latestVersionRoCrate);

        var prepErrors = roCrateImportPrepResult.errors;
        if (!prepErrors.isEmpty()) {
            throw new ArpException(String.join("\n", prepErrors));
        }

        RoCrate roCrateToImport = roCrateImportPrepResult.getRoCrate();

        collectConformsToIds(dataset, roCrateToImport.getRootDataEntity());

        return roCrateToImport;
    }

    public RoCrateImportPrepResult prepareRoCrateForDataverseImport(String roCrateJsonString) throws JsonProcessingException {
        return prepareRoCrateForDataverseImport(roCrateJsonString, null);
    }

    // Prepare a given RO-Crate JSON String to be imported into Dataverse
    // This includes validation by the schema (mdbs) and removing unprocessable fields
    public RoCrateImportPrepResult prepareRoCrateForDataverseImport(String roCrateJsonString, RoCrate latestRoCrate) throws JsonProcessingException {
        RoCrateImportPrepResult preProcessResult = new RoCrateImportPrepResult();
        ObjectMapper mapper = new ObjectMapper();
        RoCrateReader roCrateStringReader = new RoCrateReader(new StringReader());
        JsonNode rootNode = mapper.readTree(roCrateJsonString);

        JsonNode roCrateEntities = rootNode.withArray("@graph");
        // collect all entity id-s with types, to make sure every RO-Crate entity is processed.
        // before converting to actual RO-Crate, check whether the roCrateJsonString contains duplicated id-s
        // also remove the "reverse" properties, since the RO-Crate lib can not handle those yet
        HashMap<String, String> roCrateEntityIdsAndTypes = preCheckEntities(roCrateEntities, preProcessResult);
        if (!preProcessResult.errors.isEmpty()) {
            return preProcessResult;
        }
        
        RoCrate preProcessedRoCrate = roCrateStringReader.parseCrate(rootNode.toPrettyString());
        RoCrate.RoCrateBuilder roCrateContextUpdater = new RoCrate.RoCrateBuilder(preProcessedRoCrate);
        var roCrateContext = new ObjectMapper().readTree(preProcessedRoCrate.getJsonMetadata()).get("@context").get(1);

        preProcessedRoCrate.getRootDataEntity().getProperties().fields().forEachRemaining(field ->
            validateField(field, preProcessedRoCrate, roCrateContext, roCrateContextUpdater, preProcessResult, roCrateEntityIdsAndTypes, false)
        );

        // remove the id of the rootDataset and the jsonDescriptor, the other id-s will be removed during processing the entities
        roCrateEntityIdsAndTypes.remove(preProcessedRoCrate.getRootDataEntity().getId());
        roCrateEntityIdsAndTypes.remove(preProcessedRoCrate.getJsonDescriptor().getId());
        // the license obj coming from AROMA is not really an id object, and its URL value is stored as a string in dv
        // remove this type of "id"-s too
        roCrateEntityIdsAndTypes.entrySet().removeIf(entry -> "URL".equals(entry.getValue()));

        // process the dataset and file entities
        // check id and hash pairs for files, these can not be modified
        validateDatasetAndFileEntities(preProcessedRoCrate, latestRoCrate, roCrateEntityIdsAndTypes, preProcessResult, roCrateContext, roCrateContextUpdater);
        
        // make sure all ids are processed
        if (!roCrateEntityIdsAndTypes.isEmpty()) {
            // remove the compound values that are not part of dv, and their types could not be validated
            Iterator<Map.Entry<String , String>> iterator = roCrateEntityIdsAndTypes.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, String> entry = iterator.next();
                var fieldByName = fieldService.findByName(entry.getValue());
                // if fieldByName is null that means the field not exists in dv
                if (fieldByName == null) {
                    // if the Root RO-Crate has any entity containing the id, the id can be removed
                    if (containsId(preProcessedRoCrate.getRootDataEntity().getProperties(), entry.getKey())) {
                        iterator.remove();   
                    } else {
                        // there is no parent entity for the child entity
                    }
                }
            }

            // remaining values must contain errors
            if (!roCrateEntityIdsAndTypes.isEmpty()) {
                preProcessResult.errors.add("Entities with the following '@id'-s could not be validated, check their relations in the RO-Crate: " + roCrateEntityIdsAndTypes.keySet());   
            }
        }

        preProcessResult.setRoCrate(preProcessedRoCrate);
        
        return preProcessResult;
    }

    private boolean containsId(JsonNode node, String id) {
        if (node.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                if (field.getValue().isTextual() && field.getValue().textValue().contains(id)) {
                    return true;
                }
                if (field.getValue().isObject() || field.getValue().isArray()) {
                    if (containsId(field.getValue(), id)) {
                        return true;
                    }
                }
            }
        } else if (node.isArray()) {
            for (JsonNode element : node) {
                if (containsId(element, id)) {
                    return true;
                }
            }
        }
        return false;
    }

    // validate the file and dataset entities through the RO-Crate's hasPart
    // this way we can check the files that are not part of any datasets and can traverse datasets from the top level
    private void validateDatasetAndFileEntities(RoCrate preProcessedRoCrate, RoCrate latestRoCrate, HashMap<String, String> roCrateEntityIdsAndTypes, RoCrateImportPrepResult preProcessResult, JsonNode roCrateContext, RoCrate.RoCrateBuilder roCrateContextUpdater) {
        // collect the file entities and their hashes in a hashmap for better performance
        Map<String, ObjectNode> latestRoCrateFilesWithHashes = latestRoCrate == null ? new HashMap<>() : Stream.concat(
                        latestRoCrate.getAllContextualEntities().stream().map(AbstractEntity::getProperties),
                        latestRoCrate.getAllDataEntities().stream().map(AbstractEntity::getProperties))
                .filter(entity -> hasType(entity, "File") && !isVirtualFile(entity))
                .collect(Collectors.toMap(
                        entity -> entity.get("hash").textValue(),
                        Function.identity()));

        preProcessedRoCrate.getRootDataEntity().hasPart.forEach(entityId -> {
            validateDataEntity(entityId, preProcessedRoCrate, latestRoCrate, latestRoCrateFilesWithHashes, roCrateEntityIdsAndTypes, preProcessResult, new HashSet<>(), roCrateContext, roCrateContextUpdater);
        });
    }

    // Validate a given RO-Crate Data Entity
    private void validateDataEntity(String entityId,RoCrate preProcessedRoCrate, RoCrate latestRoCrate, Map<String, ObjectNode> latestRoCrateFilesWithHashes, HashMap<String, String> roCrateEntityIdsAndTypes, RoCrateImportPrepResult preProcessResult, HashSet<String> circularReferenceIds, JsonNode roCrateContext, RoCrate.RoCrateBuilder roCrateContextUpdater) {
        var entity = preProcessedRoCrate.getEntityById(entityId).getProperties();
        var entityType = getTypeAsString(entity);
        roCrateEntityIdsAndTypes.remove(entityId);
        if (!entityType.equals("File") && !entityType.equals("Dataset")) {
            preProcessResult.errors.add("Entity with id: '" + entityId + "' has an invalid type: " + entityType);
        }
        entity.fields().forEachRemaining(field ->
                validateField(field, preProcessedRoCrate, roCrateContext, roCrateContextUpdater, preProcessResult, roCrateEntityIdsAndTypes, false)
        );
        if (entityType.equals("File")) {
            var invalidFileProps = validateFileEntityProps(entity);
            if (!invalidFileProps.isEmpty()) {
                preProcessResult.errors.add("File entity with id: '" + entityId + "' contains the following invalid properties: " + invalidFileProps);
            }
            // compare the file ids and hashes with the values from the previous version of the RO-Crate
            if (!latestRoCrateFilesWithHashes.isEmpty()) {
                var originalFileEntity = latestRoCrate.getEntityById(entityId);
                if (originalFileEntity == null) {
                    if (!entity.has("hash")) {
                        // this is a new virtual file
                        return;
                    }
                    var fileHash = entity.get("hash").textValue();
                    if (latestRoCrateFilesWithHashes.containsKey(entity.get("hash").textValue())) {
                        preProcessResult.errors.add("Corrupted id found for a File entity with hash: " + fileHash);
                    }
                } else {
                    if (!entity.has("hash") && !originalFileEntity.getProperties().has("hash")) {
                        // this is a virtual file
                        return;
                    }
                    var fileHash = entity.get("hash").textValue();
                    if (!originalFileEntity.getProperty("hash").textValue().equals(fileHash)) {
                        preProcessResult.errors.add("Corrupted hash found for a File entity with id: " + entityId);
                    }
                }
            }
        } else {
            var invalidDatasetProps = validateDatasetEntityProps(entity);
            if (!invalidDatasetProps.isEmpty()) {
                preProcessResult.errors.add("Dataset entity with id: '" + entityId + "' contains the following invalid properties: " + invalidDatasetProps);
            }
            if (entity.has("hasPart")) {
                var dsHasPart = entity.get("hasPart");
                if (!circularReferenceIds.add(entityId)) {
                    preProcessResult.errors.add("Circular reference in: " + circularReferenceIds + ". Caused by: " + entityId);
                }
                if (dsHasPart.isArray()) {
                    for (var idObj : dsHasPart) {
                        validateDataEntity(idObj.get("@id").textValue(), preProcessedRoCrate, latestRoCrate, latestRoCrateFilesWithHashes, roCrateEntityIdsAndTypes, preProcessResult, circularReferenceIds, roCrateContext, roCrateContextUpdater);
                    }
                } else {
                    validateDataEntity(dsHasPart.get("@id").textValue(), preProcessedRoCrate, latestRoCrate, latestRoCrateFilesWithHashes, roCrateEntityIdsAndTypes, preProcessResult, circularReferenceIds, roCrateContext, roCrateContextUpdater);
                }
            }
        }
    }

    // Validate field values and collect the field URIs in the RO-Crate context
    private void validateField(Map.Entry<String, JsonNode> field, RoCrate roCrate, JsonNode roCrateContext, RoCrate.RoCrateBuilder roCrateContextUpdater, RoCrateImportPrepResult preProcessResult, HashMap<String, String> roCrateEntityIdsAndTypes, boolean lvl2) {
        var fieldName = field.getKey();
        var fieldValue = field.getValue();
        if (!fieldName.startsWith("@") && !propsToIgnore.contains(fieldName)) {
            var fieldByName = fieldService.findByName(fieldName);
            // field already exists in dv
            if (fieldByName != null) {
                if (!fieldByName.isAllowMultiples() && fieldValue.isArray() && fieldValue.size() > 1) {
                    preProcessResult.errors.add("The field '" + fieldName + "' does not allow multiple values, but got: " + fieldValue);
                }
                var fieldType = fieldByName.getFieldType();
                if (!roCrateContext.has(fieldName)) {
                    roCrateContextUpdater.addValuePairToContext(fieldName, fieldByName.getUri());
                }
                // check value types based on the datasetFieldType
                if (fieldByName.isPrimitive()) {
                    ArrayList<String> controlledVocabularyValues = new ArrayList<>();
                    if (fieldByName.isAllowControlledVocabulary()) {
                        /*fieldByName.getChildDatasetFieldTypes().stream()
                                .map(DatasetFieldType::getControlledVocabularyValues)
                                .forEach(cvvList -> cvvList.forEach(cvv -> controlledVocabularyValues.add(cvv.getStrValue())));*/
                        fieldByName.getControlledVocabularyValues().forEach(cvv -> controlledVocabularyValues.add(cvv.getStrValue()));
                    }
                    validatePrimitiveField(fieldType, fieldName, fieldValue, controlledVocabularyValues, false, preProcessResult);
                } else if (fieldByName.isCompound()) {
                    if (lvl2) {
                        preProcessResult.errors.add("Compound values are not allowed at this level! Invalid compound value: '" + fieldName + "'.");
                    }
                    if (fieldValue.isArray()) {
                        for (JsonNode element : fieldValue) {
                            validateCompoundField(roCrate, roCrateContext, roCrateContextUpdater, fieldName, element, roCrateEntityIdsAndTypes, preProcessResult);
                        }
                    } else {
                        validateCompoundField(roCrate, roCrateContext, roCrateContextUpdater, fieldName, fieldValue, roCrateEntityIdsAndTypes, preProcessResult);
                    }
                }
            } else {
                // field not yet exists in dv, in this case we can not check the types
                // only the presence of a child entity
                if (fieldValue.isObject()) {
                    var childId = fieldValue.get("@id").textValue();
                    var childEntity = roCrate.getEntityById(childId);
                    if (childEntity == null) {
                        preProcessResult.errors.add("No child entity found for the parent entity with id: '" + childId + "'");
                    }
                } else if (fieldValue.isArray()) {
                    for (var idObj : fieldValue) {
                        var childId = idObj.get("@id").textValue();
                        var childEntity = roCrate.getEntityById(childId);
                        if (childEntity == null) {
                            preProcessResult.errors.add("No child entity found for the parent entity with id: '" + childId + "'");
                        }
                    }
                }
                
            }
        }
    }

    // Validate a given Compound Field and its children
    private void validateCompoundField(RoCrate roCrate, JsonNode roCrateContext, RoCrate.RoCrateBuilder roCrateContextUpdater, String fieldName, JsonNode idObj, HashMap<String, String> roCrateEntityIdsAndTypes, RoCrateImportPrepResult preProcessResult) {
        if (!idObj.has("@id")) {
            preProcessResult.errors.add("The parent obj with DatasetFieldType: " + fieldName + " must contain a '@id' reference'");
            return;
        }

        var entityId = idObj.get("@id").textValue();
        roCrateEntityIdsAndTypes.remove(entityId);
        var entity = roCrate.getEntityById(entityId);
        if (entity == null) {
            preProcessResult.errors.add("No child entity found for the parent entity with id: '" + entityId + "'");
            return;
        }
        var entityProperties = entity.getProperties();

        if (!entityProperties.has("@type") || !getTypeAsString(entityProperties).equals(fieldName)) {
            preProcessResult.errors.add("The entity with id: '" + entityId + "' has invalid type! The correct type would be: " + fieldName);
            return;
        }

        if (entityProperties.has("conformsTo")) {
            validateConformsTo(entityProperties.get("conformsTo"), entityId, preProcessResult);
        }

        entityProperties.fields().forEachRemaining(field -> validateField(field, roCrate, roCrateContext, roCrateContextUpdater, preProcessResult, roCrateEntityIdsAndTypes, true));
    }

    // check if a conformsToObj is valid
    private void validateConformsToObj(JsonNode conformsToObj, String entityId, RoCrateImportPrepResult preProcessResult) {
        String errorMessageFormat = "Entity with id: '%s' contains invalid conformsTo %s.";

        if (!conformsToObj.has("@id") || conformsToObj.size() > 1) {
            preProcessResult.errors.add(String.format(errorMessageFormat, entityId, "property"));
        } else {
            String conformsToUrl = conformsToObj.get("@id").textValue();
            if (!isURLValid(conformsToUrl)) {
                preProcessResult.errors.add(String.format(errorMessageFormat, entityId, "URL"));
            }
        }
    }

    // validate the URLs in the conformsTo property
    private void validateConformsTo(JsonNode conformsToProperty, String entityId, RoCrateImportPrepResult preProcessResult) {
        if (conformsToProperty.isArray()) {
            for (var conformsToObj : conformsToProperty) {
                validateConformsToObj(conformsToObj, entityId, preProcessResult);
            }
        } else {
            validateConformsToObj(conformsToProperty, entityId, preProcessResult);
        }
    }

    // check if the primitive field contains appropriate value(s), based on the fieldType definitions below:
    // https://guides.dataverse.org/en/latest/admin/metadatacustomization.html?highlight=metadata#fieldtype-definitions
    private void validatePrimitiveField(DatasetFieldType.FieldType fieldType, String fieldName, JsonNode fieldValue, ArrayList<String> controlledVocabularyValues, boolean lvl2, RoCrateImportPrepResult preProcessResult) {
        if (fieldValue.isArray()) {
            if (lvl2) {
                preProcessResult.errors.add("The field '" + fieldName + "' can not have Arrays as values, but got: " + fieldValue);
            }
            // If fieldValue is an array, validate each element
            for (JsonNode element : fieldValue) {
                validatePrimitiveField(fieldType, fieldName, element, controlledVocabularyValues, true, preProcessResult);
            }
        } else {
            if (!controlledVocabularyValues.isEmpty()) {
                if (!controlledVocabularyValues.contains(fieldValue.textValue())) {
                    preProcessResult.errors.add("Invalid controlled vocabulary value: '" + fieldValue + "' for field: '" + fieldName + "'.");
                }
            } else {
                switch (fieldType) {
                    case NONE:
                        preProcessResult.errors.add("The field: " + fieldName + " can not have fieldType 'none'");
                        break;

                    case DATE:
                        // Validate date format (YYYY-MM-DD, YYYY-MM, or YYYY)
                        if (!fieldValue.isTextual() ||
                                !fieldValue.textValue().matches("\\d{4}-\\d{2}-\\d{2}|\\d{4}-\\d{2}|\\d{4}")) {
                            preProcessResult.errors.add("The provided value is not a valid date for field: " + fieldName);
                        }
                        break;

                    case EMAIL:
                        // Validate email format
                        if (!fieldValue.isTextual() || !isEmailValid(fieldValue.textValue())) {
                            preProcessResult.errors.add("The provided value is not a valid email address for field: " + fieldName);
                        }
                        break;

                    case TEXT:
                        // Validate that the value is a string and any text other than newlines
                        if (!fieldValue.isTextual() || fieldValue.textValue().matches(".*\\n.*")) {
                            preProcessResult.errors.add("Newlines are not allowed for field: " + fieldName);
                        }
                        break;

                    case TEXTBOX:
                        // Validate that the value is a string
                        if (!fieldValue.isTextual()) {
                            preProcessResult.errors.add("The provided value should be a string for " + fieldName + " with 'textbox' type.");
                        }
                        break;

                    case URL:
                        // Validate URL
                        if (!fieldValue.isTextual() || !isURLValid(fieldValue.textValue())) {
                            preProcessResult.errors.add("If not empty, the field must contain a valid URL for field: " + fieldName);
                        }
                        break;

                    case INT:
                        // Validate integer value
                        if (!fieldValue.isInt()) {
                            preProcessResult.errors.add("The provided value is not a valid integer for field: " + fieldName);
                        }
                        break;

                    case FLOAT:
                        // Validate floating-point number
                        if (!fieldValue.isFloat()) {
                            preProcessResult.errors.add("The provided value is not a valid floating-point number for field: " + fieldName);
                        }
                        break;

                    default:
                        preProcessResult.errors.add("Invalid fieldType: " + fieldType + " for field: " + fieldName);
                        break;
                }
            }
        }
    }

    // check whether the dataset entity contains file entity related props
    private Set<String> validateDatasetEntityProps(ObjectNode dsEntity) {
        Set<String> invalidKeys = new HashSet<>();
        if (dsEntity.has("hash")) {
            invalidKeys.add("hash");
        }
        if (dsEntity.has("contentSize")) {
            invalidKeys.add("contentSize");
        }
        return invalidKeys;
    }

    // check whether the file entity contains dataset entity related props
    private Set<String> validateFileEntityProps(ObjectNode fileEntity) {
        Set<String> invalidKeys = new HashSet<>();
        if (fileEntity.has("hasPart")) {
            invalidKeys.add("hasPart");
        }
        return invalidKeys;
    }

    public void postProcessRoCrateFromAroma(Dataset dataset, RoCrate roCrate) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        String roCrateFolderPath = getRoCrateFolder(dataset.getLatestVersion());
        ObjectNode rootDataEntityProperties = roCrate.getRootDataEntity().getProperties();
        Map<String, DatasetFieldType> compoundFields = dataset.getLatestVersion().getDatasetFields().stream().map(DatasetField::getDatasetFieldType).filter(DatasetFieldType::isCompound).collect(Collectors.toMap(DatasetFieldType::getName, Function.identity()));
        ArrayNode rootHasPart = mapper.createArrayNode();
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
            // Sometimes the RO-CRATE generated by AROMA still holds the deleted entity ids in the rootDatasetEntity's hasPart
            // must double-check whether the entity is still present or not and remove it from the rootDatasetEntity's hasPart if needed
            // to prevent any errors.
            // This can be reproduced if the last file is removed from the dataset in AROMA, in this case the rootDatasetEntity's hasPart
            // will still contain the id, but there will be no file entity for it, later if the dataset is edited a new entity
            // with @type "Thing" will be generated by AROMA that breaks things.
            if (propName.equals("hasPart")) {
                if (propVal.isObject()) {
                    if (datasetAndFileEntities.containsKey(propVal.get("@id").textValue())) {
                        rootHasPart.add(propVal);
                    } else {
                        roCrate.deleteEntityById(propVal.get("@id").textValue());
                    }
                } else {
                    for (var arrayVal : propVal) {
                        if (datasetAndFileEntities.containsKey(arrayVal.get("@id").textValue())) {
                            rootHasPart.add(arrayVal);
                        } else {
                            roCrate.deleteEntityById(arrayVal.get("@id").textValue());
                        }
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

        // Must collect the datafiles this way to only process the ones that belong to the actual dataset version
        List<DataFile> dvDatasetFiles = dataset.getLatestVersion().getFileMetadatas().stream().map(FileMetadata::getDataFile).collect(Collectors.toList());
        // the root dataset's hasPart is handled differently, it has to be merged separately
        mergeHasParts(roCrate, rootHasPart, rootDataEntityProperties, mapper);
        rootHasPart.forEach(ds -> postProcessDatasetAndFileEntities(roCrate, ds, dvDatasetFiles, extraMetadata, rootDataEntityProperties, mapper));
        
        roCrate.setRoCratePreview(new AutomaticPreview());

        RoCrateWriter roCrateFolderWriter = new RoCrateWriter(new FolderWriter());
        roCrateFolderWriter.save(roCrate, roCrateFolderPath);
        writeOutRoCrateExtras(extraMetadata, getRoCrateParentFolder(dataset));
    }
    
    // Upon preparing an RO-Crate JSON to be parsed to an actual RO-Crate object, id-s and type-s must be pre-checked and
    // "reverse" properties must be removed (since the RO-Crate lib doesn't support those yet)
    private HashMap<String, String> preCheckEntities(JsonNode entities, RoCrateImportPrepResult preProcessResult) {
        HashMap<String, String> encounteredIdsWithTypes = new HashMap<>();

        entities.forEach(entity -> {
            if (entity.has("@id")) {
                String entityType = null;
                String entityId = entity.get("@id").textValue();
                if (!encounteredIdsWithTypes.containsKey(entityId)) {
                    if (!entity.has("@type")) {
                        if (entityId == null) {
                            preProcessResult.errors.add("Missing '@type' for entity: " + entity);
                        } else {
                            preProcessResult.errors.add("The entity with id: '" + entityId + "' does not have a '@type'.");
                        }
                    } else {
                        entityType = getTypeAsString(entity);
                    }
                    encounteredIdsWithTypes.put(entityId, entityType);
                } else {
                    preProcessResult.errors.add("The RO-Crate contains the following '@id' multiple times: " + entityId);
                }
            } else {
                preProcessResult.errors.add("Missing '@id' for entity: " + entity);
            }
            ((ObjectNode)entity).remove("@reverse");
        });

        return encounteredIdsWithTypes;
    }

    private boolean postProcessDatasetAndFileEntities(RoCrate roCrate, JsonNode parentEntity, List<DataFile> dvDatasetFiles, Map<String, ArrayList<String>> extraMetadata, ObjectNode parentObj, ObjectMapper mapper) {
        String oldId = parentEntity.get("@id").textValue();
        var entity = roCrate.getEntityById(oldId);
        var entityNode = entity.getProperties();
        if (hasType(entityNode, "File")) {
            String fileHash = entityNode.has("hash") ? entityNode.get("hash").textValue() : "";
            var dataFileOpt = dvDatasetFiles.stream().filter(f ->
                            f.getChecksumValue().equals(fileHash))
                            .findFirst();
            boolean isVirtualFile = dataFileOpt.isEmpty();
            if (isVirtualFile) {
                extraMetadata.get("virtualFileAdded").add(entityNode.get("@id").textValue());
            } else {
                DataFile dataFile = dataFileOpt.get();
                String arpPid = dataFile.getGlobalId() != null ? dataFile.getGlobalId().asString() : "";
                entityNode.put("@arpPid", arpPid);
            }

            if (fileHasExtraMetadata(entityNode)) {
                extraMetadata.get("fileWithMetadata").add(entityNode.get("@id").textValue());
            }
            return isVirtualFile;
        } else {
            boolean isVirtual;
            // Make it end with "/" to conform to Describo, which requires Dataset id-s to end in '/'
            // although this is just a SHOULD not a MUST by the spec.
            String newId  = createRoIdForDataset(entityNode.get("name").textValue(), parentObj);
            boolean gotNewId = !newId.equals(oldId);
            if (gotNewId) {
                ((ObjectNode) parentEntity).put("@id", newId);
                entityNode.put("@id", newId);
            }
            JsonNode hasPart = entityNode.get("hasPart");
            if (hasPart != null && !hasPart.isEmpty()) {
                if (hasPart.isObject()) {
                    isVirtual = postProcessDatasetAndFileEntities(roCrate, hasPart, dvDatasetFiles, extraMetadata, entityNode, mapper);
                } else {
                    mergeHasParts(roCrate, hasPart, entityNode, mapper);
                    ArrayList<Boolean> isVirtualResults = new ArrayList<>();
                    for (var arrayVal : hasPart) {
                        isVirtualResults.add(postProcessDatasetAndFileEntities(roCrate, arrayVal, dvDatasetFiles, extraMetadata, entityNode, mapper));
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

    private void mergeHasParts(RoCrate roCrate, JsonNode hasPart, JsonNode parentNode, ObjectMapper mapper) {
        // check if the hasPart array contains any duplicates that need to be merged
        var it = hasPart.iterator();
        while (it.hasNext()) {
            var idObj = it.next();
            var originalId = idObj.get("@id").textValue();
            var originalEntity = roCrate.getEntityById(originalId).getProperties();
            if (getTypeAsString(originalEntity).equals("Dataset")) {
                String generatedId  = createRoIdForDataset(originalEntity.get("name").textValue(), parentNode);
                // the entity got a new id, check if merging is required
                if (!originalId.equals(generatedId)) {
                    var alreadyPresentEntity = roCrate.getEntityById(generatedId);
                    if (alreadyPresentEntity != null) {
                        mergeDatasets(originalEntity, alreadyPresentEntity.getProperties(), mapper);
                        it.remove();
                        roCrate.deleteEntityById(originalId);
                    } else {
                        ((ObjectNode) idObj).put("@id", generatedId);
                    }
                }
            }
        }
    }

    private void mergeDatasets(ObjectNode fromDs, ObjectNode toDs, ObjectMapper mapper) {
        fromDs.fields().forEachRemaining(field -> {
            if (!field.getKey().equals("hasPart") && !field.getKey().equals("@id")) {
                toDs.set(field.getKey(), field.getValue());
            }
        });

        // Get hasPart properties from both nodes
        JsonNode fromHasPart = fromDs.get("hasPart");
        JsonNode toHasPart = toDs.get("hasPart");

        // Create an array node to hold the merged content
        ArrayNode mergedHasPart = mapper.createArrayNode();

        // Add hasPartOne content to mergedHasPart if it's an array or object
        if (fromHasPart != null) {
            if (fromHasPart.isArray()) {
                mergedHasPart.addAll((ArrayNode) fromHasPart);
            } else {
                mergedHasPart.add(fromHasPart);
            }
        }

        if (toHasPart != null) {
            if (toHasPart.isArray()) {
                mergedHasPart.addAll((ArrayNode) toHasPart);
            } else {
                mergedHasPart.add(toHasPart);
            }
        }

        toDs.set("hasPart", mergedHasPart);
    }

    private boolean isVirtualFile(ObjectNode file) {
        return !file.has("@arpPid");
    }
    
    private boolean isProcessedId(String id) {
        // regular expression for the id pattern of the files from dataverse
        Pattern idPattern = Pattern.compile("#\\d+::[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");
        return idPattern.matcher(id).matches();
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

    // Updates the id-s of the properties to follow the "#DatasetFieldCompoundValueId" + UUID format
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
                String newId = createRoIdForCompound(valueToObtainIdFrom);
                var rootDataEntity = roCrate.getRootDataEntity().getProperties().get(propName);
                if (rootDataEntity.isObject()) {
                    ((ObjectNode) rootDataEntity).put("@id", newId);
                } else {
                    ((ObjectNode) rootDataEntity.get(positionOfProp)).put("@id", newId);
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
            objectNode.fields().forEachRemaining(entry -> removeReverseProperties(entry.getValue())); //todo: only if the value isObject or isArray
        } else if (node.isArray()) {
            node.forEach(this::removeReverseProperties);
        }
    }

    // Saves the value(s) for a compound datasetField
    private void processCompoundField(JsonNode roCrateValues, DatasetVersion updatedVersion, DatasetFieldType datasetFieldType, Map<String, ContextualEntity> contextualEntityHashMap) {
        List<DatasetFieldCompoundValue> alreadyPresentCompoundValues = updatedVersion.getDatasetField(datasetFieldType).getDatasetFieldCompoundValues();
        if (roCrateValues.isArray()) {
            List<DatasetFieldCompoundValue> updatedCompoundValues = new ArrayList<>();
            var index = 0;
            DatasetFieldCompoundValue compoundValueToUpdate;
            for (var roCrateValue : roCrateValues) {
                if (alreadyPresentCompoundValues.size() > index) {
                    // keep the original id
                    compoundValueToUpdate = alreadyPresentCompoundValues.get(index);
                    for (DatasetFieldType dsfType : datasetFieldType.getChildDatasetFieldTypes()) {
                        if (compoundValueToUpdate.getChildDatasetFields().stream().noneMatch(cdfs -> cdfs.getDatasetFieldType().equals(dsfType))) {
                            compoundValueToUpdate.getChildDatasetFields().add(DatasetField.createNewEmptyChildDatasetField(dsfType, compoundValueToUpdate));
                        }
                    }
                } else {
                    compoundValueToUpdate = createNewEmptyDatasetFieldCompoundValue(updatedVersion.getDatasetField(datasetFieldType));
                }
                var updatedCompoundValue = processCompoundFieldValue(roCrateValue, compoundValueToUpdate, datasetFieldType, contextualEntityHashMap);
                index++;
                updatedCompoundValues.add(updatedCompoundValue);
            }
            updatedVersion.getDatasetField(datasetFieldType).setDatasetFieldCompoundValues(updatedCompoundValues);
        } else {
            DatasetFieldCompoundValue compoundValueToUpdate;
            if (alreadyPresentCompoundValues.isEmpty()) {
                compoundValueToUpdate = createNewEmptyDatasetFieldCompoundValue(updatedVersion.getDatasetField(datasetFieldType));
            } else {
                // keep the original id
                compoundValueToUpdate = alreadyPresentCompoundValues.get(0);
                for (DatasetFieldType dsfType : datasetFieldType.getChildDatasetFieldTypes()) {
                    if (compoundValueToUpdate.getChildDatasetFields().stream().noneMatch(cdfs -> cdfs.getDatasetFieldType().equals(dsfType))) {
                        compoundValueToUpdate.getChildDatasetFields().add(DatasetField.createNewEmptyChildDatasetField(dsfType, compoundValueToUpdate));
                    }
                }
            }
            var updatedCompoundValue = processCompoundFieldValue(roCrateValues, compoundValueToUpdate, datasetFieldType, contextualEntityHashMap);
            updatedVersion.getDatasetField(datasetFieldType).setDatasetFieldCompoundValues(List.of(updatedCompoundValue));
        }
    }
    
    // Saves the value(s) for a DatasetFieldCompoundValue
    private DatasetFieldCompoundValue processCompoundFieldValue(JsonNode roCrateValue, DatasetFieldCompoundValue compoundValueToUpdate, DatasetFieldType datasetFieldType, Map<String, ContextualEntity> contextualEntityHashMap) {
        ContextualEntity contextualEntity = contextualEntityHashMap.get(roCrateValue.get("@id").textValue());
        // Upon creating a new compound field for a dataset field type in AROMA, that allows multiple values, 
        // the initial roCrateJson that is sent by aroma can contain entities (only id-s really) in its root data entity that have no contextual entities
        // as a result, the contextualEntity can be null
        if (contextualEntity != null) {
            for (var it = contextualEntity.getProperties().fields(); it.hasNext();) {
                var roCrateField = it.next();
                String fieldName = roCrateField.getKey();
                JsonNode fieldValue = roCrateField.getValue();
                if (!fieldName.startsWith("@") && !propsToIgnore.contains(fieldName)) {
                    // If field doesn't belong to the DV, ignore it
                    if (datasetFieldType == null) {
                        break;
                    }
                    // RO-Crate spec: name: SHOULD identify the dataset to humans well enough to disambiguate it from other RO-Crates
                    // In our case if the MDB has a "name" field, then we use it and store the value we get, otherwise
                    // if there's no "name" field, we ignore it. Still, we should check for datasetFieldType == null,
                    // wich must be an error.
                    if (fieldName.equals("name")) {
                        break;
                    }
                    // Find the childDatasetField to update
                    Optional<DatasetField> childDatasetFieldOpt = compoundValueToUpdate.getChildDatasetFields().stream().filter(childField ->
                                    Objects.equals(childField.getDatasetFieldType().getName(), fieldName))
                                    .findFirst();
                    if (childDatasetFieldOpt.isPresent()) {
                        DatasetField childDatasetField = childDatasetFieldOpt.get();
                        if (childDatasetField.getDatasetFieldType().isAllowControlledVocabulary()) {
                            if (fieldValue.isArray()) {
                                List<String> controlledVocabValues = new ArrayList<>();
                                fieldValue.forEach(controlledVocabValue -> controlledVocabValues.add(controlledVocabValue.textValue()));
                                processControlledVocabFields(controlledVocabValues, childDatasetField, childDatasetField.getDatasetFieldType());
                            } else {
                                processControlledVocabFields(fieldValue.textValue(), childDatasetField, childDatasetField.getDatasetFieldType());
                            }
                        } else {
                            // URL entities nees to be handled differently
                            if (fieldValue.isObject() && fieldValue.has("@id")) {
                                var linkedObj = contextualEntityHashMap.get(fieldValue.get("@id").textValue()).getProperties();
                                if (hasType(linkedObj, "URL")) {
                                    childDatasetField.setSingleValue(linkedObj.get("name").textValue());
                                }
                            } else {
                                childDatasetField.setSingleValue(fieldValue.textValue());
                            }
                        }
                    } else {
                        JsfHelper.addErrorMessage("An error occurred during processing compound field: \"" + datasetFieldType.getName() + "\".");
                        throw new RuntimeException("An error occurred during processing compound field: \"" + datasetFieldType.getName() + "\".");
                    }
                }
            };
            // finally remove the values from DV that were removed in AROMA
            compoundValueToUpdate.getChildDatasetFields().forEach(childField -> {
                if (!contextualEntity.getProperties().has(childField.getDatasetFieldType().getName())) {
                    childField.setSingleValue(null);
                }
            });
        }
        return compoundValueToUpdate;
    }

    // Saves the value(s) for a primitive datasetField
    public void processPrimitiveField(JsonNode fieldValue, DatasetVersion updatedVersion, DatasetFieldType datasetFieldType, RoCrate roCrate) {
        DatasetField dsfToUpdate = updatedVersion.getDatasetField(datasetFieldType);
        if (datasetFieldType.isAllowMultiples()) {
            if (fieldValue.isArray()) {
                if (fieldValue.elements().hasNext() && fieldValue.elements().next().isTextual()) {
                    setSingleValue(dsfToUpdate, fieldValue.textValue());
                } else {
                    // collect and update the already present values to keep their id-s
                    int index = 0;
                    List<DatasetFieldValue> fieldValues = dsfToUpdate.getDatasetFieldValues();
                    List<DatasetFieldValue> newFieldValues = dsfToUpdate.getDatasetFieldValues();
                    for (Iterator<JsonNode> it = fieldValue.elements(); it.hasNext(); ) {
                        JsonNode e = it.next();
                        String newValue;
                        if (e.isTextual()) {
                            newValue = e.textValue();
                        } else {
                            newValue = roCrate.getEntityById(e.get("@id").textValue()).getProperty("name").textValue();
                        }
                        if (fieldValues.size() > index) {
                            // keep the original id
                            DatasetFieldValue updatedValue = fieldValues.get(index);
                            updatedValue.setValue(newValue);
                            newFieldValues.add(updatedValue);
                        } else {
                            DatasetField dsf = new DatasetField();
                            dsf.setDatasetFieldType(datasetFieldType);
                            newFieldValues.add(new DatasetFieldValue(dsf, newValue));
                        }
                        index++;
                    }
                    dsfToUpdate.setDatasetFieldValues(newFieldValues);
                }
            } else {
                String newValue;
                if (fieldValue.isTextual()) {
                    newValue = fieldValue.textValue();
                } else {
                    newValue = roCrate.getEntityById(fieldValue.get("@id").textValue()).getProperty("name").textValue();
                }
                setSingleValue(dsfToUpdate, newValue);
            }
        } else {
            String value = "";
            if (fieldValue.isTextual()) {
                value = fieldValue.textValue();
            } else {
                var entity = roCrate.getEntityById(fieldValue.get("@id").textValue());
                if (entity != null) {
                    value = entity.getProperty("name").textValue();
                } else {
                    // TODO: check this later, because the entity being null should mean that it was removed from the RO-CRATE in AROMA
                    // however, the property from the rootDataset is not getting removed properly, so we must set it to null here
                    roCrate.getRootDataEntity().getProperties().set(datasetFieldType.getName(), null);
                }
            }
            setSingleValue(dsfToUpdate, value);
        }
    }
    
    // Sets a single value for a datasetField, while keeping the original id if possible and removes the other values
    private void setSingleValue(DatasetField dsfToUpdate, String value) {
        List<DatasetFieldValue> alreadyPresentValues = dsfToUpdate.getDatasetFieldValues();
        if (alreadyPresentValues.isEmpty()) {
            dsfToUpdate.setSingleValue(value);
        } else {
            // keep the first value only in case there were others remove them
            DatasetFieldValue valueToUpdate = alreadyPresentValues.get(0);
            valueToUpdate.setValue(value);
            dsfToUpdate.setDatasetFieldValues(List.of(valueToUpdate));
        }
    }
    
    // Saves the controlledVocabularyValues for the datasetField
    public void processControlledVocabFields(Object fieldValue, DatasetField dsfToUpdate, DatasetFieldType datasetFieldType) {
        if (fieldValue instanceof ArrayNode) {
            AtomicInteger i = new AtomicInteger();
            List<ControlledVocabularyValue> controlledVocabValues = new ArrayList<>();
            ((ArrayNode) fieldValue).forEach(controlledVocabValue -> {
                controlledVocabValues.add(createControlledVocabularyValueForDatasetFieldType(controlledVocabValue.textValue(), datasetFieldType, i.getAndIncrement()));
            });
            dsfToUpdate.setControlledVocabularyValues(controlledVocabValues);
        } else {
            String controlledVocabValue;
            if (fieldValue instanceof TextNode) {
                controlledVocabValue = ((TextNode) fieldValue).textValue();
            } else {
                controlledVocabValue = fieldValue.toString();
            }
            var newCvv = createControlledVocabularyValueForDatasetFieldType(controlledVocabValue, datasetFieldType, 0);
            dsfToUpdate.setControlledVocabularyValues(List.of(newCvv));
        }
    }
    
    // Sets the chosen controlledVocabularyValue, also if that is not already present creates it in DV
    public ControlledVocabularyValue createControlledVocabularyValueForDatasetFieldType(String strValue, DatasetFieldType datasetFieldType, int displayOrder) {
        DatasetFieldTypeArp dftArp = arpMetadataBlockServiceBean.findDatasetFieldTypeArpForFieldType(datasetFieldType);
        List<ControlledVocabularyValue> controlledVocabValues = controlledVocabularyValueService.findByDatasetFieldTypeId(datasetFieldType.getId());
        Optional<ControlledVocabularyValue> optionalCvv = controlledVocabValues.stream().filter(cvv -> cvv.getStrValue().equals(strValue)).findFirst();
        if (dftArp != null && dftArp.getCedarDefinition() != null) {
            String cedarDef = dftArp.getCedarDefinition();
            JsonObject templateFieldJson = new Gson().fromJson(cedarDef, JsonObject.class);
            if (JsonHelper.getJsonObject(templateFieldJson, "_valueConstraints.branches[0]") != null) {
                if (optionalCvv.isEmpty()) {
                    var cvv = new ControlledVocabularyValue();
                    cvv.setStrValue(strValue);
                    cvv.setDatasetFieldType(datasetFieldType);
                    cvv.setDisplayOrder(displayOrder);
                    cvv.setIdentifier("");
                    fieldService.save(cvv);
                    return cvv;
                }
            }
        }
        
        if (optionalCvv.isEmpty()) {
            throw new RuntimeException("No controlled vocabulary value was found for value: " + strValue);
        }
        
        return optionalCvv.get();
    }

    private String createRoIdForDataFile(DataFile dataFile) {
        // https://w3id.org/arp/ro-id/doi:A10.5072/FK2/ZL0O25/file/123
        return createRoidWithFieldName(
                dataFile.getOwner(),
                "file",
                dataFile.getId()
        );
    }

    private String getDataFileIdFromRoId(String roId) {
        return getLastPathElementAsId(roId);
    }

    // We use this generation logic for datasets that are the part of an RO-Crate (folder paths in DV)
    // not for Datasets as DV Objects, this might change later when the w3id is implemented
    private String createRoIdForDataset(String folderName, JsonNode parentObj) {
        String parentId = parentObj.get("@id").textValue();
        if (parentId.equals("./")) {
            parentId = "";
        }
        return parentId + URLEncoder.encode(folderName.replaceAll("\\s", "_"), StandardCharsets.UTF_8) + "/";
    }

//    At this point this function would be useless
//    private Long getDatasetIdFromRoId(String roId) {
//
//    }

    private String createRoIdForCompound(DatasetFieldCompoundValue compoundValue) {
        // https://w3id.org/arp/ro-id/doi:A10.5072/FK2/ZL0O25/author/2088
        return createRoidWithFieldName(
                compoundValue.getParentDatasetField().getDatasetVersion().getDataset(),
                compoundValue.getParentDatasetField().getDatasetFieldType().getName(),
                compoundValue.getId()
        );
    }

    private String getCompoundIdFromRoId(String roId) {
        return getLastPathElementAsId(roId);
    }

    private String getLastPathElementAsId(String roId) {
        String[] pathSegments = roId.split("/");
        try {
            return pathSegments[pathSegments.length - 1];
        }
        catch (NumberFormatException ex) {
            throw new RuntimeException("Invalid ro-id"+roId);
        }
    }

    private String createRoidWithFieldName(Dataset dataset, String fieldName, Long dvId) {
        // https://w3id.org/arp/ro-id/doi:A10.5072/FK2/ZL0O25/author/2088
        String w3IdBase = arpConfig.get("arp.w3id.base");
        String pid = dataset.getGlobalId().asString();
        var roid = w3IdBase + "/ro-id/" + pid
                + "/" + fieldName
                + "/" + dvId;
        return roid;

    }
}
