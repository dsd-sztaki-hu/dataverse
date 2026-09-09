package edu.harvard.iq.dataverse.arp.rocrate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.harvard.iq.dataverse.*;
import edu.harvard.iq.dataverse.api.arp.util.StorageUtils;
import edu.harvard.iq.dataverse.arp.ArpConfig;
import edu.harvard.iq.dataverse.arp.ArpMetadataBlockServiceBean;
import edu.harvard.iq.dataverse.arp.ArpServiceBean;
import edu.harvard.iq.dataverse.arp.DatasetFieldTypeArp;
import edu.harvard.iq.dataverse.dataset.DatasetUtil;
import edu.kit.datamanager.ro_crate.RoCrate;
import edu.kit.datamanager.ro_crate.entities.AbstractEntity;
import edu.kit.datamanager.ro_crate.entities.contextual.ContextualEntity;
import edu.kit.datamanager.ro_crate.entities.data.FileEntity;
import edu.kit.datamanager.ro_crate.entities.data.RootDataEntity;
import edu.kit.datamanager.ro_crate.preview.AutomaticPreview;
import jakarta.ejb.EJB;
import jakarta.ejb.Stateless;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Stateless
@Named
public class RoCrateExportManager {

    private static final Logger logger = Logger.getLogger(RoCrateExportManager.class.getCanonicalName());
    
    @EJB
    RoCrateServiceBean roCrateServiceBean;

    @EJB
    RoCrateConformsToIdProvider roCrateConformsToProvider;

    @EJB
    ArpMetadataBlockServiceBean arpMetadataBlockServiceBean;

    @EJB
    RoCrateNameProvider roCrateNameProvider;

    @Inject
    RoCrateUploadServiceBean roCrateUploadServiceBean;

    @EJB
    RoCrateImportMappingStoreBean roCrateImportMappingStore;

    @EJB
    DatasetServiceBean datasetService;

    @EJB
    DatasetVersionServiceBean datasetVersionService;

    @EJB
    DataFileServiceBean datafileService;

    @EJB
    ArpConfig arpConfig;
    
    @EJB
    ArpServiceBean arpServiceBean;
    
    public void createOrUpdate(RoCrate roCrate, DatasetVersion version, boolean isCreation, Map<String, DatasetFieldType> datasetFieldTypeMap) throws JsonProcessingException {
        var t = RoCrateOpLog.start("export.fields", version).extra("creation", isCreation);
        try {
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
        roCrateContextUpdater.addValuePairToContext("license", "https://schema.org/license");

        String formattedDate = getDatePublishedForRoCrate(version);
        props.put("datePublished", formattedDate);
        roCrateContextUpdater.addValuePairToContext("datePublished", "https://schema.org/datePublished");

        rootDataEntity.setProperties(props);

        // Remove the entities from the RO-Crate that had been deleted from DV
        if (!isCreation) {
            removeDeletedEntities(roCrate, version, datasetFields, datasetFieldTypeMap);
        }
        
        // Need to collect and remove the URL entities that were modified in DV
        // Can not remove them during the fieldType processing because of the concurrentmodificationexceptions
        var urlEntityIdsToRemove = new ArrayList<String>();

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
                processCompoundFieldType(roCrate, roCrateContextUpdater, rootDataEntity, datasetField, fieldName, fieldUri, mapper, isCreation, urlEntityIdsToRemove);
            } else {
                processPrimitiveFieldType(roCrate, roCrateContextUpdater, rootDataEntity, datasetField, fieldName, fieldUri, mapper, isCreation, urlEntityIdsToRemove);
            }
        }
        
        // Remove the URL entities that are no longer used
        urlEntityIdsToRemove.forEach(roCrate::deleteEntityById);

        // MDB-s can only be get via the Dataset and its Dataverse, so we need to pass version.getDataset()
        roCrateServiceBean.collectConformsToIds(version.getDataset(), rootDataEntity);
        } catch (JsonProcessingException | RuntimeException e) {
            t.fail(e);
            throw e;
        } finally {
            t.close();
        }
    }

    /**
     * Entry point called from Dataverse core (dataset create/update commands).
     */
    public void createOrUpdateRoCrate(DatasetVersion version) throws Exception {
        if (version == null || version.getId() == null) {
            return;
        }
        Map<String, String> importMapping = roCrateImportMappingStore.take(version.getId());
        if (importMapping == null || importMapping.isEmpty()) {
            importMapping = roCrateUploadServiceBean.getImportMapping();
        }
        doCreateOrUpdateRoCrate(findDeepVersion(version), importMapping);
        roCrateUploadServiceBean.reset();
    }

    /**
     * Final RO-Crate export for API zip uploads. Runs after files are saved so the export
     * sees the complete file list and uses the uploaded file @id mapping explicitly.
     */
    public void finalizeRoCrateAfterZipUpload(DatasetVersion version, Map<String, String> importMapping) throws Exception {
        if (version == null || version.getId() == null) {
            return;
        }
        doCreateOrUpdateRoCrate(findDeepVersion(version), importMapping);
    }

    public JsonNode readRoCrateJsonFromDisk(DatasetVersion version) throws IOException {
        return readRoCrateJsonFromDisk(roCrateServiceBean.getRoCratePath(version));
    }

    public JsonNode readRoCrateJsonFromDisk(String path) throws IOException {
        return roCrateServiceBean.readRoCrateJson(path);
    }

    private DatasetVersion findDeepVersion(DatasetVersion version) {
        return datasetVersionService.findDeep(version.getId());
    }

    public void doCreateOrUpdateRoCrate(DatasetVersion version, Map<String, String> importMapping) throws Exception {
        var t = RoCrateOpLog.start("export.createOrUpdate", version);
        try {
            var dataset = version.getDataset();
            var roCratePath = Paths.get(roCrateServiceBean.getRoCratePath(version));
            RoCrate roCrate;
            String roCrateFolderPath = roCrateServiceBean.getRoCrateFolder(version);

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
                RoCrate ro = RoCrateOpLog.readCrateFolder(roCrateFolderPath);
                roCrate = new RoCrate.RoCrateBuilder(ro).setPreview(new AutomaticPreview()).build();
                Map<String, DatasetFieldType> datasetFieldTypeMap = roCrateServiceBean.getDatasetFieldTypeMapByConformsTo(roCrate);
                createOrUpdate(roCrate, version, false, datasetFieldTypeMap);
            }
            processRoCrateFiles(roCrate, version.getFileMetadatas(), importMapping);
            // If the rocrate is generated right after an rocrate zip has been uploaded, make sure we put back the
            // file and sub-dataset related metadata to the generated metadata from the uploaded ro-crate-metadata.json.
            // roCrate = roCrateUploadServiceBean.addUploadedFileMetadata(roCrate);
            RoCrateOpLog.saveCrate(roCrate, roCrateFolderPath);
            // If rocrate is saved, then we can reset the upload state, so that subsequent calls to
            // addUploadedFileMetadata would do nothing.

            // Make sure we have a released version rocrate even for older datasets where we didn't sync rocrate
            // from the beginning
            var released = dataset.getReleasedVersion();
            if (released != null) {
                var releasedVersion = released.getFriendlyVersionNumber();
                var releasedPath = roCrateServiceBean.getRoCratePath(released);
                if (!Files.exists(Paths.get(releasedPath))) {
                    t.extra("copiedReleasedVersion", releasedVersion);
                    saveRoCrateVersion(dataset, releasedVersion, false);
                }
            }
        } catch (Exception e) {
            t.fail(e);
            throw e;
        } finally {
            t.close();
        }
    }

    private void processPrimitiveFieldType(RoCrate roCrate, RoCrate.RoCrateBuilder roCrateContextUpdater, 
                                           RootDataEntity rootDataEntity, DatasetField datasetField, String fieldName, 
                                           String fieldUri, ObjectMapper mapper, boolean isCreation,
                                           ArrayList<String> urlIdsToRemove) throws JsonProcessingException {
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
                        // save the old id that will be removed
                        var prop = rootDataEntity.getProperty(fieldName);
                        if (prop != null) {
                            // have to handle the "old" string values too
                            if (prop.isTextual()) {
                                urlIdsToRemove.add(prop.textValue());
                            } else {
                                urlIdsToRemove.add(prop.get("@id").textValue());
                            }
                        }
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
                    ArrayList<String> existingUrls = new ArrayList<>();
                    var parentObj = rootDataEntity.getProperty(fieldName);
                    if (parentObj != null) {
                        if (parentObj.isArray()) {
                            parentObj.elements().forEachRemaining(idObj -> {
                                if (idObj.isTextual()) {
                                    existingUrls.add(idObj.textValue());
                                } else {
                                    existingUrls.add(idObj.get("@id").textValue());
                                }
                            });
                        } else {
                            if (parentObj.isTextual()) {
                                existingUrls.add(parentObj.textValue());
                            } else {
                                existingUrls.add(parentObj.get("@id").textValue());
                            }
                        }
                    }
                    for (var fieldValue : fieldValues) {
                        var urlString = fieldValue.getValue();
                        if (!existingUrls.contains(urlString)) {
                            addUrlContextualEntity(roCrate, fieldValue.getValue());
                        } else {
                            existingUrls.remove(urlString);
                        }
                        var idObj = mapper.createObjectNode();
                        idObj.put("@id", fieldValue.getValue());
                        valuesNode.add(idObj);
                    }
                    urlIdsToRemove.addAll(existingUrls);
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
    
    private void processCompoundFieldType(RoCrate roCrate, RoCrate.RoCrateBuilder roCrateContextUpdater, 
                                          RootDataEntity rootDataEntity, DatasetField datasetField, String fieldName, 
                                          String fieldUri, ObjectMapper mapper, boolean isCreation, 
                                          ArrayList<String> urlIdsToRemove) throws JsonProcessingException {
        List<DatasetFieldCompoundValue> compoundValues = datasetField.getDatasetFieldCompoundValues();
        Set<ContextualEntity> contextualEntities = roCrate.getAllContextualEntities();

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
                List<String> compoundValueIds = compoundValues.stream().map(compoundValue -> compoundValue.getId().toString()).toList();
                for (Iterator<JsonNode> it = entityToUpdate.elements(); it.hasNext();) {
                    JsonNode entity = it.next();
                    String rootEntityId = entity.get("@id").textValue();
                    if (!compoundValueIds.contains(roCrateServiceBean.getCompoundIdFromRoId(rootEntityId))) {
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

            processCompoundValues(roCrate, roCrateContextUpdater, compoundValues, contextualEntities, entityToUpdate, rootDataEntity, datasetField, fieldName, fieldUri, mapper, isCreation, urlIdsToRemove);
        }
    }

    private void processCompoundValues(RoCrate roCrate, RoCrate.RoCrateBuilder roCrateContextUpdater, 
                                       List<DatasetFieldCompoundValue> compoundValues, 
                                       Set<ContextualEntity> contextualEntities, 
                                       JsonNode entityToUpdate, RootDataEntity rootDataEntity, 
                                       DatasetField datasetField, String fieldName, String fieldUri, 
                                       ObjectMapper mapper, boolean isCreation, ArrayList<String> urlIdsToRemove) throws JsonProcessingException {
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
                    if (roCrateServiceBean.getCompoundIdFromRoId(idObj.get("@id").textValue()).equals(compoundValue.getId().toString())) {
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
                            // Save the "old" urls from the RO-Crate, so when the values are updated
                            // which means, the new ids from DV are added to the RO-Crate
                            // and the original values are overridden, we can later remove the corresponding entities
                            // with the replaced id-s (urls)
                            ArrayList<String> existingUrls = new ArrayList<>();
                            if (childFieldType.getFieldType().equals(DatasetFieldType.FieldType.URL)) {
                                var parentObj = actEntityToUpdate.getProperty(childFieldName);
                                if (parentObj != null) {
                                    if (parentObj.isArray()) {
                                        parentObj.elements().forEachRemaining(idObj -> {
                                            // Handle the "old" string values too
                                            if (idObj.isTextual()) {
                                                existingUrls.add(idObj.textValue());
                                            } else {
                                                existingUrls.add(idObj.get("@id").textValue());
                                            }
                                        });
                                    } else {
                                        if (parentObj.isTextual()) {
                                            existingUrls.add(parentObj.textValue());
                                        } else {
                                            existingUrls.add(parentObj.get("@id").textValue());
                                        }
                                    }
                                }
                            }
                            // TODO: for now DV wont allow multiple URLs for compound fields
                            // later if that is fixed, the process should be fixed as it works for the primitive field types
                            for (var childFieldValue : childFieldValues) {
                                if (childFieldType.getFieldType().equals(DatasetFieldType.FieldType.URL)) {
                                    var urlString = childFieldValue.getValue();
                                    if (!existingUrls.contains(urlString)) {
                                        addUrlContextualEntity(roCrate, urlString);
                                        var idObj = mapper.createObjectNode();
                                        idObj.put("@id", urlString);
                                        actEntityToUpdate.addProperty(childFieldName, idObj);
                                    } else {
                                        existingUrls.remove(urlString);
                                    }
                                } else {
                                    actEntityToUpdate.addProperty(childFieldName, childFieldValue.getValue());
                                }
                            }
                            // We need to remove the entities with the old id-s (urls),
                            // since in DV there is no more data saved to a URL than its value, which is a string
                            // so we have to find the corresponding URL entity by its id and remove it from the crate
                            urlIdsToRemove.addAll(existingUrls);
                        }
                        // Update RO-Crate entity name based on new compound value. NOTE: the update is coming from
                        // Dataverse (API or UI) so the user cannot control the Ro-Crate name property, we have to
                        // do it automatically.
                        String presentName = actEntityToUpdate.getProperty("name").isTextual() ? actEntityToUpdate.getProperty("name").textValue() : null;
                        if (presentName == null || presentName.isBlank()) {
                            String entityName = calcRoCrateEntityName(compoundValue, datasetField);
                            if (entityName != null) {
                                actEntityToUpdate.addProperty("name", entityName);
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
        ContextualEntity.ContextualEntityBuilder contextualEntityBuilder = new ContextualEntity.ContextualEntityBuilder();
        buildNewContextualEntity(roCrate, roCrateContextUpdater, contextualEntityBuilder, compoundValue, mapper, parentFieldName, parentFieldUri, isCreation);

        // The hashmark before the uuid is required in AROMA
        // "@id's starting with # as these signify the reference is internal to the crate"
        // the compoundIdAndUuidSeparator separates the id of the parent compound value from the uuid
        // the parent compound id is used to get the correct values upon modifying the RO-Crate with data from AROMA
        contextualEntityBuilder.setId(roCrateServiceBean.createRoIdForCompound(compoundValue));

        String nameFieldValue = calcRoCrateEntityName(compoundValue, parentField);
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

    public String calcRoCrateEntityName(
            DatasetFieldCompoundValue compoundValue,
            DatasetField parentField
    )
    {
        DatasetFieldType parentFieldType = parentField.getDatasetFieldType();
        DatasetFieldTypeArp dsfArp = arpMetadataBlockServiceBean.findDatasetFieldTypeArpForFieldType(parentFieldType);

        String nameFieldValue = null;

        // If compound value has a "name" field, use its value by default
        var nameField = compoundValue.getChildDatasetFields().stream()
                .filter(datasetField -> datasetField.getDatasetFieldType().getName().equals("name"))
                .findFirst();
        if (nameField.isPresent()) {
            // No need to set anything, since the context entity will already have a "name" field with a value
        }
        // Find an explicit displayNameField set in dsfArp
        else if (dsfArp != null && dsfArp.getDisplayNameField() != null) {
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
            nameFieldValue = roCrateNameProvider.generateRoCrateName(compoundValue);
        }

        return nameFieldValue;
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
                        if (childFieldType.getFieldType().equals(DatasetFieldType.FieldType.URL)) {
                            var urlString = childFieldValue.getValue();
                            addUrlContextualEntity(roCrate, urlString);
                            var idObj = mapper.createObjectNode();
                            idObj.put("@id", urlString);
                            contextualEntityBuilder.addProperty(childFieldName, idObj);
                        } else {
                            contextualEntityBuilder.addProperty(childFieldName, childFieldValue.getValue());
                        }
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

    private void removeDeletedEntities(RoCrate roCrate, DatasetVersion version, List<DatasetField> datasetFields, Map<String, DatasetFieldType> datasetFieldTypeMap) {
        List<String> fieldNames = datasetFields.stream().map(dsf -> dsf.getDatasetFieldType().getName()).collect(Collectors.toList());
        List<String> removedEntityNames = new ArrayList<>();
        RootDataEntity rootDataEntity = roCrate.getRootDataEntity();


        // First, remove the values from the RO-Crate that had been removed from DV and do not contain any additional props from AROMA,
        // even if the entity was removed in DV, if it contains any other props from AROMA, it will not be removed from the RO-Crate,
        // but it will not be displayed in DV
        rootDataEntity.getProperties().fieldNames().forEachRemaining(entityName -> {
            if (!entityName.startsWith("@") && !roCrateServiceBean.propsToIgnore.contains(entityName) && !fieldNames.contains(entityName)) {
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

    public String getDatePublishedForRoCrate(DatasetVersion version) {
        // Take either release, publication or lst update. For published version it must be always the released date
        Date publishedDate = null;
        DateTimeFormatter formatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
        if (version.getReleaseTime() != null) {
            publishedDate = version.getReleaseTime();
        } else if (version.getDataset().getPublicationDate() != null) {
            publishedDate = version.getDataset().getPublicationDate();
        } else {
            publishedDate = version.getLastUpdateTime();
        }
        ZonedDateTime zonedDateTime = publishedDate.toInstant().atZone(ZoneId.systemDefault());
        OffsetDateTime offsetDateTime = zonedDateTime.toOffsetDateTime();
        return offsetDateTime.format(formatter);
    }

    public void processRoCrateFiles(RoCrate roCrate, List<FileMetadata> fileMetadatas, Map<String, String> importMapping) throws JsonProcessingException {
        var t = RoCrateOpLog.start("export.processFiles");
        try {
            if (fileMetadatas != null) {
                t.extra("fileCount", fileMetadatas.size());
            }
            ObjectMapper mapper = new ObjectMapper();
        List<FileMetadata> datasetFiles = fileMetadatas.stream().map(FileMetadata::createCopy).collect(Collectors.toList());
        List<ObjectNode> roCrateFileEntities = Stream.concat(
                roCrate.getAllContextualEntities().stream().map(AbstractEntity::getProperties).filter(ce -> roCrateServiceBean.getTypeAsString(ce).equals("File")),
                roCrate.getAllDataEntities().stream().map(AbstractEntity::getProperties).filter(de -> roCrateServiceBean.getTypeAsString(de).equals("File"))
        ).toList();

        List<ObjectNode> roCrateDatasetEntities = Stream.concat(
                roCrate.getAllContextualEntities().stream().map(AbstractEntity::getProperties).filter(ce -> roCrateServiceBean.getTypeAsString(ce).equals("Dataset")),
                roCrate.getAllDataEntities().stream().map(AbstractEntity::getProperties).filter(de -> roCrateServiceBean.getTypeAsString(de).equals("Dataset"))
        ).toList();

        // Delete the entities from the RO-CRATE that have been removed from DV
        roCrateFileEntities.forEach(fe -> {
            String entityId = fe.get("@id").textValue();
            Optional<FileMetadata> datasetFile = findMatchingDatasetFile(fe, datasetFiles, importMapping);

            if (datasetFile.isPresent()) {
                var fmd = datasetFile.get();
                if (roCrateServiceBean.isVirtualFile(fe)) {
                    String newFileId = roCrateServiceBean.createRoIdForDataFile(fmd.getDataFile());
                    replaceFileIdInHasParts(roCrate, entityId, newFileId);
                    fe.put("@id", newFileId);
                }
                fe.put("name", fmd.getLabel());
                if (fmd.getDescription() != null) {
                    fe.put("description", fmd.getDescription());
                }
                var fmdDirLabel = fmd.getDirectoryLabel();
                var feDirLabel = fe.has("directoryLabel") ? fe.get("directoryLabel").textValue() : null;
                if (!directoryLabelsMatch(fmdDirLabel, feDirLabel)) {
                    fe.put("directoryLabel", fmd.getDirectoryLabel());
                    processModifiedPath(roCrate, feDirLabel, fmdDirLabel, fe.get("@id").textValue());
                }

                fe.set("tags", mapper.valueToTree(fmd.getCategoriesByName()));
                var globalId = fmd.getDataFile().getGlobalId();
                fe.put("@arpPid", globalId != null ? globalId.toString() : "");
                fe.put("hash", fmd.getDataFile().getChecksumValue());
                fe.put("contentSize", String.valueOf(fmd.getDataFile().getFilesize()));
                String matchedDataFileId = fmd.getDataFile().getId().toString();
                datasetFiles.removeIf(fileMetadata -> Objects.equals(matchedDataFileId, fileMetadata.getDataFile().getId().toString()));
            } else if (importMapping == null && roCrateServiceBean.isVirtualFile(fe)) {
                // AROMA-only virtual files are kept when not reconciling an upload.
                return;
            } else {
                removeChildContextualEntities(roCrate, entityId);
                roCrate.deleteEntityById(entityId);
            }
        });

        if (!roCrateFileEntities.isEmpty() || !datasetFiles.isEmpty()) {
            RoCrate.RoCrateBuilder roCrateContextUpdater = new RoCrate.RoCrateBuilder(roCrate);
            roCrateContextUpdater.addValuePairToContext("hasPart", "https://schema.org/hasPart");
            roCrateContextUpdater.addValuePairToContext("directoryLabel", "https://dataverse.org/schema/file/directoryLabel");
            roCrateContextUpdater.addValuePairToContext("tags", RoCrateServiceBean.FILE_TAGS_CONTEXT_URI);
            var fileClassEn = arpServiceBean.getFileClassEn();
            if (fileClassEn != null) {
                fileClassEn.getAsJsonArray("inputs").forEach(
                        input -> roCrateContextUpdater.addValuePairToContext(
                                input.getAsJsonObject().get("name").getAsString(),
                                input.getAsJsonObject().get("id").getAsString()
                        )
                );
            }
        }
        
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
        } catch (RuntimeException e) {
            t.fail(e);
            throw e;
        } finally {
            t.close();
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

    private Optional<FileMetadata> findMatchingDatasetFile(ObjectNode fileEntity, List<FileMetadata> datasetFiles,
            Map<String, String> importMapping) {
        String entityId = fileEntity.get("@id").textValue();

        if (importMapping != null && !importMapping.isEmpty()) {
            String storageIdentifier = importMapping.get(entityId);
            if (storageIdentifier != null) {
                Optional<FileMetadata> mappedFile = datasetFiles.stream()
                        .filter(fileMetadata -> Objects.equals(fileMetadata.getDataFile().getStorageIdentifier(), storageIdentifier))
                        .findFirst();
                if (mappedFile.isPresent()) {
                    return mappedFile;
                }
            }
        }

        if (!roCrateServiceBean.isVirtualFile(fileEntity)) {
            String dataFileId = roCrateServiceBean.getDataFileIdFromRoId(entityId);
            return datasetFiles.stream()
                    .filter(fileMetadata -> Objects.equals(dataFileId, fileMetadata.getDataFile().getId().toString()))
                    .findFirst();
        }

        return findVirtualFileMatch(fileEntity, datasetFiles);
    }

    private Optional<FileMetadata> findVirtualFileMatch(ObjectNode fileEntity, List<FileMetadata> datasetFiles) {
        if (!fileEntity.has("hash")) {
            return Optional.empty();
        }

        String hash = fileEntity.get("hash").textValue();
        String name = fileEntity.has("name") ? fileEntity.get("name").textValue() : null;
        String directoryLabel = fileEntity.has("directoryLabel") ? fileEntity.get("directoryLabel").textValue() : null;

        List<FileMetadata> hashMatches = datasetFiles.stream()
                .filter(fileMetadata -> Objects.equals(hash, fileMetadata.getDataFile().getChecksumValue()))
                .collect(Collectors.toList());

        if (hashMatches.isEmpty()) {
            return Optional.empty();
        }
        if (hashMatches.size() == 1) {
            return Optional.of(hashMatches.get(0));
        }

        return hashMatches.stream()
                .filter(fileMetadata -> Objects.equals(name, fileMetadata.getLabel())
                        && directoryLabelsMatch(fileMetadata.getDirectoryLabel(), directoryLabel))
                .findFirst();
    }

    private boolean directoryLabelsMatch(String directoryLabel, String otherDirectoryLabel) {
        return Objects.equals(
                RoCrateServiceBean.normalizeDirectoryLabel(directoryLabel),
                RoCrateServiceBean.normalizeDirectoryLabel(otherDirectoryLabel)
        );
    }

    private void replaceFileIdInHasParts(RoCrate roCrate, String oldFileId, String newFileId) {
        if (roCrate.getRootDataEntity().hasPart.removeIf(id -> id.equals(oldFileId))) {
            roCrate.getRootDataEntity().hasPart.add(newFileId);
        }

        roCrate.getAllContextualEntities().stream().map(AbstractEntity::getProperties)
                .filter(ce -> roCrateServiceBean.getTypeAsString(ce).equals("Dataset"))
                .forEach(ds -> {
                    if (ds.has("hasPart")) {
                        replaceId(ds.get("hasPart"), oldFileId, newFileId);
                    }
                });
        // This part might be removed in the future, but as of now the lib sometimes mixes the contextual/file entities
        roCrate.getAllDataEntities().stream().map(AbstractEntity::getProperties)
                .filter(de -> roCrateServiceBean.getTypeAsString(de).equals("Dataset"))
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


    //Add new dataset for every folder in the given folderNames list, and returns the id of the folder that contains the file
    private String createDatasetsFromFoldersAndReturnParentId(RoCrate roCrate, JsonNode parentObj, List<String> folderNames, ObjectMapper mapper) {
        String folderName = folderNames.remove(0);
        String newDatasetId = getDatasetIdIfAlreadyPresent(roCrate, parentObj, folderName);

        if (newDatasetId == null) {
            //add new dataset
            // Make it end with "/" to conform to Describo, which requires Dataset id-s to end in '/'
            // although this is is just a SHOULD not a MUST by the spec.
            newDatasetId = roCrateServiceBean.createRoIdForDataset(folderName, parentObj);
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

    private String getDatasetIdIfAlreadyPresent(RoCrate roCrate, JsonNode parentObj, String folderName) {
        String datasetId = null;
        if (parentObj.get("@id").textValue().equals("./")) {
            List<ObjectNode> roCrateDatasetEntities = Stream.concat(
                    roCrate.getAllContextualEntities().stream().map(AbstractEntity::getProperties).filter(ce -> roCrateServiceBean.getTypeAsString(ce).equals("Dataset")),
                    roCrate.getAllDataEntities().stream().map(AbstractEntity::getProperties).filter(de -> roCrateServiceBean.getTypeAsString(de).equals("Dataset"))
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
                                && node.has("@type") && roCrateServiceBean.getTypeAsString(node).equals("Dataset")) {
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
                                        && node.has("@type") && roCrateServiceBean.getTypeAsString(node).equals("Dataset")) {
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

    private String addFileEntity(RoCrate roCrate, FileMetadata fileMetadata, boolean toHasPart) {
        String fileName = fileMetadata.getLabel();
        String fileId;
        FileEntity.FileEntityBuilder fileEntityBuilder = new FileEntity.FileEntityBuilder();
        DataFile dataFile = fileMetadata.getDataFile();
        fileId = roCrateServiceBean.createRoIdForDataFile(dataFile);
        fileEntityBuilder.setId(fileId);
        var globalId = dataFile.getGlobalId();
        fileEntityBuilder.addProperty("@arpPid", (globalId != null ? globalId.toString() : ""));
        fileEntityBuilder.addProperty("hash", dataFile.getChecksumValue());
        fileEntityBuilder.addProperty("name", fileName);
        fileEntityBuilder.addProperty("contentSize", String.valueOf(dataFile.getFilesize()));
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
        roCrate.addDataEntity(file);
        if (!toHasPart) {
            roCrate.getRootDataEntity().removeFromHasPart(file.getId());
        }

        return fileId;
    }

    public void deleteEmptyDatasets(RoCrate roCrate) {
        List<String> emptyDatasetIds = Stream.concat(
                roCrate.getAllContextualEntities().stream().map(AbstractEntity::getProperties).filter(ce ->
                        roCrateServiceBean.getTypeAsString(ce).equals("Dataset") && hasPartIsEmpty(ce)),
                roCrate.getAllDataEntities().stream().map(AbstractEntity::getProperties).filter(de ->
                        roCrateServiceBean.getTypeAsString(de).equals("Dataset") && hasPartIsEmpty(de))
        ).map(entity -> entity.get("@id").textValue()).collect(Collectors.toList());

        if (!emptyDatasetIds.isEmpty()) {
            emptyDatasetIds.forEach(dsId -> {
                removeChildContextualEntities(roCrate, dsId);
                roCrate.deleteEntityById(dsId);
            });
            deleteEmptyDatasets(roCrate);
        }
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

    public void deleteDraftVersion(DatasetVersion datasetVersion) throws IOException {
        String draftPath = roCrateServiceBean.getRoCrateFolder(datasetVersion);
        String latestPublishedPath = roCrateServiceBean.getRoCrateFolder(datasetVersion.getDataset().getLatestVersionForCopy());
        copyRoCrateMetadataFiles(latestPublishedPath, draftPath);
    }

    // TODO: when this is called from releaseDataset the dataset won't actually be released yet, so we won't
    // have a releaseTime, so getDatePublishedForRoCrate() will fall back to the current, which will be a bit
    // different then the actual releaseDate at the end.
    public void saveRoCrateVersion(Dataset dataset, boolean isUpdate, boolean isMinor) throws IOException {
        String versionNumber = isUpdate ? dataset.getLatestVersionForCopy().getFriendlyVersionNumber() : isMinor ? dataset.getNextMinorVersionString() : dataset.getNextMajorVersionString();
        saveRoCrateVersion(dataset, versionNumber, isUpdate);
//        String roCrateFolderPath = getRoCrateFolder(dataset.getLatestVersion());
//        FileUtils.copyDirectory(new File(roCrateFolderPath), new File(roCrateFolderPath + "_v" + versionNumber));
    }


    public void saveRoCrateVersion(Dataset dataset, String versionNumber, boolean isUpdate) throws IOException {
        try (var t = RoCrateOpLog.start("export.saveVersion", dataset)
                .extra("versionNumber", versionNumber)
                .extra("update", isUpdate)) {
            try {
                String roCrateFolderPath = roCrateServiceBean.getRoCrateFolder(dataset.getLatestVersion());
                String destFolderPath;
                String versionSuffix = "_v" + versionNumber;
                if (isUpdate && roCrateFolderPath.endsWith(versionSuffix)) {
                    destFolderPath = roCrateFolderPath;
                    roCrateFolderPath = roCrateServiceBean.getDraftRoCrateFolder(dataset);
                } else {
                    destFolderPath = roCrateFolderPath + versionSuffix;
                }
                copyRoCrateMetadataFiles(roCrateFolderPath, destFolderPath);
            } catch (IOException | RuntimeException e) {
                t.fail(e);
                throw e;
            }
        }
    }

    public void saveRoCrateDraftVersion(DatasetVersion version) throws IOException {
        String roCrateFolderPath = roCrateServiceBean.getRoCrateFolder(version);
        String localDir = StorageUtils.getLocalRoCrateDir(version.getDataset());
        var draftPath = String.join(File.separator, localDir, "ro-crate-metadata");
        copyRoCrateMetadataFiles(roCrateFolderPath, draftPath);
    }

    /**
     * Copies only the RO-Crate metadata JSON and HTML preview. Avoids a full
     * directory walk on s3fs.
     */
    void copyRoCrateMetadataFiles(String sourceFolder, String destFolder) throws IOException {
        if (sourceFolder.equals(destFolder)) {
            return;
        }
        try (var t = RoCrateOpLog.startIo("copy").extra("from", sourceFolder).extra("to", destFolder)) {
            try {
                Path src = Paths.get(sourceFolder);
                Path dest = Paths.get(destFolder);
                Files.createDirectories(dest);
                copyIfPresent(src.resolve(ArpServiceBean.RO_CRATE_METADATA_JSON_NAME),
                        dest.resolve(ArpServiceBean.RO_CRATE_METADATA_JSON_NAME));
                copyIfPresent(src.resolve(ArpServiceBean.RO_CRATE_PREVIEW_HTML_NAME),
                        dest.resolve(ArpServiceBean.RO_CRATE_PREVIEW_HTML_NAME));
            } catch (IOException | RuntimeException e) {
                t.fail(e);
                throw e;
            }
        }
    }

    private void copyIfPresent(Path source, Path dest) throws IOException {
        try {
            Files.copy(source, dest, StandardCopyOption.REPLACE_EXISTING);
        } catch (java.nio.file.NoSuchFileException e) {
            // Preview HTML is optional when copying an incomplete crate folder.
        }
    }

    public void updateRoCrateFileMetadatas(Dataset dataset) throws IOException {
        try (var t = RoCrateOpLog.start("export.updateFileMetadatas", dataset)) {
            try {
                RoCrate ro = RoCrateOpLog.readCrateFolder(roCrateServiceBean.getRoCrateFolder(dataset.getLatestVersion()));
                RoCrate roCrate = new RoCrate.RoCrateBuilder(ro).setPreview(new AutomaticPreview()).build();
                processRoCrateFiles(roCrate, dataset.getLatestVersion().getFileMetadatas(), null);
                RoCrateOpLog.saveCrate(roCrate, roCrateServiceBean.getRoCrateFolder(dataset.getLatestVersion()));
            } catch (IOException | RuntimeException e) {
                t.fail(e);
                throw e;
            }
        }
    }

    public void updateRoCrateFileMetadataAfterIngest(List<Long> fileIds) throws IOException {
        try (var t = RoCrateOpLog.start("export.updateAfterIngest").extra("fileCount", fileIds == null ? 0 : fileIds.size())) {
            try {
                HashSet<Long> parentDsIds = new HashSet<>();
                // Collect the dataset and the belonging file ids, so we no longer need to assume that every ingest message
                // contains files that belong to the same dataset as it is assumed here: edu/harvard/iq/dataverse/ingest/IngestMessageBean.java
                fileIds.forEach(fileId -> {
                    var dsId = datafileService.findCheapAndEasy(fileId).getOwner().getId();
                    parentDsIds.add(dsId);
                });
                for (Long dsId : parentDsIds) {
                    var datasetVersion = datasetService.find(dsId).getLatestVersion();
                    t.dataset(datasetVersion);
                    RoCrate ro = RoCrateOpLog.readCrateFolder(roCrateServiceBean.getRoCrateFolder(datasetVersion));
                    RoCrate roCrate = new RoCrate.RoCrateBuilder(ro).setPreview(new AutomaticPreview()).build();
                    updateFileMetadataAfterIngest(roCrate, datasetVersion);
                    RoCrateOpLog.saveCrate(roCrate, roCrateServiceBean.getRoCrateFolder(datasetVersion));
                }
            } catch (IOException | RuntimeException e) {
                t.fail(e);
                throw e;
            }
        }
    }

    private void updateFileMetadataAfterIngest(RoCrate roCrate, DatasetVersion datasetVersion) {
        List<ObjectNode> roCrateFileEntities = Stream.concat(
                roCrate.getAllContextualEntities().stream().map(AbstractEntity::getProperties).filter(ce -> roCrateServiceBean.getTypeAsString(ce).equals("File")),
                roCrate.getAllDataEntities().stream().map(AbstractEntity::getProperties).filter(de -> roCrateServiceBean.getTypeAsString(de).equals("File"))
        ).toList();
        List<FileMetadata> datasetFiles = datasetVersion.getFileMetadatas();

        roCrateFileEntities.forEach(fe -> {
            String dataFileId = roCrateServiceBean.getDataFileIdFromRoId(fe.get("@id").textValue());
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

    public void removeDatasetContactEmail(RoCrate roCrate) {
        //remove the datasetContactEmail from the RO-Crate upon publishing the Dataset
        roCrate.getAllContextualEntities().stream().filter(ce ->
                        roCrateServiceBean.getTypeAsString(ce.getProperties()).equals("datasetContact"))
                .forEach(contextualEntity -> contextualEntity.getProperties().remove("datasetContactEmail"));
    }

    /*
    Upon the dataset's successful publication, removes any sensitive data from the RO-Crate and updates the publicationDate
    */
    public void finalizeRoCrateForDatasetVersion(DatasetVersion datasetVersion) {
        var t = RoCrateOpLog.start("export.finalizeVersion", datasetVersion);
        try {
            RoCrate ro = null;
            String roCratePath = roCrateServiceBean.getRoCratePath(datasetVersion);
            try {
                if (!Files.exists(Paths.get(roCratePath))) {
                    createOrUpdateRoCrate(datasetVersion);
    //                if (datasetVersion.getDataset().getLatestVersion().isPublished()) {
    //                    saveRoCrateDraftVersion(datasetVersion);
    //                }
                }
                ro = RoCrateOpLog.readCrateFolder(roCrateServiceBean.getRoCrateFolder(datasetVersion));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            RoCrate roCrateWithPreview = new RoCrate.RoCrateBuilder(ro).setPreview(new AutomaticPreview()).build();
            String roCrateFolderPath = roCrateServiceBean.getRoCrateFolder(datasetVersion);

            removeDatasetContactEmail(roCrateWithPreview);
            updateDatePublishedInRoCrate(roCrateWithPreview, getDatePublishedForRoCrate(datasetVersion));

            try {
                RoCrateOpLog.saveCrate(roCrateWithPreview, roCrateFolderPath);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            updateDatePublishedInDraftRoCrate(datasetVersion.getDataset());
        } catch (RuntimeException e) {
            t.fail(e);
            throw e;
        } finally {
            t.close();
        }
    }

    /**
     * Updates {@code datePublished} on the draft crate only. Contact emails stay
     * on the draft; they are stripped from the published crate separately.
     */
    public void updateDatePublishedInDraftRoCrate(Dataset dataset) {
        String draftFolderPath = roCrateServiceBean.getDraftRoCrateFolder(dataset);
        Path draftJson = Paths.get(draftFolderPath, ArpServiceBean.RO_CRATE_METADATA_JSON_NAME);
        if (!Files.exists(draftJson)) {
            return;
        }
        RoCrate roCrate;
        try {
            roCrate = RoCrateOpLog.readCrateFolder(draftFolderPath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        RoCrate roCrateWithPreview = new RoCrate.RoCrateBuilder(roCrate).setPreview(new AutomaticPreview()).build();
        updateDatePublishedInRoCrate(roCrateWithPreview, getDatePublishedForRoCrate(dataset.getLatestVersion()));
        try {
            RoCrateOpLog.saveCrate(roCrateWithPreview, draftFolderPath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void finalizeRoCrateForPublish(DatasetVersion datasetVersion) {
        var t = RoCrateOpLog.start("export.finalizePublish", datasetVersion);
        try {
            // Finalize as usual
            if (!datasetVersion.isDraft()) {
                finalizeRoCrateForDatasetVersion(datasetVersion);
            }

            // If we have local access to the KG (as in case of a demo install) ingest the published
            // dataset right away for instant findability
            // cd /var/lib/arp-kg/harvest; ingest.py demo.ini <roCratePath>
            String ingestCommand = arpConfig.get("arp.kg.ingestCommand");

            if (ingestCommand != null) {
                var roCratePath = roCrateServiceBean.getRoCratePath(datasetVersion);
                var fullCommand = ingestCommand + " '" + roCratePath + "'";

                try (var ingestTimer = RoCrateOpLog.start("export.kgIngest", datasetVersion)) {
                    try {
                        logger.info("Executing KG ingest command after publish: " + fullCommand);

                        // Split command for ProcessBuilder if needed
                        List<String> command = Arrays.asList("bash", "-c", fullCommand);

                        ProcessBuilder processBuilder = new ProcessBuilder(command);
                        processBuilder.redirectErrorStream(true); // Redirect stderr to stdout

                        Process process = processBuilder.start();

                        // Read the process output
                        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                            String line;
                            while ((line = reader.readLine()) != null) {
                                logger.info(line); // Log the command output
                            }
                        }

                        int exitCode = process.waitFor();
                        ingestTimer.extra("exitCode", exitCode);
                        if (exitCode != 0) {
                            ingestTimer.fail(new RuntimeException("Ingest command failed with exit code: " + exitCode));
                            logger.severe("Ingest command failed with exit code: " + exitCode);
                        } else {
                            logger.info("Ingest command executed successfully.");
                        }
                    } catch (IOException | InterruptedException e) {
                        ingestTimer.fail(e);
                        logger.severe("Error executing ingest command: " + e.getMessage());
                        Thread.currentThread().interrupt(); // Restore interrupt status if interrupted
                    }
                }
            }
        } catch (RuntimeException e) {
            t.fail(e);
            throw e;
        } finally {
            t.close();
        }
    }

    public void updateDatePublishedInRoCrate(RoCrate roCrate, String datePublished) {
        // Set datePublished
        RootDataEntity rootDataEntity = roCrate.getRootDataEntity();
        var props = rootDataEntity.getProperties();
        props.put("datePublished", datePublished);
    }

    public void saveUploadedRoCrate(Dataset dataset, String roCrateJsonString) throws IOException {
        try (var t = RoCrateOpLog.start("export.saveUploaded", dataset)) {
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                String roCratePath = roCrateServiceBean.getRoCratePath(dataset.getLatestVersion());
                JsonNode parsedRoCrate = objectMapper.readTree(roCrateJsonString);
                File roCrate = new File(roCratePath);

                if (!roCrate.getParentFile().exists()) {
                    if (!roCrate.getParentFile().mkdirs()) {
                        throw new RuntimeException("Failed to save uploaded RO-CRATE for dataset: " + dataset.getIdentifierForFileStorage());
                    }
                }
                try (var writeTimer = RoCrateOpLog.startIo("write.json", dataset).extra("path", roCratePath)) {
                    try {
                        objectMapper.writerWithDefaultPrettyPrinter().writeValue(new File(roCratePath), parsedRoCrate);
                    } catch (IOException | RuntimeException e) {
                        writeTimer.fail(e);
                        throw e;
                    }
                }
            } catch (IOException | RuntimeException e) {
                t.fail(e);
                throw e;
            }
        }
    }
}
