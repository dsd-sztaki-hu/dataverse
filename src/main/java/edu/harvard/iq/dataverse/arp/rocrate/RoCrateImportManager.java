package edu.harvard.iq.dataverse.arp.rocrate;

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
import edu.harvard.iq.dataverse.util.JsfHelper;
import edu.kit.datamanager.ro_crate.RoCrate;
import edu.kit.datamanager.ro_crate.entities.AbstractEntity;
import edu.kit.datamanager.ro_crate.entities.contextual.ContextualEntity;
import edu.kit.datamanager.ro_crate.entities.data.RootDataEntity;
import edu.kit.datamanager.ro_crate.preview.AutomaticPreview;
import edu.kit.datamanager.ro_crate.reader.FolderReader;
import edu.kit.datamanager.ro_crate.reader.RoCrateReader;
import edu.kit.datamanager.ro_crate.reader.StringReader;
import edu.kit.datamanager.ro_crate.writer.FolderWriter;
import edu.kit.datamanager.ro_crate.writer.RoCrateWriter;
import jakarta.ejb.EJB;
import jakarta.ejb.Stateless;
import jakarta.inject.Named;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static edu.harvard.iq.dataverse.DatasetField.createNewEmptyDatasetField;
import static edu.harvard.iq.dataverse.DatasetFieldCompoundValue.createNewEmptyDatasetFieldCompoundValue;
import static edu.harvard.iq.dataverse.arp.ArpServiceBean.RO_CRATE_EXTRAS_JSON_NAME;
import static edu.harvard.iq.dataverse.validation.EMailValidator.isEmailValid;
import static edu.harvard.iq.dataverse.validation.URLValidator.isURLValid;

@Stateless
@Named
public class RoCrateImportManager {

    @EJB
    DatasetFieldServiceBean fieldService;

    @EJB
    ArpMetadataBlockServiceBean arpMetadataBlockServiceBean;

    @EJB
    ControlledVocabularyValueServiceBean controlledVocabularyValueService;
    
    @EJB
    RoCrateServiceBean roCrateServiceBean;

    private final List<String> dataverseFileProps = List.of("@id", "@type", "name", "contentSize", "encodingFormat", "directoryLabel", "description", "identifier", "@arpPid", "hash");
    private final List<String> dataverseDatasetProps = List.of("@id", "@type", "name", "hasPart");
    
    public RoCrateImportManager() {}
    
    public RoCrateImportManager(DatasetFieldServiceBean fieldService) { this.fieldService = fieldService; }

    // Handles the importing of the RO-Crates (mainly sent by AROMA)
    public void importRoCrate(RoCrate roCrate, DatasetVersion updatedVersion) {
        Map<String, DatasetFieldType> datasetFieldTypeMap = roCrateServiceBean.getDatasetFieldTypeMapByConformsTo(roCrate);
        RootDataEntity rootDataEntity = roCrate.getRootDataEntity();
        Map<String, ContextualEntity> contextualEntityHashMap = roCrate.getAllContextualEntities().stream().collect(Collectors.toMap(ContextualEntity::getId, Function.identity()));

        var fieldsIterator = rootDataEntity.getProperties().fields();
        while (fieldsIterator.hasNext()) {
            var field = fieldsIterator.next();
            String fieldName = field.getKey();
            JsonNode filedValue = field.getValue();
            if (!fieldName.startsWith("@") && !roCrateServiceBean.propsToIgnore.contains(fieldName)) {
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
                if (!fieldName.startsWith("@") && !roCrateServiceBean.propsToIgnore.contains(fieldName)) {
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
                                if (roCrateServiceBean.hasType(linkedObj, "URL")) {
                                    childDatasetField.setSingleValue(linkedObj.get("name").textValue());
                                }
                            } else {
                                String valueToSet;
                                if (fieldValue.isTextual()) {
                                    valueToSet = fieldValue.textValue();
                                } else {
                                    valueToSet = fieldValue.asText();
                                }
                                childDatasetField.setSingleValue(valueToSet);
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
                    for (Iterator<JsonNode> it = fieldValue.elements(); it.hasNext(); ) {
                        JsonNode e = it.next();
                        String newValue;
                        if (e.isTextual()) {
                            newValue = e.textValue();
                        } else if (e.isObject()){
                            newValue = roCrate.getEntityById(e.get("@id").textValue()).getProperty("name").textValue();
                        } else {
                            newValue = e.asText();
                        }
                        if (fieldValues.size() > index) {
                            // keep the original id
                            DatasetFieldValue updatedValue = fieldValues.get(index);
                            updatedValue.setValue(newValue);
                        } else {
                            fieldValues.add(new DatasetFieldValue(dsfToUpdate, newValue));
                        }
                        index++;
                    }
                }
            } else {
                String newValue;
                if (fieldValue.isTextual()) {
                    newValue = fieldValue.textValue();
                } else if (fieldValue.isObject()){
                    newValue = roCrate.getEntityById(fieldValue.get("@id").textValue()).getProperty("name").textValue();
                } else {
                    newValue = fieldValue.asText();
                }
                setSingleValue(dsfToUpdate, newValue);
            }
        } else {
            String value = "";
            if (fieldValue.isObject()) {
                var entity = roCrate.getEntityById(fieldValue.get("@id").textValue());
                if (entity != null) {
                    value = entity.getProperty("name").textValue();
                } else {
                    // TODO: check this later, because the entity being null should mean that it was removed from the RO-CRATE in AROMA
                    // however, the property from the rootDataset is not getting removed properly, so we must set it to null here
                    roCrate.getRootDataEntity().getProperties().set(datasetFieldType.getName(), null);
                }
            } else if (fieldValue.isTextual()){
                value = fieldValue.textValue();
            } else {
                value = fieldValue.asText();
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

    // Prepare the RO-Crate from AROMA to be imported into Dataverse
    public RoCrate preProcessRoCrateFromAroma(Dataset dataset, String roCrateJsonToImport) throws JsonProcessingException, ArpException {
        RoCrateReader roCrateFolderReader = new RoCrateReader(new FolderReader());
        String latestVersionRoCrateFolderPath = getRoCrateFolderForPreProcess(dataset.getLatestVersion());
        RoCrate latestVersionRoCrate = latestVersionRoCrateFolderPath != null ? roCrateFolderReader.readCrate(latestVersionRoCrateFolderPath) : null;
        RoCrateImportPrepResult roCrateImportPrepResult = prepareRoCrateForDataverseImport(roCrateJsonToImport, latestVersionRoCrate);

        var prepErrors = roCrateImportPrepResult.errors;
        if (!prepErrors.isEmpty()) {
            throw new ArpException(String.join("\n", prepErrors));
        }

        RoCrate roCrateToImport = roCrateImportPrepResult.getRoCrate();

        roCrateServiceBean.collectConformsToIds(dataset, roCrateToImport.getRootDataEntity());

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
        String rootDataEntityId = preProcessedRoCrate.getRootDataEntity().getId();

        preProcessedRoCrate.getRootDataEntity().getProperties().fields().forEachRemaining(field ->
                prepareAndValidateField(field, rootDataEntityId, preProcessedRoCrate, roCrateContext, roCrateContextUpdater, preProcessResult, roCrateEntityIdsAndTypes, false)
        );

        // remove the id of the rootDataset and the jsonDescriptor, the other id-s will be removed during processing the entities
        roCrateEntityIdsAndTypes.remove(rootDataEntityId);
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
                .filter(entity -> roCrateServiceBean.hasType(entity, "File") && !roCrateServiceBean.isVirtualFile(entity))
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
        var entityType = roCrateServiceBean.getTypeAsString(entity);
        roCrateEntityIdsAndTypes.remove(entityId);
        if (!entityType.equals("File") && !entityType.equals("Dataset")) {
            preProcessResult.errors.add("Entity with id: '" + entityId + "' has an invalid type: " + entityType);
        }
        entity.fields().forEachRemaining(field ->
                prepareAndValidateField(field, entityId, preProcessedRoCrate, roCrateContext, roCrateContextUpdater, preProcessResult, roCrateEntityIdsAndTypes, false)
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
    private void prepareAndValidateField(Map.Entry<String, JsonNode> field, String parentId, RoCrate roCrate, JsonNode roCrateContext, RoCrate.RoCrateBuilder roCrateContextUpdater, RoCrateImportPrepResult preProcessResult, HashMap<String, String> roCrateEntityIdsAndTypes, boolean lvl2) {
        var fieldName = field.getKey();
        var fieldValue = field.getValue();
        if (!fieldName.startsWith("@") && !roCrateServiceBean.propsToIgnore.contains(fieldName)) {
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
                    prepareAndValidatePrimitiveField(roCrate, fieldType, fieldName, fieldValue, parentId, controlledVocabularyValues, false, preProcessResult);
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

        if (!entityProperties.has("@type") || !roCrateServiceBean.getTypeAsString(entityProperties).equals(fieldName)) {
            preProcessResult.errors.add("The entity with id: '" + entityId + "' has invalid type! The correct type would be: " + fieldName);
            return;
        }

        if (entityProperties.has("conformsTo")) {
            validateConformsTo(entityProperties.get("conformsTo"), entityId, preProcessResult);
        }

        entityProperties.fields().forEachRemaining(field -> prepareAndValidateField(field, entityId, roCrate, roCrateContext, roCrateContextUpdater, preProcessResult, roCrateEntityIdsAndTypes, true));
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
    private void prepareAndValidatePrimitiveField(RoCrate roCrate, DatasetFieldType.FieldType fieldType, String fieldName, JsonNode fieldValue, String parentId, ArrayList<String> controlledVocabularyValues, boolean lvl2, RoCrateImportPrepResult preProcessResult) {
        if (fieldValue.isArray()) {
            if (lvl2) {
                preProcessResult.errors.add("The field '" + fieldName + "' can not have Arrays as values, but got: " + fieldValue);
            }
            // If fieldValue is an array, validate each element
            for (JsonNode element : fieldValue) {
                prepareAndValidatePrimitiveField(roCrate, fieldType, fieldName, element, parentId, controlledVocabularyValues, true, preProcessResult);
            }
        } else {
            if (!controlledVocabularyValues.isEmpty()) {
                if (!controlledVocabularyValues.contains(fieldValue.textValue())) {
                    preProcessResult.errors.add("Invalid controlled vocabulary value: '" + fieldValue + "' for field: '" + fieldName + "'.");
                }
            } else {
                switch (fieldType) {
                    case NONE ->
                            preProcessResult.errors.add("The field: " + fieldName + " can not have fieldType 'none'");
                    case DATE -> {
                        // Validate and parse date format (YYYY-MM-DD, YYYY-MM, or YYYY are accepted)
                        if (!fieldValue.isTextual() ||
                                !fieldValue.textValue().matches("\\d{4}-\\d{2}-\\d{2}|\\d{4}-\\d{2}|\\d{4}")) {
                            try {
                                String parsedDate = new SimpleDateFormat("yyyy-MM-dd")
                                        .format(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
                                        .parse(fieldValue.textValue()));
                                updatePropertyInEntity(roCrate, parentId, fieldName, parsedDate);
                            } catch (ParseException e) {
                                preProcessResult.errors.add("The provided value is not a valid date for field: " + fieldName);
                            }
                        }
                    }
                    case EMAIL -> {
                        // Validate email format
                        if (!fieldValue.isTextual() || !isEmailValid(fieldValue.textValue())) {
                            preProcessResult.errors.add("The provided value is not a valid email address for field: " + fieldName);
                        }
                    }
                    case TEXT -> {
                        // Validate that the value is a string and any text other than newlines
                        if (!fieldValue.isTextual() || fieldValue.textValue().matches(".*\\n.*")) {
                            preProcessResult.errors.add("Newlines are not allowed for field: " + fieldName);
                        }
                    }
                    case TEXTBOX -> {
                        // Validate that the value is a string
                        if (!fieldValue.isTextual()) {
                            preProcessResult.errors.add("The provided value should be a string for " + fieldName + " with 'textbox' type.");
                        }
                    }
                    case URL -> {
                        // Validate and parse URL
                        String url;
                        if (fieldValue.isTextual()) {
                            url = fieldValue.textValue();
                        } else if (fieldValue.isObject()){
                            url = roCrate.getEntityById(fieldValue.get("@id").textValue()).getProperty("name").textValue();
                        } else {
                            url = "";
                        }
                        if (!isURLValid(url)) {
                            preProcessResult.errors.add("If not empty, the field must contain a valid URL for field: " + fieldName);
                        } else {
                            updatePropertyInEntity(roCrate, parentId, fieldName, url);    
                        }
                    }
                    case INT -> {
                        // Validate integer value
                        if (!fieldValue.isInt()) {
                            preProcessResult.errors.add("The provided value is not a valid integer for field: " + fieldName);
                        }
                    }
                    case FLOAT -> {
                        // Validate floating-point number
                        if (!fieldValue.isFloatingPointNumber()) {
                            if (fieldValue.isNumber()) {
                                var floatVal = Float.valueOf(fieldValue.asText());
                                var entity = parentId.equals("./") ? roCrate.getRootDataEntity() : roCrate.getEntityById(parentId);
                                entity.getProperties().put(fieldName, floatVal);
                            } else {
                                preProcessResult.errors.add("The provided value is not a valid floating-point number for field: " + fieldName);   
                            }
                        }
                    }
                    default ->
                            preProcessResult.errors.add("Invalid fieldType: " + fieldType + " for field: " + fieldName);
                }
            }
        }
    }
    
    private void updatePropertyInEntity(RoCrate roCrate, String entityId, String fieldName, String newValue) {
        var entity = entityId.equals("./") ? roCrate.getRootDataEntity() : roCrate.getEntityById(entityId);
        entity.getProperties().put(fieldName, newValue);
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
                        entityType = roCrateServiceBean.getTypeAsString(entity);
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

    // This function always returns the path of a "draft" RO-CRATE
    // This function should only be used in the pre-processing of the RO-CRATE-s coming from AROMA
    public String getRoCratePathForPreProcess(DatasetVersion version) {
        return String.join(File.separator, getRoCrateFolderForPreProcess(version), ArpServiceBean.RO_CRATE_METADATA_JSON_NAME);
    }

    // This function always returns the folder path of a "draft" RO-CRATE
    // This function should only be used in the pre-processing of the RO-CRATE-s coming from AROMA
    public String getRoCrateFolderForPreProcess(DatasetVersion version) {
        String localDir = StorageUtils.getLocalRoCrateDir(version.getDataset());
        var supposedToExistPath = String.join(File.separator, localDir, "ro-crate-metadata");
        if (!Files.exists(Paths.get(supposedToExistPath))) {
            var latestPublished = version.getDataset().getReleasedVersion();
            if (latestPublished != null) {
                return roCrateServiceBean.getRoCrateFolder(latestPublished);
            } else {
                return  null;
            }

        }
        return supposedToExistPath;
    }

    public List<FileMetadata> updateFileMetadatas(Dataset dataset, RoCrate roCrate) {
        List<FileMetadata> filesToBeDeleted = new ArrayList<>();
        List<String> fileMetadataHashes = dataset.getLatestVersion().getFileMetadatas().stream().map(fmd -> fmd.getDataFile().getChecksumValue()).collect(Collectors.toList());
        List<ObjectNode> roCrateFileEntities = Stream.concat(
                roCrate.getAllContextualEntities().stream().map(AbstractEntity::getProperties).filter(ce -> roCrateServiceBean.getTypeAsString(ce).equals("File")),
                roCrate.getAllDataEntities().stream().map(AbstractEntity::getProperties).filter(de -> roCrateServiceBean.getTypeAsString(de).equals("File"))
        ).collect(Collectors.toList());

        // Update the metadata for the files in the dataset
        roCrateFileEntities.forEach(fileEntity -> {
            if (roCrateServiceBean.isVirtualFile(fileEntity)) {
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

    public void postProcessRoCrateFromAroma(Dataset dataset, RoCrate roCrate) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        String roCrateFolderPath = roCrateServiceBean.getRoCrateFolder(dataset.getLatestVersion());
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
                .filter(entity -> roCrateServiceBean.hasType(entity, "File") || roCrateServiceBean.hasType(entity, "Dataset"))
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
            } else if (!propName.startsWith("@") && !roCrateServiceBean.propsToIgnore.contains(propName) && compoundFields.containsKey(propName)) {
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
        writeOutRoCrateExtras(extraMetadata, roCrateServiceBean.getRoCrateParentFolder(dataset));
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
                if (!fieldName.startsWith("@") && !roCrateServiceBean.propsToIgnore.contains(fieldName)) {
                    entityProps.add(fieldName);
                }
            });
            compoundFieldProps.retainAll(entityProps);
            // Take the intersection of the property names of the compoundFieldProps and entityProps, if the result is an empty set, the entity contained props only from AROMA, and none from DV
            // the id of this entity can not be updated (since there's no compound value for the entity)
            if (!compoundFieldProps.isEmpty()) {
                DatasetFieldCompoundValue valueToObtainIdFrom = compoundValues.get(positionOfProp);
                String newId = roCrateServiceBean.createRoIdForCompound(valueToObtainIdFrom);
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


    private void writeOutRoCrateExtras(Map<String, ArrayList<String>> extraMetadata, String roCrateFolderPath) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode resultJsonNode = objectMapper.createObjectNode();

        for (Map.Entry<String, ArrayList<String>> entry : extraMetadata.entrySet()) {
            resultJsonNode.put(entry.getKey(), entry.getValue().size());
        }

        objectMapper.writerWithDefaultPrettyPrinter().writeValue(new File(roCrateFolderPath + "/" + RO_CRATE_EXTRAS_JSON_NAME), resultJsonNode);
    }

    private boolean postProcessDatasetAndFileEntities(RoCrate roCrate, JsonNode parentEntity, List<DataFile> dvDatasetFiles, Map<String, ArrayList<String>> extraMetadata, ObjectNode parentObj, ObjectMapper mapper) {
        String oldId = parentEntity.get("@id").textValue();
        var entity = roCrate.getEntityById(oldId);
        var entityNode = entity.getProperties();
        if (roCrateServiceBean.hasType(entityNode, "File")) {
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
            String newId  = roCrateServiceBean.createRoIdForDataset(entityNode.get("name").textValue(), parentObj);
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

    private void mergeHasParts(RoCrate roCrate, JsonNode hasPart, JsonNode parentNode, ObjectMapper mapper) {
        // check if the hasPart array contains any duplicates that need to be merged
        var it = hasPart.iterator();
        while (it.hasNext()) {
            var idObj = it.next();
            var originalId = idObj.get("@id").textValue();
            var originalEntity = roCrate.getEntityById(originalId).getProperties();
            if (roCrateServiceBean.getTypeAsString(originalEntity).equals("Dataset")) {
                String generatedId  = roCrateServiceBean.createRoIdForDataset(originalEntity.get("name").textValue(), parentNode);
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
    

}
