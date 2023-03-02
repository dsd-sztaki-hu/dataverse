package edu.harvard.iq.dataverse.api.arp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import edu.harvard.iq.dataverse.*;
import edu.kit.datamanager.ro_crate.RoCrate;
import edu.kit.datamanager.ro_crate.entities.contextual.ContextualEntity;
import edu.kit.datamanager.ro_crate.entities.data.FileEntity;
import edu.kit.datamanager.ro_crate.entities.data.RootDataEntity;
import edu.kit.datamanager.ro_crate.reader.FolderReader;
import edu.kit.datamanager.ro_crate.reader.RoCrateReader;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.ejb.EJB;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RoCrateManager {

    //TODO: Refact later
    public static void generateRoCrateData(RoCrate.RoCrateBuilder roCrateBuilder, RootDataEntity.RootDataEntityBuilder rootDataEntityBuilder, List<DatasetField> dsFields, HashMap<String, String> contextMap, ObjectMapper mapper) throws Exception {
        for (DatasetField datasetField : dsFields) {
            List<DatasetFieldCompoundValue> compoundValues = datasetField.getDatasetFieldCompoundValues();
            List<DatasetFieldValue> fieldValues = datasetField.getDatasetFieldValues();
            List<ControlledVocabularyValue> controlledVocabValues = datasetField.getControlledVocabularyValues();
            DatasetFieldType fieldType = datasetField.getDatasetFieldType();
            String fieldName = fieldType.getName();
            String fieldUri = fieldType.getUri();
            if (!compoundValues.isEmpty()) {
                for (var compoundValue : compoundValues) {
                    ContextualEntity.ContextualEntityBuilder contextualEntityBuilder = new ContextualEntity.ContextualEntityBuilder();
                    for (var childDatasetField : compoundValue.getChildDatasetFields()) {
                        if (!childDatasetField.getDatasetFieldCompoundValues().isEmpty()) {
                            throw new Exception("Values this deep should not be allowed");
                        }
                        List<DatasetFieldValue> childFieldValues = childDatasetField.getDatasetFieldValues();
                        List<ControlledVocabularyValue> childControlledVocabValues = childDatasetField.getControlledVocabularyValues();
                        if (!childFieldValues.isEmpty() || !childControlledVocabValues.isEmpty()) {
                            if (!contextMap.containsKey(fieldName)) {
                                contextMap.put(fieldName, fieldUri);
                                roCrateBuilder.addValuePairToContext(fieldName, fieldUri);
                            } else {
                                if (!Objects.equals(fieldUri, contextMap.get(fieldName))) {
                                    fieldName = fieldUri;
                                }
                            }
                            DatasetFieldType childFieldType = childDatasetField.getDatasetFieldType();
                            String childFieldName = childFieldType.getName();
                            String childFieldUri = childFieldType.getUri();
                            if (!contextMap.containsKey(childFieldName)) {
                                contextMap.put(childFieldName, childFieldUri);
                                roCrateBuilder.addValuePairToContext(childFieldName, childFieldUri);
                            } else {
                                if (!Objects.equals(childFieldUri, contextMap.get(childFieldName))) {
                                    childFieldName = childFieldUri;
                                }
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
                    // The hashmark before the uuid is required in Describo
                    // "@id's starting with # as these signify the reference is internal to the crate"
                    contextualEntityBuilder.setId("#" + UUID.randomUUID());
                    ContextualEntity contextualEntity = contextualEntityBuilder.build();
                    // The "@id" is always a prop in a contextualEntity
                    if (contextualEntity.getProperties().size() > 1) {
                        contextualEntity.addType(fieldName);
                        rootDataEntityBuilder.addIdProperty(fieldName, contextualEntity.getId());
                        roCrateBuilder.addContextualEntity(contextualEntity);
                    }
                }
            }
            if (!fieldValues.isEmpty() || !controlledVocabValues.isEmpty()) {
                if (!contextMap.containsKey(fieldName)) {
                    contextMap.put(fieldName, fieldUri);
                    roCrateBuilder.addValuePairToContext(fieldName, fieldUri);
                } else {
                    if (!Objects.equals(fieldUri, contextMap.get(fieldName))) {
                        fieldName = fieldUri;
                    }
                }
                if (!fieldValues.isEmpty()) {
                    if (fieldValues.size() == 1) {
                        rootDataEntityBuilder.addProperty(fieldName, fieldValues.get(0).getValue());
                    } else {
                        ArrayNode valuesNode = mapper.createArrayNode();
                        for (var fieldValue : fieldValues) {
                            valuesNode.add(fieldValue.getValue());
                        }
                        rootDataEntityBuilder.addProperty(fieldName, valuesNode);
                    }
                }
                if (!controlledVocabValues.isEmpty()) {
                    if (controlledVocabValues.size() == 1) {
                        rootDataEntityBuilder.addProperty(fieldName, controlledVocabValues.get(0).getStrValue());
                    } else {
                        ArrayNode strValuesNode = mapper.createArrayNode();
                        for (var controlledVocabValue : controlledVocabValues) {
                            strValuesNode.add(controlledVocabValue.getStrValue());
                        }
                        rootDataEntityBuilder.addProperty(fieldName, strValuesNode);
                    }
                }
            }
        }
    }

    public static RoCrate generateRoCrateFiles(RoCrate roCrate, List<FileMetadata> fileMetadatas) {
        for (var fileMetadata : fileMetadatas) {
            FileEntity.FileEntityBuilder fileEntityBuilder = new FileEntity.FileEntityBuilder();
            String fileName = fileMetadata.getLabel();
            DataFile dataFile = fileMetadata.getDataFile();
            fileEntityBuilder.setId("#" + fileName);
            fileEntityBuilder.addProperty("name", fileName);
            fileEntityBuilder.addProperty("contentSize", dataFile.getFilesize());
            fileEntityBuilder.setEncodingFormat(dataFile.getContentType());
            if (fileMetadata.getDescription() != null) {
                fileEntityBuilder.addProperty("description", fileMetadata.getDescription());
            }
            roCrate.addDataEntity(fileEntityBuilder.build(), true);
        }

        return roCrate;
    }

    public static String getRoCratePath(Dataset dataset) {
        return String.join(File.separator, getRoCrateFolder(dataset), "ro-crate-metadata.json");
    }

    public static String getRoCrateFolder(Dataset dataset) {
        String filesRootDirectory = System.getProperty("dataverse.files.directory");
        if (filesRootDirectory == null || filesRootDirectory.isEmpty()) {
            filesRootDirectory = "/tmp/files";
        }

        return String.join(File.separator, filesRootDirectory, dataset.getAuthorityForFileStorage(), dataset.getIdentifierForFileStorage());
    }

    public static void createRoCrate(Dataset dataset) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        RoCrate.RoCrateBuilder roCrateBuilder = new RoCrate.RoCrateBuilder();

        RootDataEntity.RootDataEntityBuilder rootDataEntityBuilder = new RootDataEntity.RootDataEntityBuilder();
        generateRoCrateData(roCrateBuilder, rootDataEntityBuilder, dataset.getLatestVersion().getDatasetFields(), new HashMap<>(), objectMapper);

        RoCrate roCrate = roCrateBuilder.build();
        roCrate.setRootDataEntity(rootDataEntityBuilder.build());
        roCrate = generateRoCrateFiles(roCrate, dataset.getLatestVersion().getFileMetadatas());

        JSONObject json = new JSONObject(roCrate.getJsonMetadata());
        String roCratePath = getRoCratePath(dataset);
        if (!Files.exists(Paths.get(getRoCrateFolder(dataset)))) {
            Files.createDirectories(Path.of(getRoCrateFolder(dataset)));
        }
        Files.writeString(Paths.get(roCratePath), json.toString(4));
    }

    public static String importRoCrate(Dataset dataset, Map<String, DatasetFieldType> datasetFieldTypeMap) {
        JSONObject importFormatMetadataBlocks = new JSONObject();

        RoCrateReader roCrateFolderReader = new RoCrateReader(new FolderReader());
        RoCrate roCrate = roCrateFolderReader.readCrate(getRoCrateFolder(dataset));

        RootDataEntity rootDataEntity = roCrate.getRootDataEntity();
        Map<String, ContextualEntity> contextualEntityHashMap = roCrate.getAllContextualEntities().stream().collect(Collectors.toMap(ContextualEntity::getId, Function.identity()));

        rootDataEntity.getProperties().fields().forEachRemaining(field -> {
            String fieldName = field.getKey();
            if (!fieldName.startsWith("@") && !fieldName.equals("hasPart")) {
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
                    processCompoundField(importFormatMetadataBlocks, field.getValue(), datasetFieldType, datasetFieldTypeMap, contextualEntityHashMap);
                } else {
                    JSONArray container = importFormatMetadataBlocks.getJSONObject(metadataBlock.getName()).getJSONArray("fields");
                    if (datasetFieldType.isAllowControlledVocabulary()) {
                        processControlledVocabFields(fieldName, field.getValue(), container, datasetFieldTypeMap);
                    } else {
                        processPrimitiveField(fieldName, field.getValue().textValue(), container, datasetFieldTypeMap);
                    }
                }
            }
        });


        JSONObject importFormatJson = new JSONObject();
        importFormatJson.put("metadataBlocks", importFormatMetadataBlocks);
        return importFormatJson.toString(4);
    }

    // After adding a hashmark before every @id property, Describo (AROMA) probably will not generate new entities with "@type: Thing"
    // TODO: If the hashmark really solved the issue then remove the part that rebuilds the whole json without the "Thing" nodes,
    // TODO: also check if the lib that processes the RO-Crate can already handle "@reverse" nodes
    public static String preProcessRoCrateFromAroma(String roCrateJson) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode rootNode = mapper.readTree(roCrateJson);
        JsonNode graphNode = rootNode.get("@graph");

        ObjectNode filteredJson = mapper.createObjectNode();
        ArrayNode filteredGraph = mapper.createArrayNode();

        for (JsonNode node : graphNode) {
            if (!node.get("@type").textValue().equals("Thing")) {
                filteredGraph.add(node);
            }
        }

        filteredJson.set("@context", rootNode.get("@context"));
        filteredJson.set("@graph", filteredGraph);

        removeReverseProperties(filteredJson);
        return filteredJson.toPrettyString();
    }

    private static void removeReverseProperties(JsonNode node) {
        if (node.isObject()) {
            ObjectNode objectNode = (ObjectNode) node;
            objectNode.remove("@reverse");
            objectNode.fields().forEachRemaining(entry -> removeReverseProperties(entry.getValue()));
        } else if (node.isArray()) {
            node.forEach(RoCrateManager::removeReverseProperties);
        }
    }

    public static void createMetadataBlock(JSONObject jsonObject, MetadataBlock metadataBlock) {
        JSONObject mdb = new JSONObject();
        mdb.put("displayName", metadataBlock.getDisplayName());
        mdb.put("name", metadataBlock.getName());
        mdb.put("fields", new JSONArray());
        jsonObject.put(metadataBlock.getName(), mdb);
    }

    public static void processCompoundField(JSONObject metadataBlocks, JsonNode roCrateValues, DatasetFieldType datasetField, Map<String, DatasetFieldType> datasetFieldTypeMap, Map<String, ContextualEntity> contextualEntityHashMap) {
        JSONObject compoundField = new JSONObject();
        compoundField.put("typeName", datasetField.getName());
        compoundField.put("multiple", datasetField.isAllowMultiples());
        compoundField.put("typeClass", "compound");

        if (roCrateValues.isArray()) {
            JSONArray compoundFieldValues = new JSONArray();
            roCrateValues.forEach(value -> {
                JSONObject compoundFieldValue = processCompoundFieldValue(value, datasetFieldTypeMap, contextualEntityHashMap);
                compoundFieldValues.put(compoundFieldValue);
            });
            compoundField.put("value", compoundFieldValues);
        } else {
            JSONObject compoundFieldValue = processCompoundFieldValue(roCrateValues, datasetFieldTypeMap, contextualEntityHashMap);
            if (datasetField.isAllowMultiples()) {
                JSONArray valueArray = new JSONArray();
                valueArray.put(compoundFieldValue);
                compoundField.put("value", valueArray);
            } else {
                compoundField.put("value", compoundFieldValue);
            }
        }

        metadataBlocks.getJSONObject(datasetField.getMetadataBlock().getName()).getJSONArray("fields").put(compoundField);
    }

    public static JSONObject processCompoundFieldValue(JsonNode roCrateValue, Map<String, DatasetFieldType> datasetFieldTypeMap, Map<String, ContextualEntity> contextualEntityHashMap) {
        JSONObject compoundFieldValue = new JSONObject();
        ContextualEntity contextualEntity = contextualEntityHashMap.get(roCrateValue.get("@id").textValue());
        contextualEntity.getProperties().fields().forEachRemaining(roCrateField -> {
            String fieldName = roCrateField.getKey();
            if (!roCrateField.getKey().startsWith("@")) {
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
                        processControlledVocabFields(fieldName, controlledVocabValues, compoundFieldValue, datasetFieldTypeMap);
                    } else {
                        processControlledVocabFields(fieldName, roCrateField.getValue().textValue(), compoundFieldValue, datasetFieldTypeMap);
                    }
                } else {
                    processPrimitiveField(fieldName, roCrateField.getValue().textValue(), compoundFieldValue, datasetFieldTypeMap);
                }
            }
        });

        return compoundFieldValue;
    }

    public static void processPrimitiveField(String fieldName, String fieldValue, Object container, Map<String, DatasetFieldType> datasetFieldTypeMap) {
        DatasetFieldType datasetFieldType = datasetFieldTypeMap.get(fieldName);
        JSONObject primitiveField = new JSONObject();
        primitiveField.put("typeName", datasetFieldType.getName());
        primitiveField.put("multiple", datasetFieldType.isAllowMultiples());
        primitiveField.put("typeClass", "primitive");
        if (datasetFieldType.isAllowMultiples()) {
            primitiveField.put("value", List.of(fieldValue));
        } else {
            primitiveField.put("value", fieldValue);
        }

        if (container instanceof JSONObject) {
            ((JSONObject) container).put(datasetFieldType.getName(), primitiveField);
        } else if (container instanceof JSONArray) {
            ((JSONArray) container).put(primitiveField);
        }
    }

    public static void processControlledVocabFields(String fieldName, Object fieldValue, Object container, Map<String, DatasetFieldType> datasetFieldTypeMap) {
        DatasetFieldType datasetFieldType = datasetFieldTypeMap.get(fieldName);
        JSONObject field = new JSONObject();

        if (fieldValue instanceof ArrayNode) {
            List<String> controlledVocabValues = new ArrayList<>();
            ((ArrayNode) fieldValue).forEach(controlledVocabValue -> controlledVocabValues.add(controlledVocabValue.textValue()));
            field.put("value", controlledVocabValues);
        } else {
            String controlledVocabValue;
            if (fieldValue instanceof TextNode) {
                controlledVocabValue = ((TextNode) fieldValue).textValue();
            } else {
                controlledVocabValue = fieldValue.toString();
            }
            if (datasetFieldType.isAllowMultiples()) {
                field.put("value", List.of(controlledVocabValue));
            } else {
                field.put("value", controlledVocabValue);
            }
        }

        field.put("typeName", datasetFieldType.getName());
        field.put("multiple", datasetFieldType.isAllowMultiples());
        field.put("typeClass", "controlledVocabulary");


        if (container instanceof JSONObject) {
            ((JSONObject) container).put(datasetFieldType.getName(), field);
        } else if (container instanceof JSONArray) {
            ((JSONArray) container).put(field);
        }
    }

}
