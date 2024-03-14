package edu.harvard.iq.dataverse.arp.rocrate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.harvard.iq.dataverse.*;
import edu.harvard.iq.dataverse.arp.ArpServiceBean;
import edu.harvard.iq.dataverse.util.BundleUtil;
import edu.harvard.iq.dataverse.util.JsfHelper;
import edu.kit.datamanager.ro_crate.RoCrate;

import jakarta.ejb.EJB;
import jakarta.enterprise.context.SessionScoped;
import jakarta.faces.context.FacesContext;
import jakarta.inject.Named;
import java.io.*;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import static edu.harvard.iq.dataverse.DatasetFieldCompoundValue.createNewEmptyDatasetFieldCompoundValue;

@Named
@SessionScoped
public class RoCrateUploadServiceBean implements Serializable {

    @EJB
    DatasetFieldServiceBean fieldService;

    private String roCrateJsonString;
    private String roCrateAsBase64;
    private String roCrateName;
    private String roCrateType;
    private InputStream roCrateInputStream;
    // The parsed roCrateJsonString
    private ObjectNode roCrateParsed;
    // The @graph node
    private ArrayNode roCrateGraph;

    // Mapping of the imported RO-CRATE file ids and their actual dataFile representation storageIdentifiers
    private HashMap<String, String> importMapping = new HashMap<>();

    public void handleRoCrateUpload() {
        String roCrateJsonString = FacesContext.getCurrentInstance().getExternalContext().getRequestParameterMap().get("roCrateJson");
        String roCrateAsBase64 = FacesContext.getCurrentInstance().getExternalContext().getRequestParameterMap().get("roCrateAsBase64");
        String roCrateName = FacesContext.getCurrentInstance().getExternalContext().getRequestParameterMap().get("roCrateName");
        String roCrateType = FacesContext.getCurrentInstance().getExternalContext().getRequestParameterMap().get("roCrateType");
        setRoCrateJsonString(roCrateJsonString);
        setRoCrateAsBase64(roCrateAsBase64);
        setRoCrateName(roCrateName);
        setRoCrateType(roCrateType);
        try {
            setRoCrateInputStream(processRoCrateZip(roCrateAsBase64));
        } catch (Exception e) {
            setRoCrateJsonString(null);
            setRoCrateInputStream(null);
            e.printStackTrace();
            JsfHelper.addErrorMessage("Can not process the " + ArpServiceBean.RO_CRATE_METADATA_JSON_NAME + "\n" + e.getMessage());
        }
    }
    
    private boolean hasType(JsonNode jsonNode, String type) {
        JsonNode typeNode = jsonNode.get("@type");
        if (typeNode instanceof ArrayNode) {
            for (int i = 0; i < typeNode.size(); i++) {
                var t = typeNode.get(i);
                if (t.textValue().equals(type)) {
                    return true;
                }
            }
            return false;
        }
        return typeNode.textValue().equals(type);
    }

    public DatasetVersionUI resetVersionUIRoCrate(DatasetVersionUI datasetVersionUI, DatasetVersion workingVersion, Dataset dataset) throws JsonProcessingException {
        datasetVersionUI = datasetVersionUI.initDatasetVersionUI(workingVersion, true);
        if (!roCrateJsonString.isBlank()) {
            try {
                Map<String, DatasetField> dsfTypeMap = dataset.getOrCreateEditVersion().getDatasetFields().stream().collect(Collectors.toMap(dsf -> dsf.getDatasetFieldType().getName(), Function.identity()));
                JsonNode datasetNode = StreamSupport.stream(roCrateGraph.spliterator(), false).filter(node -> hasType(node, "Dataset")).findFirst().get();
    
                // process the Dataset node, from here we can get the primitive values
                // and the type of the compound values
                datasetNode.fields().forEachRemaining(prop -> {
                    String propName = prop.getKey();
                    if (!propName.equals("hasPart") && dsfTypeMap.containsKey(propName)) {
                        DatasetField datasetField = dsfTypeMap.get(propName);
                        DatasetFieldType datasetFieldType = datasetField.getDatasetFieldType();
    
                        // Process the values depending on the field's type
                        if (datasetFieldType.isCompound()) {
                            JsonNode roCrateValue = datasetNode.get(propName);
                            if (roCrateValue.isArray()) {
                                for (JsonNode value : roCrateValue) {
                                    processCompoundField(value, datasetField, roCrateGraph);
                                }
                            } else {
                                processCompoundField(roCrateValue, datasetField, roCrateGraph);
                            }
                        } else {
                            if (datasetFieldType.isAllowControlledVocabulary()) {
                                processControlledVocabularyField(datasetField, prop.getValue());
                            } else {
                                datasetField.getDatasetFieldValues().get(0).setValue(prop.getValue().textValue());
                            }
                        }
                    }
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        //setRoCrateJsonString(null);
        return datasetVersionUI;
    }

    private void processCompoundField(JsonNode roCrateValue, DatasetField datasetField, ArrayNode graphNode) {
        try {
            String id = roCrateValue.get("@id").textValue();
            JsonNode valueNode = StreamSupport.stream(graphNode.spliterator(), false).filter(node -> node.get("@id").textValue().equals(id)).findFirst().get();
            List<DatasetField> childFields;
            if (datasetField.isEmpty()) {
                childFields = datasetField.getDatasetFieldCompoundValues().get(0).getChildDatasetFields();
                processCompoundFieldValue(childFields, valueNode);
            } else {
                var newEmptyDatasetFieldCompoundValue = createNewEmptyDatasetFieldCompoundValue(datasetField);
                childFields = newEmptyDatasetFieldCompoundValue.getChildDatasetFields();
                processCompoundFieldValue(childFields, valueNode);
                datasetField.getDatasetFieldCompoundValues().add(newEmptyDatasetFieldCompoundValue);
            }
        } catch (Exception e) {
            e.printStackTrace();
            JsfHelper.addErrorMessage("An error occurred during processing compound field: \"" + datasetField.getDatasetFieldType().getName() + "\". Details: \"" + e.getMessage() + "\".");
        }
    }

    private void processCompoundFieldValue(List<DatasetField> childFields, JsonNode valueNode) {
        childFields.forEach(childField -> {
            if (valueNode.has(childField.getDatasetFieldType().getName())) {
                if (childField.getDatasetFieldType().isAllowControlledVocabulary()) {
                    processControlledVocabularyField(childField, valueNode.get(childField.getDatasetFieldType().getName()));
                } else {
                    childField.getDatasetFieldValues().get(0).setValue(valueNode.get(childField.getDatasetFieldType().getName()).textValue());
                }
            }
        });
    }

    private void processControlledVocabularyField(DatasetField datasetField, JsonNode fieldValue) {
        try {
            if (fieldValue.isArray()) {
                for (var val : fieldValue) {
                    var cvv = fieldService.findControlledVocabularyValueByDatasetFieldTypeAndStrValue(datasetField.getDatasetFieldType(), val.textValue(), true);
                    if (cvv == null) {
                        throw new RuntimeException("No controlled vocabulary value was found for value: " + val.textValue());
                    }
                    datasetField.getControlledVocabularyValues().add(cvv);
                }
            } else {
                var cvv = fieldService.findControlledVocabularyValueByDatasetFieldTypeAndStrValue(datasetField.getDatasetFieldType(), fieldValue.textValue(), true);
                if (cvv == null) {
                    throw new RuntimeException("No controlled vocabulary value was found for value: " + fieldValue.textValue());
                }
                datasetField.getControlledVocabularyValues().add(cvv);
            }
        } catch (Exception e) {
            e.printStackTrace();
            JsfHelper.addErrorMessage("An error occurred during processing controlled vocabulary value for field: \"" + datasetField.getDatasetFieldType().getName() + "\". Details: \"" + e.getMessage() + "\".");
        }
    }

    private ByteArrayInputStream processRoCrateZip(String roCrateAsBase64) throws IOException {
        byte[] roCrateBytes = Base64.getDecoder().decode(roCrateAsBase64.split(",")[1]);
        String entryNameToDelete = ArpServiceBean.RO_CRATE_METADATA_JSON_NAME;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try (baos; ByteArrayInputStream bais = new ByteArrayInputStream(roCrateBytes);
             ZipInputStream zis = new ZipInputStream(bais);
             ZipOutputStream zos = new ZipOutputStream(baos)) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                if (!entry.getName().contains(entryNameToDelete)) {
                    byte[] buffer = new byte[1024 * 32];
                    int bytesRead;
                    zos.putNextEntry(entry);
                    while ((bytesRead = zis.read(buffer)) != -1) {
                        zos.write(buffer, 0, bytesRead);
                    }
                    zos.closeEntry();
                }
            }
            // the zip contained only the ro-crate-metadata.json
            if (baos.size() == 0) {
                return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            JsfHelper.addErrorMessage("Could not process the content of the uploaded RO-Crate.zip " + e.getMessage());
            return null;
        }

        return new ByteArrayInputStream(baos.toByteArray());
    }

    public String getRoCrateJsonString() {
        return roCrateJsonString;
    }

    public void setRoCrateJsonString(String roCrateJsonString) {
        this.roCrateJsonString = roCrateJsonString;
        if (roCrateJsonString == null) {
            roCrateParsed = null;
            roCrateGraph = null;
            return;
        }
        ObjectMapper mapper = new ObjectMapper();
        try {
            roCrateParsed = (ObjectNode) mapper.readTree(roCrateJsonString);
            roCrateGraph = (ArrayNode) roCrateParsed.get("@graph");
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }


    public String getRoCrateAsBase64() {
        return roCrateAsBase64;
    }

    public void setRoCrateAsBase64(String roCrateAsBase64) {
        this.roCrateAsBase64 = roCrateAsBase64;
    }

    public String getRoCrateName() {
        return roCrateName;
    }

    public void setRoCrateName(String roCrateName) {
        this.roCrateName = roCrateName;
    }

    public String getRoCrateType() {
        return roCrateType;
    }

    public void setRoCrateType(String roCrateType) {
        this.roCrateType = roCrateType;
    }

    public InputStream getRoCrateInputStream() {
        return roCrateInputStream;
    }

    public void setRoCrateInputStream(InputStream roCrateInputStream) {
        this.roCrateInputStream = roCrateInputStream;
    }

    /**
     * Given the RoCrate generated for a DV dataset by RoCrateManager, add any file or sub-dataset related data to the
     * generatedCrate that is found in the previously uploaded roCrateJsonString.
     *
     * @param generatedCrate
     * @return
     */
    public RoCrate addUploadedFileMetadata(RoCrate generatedCrate) {
        if (roCrateParsed == null) {
            return generatedCrate;
        }

        // 1. Take File nodes from roCrateGraph and find their counterpart in generatedCrate.getAllDataEntities() (
        //   using dirPath + filename + hash). If counterpart is not found, then the file has been removed during upload
        //   in which case we are done with that file.
        // 2. Set missing metadata from roCrateGraph file node to  generatedCrate data entity
        // 3. If metadata is not a simple file, but a contextual entity, then generate the necessary contextual entity
        //    in generatedCrate -- do this recursively (contextual entity may refer to other contextual entity)
        // 4. For a roCrateGraph file node find its parent dataset and also its counterpart in
        //    generatedCrate.getAllDataEntities(). If the counterpart dataset is not found, then the user has
        //    edited te dirPath during upload and we have nothing to do with that dataset.
        // 5. Set missing metadata on the dataset in generatedCrate and process any connected context entity just like
        //     with files.

        return generatedCrate;
    }
    
    // Mapping between the fileEntity ids from the RO-CRATE and their uploaded datasetFile representation storageIdentifier
    public void createImportMapping(List<DataFile> importedFiles) {
        HashMap<String, String> idAndStorageIdentifierMapping = new HashMap<>();
        // Collect the RO-CRATE fileEntities for easier processing
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
            correspondingFileEntity.ifPresent(fileEntity -> idAndStorageIdentifierMapping.put(fileEntity.get("@id").textValue(), storageIdentifier));
        }
        
        setImportMapping(idAndStorageIdentifierMapping);
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

    public void reset() {
        setRoCrateJsonString(null);
        setRoCrateAsBase64(null);
        setRoCrateName(null);
        setRoCrateType(null);
        setRoCrateInputStream(null);
        setImportMapping(null);
    }

    public HashMap<String, String> getImportMapping() {
        return importMapping;
    }

    public void setImportMapping(HashMap<String, String> importMapping) {
        this.importMapping = importMapping;
    }

}
