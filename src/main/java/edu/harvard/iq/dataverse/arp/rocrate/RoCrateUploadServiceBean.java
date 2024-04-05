package edu.harvard.iq.dataverse.arp.rocrate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.harvard.iq.dataverse.*;
import edu.harvard.iq.dataverse.arp.ArpException;
import edu.harvard.iq.dataverse.arp.ArpServiceBean;
import edu.harvard.iq.dataverse.util.BundleUtil;
import edu.harvard.iq.dataverse.util.JsfHelper;
import edu.kit.datamanager.ro_crate.RoCrate;

import jakarta.ejb.EJB;
import jakarta.enterprise.context.SessionScoped;
import jakarta.faces.application.FacesMessage;
import jakarta.faces.context.FacesContext;
import jakarta.inject.Named;
import jakarta.ws.rs.core.Response;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.primefaces.PrimeFaces;
import org.primefaces.event.FileUploadEvent;
import org.primefaces.model.file.UploadedFile;

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
    
    private UploadedFile uploadedRoCrate;

    // Mapping of the imported RO-CRATE file ids and their actual dataFile representation storageIdentifiers
    private HashMap<String, String> importMapping = new HashMap<>();

    public void handleDnd(FileUploadEvent event) {
        UploadedFile uploadedRoCrateZip = event.getFile();
        String roCrateName = uploadedRoCrateZip.getFileName();
        String roCrateType = uploadedRoCrateZip.getContentType();
        setRoCrateName(roCrateName);
        setRoCrateType(roCrateType);
        try {
            setRoCrateInputStream(processRoCrateZip(uploadedRoCrateZip.getContent()));
            if (roCrateJsonString == null) {
                throw new ArpException("Missing " + ArpServiceBean.RO_CRATE_METADATA_JSON_NAME);
            }
        } catch (Exception e) {
            setRoCrateJsonString(null);
            setRoCrateInputStream(null);
            if (e instanceof ArpException) {
                FacesContext.getCurrentInstance().addMessage(null,
                        new FacesMessage(FacesMessage.SEVERITY_ERROR, "Error: ", e.getMessage()));
                FacesContext.getCurrentInstance().getExternalContext().setResponse(Response.serverError());
            }
            e.printStackTrace();
            JsfHelper.addErrorMessage("Could not process the " + ArpServiceBean.RO_CRATE_METADATA_JSON_NAME + "\n" + e.getMessage());
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

    public ByteArrayInputStream processRoCrateZip(byte[] roCrateBytes) {
        List<String> entryNamesToDelete = List.of(ArpServiceBean.RO_CRATE_METADATA_JSON_NAME, ArpServiceBean.RO_CRATE_PREVIEW_HTML_NAME);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try ( baos;
              ByteArrayInputStream bais = new ByteArrayInputStream(roCrateBytes);
              ZipInputStream zipInputStream = new ZipInputStream(bais);
              ZipOutputStream zos = new ZipOutputStream(baos)
        ) {
            ZipEntry entry;
            while ((entry = zipInputStream.getNextEntry()) != null) {
                int bytesRead;
                byte[] buffer = new byte[1024 * 32];
                if (!entryNamesToDelete.contains(entry.getName())) {
                    zos.putNextEntry(entry);
                    while ((bytesRead = zipInputStream.read(buffer)) != -1) {
                        zos.write(buffer, 0, bytesRead);
                    }
                    zos.closeEntry();
                } else if (entry.getName().contains(ArpServiceBean.RO_CRATE_METADATA_JSON_NAME)) {
                    var cs = new ByteArrayOutputStream();
                    while ((bytesRead = zipInputStream.read(buffer)) != -1) {
                        cs.write(buffer, 0, bytesRead);
                    }
                    var jsonString = cs.toString();
                    setRoCrateJsonString(jsonString);
                }
            }
            // If the zip contained only the ro-crate-metadata.json, return null
            if (baos.size() == 0) {
                return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            JsfHelper.addErrorMessage("Could not process the " + ArpServiceBean.RO_CRATE_METADATA_JSON_NAME + "\n" + e.getMessage());
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

    public UploadedFile getUploadedRoCrate() {
        return uploadedRoCrate;
    }

    public void setUploadedRoCrate(UploadedFile uploadedRoCrate) {
        this.uploadedRoCrate = uploadedRoCrate;
    }
}
