package edu.harvard.iq.dataverse.arp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import edu.harvard.iq.dataverse.*;
import edu.harvard.iq.dataverse.util.BundleUtil;
import edu.harvard.iq.dataverse.util.JsfHelper;

import javax.ejb.EJB;
import javax.enterprise.context.SessionScoped;
import javax.faces.context.FacesContext;
import javax.inject.Named;
import java.io.*;
import java.util.Base64;
import java.util.List;
import java.util.Map;
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

    public DatasetVersionUI resetVersionUIRoCrate(DatasetVersionUI datasetVersionUI, DatasetVersion workingVersion, Dataset dataset) throws JsonProcessingException {
        datasetVersionUI = datasetVersionUI.initDatasetVersionUI(workingVersion, true);
        if (!roCrateJsonString.isBlank()) {
            try {
                ObjectMapper mapper = new ObjectMapper();
                ArrayNode graphNode = (ArrayNode) mapper.readTree(roCrateJsonString).get("@graph");
                Map<String, DatasetField> dsfTypeMap = dataset.getOrCreateEditVersion().getDatasetFields().stream().collect(Collectors.toMap(dsf -> dsf.getDatasetFieldType().getName(), Function.identity()));
                JsonNode datasetNode = StreamSupport.stream(graphNode.spliterator(), false).filter(node -> {
                    var typeNode = node.get("@type");
                    if (typeNode instanceof ArrayNode) {
                        for (int i = 0; i < typeNode.size(); i++) {
                            var t = typeNode.get(i);
                            if (t.textValue().equals("Dataset")) {
                                return true;
                            }
                        }
                        return false;
                    }
                    return typeNode.textValue().equals("Dataset");
                }).findFirst().get();
    
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
                                    processCompoundField(value, datasetField, graphNode);
                                }
                            } else {
                                processCompoundField(roCrateValue, datasetField, graphNode);
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

        setRoCrateJsonString(null);
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
}
