package edu.harvard.iq.dataverse.api.arp;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.google.gson.*;
import edu.harvard.iq.dataverse.ControlledVocabularyValue;
import edu.harvard.iq.dataverse.api.arp.util.JsonHelper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static edu.harvard.iq.dataverse.api.arp.util.JsonHelper.*;

public class CedarTemplateToDvMdbConverter {

    public CedarTemplateToDvMdbConverter() {
    }

    public String processCedarTemplate(String cedarTemplate, Set<String> overridePropNames) throws IOException {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        CsvMapper mapper = new CsvMapper();

        JsonObject cedarTemplateJson = gson.fromJson(cedarTemplate, JsonObject.class);

        String metadataBlockId = cedarTemplateJson.get("schema:identifier").getAsString();

        DataverseMetadataBlock metadataBlockValues = processMetadataBlock(cedarTemplateJson, metadataBlockId);
        ProcessedCedarTemplateValues processedCedarTemplateValues = processTemplate(cedarTemplateJson, metadataBlockId, new ProcessedCedarTemplateValues(new ArrayList<>(), new ArrayList<>()), null, overridePropNames);

        CsvSchema mdbSchema = CsvSchema.builder()
                .addColumn("#metadataBlock")
                .addColumn("name")
                .addColumn("dataverseAlias")
                .addColumn("displayName")
                .addColumn("blockURI")
                .build()
                .withHeader()
                .withColumnSeparator('\t')
                .withoutQuoteChar();

        CsvSchema datasetFieldSchema = CsvSchema.builder()
                .addColumn("#datasetField")
                .addColumn("name")
                .addColumn("title")
                .addColumn("description")
                .addColumn("watermark")
                .addColumn("fieldType")
                .addColumn("displayOrder")
                .addColumn("displayFormat")
                .addColumn("advancedSearchField")
                .addColumn("allowControlledVocabulary")
                .addColumn("allowmultiples")
                .addColumn("facetable")
                .addColumn("displayoncreate")
                .addColumn("required")
                .addColumn("parent")
                .addColumn("metadatablock_id")
                .addColumn("termURI")
                .build()
                .withHeader()
                .withColumnSeparator('\t')
                .withoutQuoteChar()
                .withoutEscapeChar();

        CsvSchema controlledVocabularySchema = CsvSchema.builder()
                .addColumn("#controlledVocabulary")
                .addColumn("DatasetField")
                .addColumn("Value")
                .addColumn("identifier")
                .addColumn("displayOrder")
                .build()
                .withHeader()
                .withColumnSeparator('\t')
                .withoutQuoteChar()
                .withoutEscapeChar();

        String metadataBlocks = mapper.writer(mdbSchema).writeValueAsString(metadataBlockValues);
        String datasetFieldValues = mapper.writer(datasetFieldSchema).writeValueAsString(processedCedarTemplateValues.datasetFieldValues);
        String controlledVocabularyValues = mapper.writer(controlledVocabularySchema).writeValueAsString(processedCedarTemplateValues.controlledVocabularyValues);

        return metadataBlocks + datasetFieldValues + controlledVocabularyValues;
    }

    public DataverseMetadataBlock processMetadataBlock(JsonObject cedarTemplate, String metadataBlockId) {
        DataverseMetadataBlock dataverseMetadataBlock = new DataverseMetadataBlock();

        dataverseMetadataBlock.setName(metadataBlockId);
        dataverseMetadataBlock.setDisplayName(cedarTemplate.get("schema:name").getAsString());
//        dataverseMetadataBlock.setBlockURI(cedarTemplate.get("@id").getAsString());

        return dataverseMetadataBlock;
    }

    public ProcessedCedarTemplateValues processTemplate(JsonObject cedarTemplate, String metadataBlockId, ProcessedCedarTemplateValues processedCedarTemplateValues, String parentName, Set<String> overridePropNames) {
        getStringList(cedarTemplate, "_ui.order").stream().filter(propertyName -> !overridePropNames.contains(propertyName)).forEach(propertyName -> {
            JsonObject property = getJsonObject(cedarTemplate, "properties." + propertyName);
            String propertyType = Optional.ofNullable(property.get("@type")).map(JsonElement::getAsString).orElse(null);
            int displayOrder = processedCedarTemplateValues.datasetFieldValues.size();
            String propertyTermUri = getStringList(cedarTemplate, "properties.@context.properties." + propertyName + ".enum").get(0);

            if (propertyType != null) {
                String actPropertyType = propertyType.substring(propertyType.lastIndexOf("/") + 1);
                boolean isHidden = Optional.ofNullable(property.getAsJsonObject("_ui").get("hidden")).map(JsonElement::getAsBoolean).orElse(false);
                if (!isHidden && (actPropertyType.equals("TemplateField") || actPropertyType.equals("StaticTemplateField"))) {
                    JsonObject valueConstraints = property.getAsJsonObject("_valueConstraints");
                    boolean allowMultiple = valueConstraints.has("multipleChoice") && valueConstraints.get("multipleChoice").getAsBoolean();
                    processTemplateField(property, displayOrder, allowMultiple, metadataBlockId, propertyTermUri, parentName, processedCedarTemplateValues);
                } else if (actPropertyType.equals("TemplateElement")) {
                    processTemplateElement(property, processedCedarTemplateValues, metadataBlockId, propertyTermUri, false, parentName, overridePropNames);
                }
            } else {
                String actPropertyType = property.get("type").getAsString();
                if (actPropertyType.equals("array")) {
                    processArray(property, processedCedarTemplateValues, metadataBlockId, propertyTermUri, parentName, overridePropNames);
                }
            }
        });

        return processedCedarTemplateValues;
    }

    // TODO: handle watermark, displayFormat, advancedSearchField, facetable
    public void processTemplateField(JsonObject templateField, int displayOrder, boolean allowMultiple, String metadataBlockName, String propertyTermUri, String parentName, ProcessedCedarTemplateValues processedCedarTemplateValues) {
        DataverseDatasetField dataverseDatasetField = new DataverseDatasetField();
        String fieldType = Optional.ofNullable(getJsonElement(templateField, "_ui.inputType")).map(JsonElement::getAsString).orElse(null);
        boolean allowCtrlVocab = Objects.equals(fieldType, "list") || Objects.equals(fieldType, "radio");
        boolean hasExternalVocabValues = JsonHelper.getJsonObject(templateField, "_valueConstraints.branches[0]") != null;

        /*
         * fieldnames can not contain dots in CEDAR, so we replace them with colons before exporting the template
         * upon importing from CEDAR the colons are replaced with dots again
         * */
        dataverseDatasetField.setName(Optional.ofNullable(templateField.get("schema:name")).map(name -> name.getAsString().replace(':', '.')).orElse(null));
        String title = Optional.ofNullable(templateField.get("skos:prefLabel")).map(JsonElement::getAsString).orElse(templateField.get("schema:name").getAsString());
        dataverseDatasetField.setTitle(title);
        dataverseDatasetField.setDescription(Optional.ofNullable(templateField.get("schema:description")).map(JsonElement::getAsString).orElse(null));
        dataverseDatasetField.setFieldType(getDataverseFieldType(templateField));
        dataverseDatasetField.setDisplayOrder(displayOrder);
        // We need to set allowControlledVocabulary to true for datasetFieldTypes with external vocabulary values as well,
        // to prevent the edu.harvard.iq.dataverse.DatasetField.createNewEmptyDatasetField(edu.harvard.iq.dataverse.DatasetFieldType)
        // adding a default datasetFieldValue to the datasetField
        dataverseDatasetField.setAllowControlledVocabulary(allowCtrlVocab || hasExternalVocabValues);
        dataverseDatasetField.setAllowmultiples(allowMultiple);
        dataverseDatasetField.setDisplayoncreate(true);
        dataverseDatasetField.setRequired(Optional.ofNullable(getJsonElement(templateField, "_valueConstraints.requiredValue")).map(JsonElement::getAsBoolean).orElse(false));
        dataverseDatasetField.setParent(parentName);
        dataverseDatasetField.setmetadatablock_id(metadataBlockName);
        dataverseDatasetField.setTermUri(propertyTermUri);

        processedCedarTemplateValues.datasetFieldValues.add(dataverseDatasetField);

        if (allowCtrlVocab) {
            String finalPropName = dataverseDatasetField.getName();
            processCtrlVocabValues(templateField, finalPropName, processedCedarTemplateValues);
        }
    }

    public void processCtrlVocabValues(JsonObject templateField, String finalPropName, ProcessedCedarTemplateValues processedCedarTemplateValues) {
        JsonArray ctrlVocabValues = JsonHelper.getJsonElement(templateField, "_valueConstraints.literals").getAsJsonArray();

        for (int i = 0; i < ctrlVocabValues.size(); i++) {
            JsonObject value = ctrlVocabValues.get(i).getAsJsonObject();
            DataverseControlledVocabulary controlledVocabulary = new DataverseControlledVocabulary();
            controlledVocabulary.setDatasetField(finalPropName);
            controlledVocabulary.setValue(value.get("label").getAsString());
            controlledVocabulary.setDisplayOrder(i);
            processedCedarTemplateValues.controlledVocabularyValues.add(controlledVocabulary);
        }
    }

    public void processTemplateElement(JsonObject templateElement, ProcessedCedarTemplateValues processedCedarTemplateValues, String metadataBlockId, String propertyTermUri, boolean allowMultiples, String parentName, Set<String> overridePropNames) {
        int displayOrder = processedCedarTemplateValues.datasetFieldValues.size();
        boolean allowsMultiple = allowMultiples || templateElement.keySet().contains("minItems") || templateElement.keySet().contains("maxItems");
        processTemplateField(templateElement, displayOrder, allowsMultiple, metadataBlockId, propertyTermUri, parentName, processedCedarTemplateValues);
        String parent = processedCedarTemplateValues.datasetFieldValues.get(processedCedarTemplateValues.datasetFieldValues.size() - 1).getName();
        processTemplate(templateElement, metadataBlockId, processedCedarTemplateValues, parent, overridePropNames);
    }

    public void processArray(JsonObject array, ProcessedCedarTemplateValues processedCedarTemplateValues, String metadataBlockId, String propertyTermUri, String parentName, Set<String> overridePropNames) {
        JsonObject items = array.getAsJsonObject("items");
        int displayOrder = processedCedarTemplateValues.datasetFieldValues.size();
        String inputType = Optional.ofNullable(getJsonElement(items, "_ui.inputTye")).map(JsonElement::getAsString).orElse(null);
        if (inputType != null) {
            processTemplateField(items, displayOrder, true, metadataBlockId, propertyTermUri, parentName, processedCedarTemplateValues);
        } else {
            processTemplateElement(items, processedCedarTemplateValues, metadataBlockId, propertyTermUri, true, null, overridePropNames);
        }
    }

    public String getDataverseFieldType(JsonObject templateField) {
        Map<String, String> cedarDataverseFieldTypes = Map.of(
            "textfield", "text",
            "temporal", "date",
            "numeric", "int-float",
            "richtext", "textbox",
            "textarea", "textbox",
            "link", "url",
            "list", "text",
            "radio", "text",
            "attribute-value", "text",
            "email", "email"
        );

        String dataverseFieldType = null;
        String fieldType = Optional.ofNullable(getJsonElement(templateField, "_ui.inputType")).map(JsonElement::getAsString).orElse(null);

        if (fieldType != null && cedarDataverseFieldTypes.containsKey(fieldType)) {
            if (Objects.equals(fieldType, "numeric")) {
                String numericType = Optional.ofNullable(getJsonElement(templateField, "_valueConstraints.numberType")).map(JsonElement::getAsString).orElse(null);
                if (Objects.equals(numericType, "xsd:decimal")) {
                    dataverseFieldType = cedarDataverseFieldTypes.get(fieldType).split("-")[0];
                } else {
                    dataverseFieldType = cedarDataverseFieldTypes.get(fieldType).split("-")[1];
                }
            } else {
                dataverseFieldType = cedarDataverseFieldTypes.get(fieldType);
            }
        } else {
            dataverseFieldType = "none";
        }

        return dataverseFieldType;
    }

    private static class ProcessedCedarTemplateValues {
        private ArrayList<DataverseDatasetField> datasetFieldValues;
        private ArrayList<DataverseControlledVocabulary> controlledVocabularyValues;

        public ProcessedCedarTemplateValues(ArrayList<DataverseDatasetField> datasetFieldValues, ArrayList<DataverseControlledVocabulary> controlledVocabularyValues) {
            this.datasetFieldValues = datasetFieldValues;
            this.controlledVocabularyValues = controlledVocabularyValues;
        }

        public ArrayList<DataverseDatasetField> getDatasetFieldValues() {
            return datasetFieldValues;
        }

        public void setDatasetFieldValues(ArrayList<DataverseDatasetField> datasetFieldValues) {
            this.datasetFieldValues = datasetFieldValues;
        }

        public ArrayList<DataverseControlledVocabulary> getControlledVocabularyValues() {
            return controlledVocabularyValues;
        }

        public void setControlledVocabularyValues(ArrayList<DataverseControlledVocabulary> controlledVocabularyValues) {
            this.controlledVocabularyValues = controlledVocabularyValues;
        }
    }


    private static class DataverseMetadataBlock {
        private String name;
        private String dataverseAlias;
        // schema:name
        private String displayName;
        // @id
        private String blockURI;

        public DataverseMetadataBlock() {
        }

        public DataverseMetadataBlock(String name, String dataverseAlias, String displayName, String blockUri) {
            this.name = name;
            this.dataverseAlias = dataverseAlias;
            this.displayName = displayName;
            this.blockURI = blockUri;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getDataverseAlias() {
            return dataverseAlias;
        }

        public void setDataverseAlias(String dataverseAlias) {
            this.dataverseAlias = dataverseAlias;
        }

        public String getDisplayName() {
            return displayName;
        }

        public void setDisplayName(String displayName) {
            this.displayName = displayName;
        }

        public String getBlockURI() {
            return blockURI;
        }

        public void setBlockURI(String blockUri) {
            this.blockURI = blockUri;
        }

    }

    private static class DataverseDatasetField {
        // schema:name
        private String name;
        private String title;
        // schema:description
        private String description;
        private String watermark;
        private String fieldType;
        private int displayOrder;
        private String displayFormat;
        private String parent;
        private String metadatablock_id;
        private String termURI;
        private boolean advancedSearchField;
        private boolean allowControlledVocabulary;
        private boolean allowmultiples;
        private boolean facetable;
        private boolean displayoncreate;
        private boolean required;

        public DataverseDatasetField() {
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        public String getWatermark() {
            return watermark;
        }

        public void setWatermark(String watermark) {
            this.watermark = watermark;
        }

        public String getFieldType() {
            return fieldType;
        }

        public void setFieldType(String fieldType) {
            this.fieldType = fieldType;
        }

        public int getDisplayOrder() {
            return displayOrder;
        }

        public void setDisplayOrder(int displayOrder) {
            this.displayOrder = displayOrder;
        }

        public String getDisplayFormat() {
            return displayFormat;
        }

        public void setDisplayFormat(String displayFormat) {
            this.displayFormat = displayFormat;
        }

        public String getParent() {
            return parent;
        }

        public void setParent(String parent) {
            this.parent = parent;
        }

        public String getmetadatablock_id() {
            return metadatablock_id;
        }

        public void setmetadatablock_id(String metadataBlockId) {
            this.metadatablock_id = metadataBlockId;
        }

        public String getTermURI() {
            return termURI;
        }

        public void setTermUri(String termUri) {
            this.termURI = termUri;
        }

        public boolean isAdvancedSearchField() {
            return advancedSearchField;
        }

        public void setAdvancedSearchField(boolean advancedSearchField) {
            this.advancedSearchField = advancedSearchField;
        }

        public boolean isAllowControlledVocabulary() {
            return allowControlledVocabulary;
        }

        public void setAllowControlledVocabulary(boolean allowControlledVocabulary) {
            this.allowControlledVocabulary = allowControlledVocabulary;
        }

        public boolean isAllowmultiples() {
            return allowmultiples;
        }

        public void setAllowmultiples(boolean allowmultiples) {
            this.allowmultiples = allowmultiples;
        }

        public boolean isFacetable() {
            return facetable;
        }

        public void setFacetable(boolean facetable) {
            this.facetable = facetable;
        }

        public boolean isDisplayoncreate() {
            return displayoncreate;
        }

        public void setDisplayoncreate(boolean displayoncreate) {
            this.displayoncreate = displayoncreate;
        }

        public boolean isRequired() {
            return required;
        }

        public void setRequired(boolean required) {
            this.required = required;
        }
    }

    private static class DataverseControlledVocabulary {
        @JsonProperty("DatasetField")
        private String datasetField;
        @JsonProperty("Value")
        private String value;
        private String identifier;
        private int displayOrder;

        public DataverseControlledVocabulary() {
        }
        @JsonProperty("DatasetField")
        public String getDatasetField() {
            return datasetField;
        }
        @JsonProperty("DatasetField")
        public void setDatasetField(String DatasetField) {
            this.datasetField = DatasetField;
        }
        @JsonProperty("Value")
        public String getValue() {
            return value;
        }
        @JsonProperty("Value")
        public void setValue(String value) {
            this.value = value;
        }

        public String getIdentifier() {
            return identifier;
        }

        public void setIdentifier(String identifier) {
            this.identifier = identifier;
        }

        public int getDisplayOrder() {
            return displayOrder;
        }

        public void setDisplayOrder(int displayOrder) {
            this.displayOrder = displayOrder;
        }
    }

}
