package edu.harvard.iq.dataverse.arp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.google.gson.*;
import edu.harvard.iq.dataverse.api.arp.util.JsonHelper;
import edu.harvard.iq.dataverse.util.BundleUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Converts a TSV format metadata block description to a CEDAR template JSON. By default, if a field name contains
 * a '.' character it is converted to ':', because CEDAR doesn't work with dots in field names. However,
 * TsvToCedarTemplate is also used when generating Describo Profile, in which case we need to keep the dot in
 * fieldnames, therefore this class can be used in these two modes but calling the {@link TsvToCedarTemplate} with
 * either true (to convert dots) or false (keep field names as is).
 */
public class TsvToCedarTemplate implements java.io.Serializable {
    private static final Logger logger = Logger.getLogger(TsvToCedarTemplate.class.getCanonicalName());

    private static JsonObject cedarTemplate;
    private static JsonObject cedarTemplateField;
    private static JsonObject cedarStaticTemplateField;
    private static JsonObject cedarTemplateElement;

    private boolean convertDotToColon = true;

    private Locale hunLocale = new Locale("hu");

    static {
        setupCedarTemplateParts();
    }

    private static void setupCedarTemplateParts() {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        String cedarTemplatePath = "arp/cedarSchemaTemplate.json";
        String cedarTemplateFieldPath = "arp/cedarTemplateField.json";
        String cedarStaticTemplateFieldPath = "arp/cedarStaticTemplateField.json";
        String cedarTemplateElementPath = "arp/cedarTemplateElement.json";

        InputStream cedarTemplateInputStream = getCedarTemplateFromResources(cedarTemplatePath);
        InputStream cedarTemplateFieldInputStream = getCedarTemplateFromResources(cedarTemplateFieldPath);
        InputStream cedarStaticTemplateFieldInputStream = getCedarTemplateFromResources(cedarStaticTemplateFieldPath);
        InputStream cedarTemplateElementInputStream = getCedarTemplateFromResources(cedarTemplateElementPath);

        if (cedarTemplateInputStream == null) {
            logger.log(Level.SEVERE, "ArpServiceBean was unable to process " + cedarTemplatePath);
        } else {
            cedarTemplate = gson.fromJson(new InputStreamReader(cedarTemplateInputStream), JsonObject.class);
        }

        if (cedarTemplateFieldInputStream == null) {
            logger.log(Level.SEVERE, "ArpServiceBean was unable to process " + cedarTemplateFieldPath);
        } else {
            cedarTemplateField = gson.fromJson(new InputStreamReader(cedarTemplateFieldInputStream), JsonObject.class);
        }

        if (cedarStaticTemplateFieldInputStream == null) {
            logger.log(Level.SEVERE, "ArpServiceBean was unable to process " + cedarStaticTemplateFieldPath);
        } else {
            cedarStaticTemplateField = gson.fromJson(new InputStreamReader(cedarStaticTemplateFieldInputStream), JsonObject.class);
        }

        if (cedarTemplateElementInputStream == null) {
            logger.log(Level.SEVERE, "ArpServiceBean was unable to process " + cedarTemplateElementPath);
        } else {
            cedarTemplateElement = gson.fromJson(new InputStreamReader(cedarTemplateElementInputStream), JsonObject.class);
        }
    }

    public TsvToCedarTemplate(boolean convertDotToColon)
    {
        this.convertDotToColon = convertDotToColon;
    }

    public TsvToCedarTemplate()
    {
        this(true);
    }

    public JsonObject convert(String tsv) throws JsonProcessingException {
        CsvMapper mapper = new CsvMapper();
        DataverseMetadataBlock metadataBlock = new DataverseMetadataBlock();
        List<DataverseDatasetField> datasetFields = new ArrayList<>();
        List<DataverseControlledVocabulary> controlledVocabularyValues = new ArrayList<>();
        CsvSchema mdbSchema = mapper.typedSchemaFor(DataverseMetadataBlock.class)
                .withoutHeader()
                .withoutQuoteChar()
                .withColumnSeparator('\t')
                .withoutEscapeChar();
        CsvSchema datasetFieldSchema = mapper.typedSchemaFor(DataverseDatasetField.class)
                .withoutHeader()
                .withColumnSeparator('\t')
                .withoutQuoteChar()
                .withoutEscapeChar();
        CsvSchema controlledVocabularySchema = mapper.typedSchemaFor(DataverseControlledVocabulary.class)
                .withoutHeader()
                .withColumnSeparator('\t')
                .withoutQuoteChar()
                .withoutEscapeChar();

        // get the values from the tsv string
        String header = "";
        try (final var scanner = new Scanner(tsv)){
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                if (line.equals("")) {
                    continue;
                }
                if (line.startsWith("#")) {
                    header = line.split("\t")[0];
                } else {
                    switch (header) {
                        case "#metadataBlock":
                            line = String.join("\t", Arrays.copyOfRange(line.split("\t"), 1, 5)).replace("null", "");
                            metadataBlock = mapper.readerWithTypedSchemaFor(DataverseMetadataBlock.class).with(mdbSchema).readValue(line);
                            break;
                        case "#datasetField":
                            line = String.join("\t", Arrays.copyOfRange(line.split("\t"), 1, 17)).replace("null", "");
                            datasetFields.add(mapper.readerWithTypedSchemaFor(DataverseDatasetField.class).with(datasetFieldSchema).readValue(line));
                            break;
                        case "#controlledVocabulary":
                            line = String.join("\t", Arrays.copyOfRange(line.split("\t"), 1, 5)).replace("null", "");
                            controlledVocabularyValues.add(mapper.readerWithTypedSchemaFor(DataverseControlledVocabulary.class).with(controlledVocabularySchema).readValue(line));
                            break;
                        default:
                            break;
                    }
                }
            }
        }

        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        JsonObject jsonSchema = cedarTemplate.deepCopy();

        // process the mdb values
        processMetadataBlock(jsonSchema, metadataBlock);

        // process the dataset field and controlled vocabulary values
        DataverseMetadataBlock finalMetadataBlock = metadataBlock;
        datasetFields.forEach(dsf -> {
            if (!dsf.getParent().isBlank()) {
                return;
            }
            List<DataverseDatasetField> children = datasetFields.stream().filter(datasetField -> datasetField.getParent().equals(dsf.getName())).collect(Collectors.toList());
            JsonArray cvvs = new JsonArray();
            controlledVocabularyValues.stream()
                    .filter(cvv -> cvv.getDatasetField().equals(dsf.getName()))
                    .forEach(cvv -> {
                        String label = cvv.getValue();
                        JsonObject literal = new JsonObject();
                        literal.addProperty("label", label);
                        cvvs.add(literal);
                    });
            if (children.isEmpty()){
                processTemplateField(jsonSchema, dsf, cvvs, finalMetadataBlock);
            } else {
                processTemplateElement(jsonSchema, dsf, children, controlledVocabularyValues, finalMetadataBlock);
            }
        });

        return jsonSchema;
    }

    private void addValueToParent(JsonObject parentObj, JsonObject valueObj, DataverseDatasetField datasetField, boolean parentIsTemplateField, DataverseMetadataBlock dataverseMetadataBlock) {
        /*
         * fieldnames can not contain dots in CEDAR, so we replace them with colons before exporting the template
         * upon importing from CEDAR the colons are replaced with dots again
         * */
        String propName = datasetField.getName();
        if (convertDotToColon) {
            propName = datasetField.getName().replace('.', ':');
        }
        JsonHelper.getJsonElement(parentObj,"_ui.order").getAsJsonArray().add(propName);
        JsonHelper.getJsonElement(parentObj,"_ui.propertyLabels").getAsJsonObject().addProperty(propName, propName);
        JsonHelper.getJsonElement(parentObj,"_ui.propertyDescriptions").getAsJsonObject().addProperty(propName, datasetField.getDescription());

        JsonObject enumObj = new JsonObject();
        JsonArray enumArray = new JsonArray();

        /*
        * In the datasetfieldtype table for every Longitude value (westLongitude, eastLongitude, northLongitude, southLongitude) the termURI is null,
        * however getting the termURI below with the datasetField.getTermURI() function, for southLongitude the result is not an empty string but null,
        * but this value can not be null in the CEDAR Template just an empty string.
        * There's also a comment in DatasetFieldConstant.java that says southLongitude value is "Incorrect in DB".
        * */
        String termUri = datasetField.getTermURI();
        String nameSpaceUri = dataverseMetadataBlock.getBlockURI().endsWith("/") ? dataverseMetadataBlock.getBlockURI() : dataverseMetadataBlock.getBlockURI() + "/";
        String uri = !termUri.isBlank() ? termUri : nameSpaceUri + datasetField.getName() ;
        enumArray.add(uri);
        enumObj.add("enum", enumArray);
        JsonHelper.getJsonElement(parentObj,"properties.@context.properties").getAsJsonObject().add(propName, enumObj);
        parentObj.getAsJsonArray("required").add(propName);

        if (parentIsTemplateField) {
            parentObj.getAsJsonObject("properties").add(propName, valueObj);
        } else {
            JsonHelper.getJsonElement(parentObj,"properties.@context.required").getAsJsonArray().add(propName);
            if (datasetField.isAllowmultiples()) {
                JsonObject arrayObj = new JsonObject();
                arrayObj.addProperty("type", "array");
                arrayObj.addProperty("minItems", 1);
                arrayObj.add("items", valueObj);
                parentObj.getAsJsonObject("properties").add(propName, arrayObj);
            } else {
                parentObj.getAsJsonObject("properties").add(propName, valueObj);
            }
        }

    }

    private void processMetadataBlock(JsonObject jsonSchema, DataverseMetadataBlock dataverseMetadataBlock) {
        jsonSchema.addProperty("title", dataverseMetadataBlock.getName() + jsonSchema.get("title").getAsString());
        jsonSchema.addProperty("description", dataverseMetadataBlock.getName() + jsonSchema.get("description").getAsString());
        jsonSchema.addProperty("schema:name", dataverseMetadataBlock.getDisplayName().isBlank() ? dataverseMetadataBlock.getName() : dataverseMetadataBlock.getDisplayName());
        try {
            var name = BundleUtil.getStringFromPropertyFile("metadatablock.displayName", dataverseMetadataBlock.getName(), hunLocale);
            jsonSchema.addProperty("hunName", name);
        }
        catch(MissingResourceException ex) {
            try {
                var name = BundleUtil.getStringFromPropertyFile("metadatablock.name", dataverseMetadataBlock.getName(), hunLocale);
                jsonSchema.addProperty("name", name);
            } catch (MissingResourceException ex2) {
                // ignore
            }
        }
        jsonSchema.addProperty("schema:identifier", dataverseMetadataBlock.getName());
        JsonHelper.getJsonElement(jsonSchema, "properties.@type.oneOf").getAsJsonArray().forEach(jsonElement -> {
            JsonObject oneOf = jsonElement.getAsJsonObject();
            JsonArray enumArray = new JsonArray();
            enumArray.add(dataverseMetadataBlock.getBlockURI());
            if (oneOf.get("type").getAsString().equals("string")) {
                oneOf.add("enum", enumArray);
            } else if (oneOf.get("type").getAsString().equals("array")) {
                oneOf.getAsJsonObject("items").add("enum", enumArray);
            }
        });
    }

    private void processTemplateField(JsonObject jsonSchema, DataverseDatasetField datasetField, JsonArray cvvs, DataverseMetadataBlock dataverseMetadataBlock) {
        String fieldType = datasetField.getFieldType().toLowerCase();
        JsonObject templateField = cedarTemplateField.deepCopy();

        processCommonFields(templateField, datasetField);
        

        switch (fieldType) {
            case "text":
                if (datasetField.isAllowControlledVocabulary()) {
                    templateField.getAsJsonObject("_ui").addProperty("inputType", "list");
                    templateField.getAsJsonObject("_valueConstraints").addProperty("multipleChoice", datasetField.isAllowmultiples());
                    templateField.getAsJsonObject("_valueConstraints").add("literals", cvvs);


                } else {
                    templateField.getAsJsonObject("_ui").addProperty("inputType", "textfield");
                }
                break;
            case "textbox":
                templateField.getAsJsonObject("_ui").addProperty("inputType", "textarea");
                break;
            case "date":
                templateField.getAsJsonObject("_ui").addProperty("inputType", "temporal");
                templateField.getAsJsonObject("_ui").addProperty("temporalGranularity", "day");
                templateField.getAsJsonObject("_valueConstraints").addProperty("temporalType", "xsd:date");
                break;
            case "email":
                templateField.getAsJsonObject("_ui").addProperty("inputType", "email");
                break;
            case "int":
            case "float":
                String numberType = datasetField.getFieldType().equals("int") ? "xsd:int" : "xsd:double";
                templateField.getAsJsonObject("_valueConstraints").addProperty("numberType", numberType);
                templateField.getAsJsonObject("_ui").addProperty("inputType", "numeric");
                JsonObject typeObj = new JsonObject();
                typeObj.addProperty("type", "string");
                typeObj.addProperty("format", "uri");
                templateField.getAsJsonObject("properties").add("@type", typeObj);
                templateField.getAsJsonArray("required").add("@type");
                break;
            case "url":
                templateField.getAsJsonObject("_ui").addProperty("inputType", "link");
                templateField.remove("required");
                templateField.getAsJsonObject("properties").remove("@value");
                JsonObject idObj = new JsonObject();
                idObj.addProperty("type", "string");
                idObj.addProperty("format", "uri");
                templateField.getAsJsonObject("properties").add("@id", idObj);
                break;
            default:
                break;
        }

        addValueToParent(jsonSchema, templateField, datasetField, true, dataverseMetadataBlock);
    }

    private void processTemplateElement(JsonObject jsonSchema, DataverseDatasetField datasetField, List<DataverseDatasetField> children, List<DataverseControlledVocabulary> controlledVocabularyValues, DataverseMetadataBlock dataverseMetadataBlock) {
        JsonObject templateElement = cedarTemplateElement.deepCopy();
        processCommonFields(templateElement, datasetField);
        children.forEach(child -> {
            JsonArray cvvs = new JsonArray();
            controlledVocabularyValues.stream()
                    .filter(cvv -> cvv.getDatasetField().equals(child.getName()))
                    .forEach(cvv -> {
                        String label = cvv.getValue();
                        JsonObject literal = new JsonObject();
                        literal.addProperty("label", label);
                        cvvs.add(literal);
                    });
            processTemplateField(templateElement, child, cvvs, dataverseMetadataBlock);
        });

        addValueToParent(jsonSchema, templateElement, datasetField, false, dataverseMetadataBlock);
    }

    private void processCommonFields(JsonObject cedarTemplate, DataverseDatasetField datasetField) {
        /*
         * fieldnames can not contain dots in CEDAR, so we replace them with colons before exporting the template
         * upon importing from CEDAR the colons are replaced with dots again
         * */
        String propName = datasetField.getName();
        if (convertDotToColon) {
                propName = propName.replace('.', ':');
        }
        cedarTemplate.remove("@id");
        cedarTemplate.addProperty("@id", "tmp-placeholder");
        cedarTemplate.addProperty("title", propName + cedarTemplate.get("title").getAsString());
        cedarTemplate.addProperty("description", propName + cedarTemplate.get("description").getAsString());

        cedarTemplate.addProperty("schema:name", propName);
        cedarTemplate.addProperty("schema:description", datasetField.getDescription());
        try {
            cedarTemplate.addProperty("hunDescription", BundleUtil.getStringFromPropertyFile("datasetfieldtype." + propName + ".description", datasetField.getmetadatablock_id(), hunLocale));
        }
        catch(MissingResourceException ex) {
            // ignore
        }
        if (datasetField.getTitle() != null) {
            cedarTemplate.addProperty("skos:prefLabel", datasetField.getTitle());
            try {
                cedarTemplate.addProperty("hunLabel", BundleUtil.getStringFromPropertyFile("datasetfieldtype." + propName + ".title", datasetField.getmetadatablock_id(), hunLocale));
            }
            catch (MissingResourceException ex) {
                logger.fine("MissingResourceException for key: datasetfieldtype."+propName+".title, datasetField.getmetadatablock_id(): "+datasetField.getmetadatablock_id()+", hunLocale: hunLocale");
				try {
					Thread.sleep(10000L);
				} catch (InterruptedException ex1) {
					Logger.getLogger(TsvToCedarTemplate.class.getName()).log(Level.SEVERE, null, ex1);
				}
            }
        }
        if (datasetField.isAllowmultiples()) {
            cedarTemplate.addProperty("minItems", 1);
            cedarTemplate.addProperty("maxItems ", 0);
        }
        if (cedarTemplate.has("_valueConstraints")) {
            cedarTemplate.getAsJsonObject("_valueConstraints").addProperty("requiredValue", datasetField.isRequired());
        } else {
            var valueConstraint = new JsonObject();
            valueConstraint.addProperty("requiredValue", datasetField.isRequired());
            cedarTemplate.add("_valueConstraints", valueConstraint);
        }
        
        processArpFields(cedarTemplate, datasetField);
    }
    
    private void processArpFields(JsonObject cedarTemplate, DataverseDatasetField datasetField) {
        addArpProperty(cedarTemplate, "watermark", datasetField.getWatermark());
        addArpProperty(cedarTemplate, "displayFormat", datasetField.getDisplayFormat());
        addArpProperty(cedarTemplate, "advancedSearchField", datasetField.isAdvancedSearchField());
        addArpProperty(cedarTemplate, "facetable", datasetField.isFacetable());
        addArpProperty(cedarTemplate, "displayoncreate", datasetField.isDisplayoncreate());
    }
    
    private void addArpProperty(JsonObject cedarTemplate, String propName, Object value) {
        if (value != null) {
            if (value instanceof String) {
                if (((String) value).isBlank()) {
                    return;
                }
            }
            JsonElement jsonValue = value instanceof String ? new JsonPrimitive((String) value) : new JsonPrimitive((boolean) value);
            if (cedarTemplate.has("_arp")) {
                var arpPart = cedarTemplate.getAsJsonObject("_arp");
                if (arpPart.has("dataverse")) {
                    var dvPart = arpPart.getAsJsonObject("dataverse");
                    dvPart.add(propName, jsonValue);
                } else {
                    JsonObject dvValue = new JsonObject();
                    dvValue.add(propName, jsonValue);
                    arpPart.add("dataverse", dvValue);
                }
            } else {
                JsonObject arpPart = new JsonObject();
                JsonObject dvValue = new JsonObject();
                dvValue.add(propName, jsonValue);
                arpPart.add("dataverse", dvValue);
                cedarTemplate.add("_arp", arpPart);
            }   
        }
    }

    private static InputStream getCedarTemplateFromResources(String fileName) {

        try (InputStream input = TsvToCedarTemplate.class.getClassLoader().getResourceAsStream(fileName)) {
            if (input == null) {
                logger.log(Level.SEVERE, "ArpServiceBean was unable to load " + fileName);
            }
            return input;
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return null;
    }
}


