package edu.harvard.iq.dataverse.arp;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.google.gson.*;
import edu.harvard.iq.dataverse.ControlledVocabularyValueServiceBean;
import edu.harvard.iq.dataverse.MetadataBlock;
import edu.harvard.iq.dataverse.MetadataBlockServiceBean;
import edu.harvard.iq.dataverse.api.arp.util.JsonHelper;
import edu.harvard.iq.dataverse.util.BundleUtil;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.inject.Named;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static java.net.http.HttpResponse.BodyHandlers.ofString;

@Stateless
@Named
public class ArpServiceBean implements java.io.Serializable {
    private static final Logger logger = Logger.getLogger(ArpServiceBean.class.getCanonicalName());

    private static JsonObject cedarTemplate;
    private static JsonObject cedarTemplateField;
    private static JsonObject cedarStaticTemplateField;
    private static JsonObject cedarTemplateElement;


    static {
        setCedarTemplateJsons();
    }

    @EJB
    MetadataBlockServiceBean metadataBlockService;

    @EJB
    ControlledVocabularyValueServiceBean controlledVocabularyValueService;

    public String exportMdbAsTsv(String mdbId) throws JsonProcessingException {
        MetadataBlock mdb = metadataBlockService.findById(Long.valueOf(mdbId));

        CsvSchema mdbSchema = CsvSchema.builder()
                .addColumn("#metadataBlock")
                .addColumn("name")
                .addColumn("dataverseAlias")
                .addColumn("displayName")
                .addColumn("blockURI")
                .build()
                .withHeader()
                .withColumnSeparator('\t')
                .withoutQuoteChar()
                .withoutEscapeChar();

        StringJoiner mdbRow = new StringJoiner("\t");
        mdbRow.add("");
        mdbRow.add(mdb.getName() != null ? mdb.getName() : "");
//      TODO: handle dataverseAlias?
        mdbRow.add("");
        mdbRow.add(mdb.getDisplayName());
        mdbRow.add(mdb.getNamespaceUri());

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

        StringJoiner dsfRows = new StringJoiner("\n");
        StringJoiner cvRows = new StringJoiner("\n");
        mdb.getDatasetFieldTypes().forEach(dsf -> {
            StringJoiner dsfRowValues = new StringJoiner("\t");
            dsfRowValues.add("");
            dsfRowValues.add(dsf.getName());
            dsfRowValues.add(dsf.getTitle());
            dsfRowValues.add(dsf.getDescription());
            dsfRowValues.add(dsf.getWatermark());
            dsfRowValues.add(String.valueOf(dsf.getFieldType()));
            dsfRowValues.add(String.valueOf(dsf.getDisplayOrder()));
            dsfRowValues.add(dsf.getDisplayFormat());
            dsfRowValues.add(String.valueOf(dsf.isAdvancedSearchFieldType()));
            dsfRowValues.add(String.valueOf(dsf.isAllowControlledVocabulary()));
            dsfRowValues.add(String.valueOf(dsf.isAllowMultiples()));
            dsfRowValues.add(String.valueOf(dsf.isFacetable()));
            dsfRowValues.add(String.valueOf(dsf.isDisplayOnCreate()));
            dsfRowValues.add(String.valueOf(dsf.isRequired()));
            dsfRowValues.add(dsf.getParentDatasetFieldType() != null ? dsf.getParentDatasetFieldType().getName() : "");
            dsfRowValues.add(mdb.getName());
            dsfRowValues.add(dsf.getUri());
            dsfRows.add(dsfRowValues.toString().replace("null", ""));

            controlledVocabularyValueService.findByDatasetFieldTypeId(dsf.getId()).forEach(cvv -> {
                StringJoiner cvRowValues = new StringJoiner("\t");
                cvRowValues.add("");
                cvRowValues.add(dsf.getName());
                cvRowValues.add(cvv.getStrValue());
                cvRowValues.add(cvv.getIdentifier());
                cvRowValues.add(String.valueOf(cvv.getDisplayOrder()));
                cvRows.add(cvRowValues.toString().replace("null", ""));
            });
        });

        CsvMapper mapper = new CsvMapper();
        String metadataBlocks = mapper.writer(mdbSchema).writeValueAsString(mdbRow.toString().replace("null", "")).strip();
        String datasetFieldValues = mapper.writer(datasetFieldSchema).writeValueAsString(dsfRows.toString()).stripTrailing();
        String controlledVocabularyValues = mapper.writer(controlledVocabularySchema).writeValueAsString(cvRows.toString()).stripTrailing();

        return metadataBlocks + "\n" + datasetFieldValues + "\n" + controlledVocabularyValues;
    }

    public JsonObject tsvToCedarTemplate(String tsv) throws JsonProcessingException {
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
            if (!dsf.parent.isBlank()) {
                return;
            }
            List<DataverseDatasetField> children = datasetFields.stream().filter(datasetField -> datasetField.parent.equals(dsf.name)).collect(Collectors.toList());
            JsonArray cvvs = new JsonArray();
            controlledVocabularyValues.stream()
                    .filter(cvv -> cvv.datasetField.equals(dsf.name))
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
        String propName = datasetField.getName().replace('.', ':');
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

        processCommonFields(templateField, datasetField, true);
        templateField.getAsJsonObject("_valueConstraints").addProperty("requiredValue", datasetField.isRequired());
        

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
        processCommonFields(templateElement, datasetField, false);
        children.forEach(child -> {
            JsonArray cvvs = new JsonArray();
            controlledVocabularyValues.stream()
                    .filter(cvv -> cvv.datasetField.equals(child.getName()))
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

    private void processCommonFields(JsonObject cedarTemplate, DataverseDatasetField datasetField, boolean parentIsTemplateField) {
        /*
         * fieldnames can not contain dots in CEDAR, so we replace them with colons before exporting the template
         * upon importing from CEDAR the colons are replaced with dots again
         * */
        String propName = datasetField.getName().replace('.', ':');
        cedarTemplate.remove("@id");
        cedarTemplate.addProperty("@id", "tmp-placeholder");
        cedarTemplate.addProperty("title", propName + cedarTemplate.get("title").getAsString());
        cedarTemplate.addProperty("description", propName + cedarTemplate.get("description").getAsString());

        cedarTemplate.addProperty("schema:name", propName);
        cedarTemplate.addProperty("schema:description", datasetField.getDescription());
        if (datasetField.getTitle() != null) {
            cedarTemplate.addProperty("skos:prefLabel", datasetField.getTitle());
        }
        if (parentIsTemplateField && datasetField.isAllowmultiples()) {
            cedarTemplate.addProperty("minItems", 1);
            cedarTemplate.addProperty("maxItems ", 0);
        }
    }

    private static InputStream getCedarTemplateFromResources(String fileName) {

        try (InputStream input = ArpServiceBean.class.getClassLoader().getResourceAsStream(fileName)) {
            if (input == null) {
                logger.log(Level.SEVERE, "ArpServiceBean was unable to load " + fileName);
            }
            return input;
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    private String checkOrCreateFolder(String parentFolderId, String folderName, String apiKey, String cedarDomain, HttpClient client) throws Exception {
        String encodedFolderId = URLEncoder.encode(parentFolderId, StandardCharsets.UTF_8);
        String url = "https://resource." + cedarDomain + "/folders/" + encodedFolderId +
                "/contents?limit=100&offset=0&publication_status=all&resource_types=template,folder&sort=name&version=all";

        HttpRequest listFolderRequest = HttpRequest.newBuilder()
                .uri(new URI(url))
                .header("Authorization", "apiKey " + apiKey)
                .build();
        String folderJson = client.send(listFolderRequest, ofString()).body();

        AtomicReference<String> folderId = new AtomicReference<>("");
        JsonNode listFolderResources = new ObjectMapper().readTree(folderJson).get("resources");
        listFolderResources.forEach(resource -> {if (resource.get("resourceType").asText().equals("folder") && resource.get("schema:name").asText().equals(folderName)) {
            folderId.set(resource.get("@id").textValue());
        }
        });

        if (folderId.get().equals("")) {
            folderId.set(createFolder(folderName, folderName + " description", parentFolderId, cedarDomain, apiKey, client));
        }

        return folderId.get();
    }

    public String createFolder(String name, String description, String parentFolderId, String cedarDomain, String apiKey, HttpClient client) throws Exception {
        String url = "https://resource." + cedarDomain + "/folders";
        Map<String, Object> data = new HashMap<>();
        data.put("folderId", parentFolderId);
        data.put("name", name);
        data.put("description", description);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .POST(HttpRequest.BodyPublishers.ofString(new ObjectMapper().writeValueAsString(data)))
                .header("Authorization", "apiKey " + apiKey)
                .header("Content-Type", "application/json")
                .build();
        HttpResponse<String> createFolderResponse = client.send(request, ofString());
        if (createFolderResponse.statusCode() != 201) {
            throw new Exception("An error occurred during creating new folder: " + name);
        }
        return new ObjectMapper().readTree(createFolderResponse.body()).get("@id").textValue();
    }

    public void exportTemplateToCedar(JsonNode cedarTemplate, JsonNode cedarParams, String cedarDomain) throws Exception {
        //TODO: uncomment the line below and delete the UnsafeHttpClient, that is for testing purposes only, until we have working CEDAR certs
        // HttpClient client = HttpClient.newHttpClient();
        HttpClient client = getUnsafeHttpClient();
        String templateName = cedarTemplate.get("schema:name").textValue();
        String parentFolderId = cedarParams.get("folderId").textValue();
        String apiKey = cedarParams.get("apiKey").textValue();
        if (cedarParams.has("cedarDomain")) {
            cedarDomain = cedarParams.get("cedarDomain").textValue();
        }
        String templateFolderId = checkOrCreateFolder(parentFolderId, templateName, apiKey, cedarDomain, client);
        uploadTemplate(cedarTemplate, cedarParams, templateFolderId, cedarDomain, client);
    }

    private void uploadTemplate(JsonNode cedarTemplate, JsonNode cedarParams, String templateFolderId, String cedarDomain, HttpClient client) throws Exception {
        String encodedFolderId = URLEncoder.encode(templateFolderId, StandardCharsets.UTF_8);
        String url = "https://resource." + cedarDomain + "/templates?folder_id=" + encodedFolderId;

        // Update the template with the uploaded elements, to have their real @id
        List<JsonNode> elements = exportElements(cedarTemplate, cedarParams, templateFolderId, cedarDomain, client);
        elements.forEach(element -> {
            ObjectNode properties = (ObjectNode) cedarTemplate.get("properties");
            ObjectNode prop = (ObjectNode) properties.get(element.get("schema:name").textValue());
            if (prop.has("items")) {
                prop.replace("items", element);
            } else {
                properties.replace(element.get("schema:name").textValue(), element);
            }
        });
        
        String apiKey = cedarParams.get("apiKey").textValue();
        HttpRequest httpRequest = HttpRequest.newBuilder()
                .uri(new URI(url))
                .headers("Authorization", "apiKey " + apiKey, "Content-Type", "application/json", "Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(new ObjectMapper().writeValueAsString(cedarTemplate)))
                .build();

        HttpResponse<String> uploadTemplateResponse = client.send(httpRequest, ofString());
        if (uploadTemplateResponse.statusCode() != 201) {
            throw new Exception("An error occurred during uploading the template: " + cedarTemplate.get("schema:name").textValue());
        }
    }

    private List<JsonNode> exportElements(JsonNode cedarTemplate, JsonNode cedarParams, String templateFolderId, String cedarDomain, HttpClient client) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        String apiKey = cedarParams.get("apiKey").textValue();
        String elementsFolderId = checkOrCreateFolder(templateFolderId, "elements", apiKey, cedarDomain, client);
        String encodedFolderId = URLEncoder.encode(elementsFolderId, StandardCharsets.UTF_8);
        String url = "https://resource." + cedarDomain + "/template-elements?folder_id=" + encodedFolderId;
        List<HttpRequest> templateElementsRequests = new ArrayList<>();

        cedarTemplate.get("properties").forEach(prop -> {
            if ((prop.has("@type") && prop.get("@type").textValue().equals("https://schema.metadatacenter.org/core/TemplateElement")) ||
                    (prop.has("items") && (prop.get("items").has("@type") && prop.get("items").get("@type").textValue().equals("https://schema.metadatacenter.org/core/TemplateElement")))) {
                try {
                    ((ObjectNode) prop).remove("@id");
                    if (prop.has("items")) {
                        ((ObjectNode) prop.get("items")).remove("@id");
                        prop = prop.get("items");
                    }
                    HttpRequest httpRequest = HttpRequest.newBuilder()
                            .uri(new URI(url))
                            .headers("Authorization", "apiKey " + apiKey, "Content-Type", "application/json", "Accept", "application/json")
                            .POST(HttpRequest.BodyPublishers.ofString(new ObjectMapper().writeValueAsString(prop)))
                            .build();

                    templateElementsRequests.add(httpRequest);
                } catch (Exception e) {
                    throw new RuntimeException(e.getMessage());
                }
            }
        });
        
        List<HttpResponse<String>> responses = templateElementsRequests.stream()
                .map(request -> client.sendAsync(request, ofString())
                        .thenApply(response -> {
                            if (response.statusCode() != 201) {
                                throw new RuntimeException("An error occured during uploading the template elements for template: " + cedarTemplate.get("schema:name").textValue());
                            }
                            return response;
                        }))
                .map(CompletableFuture::join)
                .collect(Collectors.toList());
        
        return responses.stream().map(response -> {
            try {
                return mapper.readTree(response.body());
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());

    }

    public HttpClient getUnsafeHttpClient() throws NoSuchAlgorithmException, KeyManagementException {
        TrustManager[] trustAllCerts = new TrustManager[] { new X509TrustManager() {
            public X509Certificate[] getAcceptedIssuers() {
                return new X509Certificate[0];
            }
            public void checkClientTrusted(X509Certificate[] certs, String authType) {}
            public void checkServerTrusted(X509Certificate[] certs, String authType) {}
        } };
        SSLContext sslContext = SSLContext.getInstance("SSL");
        sslContext.init(null, trustAllCerts, new SecureRandom());
        return HttpClient.newBuilder()
                .sslContext(sslContext)
                .build();
    }

    private static void setCedarTemplateJsons() {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        String cedarTemplatePath = BundleUtil.getStringFromBundle("arp.cedar.template.schema");
        String cedarTemplateFieldPath = BundleUtil.getStringFromBundle("arp.cedar.template.field");
        String cedarStaticTemplateFieldPath = BundleUtil.getStringFromBundle("arp.cedar.static.template.field");
        String cedarTemplateElementPath = BundleUtil.getStringFromBundle("arp.cedar.template.element");

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

//TODO: use these and remove the others from CedarTemplateToDvMdbConverter
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonPropertyOrder({"name", "dataverseAlias", "displayName", "blockURI"})
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

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonPropertyOrder({"name", "title", "description", "watermark", "fieldType", "displayOrder", "displayFormat", "advancedSearchField", "allowControlledVocabulary", "allowmultiples", "facetable", "displayoncreate", "required", "parent", "metadatablock_id", "termURI"})
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

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonPropertyOrder({"DatasetField", "Value", "identifier", "displayOrder"})
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


