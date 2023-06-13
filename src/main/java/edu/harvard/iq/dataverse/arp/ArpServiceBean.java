package edu.harvard.iq.dataverse.arp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.google.gson.*;
import edu.harvard.iq.dataverse.*;
import edu.harvard.iq.dataverse.actionlogging.ActionLogRecord;
import edu.harvard.iq.dataverse.actionlogging.ActionLogServiceBean;
import edu.harvard.iq.dataverse.api.AbstractApiBean;
import edu.harvard.iq.dataverse.api.DatasetFieldServiceApi;
import edu.harvard.iq.dataverse.api.arp.*;
import edu.harvard.iq.dataverse.*;
import edu.harvard.iq.dataverse.api.arp.util.JsonHelper;
import edu.harvard.iq.dataverse.search.SearchFields;
import edu.harvard.iq.dataverse.util.BundleUtil;
import edu.harvard.iq.dataverse.util.json.NullSafeJsonBuilder;
import org.apache.commons.lang3.StringUtils;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.inject.Named;
import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.io.InputStream;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.*;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
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
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static edu.harvard.iq.dataverse.api.arp.util.JsonHelper.*;
import static edu.harvard.iq.dataverse.api.arp.util.JsonHelper.getJsonObject;
import static java.net.http.HttpResponse.BodyHandlers.ofString;
import static javax.ws.rs.core.Response.Status.FORBIDDEN;

@Stateless
@Named
public class ArpServiceBean implements java.io.Serializable {
    private static final Logger logger = Logger.getLogger(ArpServiceBean.class.getCanonicalName());

    @EJB
    protected MetadataBlockServiceBean metadataBlockSvc;

    @EJB
    MetadataBlockServiceBean metadataBlockService;

    @EJB
    ControlledVocabularyValueServiceBean controlledVocabularyValueService;

    @EJB
    DatasetFieldServiceApiBean datasetFieldServiceApi;

    @EJB
    DatasetFieldServiceBean datasetFieldService;

    @EJB
    DataverseServiceBean dataverseService;

    @EJB
    ArpMetadataBlockServiceBean arpMetadataBlockServiceBean;

    @EJB
    protected ActionLogServiceBean actionLogSvc;

    @EJB
    protected ArpConfig arpConfig;

    @PersistenceContext(unitName = "VDCNet-ejbPU")
    private EntityManager em;

    public static String RO_CRATE_METADATA_JSON_NAME = "ro-crate-metadata.json";

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
        return tsvToCedarTemplate(tsv, true);
    }

    public JsonObject tsvToCedarTemplate(String tsv, boolean convertDotToColon) throws JsonProcessingException {
        var converter = new TsvToCedarTemplate(convertDotToColon);
        return converter.convert(tsv);
    }

    private static boolean isURLEncoded(String param) {
        Pattern urlencodedPattern = Pattern.compile("%[0-9a-fA-F]{2}");
        return urlencodedPattern.matcher(param).find();
    }

    public static String encodeURLParameter(String urlParam) throws UnsupportedEncodingException
    {
        if (isURLEncoded(urlParam)) {
            return urlParam;
        } else {
            return URLEncoder.encode(urlParam, "UTF-8");
        }
    }

    public static String decodeURLParameter(String urlParam) throws UnsupportedEncodingException
    {
        if (!isURLEncoded(urlParam)) {
            return urlParam;
        } else {
            return URLDecoder.decode(urlParam, "UTF-8");
        }
    }



    private String checkOrCreateFolder(String parentFolderId, String folderName, String apiKey, String cedarDomain, HttpClient client) throws Exception {
        String decodedParentFolderId = decodeURLParameter(parentFolderId);
        String encodedFolderId = encodeURLParameter(parentFolderId);
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
            folderId.set(createFolder(folderName, folderName + " description", decodedParentFolderId, cedarDomain, apiKey, client));
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

    public String exportTemplateToCedar(JsonNode cedarTemplate, ExportToCedarParams cedarParams) throws Exception {
        //TODO: uncomment the line below and delete the UnsafeHttpClient, that is for testing purposes only, until we have working CEDAR certs
        // HttpClient client = HttpClient.newHttpClient();
        HttpClient client = getUnsafeHttpClient();
        String templateName = cedarTemplate.get("schema:name").textValue();
        String parentFolderId = cedarParams.folderId;
        String apiKey = cedarParams.apiKey;
        String cedarDomain = cedarParams.cedarDomain;
        String templateFolderId = checkOrCreateFolder(parentFolderId, templateName, apiKey, cedarDomain, client);
        return uploadTemplate(cedarTemplate, cedarParams, templateFolderId, client);
    }

    private String uploadTemplate(JsonNode cedarTemplate, ExportToCedarParams cedarParams, String templateFolderId, HttpClient client) throws Exception {
        String encodedFolderId = URLEncoder.encode(templateFolderId, StandardCharsets.UTF_8);
        String cedarDomain = cedarParams.cedarDomain;
        String url = "https://resource." + cedarDomain + "/templates?folder_id=" + encodedFolderId;

        // Update the template with the uploaded elements, to have their real @id
        List<JsonNode> elements = exportElements(cedarTemplate, cedarParams, templateFolderId, client);
        elements.forEach(element -> {
            ObjectNode properties = (ObjectNode) cedarTemplate.get("properties");
            ObjectNode prop = (ObjectNode) properties.get(element.get("schema:name").textValue());
            if (prop.has("items")) {
                prop.replace("items", element);
            } else {
                properties.replace(element.get("schema:name").textValue(), element);
            }
        });
        
        String apiKey = cedarParams.apiKey;
        HttpRequest httpRequest = HttpRequest.newBuilder()
                .uri(new URI(url))
                .headers("Authorization", "apiKey " + apiKey, "Content-Type", "application/json", "Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(new ObjectMapper().writeValueAsString(cedarTemplate)))
                .build();

        HttpResponse<String> uploadTemplateResponse = client.send(httpRequest, ofString());
        if (uploadTemplateResponse.statusCode() != 201) {
            throw new Exception("An error occurred during uploading the template: " + cedarTemplate.get("schema:name").textValue());
        }
        return uploadTemplateResponse.body();
    }

    private List<JsonNode> exportElements(JsonNode cedarTemplate, ExportToCedarParams cedarParams, String templateFolderId, HttpClient client) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        String apiKey = cedarParams.apiKey;
        String cedarDomain = cedarParams.cedarDomain;
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

    public List<String> getExternalVocabValues(JsonObject cedarFieldTemplate)
    {
        String externalVocabUrl = getExternalVocabValuesUrl(cedarFieldTemplate);
        try {
            return collectExternalVocabStrings(externalVocabUrl);
        }catch (Exception ex) {
            logger.log(Level.SEVERE, "Failed collecting external vocabulary values for field: " + cedarFieldTemplate.get("schema:name").getAsString() + " with error: " + ex.getMessage(), ex);
            return new ArrayList<>();
        }
    }

    private String getExternalVocabValuesUrl(JsonObject cedarTemplateField) {
        String externalVocabUrl = null;
        String terminologyTemplate = arpConfig.get("terminology.url.template");
        JsonObject externalVocabProps = cedarTemplateField.has("items") && cedarTemplateField.has("type") ? JsonHelper.getJsonObject(cedarTemplateField, "items._valueConstraints.branches[0]") : JsonHelper.getJsonObject(cedarTemplateField, "_valueConstraints.branches[0]");
        if (externalVocabProps != null) {
            String encodedUri = URLEncoder.encode(externalVocabProps.get("uri").getAsString(), StandardCharsets.UTF_8);
            externalVocabUrl = String.format(terminologyTemplate, externalVocabProps.get("acronym").getAsString(), encodedUri);
        }

        return externalVocabUrl;
    }

    private List<String> collectExternalVocabStrings(String externalVocabUrl) throws URISyntaxException, IOException, InterruptedException, NoSuchAlgorithmException, KeyManagementException {
        List<String > externalVocabStrings = new ArrayList<>();
        //TODO: uncomment the line below and delete the UnsafeHttpClient, that is for testing purposes only, until we have working CEDAR certs
        // HttpClient client = HttpClient.newHttpClient();
        HttpClient client = getUnsafeHttpClient();
        HttpRequest getExternalVocabValues = HttpRequest.newBuilder()
                .uri(new URI(externalVocabUrl))
                .build();
        String externalVocabValues = client.send(getExternalVocabValues, ofString()).body();
        JsonNode externalVocabValuesCollection = new ObjectMapper().readTree(externalVocabValues).get("collection");
        externalVocabValuesCollection.forEach(value -> {
            externalVocabStrings.add(value.get("prefLabel").textValue());
        });
        
        return externalVocabStrings;
        
    }

    public List<ControlledVocabularyValue> collectExternalVocabValues(DatasetFieldType datasetFieldType) throws ArpException {
        DatasetFieldTypeArp datasetFieldTypeArp = arpMetadataBlockServiceBean.findDatasetFieldTypeArpForFieldType(datasetFieldType);
        List<ControlledVocabularyValue> externalVocabValues = new ArrayList<>();
        try {
            JsonObject cedarFieldTemplate = new Gson().fromJson(datasetFieldTypeArp.getCedarDefinition(), JsonObject.class);
            String externalVocabUrl = getExternalVocabValuesUrl(cedarFieldTemplate);
            int i = 0;
            var controlledVocabValues = controlledVocabularyValueService.findByDatasetFieldTypeId(datasetFieldType.getId());
            if (externalVocabUrl != null) {
                for (var externalString : collectExternalVocabStrings(externalVocabUrl)) {
                    //At this point we presume there are no duplicated values in the lists, even if there are duplicated values
                    //only their index will be the same, but DV can handle this
                    var controlledVocabValue = controlledVocabValues.stream().filter(cvv -> cvv.getStrValue().equals(externalString)).findFirst();
                    ControlledVocabularyValue cvv;
                    if (controlledVocabValue.isPresent()) {
                        cvv = controlledVocabValue.get();
                        cvv.setDisplayOrder(i);
                    } else {
                        cvv = new ControlledVocabularyValue();
                        cvv.setStrValue(externalString);
                        cvv.setDatasetFieldType(datasetFieldType);
                        cvv.setDisplayOrder(i);
                        cvv.setIdentifier("");
                    }
                    externalVocabValues.add(cvv);
                    i++;
                }
            }
        } catch (Exception e) {
            String errorMessage;
            if (datasetFieldTypeArp != null && datasetFieldTypeArp.getFieldType() != null && datasetFieldTypeArp.getFieldType().getName() != null) {
                errorMessage = "Failed to collect external vocabulary values for " + datasetFieldTypeArp.getFieldType().getName()  + ". Details: " + e.getMessage();
            } else {
                errorMessage = "Failed to collect external vocabulary values. Details: " + e.getMessage();
            }
            logger.severe(errorMessage);
            throw new ArpException(errorMessage);
        }
        
        return externalVocabValues;
    }


    public void updateMetadatablockNamesaceUris(Map<String, String> mdbToNamespaceUri) {
        mdbToNamespaceUri.keySet().forEach(name -> {
            var mdb = metadataBlockService.findByName(name);
            if (mdb == null) {
                throw new Error("Invalid metadatablock name '"+name+"'");
            }
            var uri = mdbToNamespaceUri.get(name);
            if (uri.equals("null")) {
                mdb.setNamespaceUri(null);
            }
            else {
                mdb.setNamespaceUri(uri);
            }
        });
    }
    
    public String convertTemplateToDvMdb(String cedarTemplate, Set<String> overridePropNames) throws Exception {
        String conversionResult;
        
        try {
            CedarTemplateToDvMdbConverter cedarTemplateToDvMdbConverter = new CedarTemplateToDvMdbConverter();
            conversionResult = cedarTemplateToDvMdbConverter.processCedarTemplate(cedarTemplate, overridePropNames);
        } catch (Exception exception) {
            throw new Exception("An error occurred during converting the template", exception);
        }
        
        return conversionResult;
    }

    public String convertTemplateToDescriboProfile(String cedarTemplate, String language) throws Exception {
        String conversionResult;

        try {
            CedarTemplateToDescriboProfileConverter cedarTemplateToDescriboProfileConverter = new CedarTemplateToDescriboProfileConverter(language);
            conversionResult = cedarTemplateToDescriboProfileConverter.processCedarTemplate(cedarTemplate);
        } catch (Exception exception) {
            throw new Exception("An error occurred during converting the template", exception);
        }

        return conversionResult;
    }

    // This function is copied from edu.harvard.iq.dataverse.api.DatasetFieldServiceApi but modified to process String lists
    public void loadDatasetFields(List<String> lines, String templateName, String templateJson) throws Exception {
        ActionLogRecord alr = new ActionLogRecord(ActionLogRecord.ActionType.Admin, "loadDatasetFields");
        alr.setInfo( templateName );
        String splitBy = "\t";
        int lineNumber = 0;
        DatasetFieldServiceApi.HeaderType header = null;
        JsonArrayBuilder responseArr = Json.createArrayBuilder();
        String[] values;
        Gson gson = new Gson();
        JsonObject cedarTemplate = gson.fromJson(templateJson, JsonObject.class);
        var cedarFieldJsonDefs = JsonHelper.collectTemplateFields(cedarTemplate);
        var conformsToId = cedarTemplate.get("@id").getAsString();
        try {
            for (String line : lines) {
                if (line.equals("")) {
                    continue;
                }
                lineNumber++;
                values = line.split(splitBy);
                if (values[0].startsWith("#")) { // Header row
                    switch (values[0]) {
                        case "#metadataBlock":
                            header = DatasetFieldServiceApi.HeaderType.METADATABLOCK;
                            break;
                        case "#datasetField":
                            header = DatasetFieldServiceApi.HeaderType.DATASETFIELD;
                            break;
                        case "#controlledVocabulary":
                            header = DatasetFieldServiceApi.HeaderType.CONTROLLEDVOCABULARY;
                            break;
                        default:
                            throw new IOException("Encountered unknown #header type at line lineNumber " + lineNumber);
                    }
                } else {
                    switch (header) {
                        case METADATABLOCK:
                            var mdb = parseMetadataBlock(values);
                            responseArr.add( Json.createObjectBuilder()
                                    .add("name", mdb.getName())
                                    .add("type", "MetadataBlock"));
                            // Add/update MetadataBlockArp values
                            var mdbArp = arpMetadataBlockServiceBean.findMetadataBlockArpForMetadataBlock(mdb);
                            if (mdbArp == null) {
                                mdbArp = new MetadataBlockArp();
                            }
                            mdbArp.setMetadataBlock(mdb);
                            mdbArp.setCedarDefinition(templateJson);
                            mdbArp.setRoCrateConformsToId(conformsToId);
                            arpMetadataBlockServiceBean.save(mdbArp);
                            break;

                        case DATASETFIELD:
                            var dsf = parseDatasetField(values);
                            var fieldName = dsf.getName();
                            responseArr.add( Json.createObjectBuilder()
                                    .add("name", fieldName)
                                    .add("type", "DatasetField") );
                            // Add/update DatasetFieldTypeArp value
                            var dsfArp = arpMetadataBlockServiceBean.findDatasetFieldTypeArpForFieldType(dsf);
                            if (dsfArp == null) {
                                dsfArp = new DatasetFieldTypeArp();
                            }
                            // Make sure we convert '.' to ':' (CEDAR doesn't support file dnames with dots)
                            var cedarFieldDef = cedarFieldJsonDefs.get(fieldName.replace(".", ":")).getAsJsonObject();
                            dsfArp.setCedarDefinition(cedarFieldDef.toString());
                            // if it is an array, get the actual field def where we store the _arp values
                            if (cedarFieldDef.has("items")) {
                                cedarFieldDef = cedarFieldDef.getAsJsonObject("items");
                            }
                            // Set values form _arp.dataverse
                            var _arpData = cedarFieldDef.has("_arp")
                                    ? cedarFieldDef.get("_arp").getAsJsonObject() : null;
                            if (_arpData != null) {
                                var dataverseData = _arpData.getAsJsonObject("dataverse");
                                if (dataverseData.has("displayNameField")) {
                                    var displayNameField = dataverseData.get("displayNameField").getAsString().strip();
                                    if (displayNameField.length() != 0) {
                                        dsfArp.setDisplayNameField(displayNameField);
                                    }
                                }
                            }

                            // Connect dsfArp with the original dsf
                            dsfArp.setFieldType(dsf);
                            var override = arpMetadataBlockServiceBean.findOverrideByOriginal(dsf);
                            dsfArp.setOverride(override);
                            JsonElement cedarDef = cedarFieldJsonDefs.get(fieldName).getAsJsonObject().has("items") ? cedarFieldJsonDefs.get(fieldName).getAsJsonObject().get("items") : cedarFieldJsonDefs.get(fieldName);
                            dsfArp.setHasExternalValues(JsonHelper.getJsonObject(cedarDef, "_valueConstraints.branches[0]") != null);
                            dsfArp = arpMetadataBlockServiceBean.save(dsfArp);
                            break;

                        case CONTROLLEDVOCABULARY:
                            responseArr.add( Json.createObjectBuilder()
                                    .add("name", parseControlledVocabulary(values))
                                    .add("type", "Controlled Vocabulary") );
                            break;

                        default:
                            throw new IOException("No #header defined in file.");

                    }
                }
            }
        } finally {
            actionLogSvc.log(alr);
        }
    }

    //region Copied functions from edu.harvard.iq.dataverse.api.DatasetFieldServiceApi to avoid modifying the base code
    private MetadataBlock parseMetadataBlock(String[] values) {
        //Test to see if it exists by name
        MetadataBlock mdb = metadataBlockService.findByName(values[1]);
        if (mdb == null){
            mdb = new MetadataBlock();
        }
        mdb.setName(values[1]);
        if (!values[2].isEmpty()){
            mdb.setOwner(dataverseService.findByAlias(values[2]));
        }
        mdb.setDisplayName(values[3]);
        if (values.length>4 && !StringUtils.isEmpty(values[4])) {
            mdb.setNamespaceUri(values[4]);
        }

        return metadataBlockService.save(mdb);
    }

    private DatasetFieldType parseDatasetField(String[] values) {

        //First see if it exists
        DatasetFieldType dsf = datasetFieldService.findByName(values[1]);
        if (dsf == null) {
            //if not create new
            dsf = new DatasetFieldType();
        }
        //add(update) values
        dsf.setName(values[1]);
        dsf.setTitle(values[2]);
        dsf.setDescription(values[3]);
        dsf.setWatermark(values[4]);
        dsf.setFieldType(DatasetFieldType.FieldType.valueOf(values[5].toUpperCase()));
        dsf.setDisplayOrder(Integer.parseInt(values[6]));
        dsf.setDisplayFormat(values[7]);
        dsf.setAdvancedSearchFieldType(Boolean.parseBoolean(values[8]));
        dsf.setAllowControlledVocabulary(Boolean.parseBoolean(values[9]));
        dsf.setAllowMultiples(Boolean.parseBoolean(values[10]));
        dsf.setFacetable(Boolean.parseBoolean(values[11]));
        dsf.setDisplayOnCreate(Boolean.parseBoolean(values[12]));
        dsf.setRequired(Boolean.parseBoolean(values[13]));
        if (!StringUtils.isEmpty(values[14])) {
            dsf.setParentDatasetFieldType(datasetFieldService.findByName(values[14]));
        } else {
            dsf.setParentDatasetFieldType(null);
        }
        dsf.setMetadataBlock(dataverseService.findMDBByName(values[15]));
        if(values.length>16 && !StringUtils.isEmpty(values[16])) {
            dsf.setUri(values[16]);
        }
        return datasetFieldService.save(dsf);
    }

    private String parseControlledVocabulary(String[] values) {

        DatasetFieldType dsv = datasetFieldService.findByName(values[1]);
        //See if it already exists
        /*
         Matching relies on assumption that only one cv value will exist for a given identifier or display value
        If the lookup queries return multiple matches then retval is null
        */
        //First see if cvv exists based on display name
        ControlledVocabularyValue cvv = datasetFieldService.findControlledVocabularyValueByDatasetFieldTypeAndStrValue(dsv, values[2], true);

        //then see if there's a match on identifier
        ControlledVocabularyValue cvvi = null;
        if (values[3] != null && !values[3].trim().isEmpty()){
            cvvi = datasetFieldService.findControlledVocabularyValueByDatasetFieldTypeAndIdentifier(dsv, values[3]);
        }

        //if there's a match on identifier use it
        if (cvvi != null){
            cvv = cvvi;
        }

        //if there's no match create a new one
        if (cvv == null) {
            cvv = new ControlledVocabularyValue();
            cvv.setDatasetFieldType(dsv);
        }

        // Alternate variants for this controlled vocab. value:

        // Note that these are overwritten every time:
        cvv.getControlledVocabAlternates().clear();
        // - meaning, if an alternate has been removed from the tsv file,
        // it will be removed from the database! -- L.A. 5.4

        for (int i = 5; i < values.length; i++) {
            ControlledVocabAlternate alt = new ControlledVocabAlternate();
            alt.setDatasetFieldType(dsv);
            alt.setControlledVocabularyValue(cvv);
            alt.setStrValue(values[i]);
            cvv.getControlledVocabAlternates().add(alt);
        }

        cvv.setStrValue(values[2]);
        cvv.setIdentifier(values[3]);
        cvv.setDisplayOrder(Integer.parseInt(values[4]));
        datasetFieldService.save(cvv);
        return cvv.getStrValue();
    }
    //endregion


    public CedarTemplateErrors checkTemplate(String cedarTemplate) throws Exception {
        Map<String, String> propAndTermUriMap = listBlocksWithUri();

        // region Static fields from src/main/java/edu/harvard/iq/dataverse/api/Index.java: listOfStaticFields
        List<String> listOfStaticFields = new ArrayList<>();
        Object searchFieldsObject = new SearchFields();
        Field[] staticSearchFields = searchFieldsObject.getClass().getDeclaredFields();
        for (Field fieldObject : staticSearchFields) {
            String staticSearchField;
            try {
                staticSearchField = (String) fieldObject.get(searchFieldsObject);
                listOfStaticFields.add(staticSearchField);
            } catch (IllegalAccessException e) {
            }
        }
        //endregion

        String mdbId = new ObjectMapper().readTree(cedarTemplate).get("schema:identifier").textValue();
        return checkCedarTemplate(cedarTemplate, new CedarTemplateErrors(new ArrayList<>(), new ArrayList<>(), new HashMap<>()), propAndTermUriMap, "/properties",false, listOfStaticFields, mdbId);
    }

    public CedarTemplateErrors checkCedarTemplate(String cedarTemplate, CedarTemplateErrors cedarTemplateErrors, Map<String, String> dvPropTermUriPairs, String parentPath, Boolean lvl2, List<String> listOfStaticFields, String mdbName) throws Exception {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        JsonObject cedarTemplateJson = gson.fromJson(cedarTemplate, JsonObject.class);

        List<String> propNames = getStringList(cedarTemplateJson, "_ui.order");
        List<String> propLabels = getJsonObject(cedarTemplateJson, "_ui.propertyLabels").entrySet().stream()
                .map(e -> e.getValue().getAsString()).collect(Collectors.toList());

        List<String> invalidNames = propLabels.stream().collect(
                        Collectors.groupingBy(Function.identity(), Collectors.counting()))
                .entrySet()
                .stream()
                .filter(s -> s.getValue() > 1 ||
                        !s.getKey().matches("^(?![_\\W].*_$)[^0-9:]\\w*(:?\\w*)*") ||
                        listOfStaticFields.contains(s.getKey()) && metadataBlockService.findByName(mdbName) == null)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

        if (!invalidNames.isEmpty()) {
            cedarTemplateErrors.invalidNames.addAll(invalidNames);
        }

        List<String> mdbNames = metadataBlockService.listMetadataBlocks().stream().map(MetadataBlock::getName).collect(Collectors.toList());

        for (String prop : propNames) {
            // It turns out that collision of prop names with MDB names doesn't cause a problem so no need to check.
            // ie. we can have an MDB named "journal" and a prop name "journal" as well.
            // if (mdbNames.contains(prop)) {
            //    throw new Exception(String.format("Property: '%s' can not be added, because a MetadataBlock already exists with it's name.", prop));
            // }
            JsonObject actProp = getJsonObject(cedarTemplateJson, "properties." + prop);
            String newPath = parentPath + "/" + prop;
            String propType;
            if (actProp.has("@type")) {
                propType = actProp.get("@type").getAsString();
                propType = propType.substring(propType.lastIndexOf("/") + 1);
                String termUri = Optional.ofNullable(getJsonElement(cedarTemplateJson, "properties.@context.properties." + prop + ".enum[0]")).map(String::valueOf).orElse("").replaceAll("\"", "");
                termUri = termUri.equals("null") ? "" : termUri;
                boolean isNew = true;
                for (var entry: dvPropTermUriPairs.entrySet()) {
                    var k = entry.getKey();
                    var v = entry.getValue();
                    if (v.equals(termUri)) {
                        DatasetFieldType original = datasetFieldService.findByName(k);
                        // There's no need to create overrides if the original metadata block is updated
                        if (!mdbName.equals(original.getMetadataBlock().getName())) {
                            DatasetFieldTypeOverride override = new DatasetFieldTypeOverride();
                            override.setOriginal(original);
                            override.setTitle(actProp.has("skos:prefLabel") ? actProp.get("skos:prefLabel").getAsString() : "");
                            override.setLocalName(actProp.has("schema:name") ? actProp.get("schema:name").getAsString() : "");
                            cedarTemplateErrors.incompatiblePairs.put(prop, override);
                            isNew = false;
                        }
                    }
                }

                if (isNew && !dvPropTermUriPairs.containsKey(prop)) {
                    dvPropTermUriPairs.put(prop, termUri);
                }

            } else {
                propType = actProp.get("type").getAsString();
                actProp = getJsonObject(actProp, "items");
                String itemsType = actProp.get("@type").getAsString();
                if (itemsType.substring(itemsType.lastIndexOf("/") + 1).equals("TemplateField")) {
                    continue;
                }
            }
            if (propType.equals("TemplateElement") || propType.equals("array")) {
                if (lvl2) {
                    cedarTemplateErrors.unprocessableElements.add(newPath);
                    checkCedarTemplate(actProp.toString(), cedarTemplateErrors, dvPropTermUriPairs, newPath, false, listOfStaticFields, mdbName);
                } else {
                    checkCedarTemplate(actProp.toString(), cedarTemplateErrors, dvPropTermUriPairs, newPath, true, listOfStaticFields, mdbName);
                }
            } else {
                if (!propType.equals("TemplateField") && !propType.equals("StaticTemplateField")) {
                    cedarTemplateErrors.unprocessableElements.add(newPath);
                }
            }
        }

        return cedarTemplateErrors;
    }

    /**
     * Get all metadata blocks with URI-s
     */
    public Map<String, String> listBlocksWithUri() {
        Map<String, String> propAndTermUriMap = new HashMap<>();
        for (MetadataBlock blk : metadataBlockSvc.listMetadataBlocks()) {
            jsonWithUri(blk).forEach((k, v) -> propAndTermUriMap.merge(k, v, (v1, v2) -> {
                if(!v1.equals(v2)) {
                    throw new AssertionError("duplicate values for key: " + k + " " + v);
                }
                return v1;
            }));
        }

        return propAndTermUriMap;
    }

    /**
     * Collects propertyName-termUri pairs for Dataset fields
     */
    public Map<String, String> jsonWithUri(DatasetFieldType fld) {
        Map<String, String> propAndTermUriMap = new HashMap<>();
        propAndTermUriMap.put(fld.getName(), Optional.ofNullable(fld.getUri()).map(Objects::toString).orElse(""));
        if (!fld.getChildDatasetFieldTypes().isEmpty()) {
            for (DatasetFieldType subFld : fld.getChildDatasetFieldTypes()) {
                jsonWithUri(subFld).forEach((k, v) -> propAndTermUriMap.merge(k, v, (v1, v2) -> {
                    if(!v1.equals(v2))
                        throw new AssertionError("duplicate values for key: " + k + " " + v);
                    return v1;
                }));
            }
        }

        return propAndTermUriMap;
    }

    /**
     * Collects propertyName-termUri pairs for Metadata blocks
     */
    public Map<String, String> jsonWithUri(MetadataBlock blk) {
        Map<String, String> propUriMap = new HashMap<>();
        // It turns out that collision of prop names with MDB names doesn't cause a problem so no need to add to
        // propUriMap. ie. we can have an MDB named "journal" and a prop name "journal" as well.
        //
        // if (mdbNames.contains(prop)) {
        //    throw new Exception(String.format("Property: '%s' can not be added, because a MetadataBlock already exists with it's name.", prop));
        // }
        //propUriMap.put(blk.getName(), Optional.ofNullable(blk.getNamespaceUri()).map(Objects::toString).orElse(""));

        for (DatasetFieldType df : new TreeSet<>(blk.getDatasetFieldTypes())) {
            jsonWithUri(df).forEach((k, v) -> propUriMap.merge(k, v, (v1, v2) -> {
                if(!v1.equals(v2))
                    throw new AssertionError("duplicate values for key: " + k + " " + v);
                return v1;
            }));
        }
        return propUriMap;
    }

    public void updateEnabledMetadataBlocks(String dvIdtf, String metadataBlockName) throws Exception {
        Dataverse dataverse = dataverseService.findAll().stream().filter(dv -> dv.getAlias().equals(dvIdtf)).findFirst().get();
        List<MetadataBlock> metadataBlocks = dataverse.getMetadataBlocks();
        MetadataBlock enabledMdb = findMetadataBlock(metadataBlockName);
        if (!metadataBlocks.contains(enabledMdb)) {
            metadataBlocks.add(enabledMdb);
        }
        dataverse.setMetadataBlocks(metadataBlocks);
        dataverseService.save(dataverse);
    }

    public void updateMetadataBlock(String dvIdtf, String metadataBlockName) throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        String solrUpdaterAddress = arpConfig.get("arp.solr.updater.address");
        HttpRequest request = HttpRequest.newBuilder()
                .uri(new URI(solrUpdaterAddress))
                .GET()
                .build();
        HttpResponse<String> solrResponse = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (solrResponse.statusCode() != 200) {
            throw new Exception("Failed to update solr schema");
        }
        updateEnabledMetadataBlocks(dvIdtf, metadataBlockName);
    }


    public static ArrayList<String> collectHunTranslations(String cedarTemplate, String parentPath, ArrayList<String> hunTranslations) {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        JsonObject cedarTemplateJson = gson.fromJson(cedarTemplate, JsonObject.class);

        if (parentPath.equals("/properties")) {
            hunTranslations.add("metadatablock.name = " + getJsonElement(cedarTemplateJson, "schema:name").getAsString());
            if (cedarTemplateJson.has("hunName")) {
                hunTranslations.add("metadatablock.displayName = " + getJsonElement(cedarTemplateJson, "hunName").getAsString());
            }
            if (cedarTemplateJson.has("hunDescription")) {
                hunTranslations.add("metadatablock.description = " + getJsonElement(cedarTemplateJson, "hunDescription").getAsString());
            }
        }

        List<String> propNames = getStringList(cedarTemplateJson, "_ui.order");
        JsonObject propertyDescriptions = getJsonObject(cedarTemplateJson, "_ui.propertyDescriptions");

        for (String prop : propNames) {
            JsonObject actProp = getJsonObject(cedarTemplateJson, "properties." + prop);
            String newPath = parentPath + "/" + prop;
            String propType;

            if (actProp.has("items")) {
                actProp = actProp.getAsJsonObject("items");
            }

            String dftName = getJsonElement(actProp, "schema:name").getAsString();

            //Label
            if (actProp.has("hunLabel")) {
                String hunLabel = getJsonElement(actProp, "hunLabel").getAsString();
                hunTranslations.add(String.format("datasetfieldtype.%1$s.title = %2$s", dftName, hunLabel));
            }
            else{
                hunTranslations.add(String.format("datasetfieldtype.%1$s.title = %2$s",
                        dftName,
                        getJsonElement(actProp, "skos:prefLabel").getAsString())+" (magyarul)");
            }

            // Help text / tip
            if (actProp.has("hunDescription")) {
                hunTranslations.add(String.format("datasetfieldtype.%1$s.description = %2$s",
                        dftName,
                        actProp.get("hunDescription").getAsString()));
            }
            else {
                // Note: english help text should be in schema:description, but it is not, it is in
                // propertyDescriptions
                hunTranslations.add(String.format("datasetfieldtype.%1$s.description = %2$s",
                        dftName,
                        propertyDescriptions.get(dftName).getAsString())+" (magyarul)");
            }

            // TODO: revise how elemnets work!

            if (actProp.has("@type")) {
                propType = actProp.get("@type").getAsString();
                propType = propType.substring(propType.lastIndexOf("/") + 1);

            } else {
                propType = actProp.get("type").getAsString();
                actProp = getJsonObject(actProp, "items");
                String itemsType = actProp.get("@type").getAsString();
                if (itemsType.substring(itemsType.lastIndexOf("/") + 1).equals("TemplateField")) {
                    continue;
                }
            }
            if (propType.equals("TemplateElement") || propType.equals("array")) {
                dftName = getJsonElement(actProp, "schema:name").getAsString();
                if (actProp.has("hunTitle") && !actProp.has("hunLabel")) {
                    String hunTitle = getJsonElement(actProp, "hunTitle").getAsString();
                    hunTranslations.add(String.format("datasetfieldtype.%1$s.title = %2$s", dftName, hunTitle));
                }
                if (actProp.has("hunDescription")) {
                    String hunDescription = getJsonElement(actProp, "hunDescription").getAsString();
                    hunTranslations.add(String.format("datasetfieldtype.%1$s.description = %2$s", dftName, hunDescription));
                }
                collectHunTranslations(actProp.toString(), newPath, hunTranslations);
            }
        }

        return hunTranslations;
    }

    protected MetadataBlock findMetadataBlock(Long id)  {
        return metadataBlockSvc.findById(id);
    }
    protected MetadataBlock findMetadataBlock(String idtf) throws NumberFormatException {
        return metadataBlockSvc.findByName(idtf);
    }

    public String createOrUpdateMdbFromCedarTemplate(String dvIdtf, String templateJson, boolean skipUpload) throws JsonProcessingException, CedarTemplateErrorsException
    {
        String mdbTsv;
        List<String> lines;
        Set<String> overridePropNames = new HashSet<>();
        String metadataBlockName = new ObjectMapper().readTree(templateJson).get("schema:identifier").textValue();

        try {

            CedarTemplateErrors cedarTemplateErrors = checkTemplate(templateJson);
            if (!(cedarTemplateErrors.unprocessableElements.isEmpty() && cedarTemplateErrors.invalidNames.isEmpty())) {
                throw new CedarTemplateErrorsException(cedarTemplateErrors);
            }
            if (!cedarTemplateErrors.incompatiblePairs.isEmpty()) {
                overridePropNames = cedarTemplateErrors.incompatiblePairs.keySet();
            }

            mdbTsv = convertTemplateToDvMdb(templateJson, overridePropNames);
            lines = List.of(mdbTsv.split("\n"));
            if (!skipUpload) {
                loadDatasetFields(lines, metadataBlockName, templateJson);
                // at this point the new mdb is already in the db
                if (!cedarTemplateErrors.incompatiblePairs.isEmpty()) {
                    MetadataBlock newMdb = metadataBlockService.findByName(metadataBlockName);
                    cedarTemplateErrors.incompatiblePairs.values().forEach(override -> override.setMetadataBlock(newMdb));
                    arpMetadataBlockServiceBean.save(new ArrayList<>(cedarTemplateErrors.incompatiblePairs.values()));
                }
                updateMetadataBlock(dvIdtf, metadataBlockName);
            }

            String langDirPath = System.getProperty("dataverse.lang.directory");
            if (langDirPath != null) {
                String fileName = metadataBlockName + "_hu.properties";
                List<String> hunTranslations = collectHunTranslations(templateJson, "/properties", new ArrayList<>());
                FileWriter writer = new FileWriter(langDirPath + "/" + fileName);
                writer.write(String.join("\n", hunTranslations));
                writer.close();
            }
            // Force reloading language bundles/
            ResourceBundle.clearCache();
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Updating metadatablock "+""+" from CEDAR template failed", e);
            throw new RuntimeException(e);
        }

        return mdbTsv;
    }

    public void syncMetadataBlockWithCedar(String mdbName, ExportToCedarParams cedarParams) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            String cedarDomain = cedarParams.cedarDomain;

            if (cedarDomain == null || cedarDomain.isBlank()){
                cedarDomain = arpConfig.get("arp.cedar.domain");
            }
            cedarParams.cedarDomain = cedarDomain;

            var mdb = metadataBlockService.findByName(mdbName);
            JsonNode cedarTemplate = mapper.readTree(tsvToCedarTemplate(exportMdbAsTsv(mdb.getIdString())).toString());
            String templateJson = exportTemplateToCedar(cedarTemplate, cedarParams);
            createOrUpdateMdbFromCedarTemplate("root", templateJson, false);
        } catch (Exception e) {
            throw new RuntimeException("Syncing metadatablock '"+mdbName+"' with CEDAR failed.",  e);
        }
    }
}


