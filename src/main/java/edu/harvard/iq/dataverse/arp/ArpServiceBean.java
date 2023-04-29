package edu.harvard.iq.dataverse.arp;

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
import edu.harvard.iq.dataverse.api.arp.ArpApi;
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
        return tsvToCedarTemplate(tsv, true);
    }

    public JsonObject tsvToCedarTemplate(String tsv, boolean convertDotToColon) throws JsonProcessingException {
        var converter = new TsvToCedarTemplate(convertDotToColon);
        return converter.convert(tsv);
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

    public void exportTemplateToCedar(JsonNode cedarTemplate, ExportToCedarParams cedarParams) throws Exception {
        //TODO: uncomment the line below and delete the UnsafeHttpClient, that is for testing purposes only, until we have working CEDAR certs
        // HttpClient client = HttpClient.newHttpClient();
        HttpClient client = getUnsafeHttpClient();
        String templateName = cedarTemplate.get("schema:name").textValue();
        String parentFolderId = cedarParams.folderId;
        String apiKey = cedarParams.apiKey;
        String cedarDomain = cedarParams.cedarDomain;
        String templateFolderId = checkOrCreateFolder(parentFolderId, templateName, apiKey, cedarDomain, client);
        uploadTemplate(cedarTemplate, cedarParams, templateFolderId, client);
    }

    private void uploadTemplate(JsonNode cedarTemplate, ExportToCedarParams cedarParams, String templateFolderId, HttpClient client) throws Exception {
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

}


