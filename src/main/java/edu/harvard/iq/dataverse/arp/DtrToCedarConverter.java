package edu.harvard.iq.dataverse.arp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.harvard.iq.dataverse.api.arp.util.JsonHelper;
import edu.harvard.iq.dataverse.util.BundleUtil;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URI;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import com.fasterxml.jackson.core.JsonPointer;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import static java.net.http.HttpResponse.BodyHandlers.ofString;

/**
 * Converts DTR (Data Type Registry) format to CEDAR template JSON.
 * This class handles the conversion of DTR schemas to CEDAR-compatible formats,
 * including basic info types and complex types.
 */
public class DtrToCedarConverter implements java.io.Serializable {
    private static final Logger logger = Logger.getLogger(DtrToCedarConverter.class.getCanonicalName());
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static JsonNode EMPTY_CEDAR_TEMPLATE;
    private static JsonNode EMPTY_CEDAR_TEMPLATE_FIELD;
    private static JsonNode EMPTY_CEDAR_TEMPLATE_ELEMENT;

    private String dtrUrl;
    private String cedarApiKey;
    private String cedarBaseDomain;
    private String parentFolderId;
    private String fieldsFolderId;
    private String elementsFolderId;
    private String templateFolderId;
    private boolean createFolder = true;

    static {
        setupCedarTemplateParts();
    }

    private static void setupCedarTemplateParts() {
        String cedarTemplatePath = "arp/cedarSchemaTemplate.json";
        String cedarTemplateFieldPath = "arp/cedarTemplateField.json";
        String cedarTemplateElementPath = "arp/cedarTemplateElement.json";

        InputStream cedarTemplateInputStream = getCedarTemplateFromResources(cedarTemplatePath);
        InputStream cedarTemplateFieldInputStream = getCedarTemplateFromResources(cedarTemplateFieldPath);
        InputStream cedarTemplateElementInputStream = getCedarTemplateFromResources(cedarTemplateElementPath);

        try {
            if (cedarTemplateInputStream == null) {
                logger.log(Level.SEVERE, "DtrToCedarConverter was unable to process " + cedarTemplatePath);
            } else {
                EMPTY_CEDAR_TEMPLATE = objectMapper.readTree(cedarTemplateInputStream);
            }

            if (cedarTemplateFieldInputStream == null) {
                logger.log(Level.SEVERE, "DtrToCedarConverter was unable to process " + cedarTemplateFieldPath);
            } else {
                EMPTY_CEDAR_TEMPLATE_FIELD = objectMapper.readTree(cedarTemplateFieldInputStream);
            }

            if (cedarTemplateElementInputStream == null) {
                logger.log(Level.SEVERE, "DtrToCedarConverter was unable to process " + cedarTemplateElementPath);
            } else {
                EMPTY_CEDAR_TEMPLATE_ELEMENT = objectMapper.readTree(cedarTemplateElementInputStream);
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Error setting up CEDAR template parts: " + e.getMessage(), e);
        }
    }

    public DtrToCedarConverter(String dtrUrl, String cedarApiKey, String parentFolderId, String cedarBaseDomain) {
        this.dtrUrl = dtrUrl;
        this.cedarApiKey = cedarApiKey;
        this.parentFolderId = parentFolderId;
        this.cedarBaseDomain = cedarBaseDomain;
    }

    private static InputStream getCedarTemplateFromResources(String fileName) {
        try (InputStream input = DtrToCedarConverter.class.getClassLoader().getResourceAsStream(fileName)) {
            if (input == null) {
                logger.log(Level.SEVERE, "DtrToCedarConverter was unable to load " + fileName);
            }
            return input;
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    /**
     * Converts a basic info type to a CEDAR template field with mapping
     */
    private JsonNode convertBasicInfoTypeToTemplateField(JsonNode sourceData, Map<String, String> mapping, boolean skipUpload, boolean createFolder) {
        ObjectNode targetData = EMPTY_CEDAR_TEMPLATE_FIELD.deepCopy();
        if (mapping == null) {
            mapping = readBidirectionalMapping("common_prop_mappings.tsv", ConversionDirection.DTR_TO_CEDAR);
        }
        processCommonProperties(sourceData, targetData, ConversionDirection.DTR_TO_CEDAR, mapping, null);
        convertPropsFromDtrToCedar(sourceData.get("Schema"), targetData);
        if (!skipUpload) {
            targetData = (ObjectNode) uploadResourceToCedar(targetData, createFolder);
        }
        return targetData;
    }

    /**
     * Converts a complex DTR structure to CEDAR format with mapping
     */
    private JsonNode convertComplexDtrToCedar(JsonNode sourceData, JsonNode targetData, Map<String, String> mapping, boolean skipUpload, boolean createFolder) {
        ObjectNode targetObjectNode = (ObjectNode) targetData;
        if (mapping == null) {
            mapping = readBidirectionalMapping("common_prop_mappings.tsv", ConversionDirection.DTR_TO_CEDAR);
        }
        processCommonProperties(sourceData, targetObjectNode, ConversionDirection.DTR_TO_CEDAR, mapping, null);
        
        JsonNode schema = sourceData.get("Schema");
        if (!"Object".equals(schema.get("Type").asText())) {
            logger.severe("Schema is not an Object. Array type is not compatible with CEDAR.");
            return null;
        }

        JsonNode properties = schema.get("Properties");
        for (JsonNode property : properties) {
            String type = property.get("Type").asText();
            JsonNode resource = getResource(type);
            
            if (resource != null) {
                Map<String, String> overriddenValues = collectOverriddenValues(property, resource.get("id").asText());
                
                if ("BasicInfoType".equals(resource.get("type").asText())) {
                    if (!createFolder) {
                        skipUpload = true;
                    }
                    JsonNode cedarField = convertBasicInfoTypeToTemplateField(resource.get("content"), mapping, skipUpload, createFolder);
                    addDtrResourceToCedarParent(overriddenValues, targetObjectNode, cedarField);
                } else {
                    JsonNode elementTemplate = EMPTY_CEDAR_TEMPLATE_ELEMENT.deepCopy();
                    JsonNode cedarElement = convertComplexDtrToCedar(resource.get("content"), elementTemplate, mapping, skipUpload, createFolder);
                    if (!skipUpload) {
                        cedarElement = uploadResourceToCedar(cedarElement, createFolder);
                    }
                    addDtrResourceToCedarParent(overriddenValues, targetObjectNode, cedarElement);
                }
            }
        }
        
        return targetData;
    }

    /**
     * Direction of property conversion
     */
    public enum ConversionDirection {
        DTR_TO_CEDAR,
        CEDAR_TO_DTR
    }

    /**
     * Process and convert properties between DTR and Cedar JSON schemas.
     * 
     * @param sourceData Source schema data
     * @param targetData Target schema data
     * @param direction Direction of conversion (DTR → Cedar or Cedar → DTR)
     * @param mapping Dictionary of property mappings
     * @param overriddenValues Overridden values for fields that have been overridden in the parent resource
     * @return The processed target data
     */
    private ObjectNode processCommonProperties(
            JsonNode sourceData,
            ObjectNode targetData,
            ConversionDirection direction,
            Map<String, String> mapping,
            Map<String, String> overriddenValues) 
    {
        try {
            // Process each property from the mapping
            for (Map.Entry<String, String> entry : mapping.entrySet()) {
                String sourcePath = entry.getKey();
                String targetPath = entry.getValue();
                JsonNode value = getNestedValue(sourceData, sourcePath);
                if (value != null) {
                    setNestedValue(targetData, targetPath, value);
                }
            }

            // Process overridden values
            if (overriddenValues != null) {
                for (Map.Entry<String, String> entry : overriddenValues.entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();
                    
                    if (!key.equals("Identifier") && !key.equals("Cardinality")) {
                        setNestedValue(targetData, mapping.get(key), objectMapper.valueToTree(value));
                    }
                }

                // Handle Cardinality
                if (overriddenValues.containsKey("Cardinality")) {
                    String cardinality = overriddenValues.get("Cardinality");
                    boolean isRequired = cardinality.equals("1") || cardinality.equals("1 - n");
                    setNestedValue(targetData, "_valueConstraints.requiredValue", objectMapper.valueToTree(isRequired));
                    
                    boolean multipleAllowed = cardinality.equals("0 - n") || cardinality.equals("1 - n");
                    if (multipleAllowed) {
                        setNestedValue(targetData, "minItems", objectMapper.valueToTree(1));
                        setNestedValue(targetData, "maxItems", objectMapper.valueToTree(0));
                    }
                }
            } else {
                setNestedValue(targetData, "_valueConstraints.requiredValue", objectMapper.valueToTree(false));
            }

            // Save additional CEDAR properties
            if (direction == ConversionDirection.DTR_TO_CEDAR) {
                String fieldName = overriddenValues == null ? 
                        sourceData.get("name").asText() : 
                        overriddenValues.get("name");
                
                setNestedValue(targetData, "@id", objectMapper.valueToTree(UUID.randomUUID().toString()));
                setNestedValue(targetData, "title", objectMapper.valueToTree(fieldName + " field schema"));
                setNestedValue(targetData, "description", objectMapper.valueToTree(fieldName + " generated by the DTR-CEDAR converter"));
                setNestedValue(targetData, "schema:identifier", objectMapper.valueToTree(fieldName));
            }
            
            return targetData;
            
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error processing common properties: " + e.getMessage(), e);
            return targetData;
        }
    }

    /**
     * Converts properties from DTR to CEDAR format
     */
    private void convertPropsFromDtrToCedar(JsonNode sourceData, JsonNode targetData) {
        String dtrType = sourceData.get("Type").asText();
        setCedarType(dtrType, targetData);

        if (sourceData.has("PropRelations")) {
            if ("Integer".equals(dtrType) || "Number".equals(dtrType)) {
                ((ObjectNode) targetData.get("_valueConstraints")).set("propRelations", sourceData.get("PropRelations"));
            } else {
                ObjectNode arp = objectMapper.createObjectNode();
                arp.set("dtr.common.propRelations", sourceData.get("PropRelations"));
                ((ObjectNode) targetData).set("_arp", arp);
            }
        }

        if ("Enum".equals(dtrType)) {
            enumFromDtrToCedar(sourceData.get("Properties").get(0), targetData);
        } else {
            propsFromDtrToCedar(sourceData.get("Properties"), targetData);
        }
    }

    /**
     * Sets the CEDAR type based on DTR type
     */
    private void setCedarType(String dtrType, JsonNode targetData) {
        String cedarType = switch (dtrType) {
            case "String" -> "textfield";
            case "Integer" -> "xsd:int";
            case "Number" -> "xsd:double";
            case "Enum" -> "list";
            default -> "textfield";
        };
        ((ObjectNode) targetData.get("_ui")).put("inputType", cedarType);
    }

    /**
     * Converts enum properties from DTR to CEDAR format
     */
    private void enumFromDtrToCedar(JsonNode sourceData, JsonNode targetData) {
        if ("String Enum".equals(sourceData.get("Property").asText())) {
            ArrayNode enumList = (ArrayNode) sourceData.get("Value");
            ArrayNode cedarList = objectMapper.createArrayNode();
            for (JsonNode value : enumList) {
                ObjectNode literal = objectMapper.createObjectNode();
                literal.put("label", value.asText());
                cedarList.add(literal);
            }
            ((ObjectNode) targetData.get("_valueConstraints")).set("literals", cedarList);
        }
    }

    /**
     * Convert properties from DTR to CEDAR JSON schema
     */
    private void propsFromDtrToCedar(JsonNode sourceData, JsonNode targetData) {
        Map<String, String> constraintMapping = readConstraintMappings();
        
        if (sourceData.isArray()) {
            for (JsonNode pair : sourceData) {
                String propertyName = pair.get("Property").asText();
                JsonNode value = pair.get("Value");
                
                logger.fine("Processing property: " + propertyName + ", value: " + value);
                
                if (propertyName == null || value == null) {
                    logger.warning("Warning: Invalid property-value pair: " + pair);
                    continue;
                }
                
                // Find the mapping for this property
                if (constraintMapping.containsKey(propertyName)) {
                    String cedarPath = constraintMapping.get(propertyName);
                    setNestedValue((ObjectNode) targetData, "_valueConstraints." + cedarPath, value);
                } else {
                    setNestedValue((ObjectNode) targetData, "_arp.dtr.specific." + propertyName, value);
                }
            }
        }
    }

    /**
     * Adds a DTR resource to its CEDAR parent
     */
    private void addDtrResourceToCedarParent(Map<String, String> sourceData, JsonNode parentData, JsonNode resourceData) {
        String resourceName = sourceData.get("name");
        
        // Add to order array
        ArrayNode orderArray = (ArrayNode) parentData.get("_ui").get("order");
        if (!orderArray.has(resourceName)) {
            orderArray.add(resourceName);
        }
        
        // Add property labels and descriptions
        ((ObjectNode) parentData.get("_ui").get("propertyLabels")).put(resourceName, resourceName);
        if (sourceData.containsKey("description")) {
            ((ObjectNode) parentData.get("_ui").get("propertyDescriptions")).put(resourceName, sourceData.get("description"));
        }

        // Add enum object
        ObjectNode enumObj = objectMapper.createObjectNode();
        ArrayNode enumArray = objectMapper.createArrayNode();
        enumArray.add(sourceData.get("Identifier"));
        enumObj.set("enum", enumArray);
        ((ObjectNode) parentData.get("properties").get("@context").get("properties")).set(resourceName, enumObj);

        // Add to required array
        ArrayNode requiredArray = (ArrayNode) parentData.get("required");
        if (!requiredArray.has(resourceName)) {
            requiredArray.add(resourceName);
        }

        // Add title if present
        if (sourceData.containsKey("title")) {
            ((ObjectNode) resourceData).put("skos:prefLabel", sourceData.get("title"));
        }

        // Add to properties
        ((ObjectNode) parentData.get("properties")).set(resourceName, resourceData);
    }

    /**
     * Collects overridden values from the source data
     */
    private Map<String, String> collectOverriddenValues(JsonNode property, String identifier) {
        Map<String, String> overriddenValues = new HashMap<>();
        overriddenValues.put("Identifier", identifier);
        
        if (property.has("Name")) {
            overriddenValues.put("name", property.get("Name").asText());
        }
        if (property.has("Description")) {
            overriddenValues.put("description", property.get("Description").asText());
        }
        if (property.has("Title")) {
            overriddenValues.put("title", property.get("Title").asText());
        }
        
        if (property.has("Properties")) {
            JsonNode props = property.get("Properties");
            if (props.has("Cardinality")) {
                overriddenValues.put("Cardinality", props.get("Cardinality").asText());
            }
        }
        
        return overriddenValues;
    }

    /**
     * Fetches a resource from the DTR API
     */
    private JsonNode getResource(String id) {
        try {
            String url = dtrUrl + "/" + id + "?full=true";
            HttpClient client = getUnsafeHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                .uri(new URI(url))
                .build();
            
            HttpResponse<String> response = client.send(request, ofString());
            String result = response.body();
            
            return objectMapper.readTree(result);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error fetching resource from DTR: " + e.getMessage(), e);
        }
        return null;
    }

    /**
     * Uploads a resource to CEDAR
     * 
     * @param resource The resource to upload
     * @param createFolder Whether to create a folder for the resource
     * @return The uploaded resource
     */
    private JsonNode uploadResourceToCedar(JsonNode resource, boolean createFolder) {
        try {
            String[] parts = resource.get("@type").asText().split("/");
            String cedarType = parts[parts.length - 1];
            String folderId;
            String url;

            switch (cedarType) {
                case "TemplateField":
                    if (createFolder) {
                        if (fieldsFolderId == null) {
                            fieldsFolderId = normalizeCedarUrl(createCedarFolder("Fields", "Fields from the DTR."));
                        }
                        folderId = fieldsFolderId;
                    } else {
                        folderId = parentFolderId;
                    }
                    url = cedarBaseDomain + "/template-fields?folder_id=" + folderId;
                    logger.fine("Uploading field to: " + url);
                    break;

                case "TemplateElement":
                    if (createFolder) {
                        if (elementsFolderId == null) {
                            elementsFolderId = normalizeCedarUrl(createCedarFolder("Elements", "Elements from the DTR."));
                        }
                        folderId = elementsFolderId;
                    } else {
                        folderId = parentFolderId;
                    }
                    url = cedarBaseDomain + "/template-elements?folder_id=" + folderId;
                    break;

                case "Template":
                    folderId = templateFolderId;
                    url = cedarBaseDomain + "/templates?folder_id=" + folderId;
                    break;

                default:
                    logger.severe("Error: Unsupported resource type: " + cedarType);
                    return null;
            }

            //TODO: uncomment the line below and delete the UnsafeHttpClient, that is for testing purposes only, until we have working CEDAR certs
            // HttpClient client = HttpClient.newHttpClient();
            HttpClient client = getUnsafeHttpClient();
            
            // always remove the "@id" from the resource before sending it to CEDAR
            ((ObjectNode) resource).remove("@id");
            
            HttpRequest request = HttpRequest.newBuilder()
                .uri(new URI("https://resource." + url))
                .header("Content-Type", "application/json")
                .header("Authorization", "apiKey " + cedarApiKey)
                .POST(HttpRequest.BodyPublishers.ofString(resource.toString()))
                .build();
            
            HttpResponse<String> response = client.send(request, ofString());
            
            if (response.statusCode() == 201) {
                return objectMapper.readTree(response.body());
            } else {
                logger.severe("Error: Failed to upload resource to CEDAR. Status code: " + response.statusCode());
                logger.severe("Response body: " + response.body());
                return null;
            }

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error uploading resource to CEDAR: " + e.getMessage(), e);
            return null;
        }
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

    /**
     * Creates a folder in CEDAR
     */
    private String createCedarFolder(String name, String description) {
        try {
            String url = "https://resource." + cedarBaseDomain + "/folders";
            
            HttpClient client = getUnsafeHttpClient();

            
            ObjectNode data = objectMapper.createObjectNode();
            var folderId = templateFolderId != null ? templateFolderId : parentFolderId;
            data.put("folderId", java.net.URLDecoder.decode(folderId, StandardCharsets.UTF_8));
            data.put("name", name);
            data.put("description", description);

            HttpRequest request = HttpRequest.newBuilder()
            .uri(new URI(url))
            .header("Authorization", "apiKey " + cedarApiKey)
            .POST(HttpRequest.BodyPublishers.ofString(data.toString()))
            .build();
            
            HttpResponse<String> response = client.send(request, ofString());
            String result = response.body();
            JsonNode responseJson = objectMapper.readTree(result);

            return responseJson.get("@id").asText();

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error creating CEDAR folder: " + e.getMessage(), e);
        }
        return null;
    }

    /**
     * Gets the appropriate CEDAR endpoint based on resource type
     */
    private String getCedarEndpoint(String cedarType) {
        return switch (cedarType) {
            case "TemplateField" -> "template-fields";
            case "TemplateElement" -> "template-elements";
            case "Template" -> "templates";
            default -> throw new IllegalArgumentException("Unsupported CEDAR type: " + cedarType);
        };
    }

    /**
     * Normalizes a CEDAR URL by percent-encoding it
     */
    private String normalizeCedarUrl(String url) {
        try {
            // Check if URL is already encoded
            if (url.contains("%2F")) {
                return url;
            }
            
            // Split protocol and rest
            String[] parts = url.split("://", 2);
            if (parts.length == 2) {
                return parts[0] + ":" + java.net.URLEncoder.encode("//" + parts[1], StandardCharsets.UTF_8);
            } else {
                return java.net.URLEncoder.encode(url, StandardCharsets.UTF_8);
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error normalizing CEDAR URL: " + e.getMessage(), e);
            return url;
        }
    }

    /**
     * Get a value from a nested JsonNode using dot notation and array indexing.
     * 
     * @param data The JsonNode to search in
     * @param path Path to the value (e.g., "metadata.author.name" or "aliases[0]")
     * @return The value if found, null otherwise
     */
    private JsonNode getNestedValue(JsonNode data, String path) {
        try {
            if (path == null || path.trim().isEmpty()) {
                logger.warning("Warning: Empty path provided to getNestedValue");
                return null;
            }

            // Convert dot notation to JSON Pointer format
            String jsonPointer = convertToJsonPointer(path);
            JsonPointer pointer = JsonPointer.compile(jsonPointer);
            
            JsonNode result = data.at(pointer);
            if (result.isMissingNode()) {
                logger.warning("Warning: Path not found: " + path);
                return null;
            }
            
            return result;
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in getNestedValue for path " + path + ": " + e.getMessage(), e);
            return null;
        }
    }

    /**
     * Set a value in a nested JsonNode using dot notation and array indexing.
     * Creates intermediate nodes if they don't exist.
     * 
     * @param data The JsonNode to modify
     * @param path Path to set the value (e.g., "metadata.author.name" or "aliases[0]")
     * @param value The value to set
     */
    private void setNestedValue(ObjectNode data, String path, JsonNode value) {
        try {
            if (value == null || path == null || path.trim().isEmpty()) {
                return;
            }

            // Convert dot notation to JSON Pointer format
            String jsonPointer = convertToJsonPointer(path);
            JsonPointer pointer = JsonPointer.compile(jsonPointer);
            
            // Create parent nodes if they don't exist
            JsonPointer parentPointer = pointer.head();
            JsonNode parentNode = data.at(parentPointer);
            
            if (parentNode.isMissingNode()) {
                createParentNodes(data, parentPointer);
            }
            
            // Set the value
            JsonPointer lastPointer = pointer.last();
            String lastToken = lastPointer.getMatchingProperty();
            
            if (lastToken != null) {
                JsonNode parent = data.at(parentPointer);
                if (parent.isObject()) {
                    ((ObjectNode) parent).set(lastToken, value);
                }
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error in setNestedValue for path " + path + ": " + e.getMessage(), e);
        }
    }

    /**
     * Convert dot notation path to JSON Pointer format
     * 
     * @param path Path in dot notation (e.g., "metadata.author.name" or "aliases[0]")
     * @return Path in JSON Pointer format (e.g., "/metadata/author/name" or "/aliases/0")
     */
    private String convertToJsonPointer(String path) {
        StringBuilder pointer = new StringBuilder();
        String[] parts = path.split("\\.");
        
        for (String part : parts) {
            part = part.trim();
            if (part.isEmpty()) {
                continue;
            }
            
            if (part.contains("[") && part.contains("]")) {
                // Handle array indexing
                String key = part.substring(0, part.indexOf("["));
                String index = part.substring(part.indexOf("[") + 1, part.indexOf("]"));
                pointer.append("/").append(key).append("/").append(index);
            } else {
                pointer.append("/").append(part);
            }
        }
        
        return pointer.toString();
    }

    /**
     * Create parent nodes for a given JSON Pointer path
     * 
     * @param data The root JsonNode
     * @param pointer The JSON Pointer to create nodes for
     */
    private void createParentNodes(ObjectNode data, JsonPointer pointer) {
        JsonPointer current = pointer;
        JsonNode node = data;
        
        while (!current.matches()) {
            String token = current.getMatchingProperty();
            if (token != null) {
                if (node.isObject()) {
                    ObjectNode objectNode = (ObjectNode) node;
                    if (!objectNode.has(token)) {
                        objectNode.set(token, objectMapper.createObjectNode());
                    }
                    node = objectNode.get(token);
                }
            }
            current = current.tail();
        }
    }

    /**
     * Reads property mappings from a TSV file and creates a direction-specific mapping.
     * 
     * @param mappingFile Name of the TSV file containing mappings
     * @param direction Direction of conversion (DTR → Cedar or Cedar → DTR)
     * @return Map mapping source paths to target paths based on direction
     */
    private Map<String, String> readBidirectionalMapping(String mappingFile, ConversionDirection direction) {
        Map<String, String> mappings = new HashMap<>();
        
        try {
            // Get the resource file from the classpath
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream("arp/" + mappingFile);
            if (inputStream == null) {
                logger.severe("Error: Could not find mapping file at arp/" + mappingFile);
                return mappings;
            }

            // Read the TSV file
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                // Read header line
                String headerLine = reader.readLine();
                if (headerLine == null) {
                    logger.warning("Mapping file is empty");
                    return mappings;
                }

                // Parse header to get column indices
                String[] headers = headerLine.split("\t");
                int dtrIndex = -1;
                int cedarIndex = -1;
                
                for (int i = 0; i < headers.length; i++) {
                    if (headers[i].equals("dtr")) {
                        dtrIndex = i;
                    } else if (headers[i].equals("cedar")) {
                        cedarIndex = i;
                    }
                }

                if (dtrIndex == -1 || cedarIndex == -1) {
                    logger.severe("Error: Mapping file must contain 'dtr' and 'cedar' columns");
                    return mappings;
                }

                // Read data lines
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] values = line.split("\t");
                    if (values.length > Math.max(dtrIndex, cedarIndex)) {
                        if (direction == ConversionDirection.DTR_TO_CEDAR) {
                            mappings.put(values[dtrIndex], values[cedarIndex]);
                        } else {
                            mappings.put(values[cedarIndex], values[dtrIndex]);
                        }
                    }
                }
            }
            
            return mappings;
            
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Error reading mapping file: " + e.getMessage(), e);
            return mappings;
        }
    }

    /**
     * Reads constraint mappings from a TSV file
     * 
     * @return Map of DTR property names to CEDAR constraint paths
     */
    private Map<String, String> readConstraintMappings() {
        Map<String, String> mappings = new HashMap<>();
        
        try {
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream("arp/value_constraint_mappings.tsv");
            if (inputStream == null) {
                logger.severe("Error: Could not find constraint mappings file at arp/value_constraint_mappings.tsv");
                return mappings;
            }

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                // Skip header line
                reader.readLine();

                String line;
                while ((line = reader.readLine()) != null) {
                    String[] values = line.split("\t");
                    if (values.length >= 2) {
                        mappings.put(values[0], values[1]);
                    }
                }
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Error reading constraint mappings: " + e.getMessage(), e);
        }
        
        return mappings;
    }

    /**
     * Main conversion method that converts a DTR schema to CEDAR format
     * 
     * @param sourceId The ID of the source resource
     * @param direction Direction of conversion (DTR → Cedar or Cedar → DTR)
     * @return The converted schema data
     */
    public JsonNode convert(String sourceId, ConversionDirection direction, boolean skipUpload) {
        try {
            // Read source data
            JsonNode sourceData = getResource(sourceId);
            if (sourceData == null) {
                logger.severe("Failed to fetch source resource: " + sourceId);
                return null;
            }

            // Read property mappings
            Map<String, String> mapping = readBidirectionalMapping("common_prop_mappings.tsv", direction);
            if (mapping.isEmpty()) {
                logger.severe("Failed to read property mappings");
                return null;
            }

            JsonNode convertedData;
            String type = sourceData.get("type").asText();
            JsonNode content = sourceData.get("content");

            if ("BasicInfoType".equals(type)) {
                convertedData = convertBasicInfoTypeToTemplateField(content, mapping, skipUpload, false);
            } else {
                JsonNode targetData;
                boolean createFolder;

                if ("InfoType".equals(type)) {
                    targetData = EMPTY_CEDAR_TEMPLATE_ELEMENT.deepCopy();
                    createFolder = false;
                } else {
                    targetData = EMPTY_CEDAR_TEMPLATE.deepCopy();
                    createFolder = true;
                }

                // Create folder in CEDAR if needed
                if (createFolder && !skipUpload) {
                    String name = content.get("name").asText() + "_" +
                            LocalDateTime.now().format(DateTimeFormatter.ofPattern("MMdd_HHmm"));
                    String folderId = createCedarFolder(name, "Template from the DTR.");
                    templateFolderId = normalizeCedarUrl(folderId);
                }

                convertedData = convertComplexDtrToCedar(content, targetData, mapping, skipUpload, createFolder);

                 // Upload to CEDAR if needed
                 if (!skipUpload) {
                     convertedData = uploadResourceToCedar(convertedData, createFolder);
                 }
            }

            return convertedData;

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error during schema conversion: " + e.getMessage(), e);
            return null;
        }
    }
} 