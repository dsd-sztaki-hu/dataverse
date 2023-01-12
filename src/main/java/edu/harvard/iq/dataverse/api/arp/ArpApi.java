package edu.harvard.iq.dataverse.api.arp;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import edu.harvard.iq.dataverse.*;
import edu.harvard.iq.dataverse.actionlogging.ActionLogRecord;
import edu.harvard.iq.dataverse.api.AbstractApiBean;
import edu.harvard.iq.dataverse.api.DatasetFieldServiceApi;
import edu.harvard.iq.dataverse.search.SearchFields;
import edu.harvard.iq.dataverse.util.json.NullSafeJsonBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.util.Pair;
import org.json.JSONObject;

import javax.ejb.EJB;
import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObjectBuilder;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

@Path("arp")
public class ArpApi extends AbstractApiBean {

    private static final Logger logger = Logger.getLogger(ArpApi.class.getCanonicalName());
    private static final Properties prop = new Properties();

    @EJB
    IndexBean index;

    @EJB
    DataversesBean dataverses;

    @EJB
    DatasetFieldServiceApiBean datasetFieldServiceApi;

    @EJB
    DatasetFieldServiceBean datasetFieldService;

    @EJB
    DataverseServiceBean dataverseService;

    @EJB
    MetadataBlockServiceBean metadataBlockService;

    static {
        try (InputStream input = ArpApi.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (input == null) {
                logger.log(Level.SEVERE, "ArpApi was unable to load config.properties");
            }
            prop.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    @POST
    @Path("/checkCedarTemplate")
    @Consumes("application/json")
    public Response checkTemplate(String templateJson) {
        Map<String, String> propAndTermUriMap;
        try {
            propAndTermUriMap = listBlocksWithUri();
        } catch (Exception e) {
            return error(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

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

        CedarTemplateErrors errors = checkCedarTemplate(templateJson, new CedarTemplateErrors(new ArrayList<>(), new ArrayList<>(), new HashMap<>()), propAndTermUriMap, "/properties",false, listOfStaticFields);

        if (!(errors.incompatiblePairs.isEmpty() && errors.invalidNames.isEmpty() && errors.unprocessableElements.isEmpty())) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity( NullSafeJsonBuilder.jsonObjectBuilder()
                            .add("status", STATUS_ERROR)
                            .add( "message", errors.toJson() ).build()
                    ).type(MediaType.APPLICATION_JSON_TYPE).build();
        }

        return ok("Valid Template");
    }

    @POST
    @Path("/cedarToMdb/{identifier}")
    @Consumes("application/json")
    @Produces("text/tab-separated-values")
    public Response cedarToMdb(@PathParam("identifier") String dvIdtf,
                               @QueryParam("skipUpload") @DefaultValue("false") boolean skipUpload,
                               String templateJson) {
        String mdbTsv;
        List<String> lines;
        String metadataBlockId = new JSONObject(templateJson).getString("@id");
        String metadataBlockName = metadataBlockId.substring(metadataBlockId.lastIndexOf('/') + 1);

        try {
            Response checkTemplateResponse = checkTemplate(templateJson);
            if (!checkTemplateResponse.getStatusInfo().toEnum().equals(Response.Status.OK)) {
                String errors = checkTemplateResponse.getEntity().toString();
                throw new Exception(errors);
            }

            lines = convertTemplate(templateJson, "dv");
            mdbTsv = String.join("\n", lines);
            if (!skipUpload) {
                loadDatasetFields(lines, metadataBlockName);
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
        } catch (Exception e) {
            return Response.serverError().entity(e.getMessage()).header("Access-Control-Allow-Origin", "*").build();
        }

        //TODO: check why is the origin duplicated if the header is not added here as well as in the ApiBlockingFilter
        //TODO: maybe the cors filter?
        return Response.ok(mdbTsv).header("Access-Control-Allow-Origin", "*").build();
    }

    @POST
    @Path("/cedarToDescribo")
    @Consumes("application/json")
    @Produces("application/json")
    public Response cedarToDescribo(String templateJson) {
        String describoProfile;
        List<String> lines;

        try {
            Response checkTemplateResponse = checkTemplate(templateJson);
            if (!checkTemplateResponse.getStatusInfo().toEnum().equals(Response.Status.OK)) {
                String errors = checkTemplateResponse.getEntity().toString();
                throw new Exception(errors);
            }

            lines = convertTemplate(templateJson, "describo");
            describoProfile = String.join("\n", lines);
        } catch (Exception e) {
            return Response.serverError().entity(e.getMessage()).build();
        }

        return Response.ok(describoProfile).build();
    }

    private List<String> convertTemplate(String templateJson, String outputType) throws Exception {
        String line;
        ArrayList<String> converted = new ArrayList<>();
        BufferedReader bufferedReader = null;

        try {
            String cedarTemplateConverterPath = System.getProperty("arp.script.path") != null ? System.getProperty("arp.script.path") : prop.getProperty("arp.script.path");
            String pythonPath = System.getProperty("arp.python.path") != null ? System.getProperty("arp.python.path") : prop.getProperty("arp.python.path");
            String dvApiKey = System.getProperty("dv.api.key") != null ? System.getProperty("dv.api.key") : prop.getProperty("dv.api.key");
            String dvUpdaterScriptPath = System.getProperty("dv.updater.script.path") != null ? System.getProperty("dv.updater.script.path") : prop.getProperty("dv.updater.script.path");
            String solrSchemaPath = System.getProperty("solr.schema.path") != null ? System.getProperty("solr.schema.path") : prop.getProperty("solr.schema.path");
            String solrCollectionName = System.getProperty("solr.collection.name") != null ? System.getProperty("solr.collection.name") : prop.getProperty("solr.collection.name");
            String solrAddress = System.getProperty("solr.host") != null ? System.getProperty("solr.host") : prop.getProperty("solr.host");
            ProcessBuilder pb = new ProcessBuilder()
                    .command(pythonPath, cedarTemplateConverterPath, "--cedar_template", templateJson,
                    "--output_type", outputType, "--dv_api_key", dvApiKey, "--solr_collection", solrCollectionName,
                    "--solr_address", solrAddress, "--dv_solr_script", dvUpdaterScriptPath,
                    "--dv_solr_schema", solrSchemaPath, "--skip_dv_upload");
            pb.redirectErrorStream(true);

            Process p = pb.start();
            bufferedReader = new BufferedReader(new InputStreamReader(p.getInputStream()));

            while ((line = bufferedReader.readLine()) != null) {
                converted.add(line);
            }

            int exitCode = p.waitFor();
            if (exitCode != 0) {
                throw new Exception("An error occurred during converting the template");
            }
        } finally {
            if (bufferedReader != null) {
                bufferedReader.close();
            }
        }

        return converted;
    }

    @POST
    @Consumes("text/tab-separated-values")
    @Path("updateMdb/{identifier}")
    public Response updateMdb(@PathParam("identifier") String dvIdtf, File file) {
        String metadataBlockName;

        try {
            Response response = datasetFieldServiceApi.loadDatasetFields(file);
            if (!response.getStatusInfo().toEnum().equals(Response.Status.OK)) {
                throw new Exception("Failed to load dataset fields");
            }
            metadataBlockName = ((javax.json.JsonObject) response.getEntity()).getJsonObject("data").getJsonArray("added").getJsonObject(0).getString("name");
            updateMetadataBlock(dvIdtf, metadataBlockName);
        } catch (Exception e) {
            return error(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }
        return Response.ok("Metadata block of dataverse with name: " + metadataBlockName + " updated").build();
    }

    private void updateMetadataBlock(String dvIdtf, String metadataBlockName) throws Exception {
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command(System.getProperty("dv.updater.script.path") != null ? System.getProperty("dv.updater.script.path") : prop.getProperty("dv.updater.script.path"), System.getProperty("solr.schema.path") != null ? System.getProperty("solr.schema.path") : prop.getProperty("solr.schema.path"));
        processBuilder.redirectErrorStream(true);

        updateEnabledMetadataBlocks(dvIdtf, metadataBlockName);
        String newSchema = index.getSolrSchema();

        Process process = processBuilder.start();
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(process.getOutputStream()));

        writer.write(newSchema);
        writer.flush();
        writer.close();

        int exitVal = process.waitFor();
        if (exitVal == 0) {
            HttpClient client= HttpClient.newHttpClient();
            String solrHost = System.getProperty("solr.host") != null ? System.getProperty("solr.host") : prop.getProperty("solr.host");
            String solrCollection = System.getProperty("solr.collection.name") != null ? System.getProperty("solr.collection.name") : prop.getProperty("solr.collection.name");
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(new URI(solrHost + "/solr/admin/cores?action=RELOAD&core=" + solrCollection))
                    .GET()
                    .build();
            HttpResponse<String> solrResponse = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (solrResponse.statusCode() != 200) {
                throw new Exception("Failed to update solr schema");
            }
        } else {
            throw new Exception("An error occurred during updating the solr schema");
        }
    }

    /**
     * Get all metadata blocks with URI-s
     */
    public Map<String, String> listBlocksWithUri() throws Exception {
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
        propUriMap.put(blk.getName(), Optional.ofNullable(blk.getNamespaceUri()).map(Objects::toString).orElse(""));

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
        Set<String> names = new HashSet<>() {};
        Response listResp = dataverses.listMetadataBlocks(dvIdtf);
        if (!listResp.getStatusInfo().toEnum().equals(Response.Status.OK)) {
            throw new Exception("Failed to list Metadata blocks");
        }
        javax.json.JsonArray metadataBlocks = ((javax.json.JsonObject) listResp.getEntity()).getJsonArray("data");

        metadataBlocks.forEach(block -> names.add(block.asJsonObject().get("name").toString()));
        names.add("\"" + metadataBlockName + "\"");

        try (Response response = dataverses.setMetadataBlocks(dvIdtf, names.toString())) {
            if (!response.getStatusInfo().toEnum().equals(Response.Status.OK)) {
                throw new Exception("Failed to set Metadata blocks");
            }
        }
    }

    /**
     * Returns a JSON sub-element from the given JsonElement and the given path
     *
     * @param json - a Gson JsonElement
     * @param path - a JSON path, e.g. a.b.c[2].d
     * @return - a sub-element of json according to the given path
     */
    public static JsonElement getJsonElement(JsonElement json, String path){

        String[] parts = path.split("\\.|\\[|\\]");
        JsonElement result = json;

        for (String key : parts) {

            key = key.trim();
            if (key.isEmpty())
                continue;

            if (result == null){
                result = JsonNull.INSTANCE;
                break;
            }

            if (result.isJsonObject()){
                result = ((JsonObject)result).get(key);
            }
            else if (result.isJsonArray()){
                int ix = Integer.parseInt(key);
                result = ((com.google.gson.JsonArray)result).get(ix);
            }
            else break;
        }

        return result;
    }

    public static List<String> getStringList(JsonElement json, String path) {
        Gson gson = new Gson();
        Type type = new TypeToken<List<String>>(){}.getType();
        com.google.gson.JsonArray jsonArray = getJsonElement(json, path).getAsJsonArray();
        return gson.fromJson(jsonArray, type);
    }

    public static JsonObject getJsonObject(JsonElement json, String path) {
        return getJsonElement(json, path).getAsJsonObject();
    }

    public static CedarTemplateErrors checkCedarTemplate(String cedarTemplate, CedarTemplateErrors cedarTemplateErrors, Map<String, String> dvPropTermUriPairs, String parentPath, Boolean lvl2, List<String> listOfStaticFields) {
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
                        !s.getKey().matches("^(?![_\\W].*_$)\\D\\w+") ||
                        listOfStaticFields.contains(s.getKey()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

        if (!invalidNames.isEmpty()) {
            cedarTemplateErrors.invalidNames.addAll(invalidNames);
        }

        for (String prop : propNames) {
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
                    if ((k.equals(prop) && !v.equals(termUri)) || (!termUri.equals("") && v.equals(termUri) && !k.equals(prop))) {
                        Pair<String, String> actPair = Pair.create(prop, termUri);
                        if (cedarTemplateErrors.incompatiblePairs.containsKey(actPair)) {
                            cedarTemplateErrors.incompatiblePairs.get(actPair).put(k, v);
                        } else {
                            cedarTemplateErrors.incompatiblePairs.put(actPair, new HashMap<>(Map.of(k, v)));
                        }
                        isNew = false;
                    }
                }

                if (isNew) {
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
                    checkCedarTemplate(actProp.toString(), cedarTemplateErrors, dvPropTermUriPairs, newPath, false, listOfStaticFields);
                } else {
                    checkCedarTemplate(actProp.toString(), cedarTemplateErrors, dvPropTermUriPairs, newPath, true, listOfStaticFields);
                }
            } else {
                if (!propType.equals("TemplateField") && !propType.equals("StaticTemplateField")) {
                    cedarTemplateErrors.unprocessableElements.add(newPath);
                }
            }
        }

        return cedarTemplateErrors;
    }

    public static ArrayList<String> collectHunTranslations(String cedarTemplate, String parentPath, ArrayList<String> hunTranslations) {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        JsonObject cedarTemplateJson = gson.fromJson(cedarTemplate, JsonObject.class);

        if (parentPath.equals("/properties")) {
            hunTranslations.add("metadatablock.name = " + getJsonElement(cedarTemplateJson, "schema:name").getAsString());
            if (cedarTemplateJson.has("hunTitle")) {
                hunTranslations.add("metadatablock.displayName = " + getJsonElement(cedarTemplateJson, "hunTitle").getAsString());
            }
            if (cedarTemplateJson.has("hunDescription")) {
                hunTranslations.add("metadatablock.description = " + getJsonElement(cedarTemplateJson, "hunDescription").getAsString());
            }
        }

        List<String> propNames = getStringList(cedarTemplateJson, "_ui.order");

        for (String prop : propNames) {
            JsonObject actProp = getJsonObject(cedarTemplateJson, "properties." + prop);
            String newPath = parentPath + "/" + prop;
            String propType;
            if (actProp.has("hunLabel")) {
                String dftName = getJsonElement(actProp, "schema:name").getAsString();
                String hunLabel = getJsonElement(actProp, "hunLabel").getAsString();
                hunTranslations.add(String.format("datasetfieldtype.%1$s.title = %2$s", dftName, hunLabel));
            }
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
                if (actProp.has("hunTitle") && !actProp.has("hunLabel")) {
                    String dftName = getJsonElement(actProp, "schema:name").getAsString();
                    String hunTitle = getJsonElement(actProp, "hunTitle").getAsString();
                    hunTranslations.add(String.format("datasetfieldtype.%1$s.title = %2$s", dftName, hunTitle));
                }
                if (actProp.has("hunDescription")) {
                    String dftName = getJsonElement(actProp, "schema:name").getAsString();
                    String hunDescription = getJsonElement(actProp, "hunDescription").getAsString();
                    hunTranslations.add(String.format("datasetfieldtype.%1$s.description = %2$s", dftName, hunDescription));
                }
                collectHunTranslations(actProp.toString(), newPath, hunTranslations);
            }
        }

        return hunTranslations;
    }


    //region Copied functions from edu.harvard.iq.dataverse.api.DatasetFieldServiceApi to avoid modifying the base code
    private String parseMetadataBlock(String[] values) {
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

        metadataBlockService.save(mdb);
        return mdb.getName();
    }

    private String parseDatasetField(String[] values) {

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
        datasetFieldService.save(dsf);
        return dsf.getName();
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

    // This function is copied from edu.harvard.iq.dataverse.api.DatasetFieldServiceApi but modified to process String lists
    public void loadDatasetFields(List<String> lines, String templateName) throws Exception {
        ActionLogRecord alr = new ActionLogRecord(ActionLogRecord.ActionType.Admin, "loadDatasetFields");
        alr.setInfo( templateName );
        String splitBy = "\t";
        int lineNumber = 0;
        DatasetFieldServiceApi.HeaderType header = null;
        JsonArrayBuilder responseArr = Json.createArrayBuilder();
        String[] values;
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
                            responseArr.add( Json.createObjectBuilder()
                                    .add("name", parseMetadataBlock(values))
                                    .add("type", "MetadataBlock"));
                            break;

                        case DATASETFIELD:
                            responseArr.add( Json.createObjectBuilder()
                                    .add("name", parseDatasetField(values))
                                    .add("type", "DatasetField") );
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

    private static class CedarTemplateErrors {
        public ArrayList<String> unprocessableElements;
        public ArrayList<String> invalidNames;
        public Map<Pair<String, String>, HashMap<String, String>> incompatiblePairs;

        public CedarTemplateErrors(ArrayList<String> unprocessableElements, ArrayList<String> invalidNames, Map<Pair<String, String>, HashMap<String, String>> incompatiblePairs) {
            this.unprocessableElements = unprocessableElements;
            this.invalidNames = invalidNames;
            this.incompatiblePairs = incompatiblePairs;
        }

        public javax.json.JsonObject toJson() {
            NullSafeJsonBuilder builder = NullSafeJsonBuilder.jsonObjectBuilder();

            if (!unprocessableElements.isEmpty()) {
                JsonArrayBuilder jsonArrayBuilder = Json.createArrayBuilder();
                unprocessableElements.forEach(jsonArrayBuilder::add);
                builder.add("unprocessableElements", jsonArrayBuilder);
            }

            if (!invalidNames.isEmpty()) {
                JsonArrayBuilder jsonArrayBuilder = Json.createArrayBuilder();
                invalidNames.forEach(jsonArrayBuilder::add);
                builder.add("invalidNames", jsonArrayBuilder);
            }

            if (!incompatiblePairs.isEmpty()) {
                JsonObjectBuilder jsonObjectBuilder = Json.createObjectBuilder();
                for (var a : incompatiblePairs.entrySet()) {
                    jsonObjectBuilder.add(a.getKey().toString(), Json.createObjectBuilder(new HashMap<>(a.getValue())));
                }
                builder.add("incompatiblePairs", jsonObjectBuilder);
            }

            return builder.build();
        }
    }

}
