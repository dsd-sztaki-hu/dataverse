package edu.harvard.iq.dataverse.api.arp;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.gson.*;
import edu.harvard.iq.dataverse.*;
import edu.harvard.iq.dataverse.actionlogging.ActionLogRecord;
import edu.harvard.iq.dataverse.api.AbstractApiBean;
import edu.harvard.iq.dataverse.api.DatasetFieldServiceApi;
import edu.harvard.iq.dataverse.api.arp.util.JsonHelper;
import edu.harvard.iq.dataverse.arp.*;
import edu.harvard.iq.dataverse.authorization.users.AuthenticatedUser;
import edu.harvard.iq.dataverse.engine.command.DataverseRequest;
import edu.harvard.iq.dataverse.engine.command.impl.CreateDatasetVersionCommand;
import edu.harvard.iq.dataverse.engine.command.impl.UpdateDatasetVersionCommand;
import edu.harvard.iq.dataverse.search.SearchFields;
import edu.harvard.iq.dataverse.util.BundleUtil;
import edu.harvard.iq.dataverse.util.json.NullSafeJsonBuilder;
import edu.kit.datamanager.ro_crate.RoCrate;
import org.apache.commons.lang3.StringUtils;

import javax.ejb.EJB;
import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.*;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static edu.harvard.iq.dataverse.api.arp.util.JsonHelper.*;

import static edu.harvard.iq.dataverse.api.arp.util.JsonHelper.getStringList;
import static edu.harvard.iq.dataverse.util.json.JsonPrinter.json;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.FORBIDDEN;

@Path("arp")
public class ArpApi extends AbstractApiBean {

    private static final Logger logger = Logger.getLogger(ArpApi.class.getCanonicalName());
    private static final Properties prop = new Properties();

    @EJB
    IndexBean index;

    @EJB
    DatasetFieldServiceApiBean datasetFieldServiceApi;

    @EJB
    DatasetFieldServiceBean datasetFieldService;

    @EJB
    DataverseServiceBean dataverseService;

    @EJB
    MetadataBlockServiceBean metadataBlockService;

    @EJB
    DatasetServiceBean datasetService;

    @EJB
    DatasetFieldServiceBean fieldService;

    @EJB
    ArpMetadataBlockServiceBean arpMetadataBlockServiceBean;

    @EJB
    ArpServiceBean arpService;
    
    @EJB
    RoCrateManager roCrateManager;

    @EJB
    ArpConfig arpConfig;

    /**
     * Checks whether a CEDAR template is valid for use as a Metadatablock.
     *
     * Requires no authentication.
     *
     * @param templateJson
     * @return
     */
    @POST
    @Path("/checkCedarTemplate")
    @Consumes("application/json")
    public Response checkCedarTemplateCall(String templateJson) {
        CedarTemplateErrors errors;
        try {
            errors = arpService.checkTemplate(templateJson);
        } catch (Exception e) {
            return error(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        if (!(errors.invalidNames.isEmpty() && errors.unprocessableElements.isEmpty())) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity( NullSafeJsonBuilder.jsonObjectBuilder()
                            .add("status", STATUS_ERROR)
                            .add( "message", errors.toJson() ).build()
                    ).type(MediaType.APPLICATION_JSON_TYPE).build();
        }

        return ok("Valid Template");
    }

    /**
     * Crates a MetadataBlock in the dataverse identified by `dvIdtf` from a CEDAR template.
     *
     * Requires superuser authentication.
     *
     * TODO: why do we have the skipUpload parameter?
     * @param dvIdtf
     * @param skipUpload
     * @param templateJson
     * @return
     * @throws JsonProcessingException
     */
    //TODO: remove added headers
    @POST
    @Path("/cedarToMdb/{dvIdtf}") // TODO: should be importMdbFromCedar, used iin CEDAR template editor
    @Consumes("application/json")
    @Produces("text/tab-separated-values")
    public Response cedarToMdb(
            @PathParam("dvIdtf") String dvIdtf,
            @QueryParam("skipUpload") @DefaultValue("false") boolean skipUpload,
            String templateJson
    ) throws JsonProcessingException
    {
        try {
            AuthenticatedUser user = findAuthenticatedUserOrDie();
            if (!user.isSuperuser()) {
                return error(Response.Status.FORBIDDEN, "Superusers only.");
            }
        } catch (WrappedResponse ex) {
            return error(Response.Status.FORBIDDEN, "Superusers only.");
        }

        String mdbTsv;

        try {
            mdbTsv = arpService.createOrUpdateMdbFromCedarTemplate(dvIdtf, templateJson, skipUpload);

        } catch (CedarTemplateErrorsException cte) {
            logger.log(Level.SEVERE, "CEDAR template upload failed:"+cte.getErrors().toJson());
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity( NullSafeJsonBuilder.jsonObjectBuilder()
                            .add("status", STATUS_ERROR)
                            .add( "message", cte.getErrors().toJson() ).build()
                    ).type(MediaType.APPLICATION_JSON_TYPE).header("Access-Control-Allow-Origin", "*").build();

        } catch (Exception e) {
            logger.log(Level.SEVERE, "CEDAR template upload failed", e);
            return Response.serverError().entity(e.getMessage()).header("Access-Control-Allow-Origin", "*").build();
        }

        //TODO: check why is the origin duplicated if the header is not added here as well as in the ApiBlockingFilter
        //TODO: maybe the cors filter?
        return Response.ok(mdbTsv).header("Access-Control-Allow-Origin", "*").build();
    }

    /**
     * Returns a MetadatabLock in TSV format.
     *
     * Requires no authentication.
     *
     * @param mdbIdtf
     * @return
     */
    @GET
    @Path("/convertMdbToTsv/{identifier}")
    @Produces("text/tab-separated-values")
    public Response convertMdbToTsv(
            @PathParam("identifier") String mdbIdtf
        )
    {
        String mdbTsv;

        try {
            mdbTsv = arpService.exportMdbAsTsv(mdbIdtf);
        } catch (JsonProcessingException e) {
            return Response.serverError().entity(e.getMessage()).build();
        }

        return Response.ok(mdbTsv).build();
    }

    /**
     * Exports a MetadataBlock to CEDAR.
     *
     * Requires superuser authentication
     *
     * @param mdbIdtf
     * @param cedarParams
     * @return
     */
    @POST
    @Path("/exportMdbToCedar/{mdbIdtf}")
    @Consumes("application/json")
    @Produces("application/json")
    public Response exportMdbToCedar(@PathParam("mdbIdtf") String mdbIdtf, ExportToCedarParams cedarParams)
    {
        try {
            AuthenticatedUser user = findAuthenticatedUserOrDie();
            if (!user.isSuperuser()) {
                return error(Response.Status.FORBIDDEN, "Superusers only.");
            }
        } catch (WrappedResponse ex) {
            return error(Response.Status.FORBIDDEN, "Superusers only.");
        }

        String res = null;
        try {
            ObjectMapper mapper = new ObjectMapper();
            String cedarDomain = cedarParams.cedarDomain;

            if (cedarDomain == null || cedarDomain.isBlank()){
                cedarDomain = arpConfig.get("arp.cedar.domain");
            }
            cedarParams.cedarDomain = cedarDomain;

            JsonNode cedarTemplate = mapper.readTree(arpService.tsvToCedarTemplate(arpService.exportMdbAsTsv(mdbIdtf)).toString());
            res = arpService.exportTemplateToCedar(cedarTemplate, cedarParams);
        } catch (WrappedResponse ex) {
            System.out.println(ex.getResponse());
            return error(FORBIDDEN, "Authorized users only.");
        } catch (Exception e) {
            return Response.serverError().entity(e.getMessage()).build();
        }

        return Response.ok(res).build();
    }

    /**
     * Converts a MetadataBlock TSV to a CEDAR template and returns it.
     *
     * Requires no authentication.
     *
     * @param mdbTsv
     * @return
     */
    @GET
    @Path("/convertTsvToCedarTemplate")
    @Consumes("text/tab-separated-values")
    @Produces("application/json")
    public Response convertTsvToCedarTemplate(String mdbTsv)
    {
        String cedarTemplate;

        try {
            cedarTemplate = arpService.tsvToCedarTemplate(mdbTsv).getAsString();
        } catch (JsonProcessingException e) {
            return Response.serverError().entity(e.getMessage()).build();
        }

        return Response.ok(cedarTemplate).build();
    }

    /**
     * Exports a MetadataBlock given as TSV to CEDAR.
     *
     * cedarData
     * @param cedarDataAndTsv
     * @return
     */
    @POST
    @Path("/exportTsvToCedar/")
    @Consumes("application/json")
    @Produces("application/json")
    public Response exportTsvToCedar(ExportTsvToCedarData cedarDataAndTsv)
    {
        try {
            AuthenticatedUser user = findAuthenticatedUserOrDie();
            if (!user.isSuperuser()) {
                return error(Response.Status.FORBIDDEN, "Superusers only.");
            }
        } catch (WrappedResponse ex) {
            return error(Response.Status.FORBIDDEN, "Superusers only.");
        }

        try {
            ObjectMapper mapper = new ObjectMapper();
            ExportToCedarParams cedarParams = cedarDataAndTsv.cedarParams;
            String cedarTsv = cedarDataAndTsv.cedarTsv;

            String cedarDomain = cedarParams.cedarDomain;

            if (cedarDomain == null || cedarDomain.isBlank()){
                cedarDomain = arpConfig.get("arp.cedar.domain");
            }
            cedarParams.cedarDomain = cedarDomain;


            JsonNode cedarTemplate = mapper.readTree(arpService.tsvToCedarTemplate(cedarTsv).toString());
            arpService.exportTemplateToCedar(cedarTemplate, cedarParams);
        } catch (WrappedResponse ex) {
            System.out.println(ex.getResponse());
            return error(FORBIDDEN, "Authorized users only.");
        } catch (Exception e) {
            return Response.serverError().entity(e.getMessage()).build();
        }

        return Response.ok().build();
    }

    /**
     * Converts a CEDAR template to a Describo profile and returns it.
     *
     * Requires no authentication.
     *
     * @param templateJson
     * @return
     */
    @POST
    @Path("/convertCedarTemplateToDescriboProfile")
    @Consumes("application/json")
    @Produces("application/json")
    public Response convertCedarTemplateToDescriboProfile(
            @QueryParam("lang") String language,
            String templateJson
    ) {
        String describoProfile;

        try {
            Response checkTemplateResponse = checkCedarTemplateCall(templateJson);
            if (!checkTemplateResponse.getStatusInfo().toEnum().equals(Response.Status.OK)) {
                String errors = checkTemplateResponse.getEntity().toString();
                throw new Exception(errors);
            }
            describoProfile = arpService.convertTemplate(templateJson, "describo", language, new HashSet<>());
        } catch (Exception e) {
            return Response.serverError().entity(e.getMessage()).build();
        }

        return Response.ok(describoProfile).build();
    }

    /**
     * Converts a MetadatBlock to Describo profile and returns it.
     *
     * Requires no authentication.
     *
     * @param mdbIdtf
     * @return
     */
    @GET
    @Path("/convertMdbToDescriboProfile/{mdbIdtf}")
    @Produces("application/json")
    public Response convertMdbToDescriboProfile(
            @PathParam("mdbIdtf") String mdbIdtf,
            @QueryParam("lang") String language
    ) {
        String describoProfile;
        
        try {
            String templateJson = arpService.tsvToCedarTemplate(arpService.exportMdbAsTsv(mdbIdtf)).toString();
            describoProfile = arpService.convertTemplate(templateJson, "describo", language, new HashSet<>());
        } catch (Exception e) {
            return Response.serverError().entity(e.getMessage()).build();
        }

        return Response.ok(describoProfile).build();
    }

    /**
     * Converts a number of MetadataBlock to a merged Describo profile where each MDB is represented
     * as a layout group of the profile.
     *
     * Requires no authentication.
     *
     * @param identifiers
     * @return
     */
    @GET
    @Path("/convertMdbsToDescriboProfile/{identifiers}")
    @Produces("application/json")
    public Response convertMdbsToDescriboProfile(
            @PathParam("identifiers") String identifiers,
            @QueryParam("lang") String language
    ) {
        try {
            // ids separated by commas
            var ids = identifiers.split(",\\s*");
            JsonObject mergedProfile = null;
            JsonArray mergedProfileInputs = null;
            JsonObject mergedProfileClasses = null;
            JsonArray enabledClasses = null;
            JsonObject layouts = null;
            Gson gson = new GsonBuilder().setPrettyPrinting().create();

            for (int i=0; i<ids.length; i++) {
                // Convert TSV to CEDAR template without converting '.' to ':' in field names
                String templateJson = arpService.tsvToCedarTemplate(arpService.exportMdbAsTsv(ids[i]), false).toString();
                String profile = arpService.convertTemplate(templateJson, "describo", language, new HashSet<>());
                JsonObject profileJson = gson.fromJson(profile, JsonObject.class);

                if (mergedProfile == null) {
                    mergedProfile = profileJson;
                    mergedProfileClasses = mergedProfile.getAsJsonObject("classes");
                    mergedProfileInputs = mergedProfileClasses
                            .getAsJsonObject("Dataset")
                            .getAsJsonArray("inputs");
                    enabledClasses = mergedProfile.getAsJsonArray("enabledClasses");
                    layouts = new JsonObject();
                    mergedProfile.add("layouts", layouts);
                }

                // Add all inputs from the profile to the merged profile
                JsonObject classes = profileJson.getAsJsonObject("classes");
                JsonArray inputs = classes
                        .getAsJsonObject("Dataset")
                        .getAsJsonArray("inputs");
                JsonObject metadata = profileJson.getAsJsonObject("metadata");
                // Get rid of the "Metadata" suffix.
                if (metadata.get("name").getAsString().endsWith(" Metadata")) {
                    String name = metadata.get("name").getAsString();
                    metadata.addProperty("name", name.substring(0, name.length()-" Metadata".length()));
                }
                mergedProfileInputs.addAll(inputs);

                // Add all classes from the profile to the merged profile
                final var finalMergedProfileClasses = mergedProfileClasses;
                final var finalEnabledClasses = enabledClasses;
                classes.keySet().forEach(k -> {
                    if (!k.equals("Dataset")) {
                        finalMergedProfileClasses.add(k, classes.get(k));
                        finalEnabledClasses.add(new JsonPrimitive(k));
                    }
                });

                // Add a group for the profile in the merged profile's Dataset layout
                JsonArray datasetLayout = layouts.getAsJsonArray("Dataset");
                if (datasetLayout == null) {
                    datasetLayout = new JsonArray();
                    layouts.add("Dataset", datasetLayout);
                }
                JsonObject layoutObj = new JsonObject();
                layoutObj.addProperty("name", metadata.get("name").getAsString());
                layoutObj.addProperty("description", metadata.get("description").getAsString());
                JsonArray layoutInputs = new JsonArray();
                inputs.iterator().forEachRemaining(jsonElement -> {
                    layoutInputs.add(jsonElement.getAsJsonObject().get("name"));
                });
                layoutObj.add("inputs", layoutInputs);
                datasetLayout.add(layoutObj);
            }

            return Response.ok(gson.toJson(mergedProfile)).build();
        } catch (Exception e) {
            e.printStackTrace();
            return Response.serverError().entity(e.getMessage()).build();
        }
    }


    /**
     * Updates MDB from an uploaded TSV file.
     *
     * @param dvIdtf
     * @param file
     * @return
     */
    @POST
    @Consumes("text/tab-separated-values")
    @Path("/updateMdb/{dvIdtf}")
    public Response updateMdb(@PathParam("dvIdtf") String dvIdtf, File file) {
        String metadataBlockName;

        try {
            Response response = datasetFieldServiceApi.loadDatasetFields(file);
            if (!response.getStatusInfo().toEnum().equals(Response.Status.OK)) {
                throw new Exception("Failed to load dataset fields");
            }
            metadataBlockName = ((javax.json.JsonObject) response.getEntity()).getJsonObject("data").getJsonArray("added").getJsonObject(0).getString("name");
            arpService.updateMetadataBlock(dvIdtf, metadataBlockName);
        } catch (Exception e) {
            return error(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }
        return Response.ok("Metadata block of dataverse with name: " + metadataBlockName + " updated").build();
    }

    @GET
    @Path("/rocrate/{persistentId : .+}")
    @Produces("application/json")
    public Response getRoCrate(@PathParam("persistentId") String persistentId) {
        JsonObject roCrateJson;

        try {
            findAuthenticatedUserOrDie();
            Dataset dataset = datasetService.findByGlobalId(persistentId);
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            String roCratePath = roCrateManager.getRoCratePath(dataset);
            if (!Files.exists(Paths.get(roCratePath))) {
                roCrateManager.createOrUpdateRoCrate(dataset);
            }
            BufferedReader bufferedReader = new BufferedReader(new FileReader(roCratePath));
            roCrateJson = gson.fromJson(bufferedReader, JsonObject.class);
        } catch (FileNotFoundException e) {
            return Response.serverError().entity(e.getMessage()).build();
        } catch (WrappedResponse ex) {
            System.out.println(ex.getResponse());
            return error(FORBIDDEN, "Authorized users only.");
        } catch (Exception e) {
            return error(Response.Status.INTERNAL_SERVER_ERROR, e.getLocalizedMessage());
        }

        return Response.ok(roCrateJson.toString()).build();
    }

    @POST
    @Path("/rocrate/{persistentId : .+}")
    @Consumes("application/json")
    @Produces("application/json")
    public Response updateRoCrate(@PathParam("persistentId") String persistentId, String roCrateJson) throws JsonProcessingException {
        Map<String, DatasetFieldType> datasetFieldTypeMap;
        Dataset dataset;
        try {
            findAuthenticatedUserOrDie();
            // TODO: collect only from mdbs in conformsTo
            dataset = datasetService.findByGlobalId(persistentId);
            try (FileWriter writer = new FileWriter(roCrateManager.getRoCratePath(dataset))) {
                writer.write(roCrateManager.preProcessRoCrateFromAroma(roCrateJson));
            }
        } catch (IOException e) {
            return Response.serverError().entity(e.getMessage()).build();
        } catch (WrappedResponse ex) {
            System.out.println(ex.getResponse());
            return error(FORBIDDEN, "Authorized users only.");
        }

        String importFormat = roCrateManager.importRoCrate(dataset);

        //region Copied from edu.harvard.iq.dataverse.api.Datasets.updateDraftVersion
        try ( StringReader rdr = new StringReader(importFormat) ) {
            DataverseRequest req = createDataverseRequest(findUserOrDie());
            Dataset ds = datasetService.findByGlobalId(persistentId);
            javax.json.JsonObject json = Json.createReader(rdr).readObject();
            DatasetVersion incomingVersion = jsonParser().parseDatasetVersion(json);

            // clear possibly stale fields from the incoming dataset version.
            // creation and modification dates are updated by the commands.
            incomingVersion.setId(null);
            incomingVersion.setVersionNumber(null);
            incomingVersion.setMinorVersionNumber(null);
            incomingVersion.setVersionState(DatasetVersion.VersionState.DRAFT);
            incomingVersion.setDataset(ds);
            incomingVersion.setCreateTime(null);
            incomingVersion.setLastUpdateTime(null);

            if (!incomingVersion.getFileMetadatas().isEmpty()){
                return error( Response.Status.BAD_REQUEST, "You may not add files via this api.");
            }

            boolean updateDraft = ds.getLatestVersion().isDraft();

            DatasetVersion managedVersion;
            if (updateDraft) {
                final DatasetVersion editVersion = ds.getOrCreateEditVersion();
                editVersion.setDatasetFields(incomingVersion.getDatasetFields());
                editVersion.setTermsOfUseAndAccess(incomingVersion.getTermsOfUseAndAccess());
                editVersion.getTermsOfUseAndAccess().setDatasetVersion(editVersion);
                boolean hasValidTerms = TermsOfUseAndAccessValidator.isTOUAValid(editVersion.getTermsOfUseAndAccess(), null);
                if (!hasValidTerms) {
                    return error(Response.Status.CONFLICT, BundleUtil.getStringFromBundle("dataset.message.toua.invalid"));
                }
                Dataset managedDataset = execCommand(new UpdateDatasetVersionCommand(ds, req));
                managedVersion = managedDataset.getOrCreateEditVersion();
            } else {
                boolean hasValidTerms = TermsOfUseAndAccessValidator.isTOUAValid(incomingVersion.getTermsOfUseAndAccess(), null);
                if (!hasValidTerms) {
                    return error(Response.Status.CONFLICT, BundleUtil.getStringFromBundle("dataset.message.toua.invalid"));
                }
                managedVersion = execCommand(new CreateDatasetVersionCommand(req, ds, incomingVersion));
            }

            ObjectMapper mapper = (new ObjectMapper()).enable(SerializationFeature.WRITE_SINGLE_ELEM_ARRAYS_UNWRAPPED).enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY).enable(DeserializationFeature.UNWRAP_SINGLE_VALUE_ARRAYS);
            RoCrate postProcessedRoCrate = roCrateManager.postProcessRoCrateFromAroma(managedVersion.getDataset());
            String postProcessedRoCrateString = mapper.readTree(postProcessedRoCrate.getJsonMetadata()).toPrettyString();
            try (FileWriter writer = new FileWriter(roCrateManager.getRoCratePath(dataset))) {
                writer.write(postProcessedRoCrateString);
            }
            return ok( json(managedVersion) );

        } catch (edu.harvard.iq.dataverse.util.json.JsonParseException ex) {
            logger.log(Level.SEVERE, "Semantic error parsing dataset version Json: " + ex.getMessage(), ex);
            return error( Response.Status.BAD_REQUEST, "Error parsing dataset version: " + ex.getMessage() );

        } catch (WrappedResponse ex) {
            return ex.getResponse();
        } catch (IOException ex) {
            logger.severe("Error occurred during post processing RO-Crate from AROMA" + ex.getMessage());
            return error(BAD_REQUEST, "Error occurred during post processing RO-Crate from AROMA" + ex.getMessage());
        }
        //endregion
    }


    private void updateMetadataBlock(String dvIdtf, String metadataBlockName) throws Exception {
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
                            dsfArp.setCedarDefinition(cedarFieldJsonDefs.get(fieldName).toString());
                            dsfArp.setFieldType(dsf);
                            var override = arpMetadataBlockServiceBean.findOverrideByOriginal(dsf);
                            dsfArp.setOverride(override);
                            JsonElement cedarDef = cedarFieldJsonDefs.get(fieldName).getAsJsonObject().has("items") ? cedarFieldJsonDefs.get(fieldName).getAsJsonObject().get("items") : cedarFieldJsonDefs.get(fieldName);
                            dsfArp.setHasExternalValues(JsonHelper.getJsonObject(cedarDef, "_valueConstraints.branches[0]") != null);
                            arpMetadataBlockServiceBean.save(dsfArp);
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
        public HashMap<String, DatasetFieldTypeOverride> incompatiblePairs;

        public CedarTemplateErrors(ArrayList<String> unprocessableElements, ArrayList<String> invalidNames, HashMap<String, DatasetFieldTypeOverride> incompatiblePairs) {
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

            return builder.build();
        }
    }

}
