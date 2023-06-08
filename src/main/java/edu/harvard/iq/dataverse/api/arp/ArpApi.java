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

}