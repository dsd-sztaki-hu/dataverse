package edu.harvard.iq.dataverse.api.arp;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.*;
import edu.harvard.iq.dataverse.*;
import edu.harvard.iq.dataverse.api.AbstractApiBean;
import edu.harvard.iq.dataverse.api.auth.AuthRequired;
import edu.harvard.iq.dataverse.arp.*;
import edu.harvard.iq.dataverse.arp.rocrate.RoCrateExportManager;
import edu.harvard.iq.dataverse.arp.rocrate.RoCrateImportManager;
import edu.harvard.iq.dataverse.arp.rocrate.RoCrateServiceBean;
import edu.harvard.iq.dataverse.authorization.Permission;
import edu.harvard.iq.dataverse.authorization.users.AuthenticatedUser;
import edu.harvard.iq.dataverse.authorization.users.PrivateUrlUser;
import edu.harvard.iq.dataverse.engine.command.DataverseRequest;
import edu.harvard.iq.dataverse.engine.command.impl.CreateDatasetVersionCommand;
import edu.harvard.iq.dataverse.engine.command.impl.GetLatestAccessibleDatasetVersionCommand;
import edu.harvard.iq.dataverse.engine.command.impl.UpdateDatasetVersionCommand;
import edu.harvard.iq.dataverse.search.IndexServiceBean;
import edu.harvard.iq.dataverse.util.BundleUtil;
import edu.harvard.iq.dataverse.util.ConstraintViolationUtil;
import edu.harvard.iq.dataverse.util.json.JsonUtil;
import edu.harvard.iq.dataverse.util.json.NullSafeJsonBuilder;
import edu.kit.datamanager.ro_crate.RoCrate;
import jakarta.annotation.PreDestroy;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Context;
import org.apache.solr.client.solrj.SolrServerException;

import jakarta.ejb.EJB;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static edu.harvard.iq.dataverse.util.json.JsonPrinter.json;
import static jakarta.ws.rs.core.Response.Status.*;
import static edu.harvard.iq.dataverse.api.ApiConstants.STATUS_ERROR;

import java.net.URI;
import jakarta.ws.rs.core.HttpHeaders;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;


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
    RoCrateImportManager roCrateImportManager;
    
    @EJB
    RoCrateExportManager roCrateExportManager;
    
    @EJB
    RoCrateServiceBean roCrateServiceBean;

    @EJB
    ArpConfig arpConfig;

    @EJB
    PermissionServiceBean permissionService;

    @EJB
    IndexServiceBean indexService;

    @Inject
    DataverseSession dataverseSession;

    public ArpApi() throws NoSuchAlgorithmException, KeyManagementException {
    }

    private HttpClient createHttpClient(ExecutorService executorService) throws NoSuchAlgorithmException, KeyManagementException {
        TrustManager[] trustAllCerts = new TrustManager[]{
                new X509TrustManager() {
                    public X509Certificate[] getAcceptedIssuers() {
                        return new X509Certificate[]{};
                    }

                    public void checkClientTrusted(X509Certificate[] certs, String authType) {
                    }

                    public void checkServerTrusted(X509Certificate[] certs, String authType) {
                    }
                }
        };

        // Install the all-trusting trust manager
        SSLContext sslContext = SSLContext.getInstance("SSL");
        sslContext.init(null, trustAllCerts, new SecureRandom());

        // Create a HttpClient with the custom SSLContext and ExecutorService
        return HttpClient.newBuilder()
                .sslContext(sslContext)
                .executor(executorService)
                .build();
    }




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
            e.printStackTrace();
            return error(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }

        if (!(errors.invalidNames.isEmpty() && errors.unprocessableElements.isEmpty() && errors.errors.isEmpty())) {
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
    @AuthRequired
    public Response cedarToMdb(
            @Context ContainerRequestContext crc,
            @PathParam("dvIdtf") String dvIdtf,
            @QueryParam("skipUpload") @DefaultValue("false") boolean skipUpload,
            String templateJson
    ) throws JsonProcessingException
    {
        try {
            AuthenticatedUser user = getRequestAuthenticatedUserOrDie(crc);
            if (!user.isSuperuser()) {
                return error(Response.Status.FORBIDDEN, "Superusers only.");
            }
        } catch (WrappedResponse ex) {
            ex.printStackTrace();
            return error(Response.Status.FORBIDDEN, "Superusers only.");
        }

        String mdbTsv;

        try {
            mdbTsv = arpService.createOrUpdateMdbFromCedarTemplate(dvIdtf, templateJson, skipUpload);

        } catch (CedarTemplateErrorsException cte) {
            cte.printStackTrace();
            logger.log(Level.SEVERE, "CEDAR template upload failed:"+cte.getErrors().toJson());
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity( NullSafeJsonBuilder.jsonObjectBuilder()
                            .add("status", STATUS_ERROR)
                            .add( "message", cte.getErrors().toJson() ).build()
                    ).type(MediaType.APPLICATION_JSON_TYPE).header("Access-Control-Allow-Origin", "*").build();

        } catch (Exception e) {
            e.printStackTrace();
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
     * @param mdbName
     * @return
     */
    @GET
    @Path("/convertMdbToTsv/{mdbName}")
    @Produces("text/tab-separated-values")
    public Response convertMdbToTsv(
            @PathParam("mdbName") String mdbName
    )
    {
        String mdbTsv;

        try {
            mdbTsv = arpService.exportMdbAsTsv(mdbName);
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
     * @param mdbName
     * @param cedarParams
     * @return
     */
    @POST
    @Path("/exportMdbToCedar/{mdbName}")
    @Consumes("application/json")
    @Produces("application/json")
    @AuthRequired
    public Response exportMdbToCedar(
            @Context ContainerRequestContext crc,
            @PathParam("mdbName") String mdbName,
            @QueryParam("uuid") String cedarUuid,
            ExportToCedarParams cedarParams)
    {
        try {
            AuthenticatedUser user = getRequestAuthenticatedUserOrDie(crc);
            if (!user.isSuperuser()) {
                return error(Response.Status.FORBIDDEN, "Superusers only.");
            }
        } catch (WrappedResponse ex) {
            ex.printStackTrace();
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

            JsonObject existingTemplate = arpService.getCedarTemplateForMdb(mdbName);
            var actualUuid = cedarUuid != null ? cedarUuid : ArpServiceBean.generateNamedUuid(mdbName);
            JsonNode cedarTemplate = mapper.readTree(arpService.tsvToCedarTemplate(arpService.exportMdbAsTsv(mdbName), existingTemplate).toString());
            res = arpService.exportTemplateToCedar(cedarTemplate, actualUuid, cedarParams);
        } catch (WrappedResponse ex) {
            ex.printStackTrace();
            return error(FORBIDDEN, "Authorized users only.");
        } catch (Exception e) {
            e.printStackTrace();
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
    @POST
    @Path("/convertTsvToCedarTemplate")
    @Consumes("text/tab-separated-values")
    @Produces("application/json")
    public Response convertTsvToCedarTemplate(String mdbTsv)
    {
        String cedarTemplate;

        try {
            cedarTemplate = arpService.tsvToCedarTemplate(mdbTsv, null).toString();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return Response.serverError().entity(e.getMessage()).build();
        }

        return Response.ok(cedarTemplate).build();
    }

    /**
     * Exports a MetadataBlock given as TSV to CEDAR.
     *
     * cedarData
     * @param data
     * @return
     */
    @POST
    @Path("/exportTsvToCedar/")
    @Consumes("application/json")
    @Produces("application/json")
    @AuthRequired
    public Response exportTsvToCedar(
            @Context ContainerRequestContext crc,
            ExportTsvToCedarData data
    )
    {
        try {
            AuthenticatedUser user = getRequestAuthenticatedUserOrDie(crc);
            if (!user.isSuperuser()) {
                return error(Response.Status.FORBIDDEN, "Superusers only.");
            }
        } catch (WrappedResponse ex) {
            ex.printStackTrace();
            return error(Response.Status.FORBIDDEN, "Superusers only.");
        }

        try {
            ObjectMapper mapper = new ObjectMapper();
            ExportToCedarParams cedarParams = data.cedarParams;
            String cedarTsv = data.tsv;

            String cedarDomain = cedarParams.cedarDomain;

            if (cedarDomain == null || cedarDomain.isBlank()){
                cedarDomain = arpConfig.get("arp.cedar.domain");
            }
            cedarParams.cedarDomain = cedarDomain;

            JsonNode cedarTemplate = mapper.readTree(arpService.tsvToCedarTemplate(cedarTsv, null).toString());

            // Use the explicitly provided UUID or create one based on the name in the TSV, ie. "schema:identifier"
            // ine the CEDAR template.
            var actualUuid = data.cedarUuid;
            if (actualUuid == null || actualUuid.isBlank()) {
                actualUuid = ArpServiceBean.generateNamedUuid(cedarTemplate.get("schema:identifier").textValue());
            }

            arpService.exportTemplateToCedar(cedarTemplate, actualUuid, cedarParams);
        } catch (WrappedResponse ex) {
            ex.printStackTrace();
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
            describoProfile = arpService.convertTemplateToDescriboProfile(templateJson, language);
        } catch (Exception e) {
            e.printStackTrace();
            return Response.serverError().entity(e.getMessage()).build();
        }

        return Response.ok(describoProfile).build();
    }

    /**
     * Converts a MetadatBlock to Describo profile and returns it.
     *
     * Requires no authentication.
     *
     * @param mdbName
     * @return
     */
    @GET
    @Path("/convertMdbToDescriboProfile/{mdbName}")
    @Produces("application/json")
    public Response convertMdbToDescriboProfile(
            @PathParam("mdbName") String mdbName,
            @QueryParam("lang") String language
    ) {
        String describoProfile;
        
        try {
            JsonObject existingTemplate = arpService.getCedarTemplateForMdb(mdbName);
            String templateJson = arpService.tsvToCedarTemplate(arpService.exportMdbAsTsv(mdbName), existingTemplate).toString();
            describoProfile = arpService.convertTemplateToDescriboProfile(templateJson, language);
        } catch (Exception e) {
            e.printStackTrace();
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
    @Path("/convertMdbsToDescriboProfile/{mdbNames}")
    @Produces("application/json")
    public Response convertMdbsToDescriboProfile(
            @PathParam("mdbNames") String identifiers,
            @QueryParam("lang") String language
    ) {
        try {
            // names separated by commas
            var names = identifiers.split(",\\s*");
            JsonObject mergedProfile = null;
            JsonArray mergedProfileInputs = null;
            JsonObject mergedProfileClasses = null;
            JsonArray enabledClasses = null;
            JsonObject layouts = null;
            Gson gson = new GsonBuilder().setPrettyPrinting().create();

            for (int i=0; i<names.length; i++) {
                // Convert TSV to CEDAR template without converting '.' to ':' in field names
                JsonObject existingTemplate = arpService.getCedarTemplateForMdb(names[i]);
                String templateJson = arpService.tsvToCedarTemplate(arpService.exportMdbAsTsv(names[i]), false, existingTemplate).toString();
                String profile = arpService.convertTemplateToDescriboProfile(templateJson, language);
                JsonObject profileJson = gson.fromJson(profile, JsonObject.class);
                boolean profileJsonAdded = false;

                if (mergedProfile == null) {
                    mergedProfile = profileJson;
                    profileJsonAdded = true;
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
                // TODO: More generic solution would be:
                // String nameWithoutMetadataSuffix = metadata.get("name").getAsString().replaceAll("(?i) (metadata|metaadatok|metaadatai)$", "");
                // metadata.addProperty("name", nameWithoutMetadataSuffix);
                if (metadata.get("name").getAsString().endsWith(" Metadata")) {
                    String name = metadata.get("name").getAsString();
                    metadata.addProperty("name", name.substring(0, name.length()-" Metadata".length()));
                }

                // If it is not the initial profile, add the inputs
                if (!profileJsonAdded) {
                    mergedProfileInputs.addAll(inputs);
                }

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

            // Add File class matching DV's file metadata structure.
            mergedProfile.getAsJsonObject("classes")
                    .add("File", arpService.getDefaultDescriboProfileFileClass(language));
            mergedProfile.getAsJsonArray("enabledClasses")
                    .add("File");

            // Allow adding File and Dataset as parts of other Datasets
            mergedProfileInputs.add(arpService.getHasPartInput(language));

            return Response.ok(gson.toJson(mergedProfile)).build();
        } catch (Exception e) {
            e.printStackTrace();
            return Response.serverError().entity(e.getMessage()).build();
        }
    }

    @GET
    @Path("/mdbsBasedConformsToIdsOfDataset/{persistentId : .+}")
    @Produces("application/json")
    public Response getMdbsBasedConformsToIdsOfDataset(
            @PathParam("persistentId") String persistentId,
            @QueryParam("lang") String language
    ) throws WrappedResponse, JsonProcessingException
    {
        var ds = datasetSvc.findByGlobalId(persistentId);
        if (ds == null) {
            throw new WrappedResponse(notFound(BundleUtil.getStringFromBundle("find.dataset.error.dataset.not.found.persistentId", Collections.singletonList(persistentId))));
        }
        var gson = new Gson();
        Dataverse dv = ds.getDataverseContext();
        List<String> conformsToIds = dv.getMetadataBlocks().stream()
                .map(metadataBlock -> arpMetadataBlockServiceBean.findMetadataBlockArpForMetadataBlock(metadataBlock).getRoCrateConformsToId())
                .collect(Collectors.toList());
        return Response.ok(gson.toJson(conformsToIds)).build();
    }

    @GET
    @Path("/minimalDescriboProfileForDataset/{persistentId : .+}")
    @Produces("application/json")
    public Response getMinimalDescriboProfileForDataset(
            @PathParam("persistentId") String persistentId,
            @QueryParam("lang") String language
    ) throws WrappedResponse, JsonProcessingException
    {
        var ds = datasetSvc.findByGlobalId(persistentId);
        if (ds == null) {
            throw new WrappedResponse(notFound(BundleUtil.getStringFromBundle("find.dataset.error.dataset.not.found.persistentId", Collections.singletonList(persistentId))));
        }

        var gson = new Gson();
        var profile = gson.fromJson("{\n" +
                "  \"classes\": {\n" +
                "    \"Dataset\": {\n" +
                "      \"inputs\": []\n" +
                "    }\n" +
                "  },\n" +
                "  \"enabledClasses\": [\n" +
                "    \"Dataset\"\n" +
                "  ]\n" +
                "}", JsonObject.class);
        var inputs = profile.getAsJsonObject("classes").getAsJsonObject("Dataset").getAsJsonArray("inputs");
        inputs.add(arpService.getHasPartInput(language));
        inputs.add(arpService.getLicenseInput(language));
        inputs.add(arpService.getDatePublishedInput(language));
        profile.getAsJsonObject("classes")
                .add("File", arpService.getDefaultDescriboProfileFileClass(language));
        profile.getAsJsonArray("enabledClasses")
                .add("File");

        return Response.ok(gson.toJson(profile)).build();
    }

    @GET
    @Path("/describoProfileForDataset/{persistentId : .+}")
    @Produces("application/json")
    public Response getDescriboProfileForDataset(
            @PathParam("persistentId") String persistentId,
            @QueryParam("lang") String language
    ) throws WrappedResponse
    {
        var ds = datasetSvc.findByGlobalId(persistentId);
        if (ds == null) {
            throw new WrappedResponse(notFound(BundleUtil.getStringFromBundle("find.dataset.error.dataset.not.found.persistentId", Collections.singletonList(persistentId))));
        }
        Dataverse dv = ds.getDataverseContext();
        String mdbIds = dv.getMetadataBlocks().stream()
                .map(MetadataBlock::getId)
                .map(String::valueOf)
                .collect(Collectors.joining(","));
        return convertMdbsToDescriboProfile(mdbIds, language);
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
            metadataBlockName = ((jakarta.json.JsonObject) response.getEntity()).getJsonObject("data").getJsonArray("added").getJsonObject(0).getString("name");
            arpService.updateMetadataBlock(dvIdtf, metadataBlockName);
        } catch (Exception e) {
            e.printStackTrace();
            return error(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage());
        }
        return Response.ok("Metadata block of dataverse with name: " + metadataBlockName + " updated").build();
    }

    @GET
    @Path("/rocrate/{persistentId : .+}")
    @Produces("application/json")
    @AuthRequired
    public Response getRoCrate(
            @Context ContainerRequestContext crc,
            @QueryParam("version") String version,
            @PathParam("persistentId") String persistentId) throws WrappedResponse
    {
        // Get the dataset by pid so that we get is actual ID.
        Dataset dataset = datasetService.findByGlobalId(persistentId);

        // make sure arpConfig is initialized, so that ArpConfig.instance can be used from anywhere
        arpConfig.get("arp.rocrate.previewgenerator.address");

        return response( req -> {
            boolean privateUrlUser = false;
            AuthenticatedUser authenticatedUser = null;
            try {
                authenticatedUser = getRequestAuthenticatedUserOrDie(crc);
            } catch (WrappedResponse ex) {
                // If authenticatedUser == null then it is a guest, and can only be readonly anyway,
                // but we must check if it's a PrivateUrlUser
                if (req.getUser() instanceof PrivateUrlUser) {
                    privateUrlUser = true;
                }
            }
            
            // The opened version is either the version that was requested if that is available to the user or the latest version accessible to the user.
            // For a guest it must be a published version for an author it is either the opened version or DRAFT.
            DatasetVersion opened = null;
            if (version != null && (authenticatedUser != null || privateUrlUser)) {
                if (version.equals("DRAFT") && authenticatedUser != null && !permissionService.userOn(authenticatedUser, dataset).has(Permission.EditDataset)) {
                    opened = execCommand(new GetLatestAccessibleDatasetVersionCommand(req, dataset));
                } else {
                    var openedVersion = dataset.getVersions().stream().filter(dsv -> dsv.getFriendlyVersionNumber().equals(version)).findFirst();
                    if (openedVersion.isPresent()) {
                        opened = openedVersion.get();
                    }   
                }
            }

            // If the opened is not found at this point that can be because the version == null or a wrong version was requested
            // that means the last published version is accessible for a guest and the latest version for a privateUrlUser
            if (opened == null) {
                if (privateUrlUser) {
                    opened = dataset.getLatestVersion();
                } else {
                    opened = execCommand(new GetLatestAccessibleDatasetVersionCommand(req, dataset));
                }
                // At this point if the opened is still null, there must have been something fishy going on with the url
                if (opened == null) {
                    throw new WrappedResponse(error(FORBIDDEN, "Insufficient permission."));
                }
            }
            
            try {
                Gson gson = new GsonBuilder().setPrettyPrinting().create();
                String roCratePath = roCrateServiceBean.getRoCratePath(opened);
                if (!Files.exists(Paths.get(roCratePath))) {
                    roCrateExportManager.createOrUpdateRoCrate(opened);
                    if (dataset.getLatestVersion().isPublished()) {
                        roCrateExportManager.saveRoCrateDraftVersion(opened);
                        roCrateExportManager.finalizeRoCrateForDatasetVersion(opened);
                    }
                }
                BufferedReader bufferedReader = new BufferedReader(new FileReader(roCratePath));
                JsonObject roCrateJson = gson.fromJson(bufferedReader, JsonObject.class);
                // Check whether something is missing or wrong with this ro crate, in which case we regenerate
                if (needToRegenerate(roCrateJson)) {
                    roCrateExportManager.createOrUpdateRoCrate(opened);
                    if (dataset.getLatestVersion().isPublished()) {
                        roCrateExportManager.saveRoCrateDraftVersion(opened);
                        roCrateExportManager.finalizeRoCrateForDatasetVersion(opened);
                    }
                    bufferedReader = new BufferedReader(new FileReader(roCratePath));
                    roCrateJson = gson.fromJson(bufferedReader, JsonObject.class);
                }
                Response.ResponseBuilder resp;
                // If returning the released version it is readonly
                // In any other case the user is already checked to have access to a draft version and can edit
                // Note: need to add Access-Control-Expose-Headers to make X-Arp-RoCrate-Readonly accessible via CORS
                if (privateUrlUser || authenticatedUser == null || (dataset.isLocked() && !dataset.isLockedFor(DatasetLock.Reason.InReview)) 
                        || !permissionService.userOn(authenticatedUser, dataset).has(Permission.EditDataset) 
                        || (opened.isReleased() && !dataset.getLatestVersion().equals(opened))) {
                    resp = Response.ok(roCrateJson.toString());
                    resp = resp.header("X-Arp-RoCrate-Readonly", true)
                            .header("Access-Control-Expose-Headers", "X-Arp-RoCrate-Readonly");
                } else {
                    // the editable version of the requested latest version
                    BufferedReader br = new BufferedReader(new FileReader(roCrateServiceBean.getDraftRoCrateFolder(dataset)));
                    JsonObject draftRoCrateJson = gson.fromJson(br, JsonObject.class);
                    resp = Response.ok(draftRoCrateJson.toString());
                }

                return resp.build();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                return Response.serverError().entity(e.getMessage()).build();
            } catch (WrappedResponse ex) {
                ex.printStackTrace();
                return error(FORBIDDEN, "Authorized users only.");
            } catch (Exception e) {
                e.printStackTrace();
                return error(Response.Status.INTERNAL_SERVER_ERROR, e.getLocalizedMessage());
            }
        }, getRequestUser(crc));
    }

    private boolean needToRegenerate(JsonObject roCrateJson) {
        // Check if license is already generated. If not, regenerate
        var iterator = roCrateJson.getAsJsonArray("@graph").iterator();
        while (iterator.hasNext()) {
            var elem = iterator.next();
            if (elem.getAsJsonObject().get("@id").getAsString().equals("./")) {
                if (!elem.getAsJsonObject().has("license")) {
                    return true;
                }
                if (!elem.getAsJsonObject().has("datePublished")) {
                    return true;
                }
            }
        }
        return false;
    }

    @POST
    @Path("/rocrate/{persistentId : .+}")
    @Consumes("application/json")
    @Produces("application/json")
    @AuthRequired
    public Response updateRoCrate(
            @Context ContainerRequestContext crc,
            @PathParam("persistentId") String persistentId,
            String roCrateJson
    ) {
        Dataset dataset;
        RoCrate preProcessedRoCrate;
        AuthenticatedUser user;
        try {
            user = getRequestAuthenticatedUserOrDie(crc);
            dataset = datasetService.findByGlobalId(persistentId);
            if (dataset.isLocked()) {
                throw new RuntimeException(
                        BundleUtil.getStringFromBundle("dataset.message.locked.editNotAllowed"));
            }
            preProcessedRoCrate = roCrateImportManager.preProcessRoCrateFromAroma(dataset, roCrateJson);
        } catch (IOException | RuntimeException | ArpException e) {
            e.printStackTrace();
            return error(INTERNAL_SERVER_ERROR, e.getMessage());
        } 
        catch (WrappedResponse ex) {
            ex.printStackTrace();
            return error(FORBIDDEN, "Authorized users only.");
        }

        boolean updateDraft = dataset.getLatestVersion().isDraft();
        DatasetVersion newVersion;
        if (updateDraft) {
            newVersion = dataset.getOrCreateEditVersion();
        } else {
            newVersion = new DatasetVersion();
            newVersion.setDataset(dataset);
            newVersion.setVersionState(DatasetVersion.VersionState.DRAFT);
        }
        newVersion.setTermsOfUseAndAccess(dataset.getLatestVersion().getTermsOfUseAndAccess());
        newVersion.getTermsOfUseAndAccess().setDatasetVersion(newVersion);
        boolean hasValidTerms = TermsOfUseAndAccessValidator.isTOUAValid(newVersion.getTermsOfUseAndAccess(), null);
        if (!hasValidTerms) {
            return error(Response.Status.CONFLICT, BundleUtil.getStringFromBundle("dataset.message.toua.invalid"));
        }
        roCrateImportManager.importRoCrate(preProcessedRoCrate, newVersion);

        try {
            DataverseRequest req = createDataverseRequest(user);
            DatasetVersion managedVersion;
            Dataset managedDataset;
            if (updateDraft) {
                var filesToBeDeleted = roCrateImportManager.updateFileMetadatas(newVersion.getDataset(), preProcessedRoCrate);
                if (!filesToBeDeleted.isEmpty()) {
                    for (FileMetadata markedForDelete : filesToBeDeleted) {
                        if (markedForDelete.getId() != null) {
                            dataset.getOrCreateEditVersion().getFileMetadatas().remove(markedForDelete);
                        }
                    }
                    managedDataset = execCommand(new UpdateDatasetVersionCommand(dataset, req, filesToBeDeleted));
                } else {
                    managedDataset = execCommand(new UpdateDatasetVersionCommand(dataset, req));
                }
                managedVersion = managedDataset.getOrCreateEditVersion();
            } else {
                var filesToBeDeleted = roCrateImportManager.updateFileMetadatas(dataset, preProcessedRoCrate);
                managedVersion = execCommand(new CreateDatasetVersionCommand(req, dataset, newVersion));
                if (!filesToBeDeleted.isEmpty()) {
                    for (FileMetadata markedForDelete : filesToBeDeleted) {
                        if (markedForDelete.getId() != null) {
                            managedVersion.getDataset().getOrCreateEditVersion().getFileMetadatas().remove(markedForDelete);
                        }
                    }
                    managedVersion = execCommand(new UpdateDatasetVersionCommand(managedVersion.getDataset(), req, filesToBeDeleted)).getOrCreateEditVersion();
                }
                indexService.indexDataset(dataset, true);
            }

            roCrateImportManager.postProcessRoCrateFromAroma(managedVersion.getDataset(), preProcessedRoCrate);
            String roCratePath = roCrateServiceBean.getRoCratePath(managedVersion);
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            BufferedReader bufferedReader = new BufferedReader(new FileReader(roCratePath));
            JsonObject updatedRoCrate = gson.fromJson(bufferedReader, JsonObject.class);

            return ok( JsonUtil.getJsonObject(updatedRoCrate.toString()) );

        } catch (WrappedResponse ex) {
            ex.printStackTrace();
            return ex.getResponse();
        } catch (IOException | SolrServerException ex ) {
            ex.printStackTrace();
            logger.severe("Error occurred during post processing RO-Crate from AROMA" + ex.getMessage());
            return error(BAD_REQUEST, "Error occurred during post processing RO-Crate from AROMA" + ex.getMessage());
        }
    }

    @GET
    @Path("/cedarResourceProxy/{cedarUrl}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response cedarResourceProxyUrlInPath(
            @PathParam("cedarUrl") String cedarUrl,
            @Context HttpHeaders incomingHeaders
    )
    {
        return doCedarResourceProxy(cedarUrl, incomingHeaders);
    }

    @GET
    @Path("/cedarResourceProxy")
    @Produces(MediaType.APPLICATION_JSON)
    public Response cedarResourceProxyUrlInQuery(
            @QueryParam("url") String cedarUrl,
            @Context HttpHeaders incomingHeaders
    )
    {
        return doCedarResourceProxy(cedarUrl, incomingHeaders);
    }

    /**
     * Proxies requests to the cedar resource server for reading public schemas. Since we cannot have a CEDAR user which
     * only has read right, therefore we cannot have the api key in AROMA, otherwise malicious users may make modifications
     * in the name of that user. Instead we have this poxy, which aonly allows GET access to schemas and also
     * adds the necessary api key authentication to request. For this to work the arp.cedar.domain and
     * arp.cedar.proxyApiKey configurations must be set.
     *
     * @param cedarUrl        the URL to download
     * @param incomingHeaders the request headers
     * @return contents of cedarUrl
     */
    @GET
    @Path("proxy")
    public Response doCedarResourceProxy(
            @QueryParam("cedarUrl") String cedarUrl,
            @Context HttpHeaders incomingHeaders
    ) {
        String subdomain = "resource." + arpConfig.get("arp.cedar.domain");
        String apiKey = arpConfig.get("arp.cedar.proxyApiKey");

        if (cedarUrl == null || cedarUrl.isBlank()) {
            logger.severe("/cedarResourceProxy: URL path parameter is missing");
            return Response.status(Response.Status.BAD_REQUEST).entity("URL parameter is required").build();
        }

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            HttpClient proxyClient = createHttpClient(executorService);

            URI uri = new URI(cedarUrl);
            if (!uri.getHost().endsWith(subdomain)) {
                logger.severe("/cedarResourceProxy: Invalid URL: " + uri);
                return Response.status(Response.Status.BAD_REQUEST).entity("Invalid URL").build();
            }
            logger.info("/cedarResourceProxy: proxying URL: " + uri);

            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                    .uri(uri)
                    .GET();

            // Set authorization and accept headers explicitly
            requestBuilder.header("Authorization", "apiKey " + apiKey);
            requestBuilder.header("Accept", "application/json");

            // Forward the request asynchronously
            CompletableFuture<HttpResponse<byte[]>> responseFuture = proxyClient.sendAsync(requestBuilder.build(), HttpResponse.BodyHandlers.ofByteArray());

            HttpResponse<byte[]> proxiedResponse = responseFuture.get(); // Blocking wait for the response

            // Begin building the response to the client
            Response.ResponseBuilder responseBuilder = Response
                    .status(proxiedResponse.statusCode())
                    .entity(proxiedResponse.body());

            // Filter and set response headers
            java.net.http.HttpHeaders responseHeaders = proxiedResponse.headers();
            responseHeaders.map().forEach((key, values) -> {
                if (!List.of("Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "TE", "Trailer", "Transfer-Encoding", "Upgrade", "Content-Encoding", "Content-Length").contains(key)) {
                    responseBuilder.header(key, String.join(",", values));
                }
            });

            // Add CORS headers to the response
            responseBuilder.header("Access-Control-Allow-Origin", "*")
                    .header("Access-Control-Allow-Methods", "GET")
                    .header("Access-Control-Allow-Headers", "*");

            return responseBuilder.build();
        } catch (Exception e) {
            logger.severe("/cedarResourceProxy: " + e.getMessage());
            e.printStackTrace();
            return Response.status(Response.Status.BAD_REQUEST).entity("Invalid URL").build();
        } finally {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(1, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                    if (!executorService.awaitTermination(1, TimeUnit.SECONDS))
                        System.err.println("ExecutorService did not terminate");
                }
            } catch (InterruptedException ie) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Get the api key (api token) for a user authenticated via JSESSIONID
     * @param incomingHeaders
     * @return
     */
    @GET
    @Path("/apiKey")
    @Produces(MediaType.APPLICATION_JSON)
    @AuthRequired
    public Response getApiKey(
            @Context ContainerRequestContext crc,
            @Context HttpHeaders incomingHeaders
    )
    {
        try {
            var apiKey = arpService.getCurrentUserApiKey(dataverseSession);
            Map<String, String> map = null;
            if (apiKey == null) {
                map = Map.of();
            }
            else {
                map = Map.of("apiKey", apiKey);
            }
            return Response.status(OK).entity(map).build();
        }
        catch (Exception ex) {
            ex.printStackTrace();
            return Response.status(OK).entity(ex.getMessage()).build();
        }

//        try {
//            // Check auth. Even without this, we would just return no apiKey, but rather return an appropriate error
//            AuthenticatedUser u = getRequestAuthenticatedUserOrDie(crc);
//        } catch (WrappedResponse e) {
//            String error = ConstraintViolationUtil.getErrorStringForConstraintViolations(e.getCause());
//            if (!error.isEmpty()) {
//                logger.log(Level.INFO, error);
//                return e.refineResponse(error);
//            }
//            return e.getResponse();
//        }
    }


}
