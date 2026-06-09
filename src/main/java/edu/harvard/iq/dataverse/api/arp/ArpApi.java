package edu.harvard.iq.dataverse.api.arp;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.gson.*;
import edu.harvard.iq.dataverse.*;
import edu.harvard.iq.dataverse.api.AbstractApiBean;
import edu.harvard.iq.dataverse.api.auth.AuthRequired;
import edu.harvard.iq.dataverse.arp.*;
import edu.harvard.iq.dataverse.arp.rocrate.*;
import edu.harvard.iq.dataverse.authorization.Permission;
import edu.harvard.iq.dataverse.authorization.users.AuthenticatedUser;
import edu.harvard.iq.dataverse.authorization.users.PrivateUrlUser;
import edu.harvard.iq.dataverse.engine.command.Command;
import edu.harvard.iq.dataverse.engine.command.DataverseRequest;
import edu.harvard.iq.dataverse.engine.command.exception.CommandException;
import edu.harvard.iq.dataverse.engine.command.impl.*;
import edu.harvard.iq.dataverse.ingest.IngestServiceBean;
import edu.harvard.iq.dataverse.license.LicenseServiceBean;
import edu.harvard.iq.dataverse.search.IndexServiceBean;
import edu.harvard.iq.dataverse.util.BundleUtil;
import edu.harvard.iq.dataverse.util.file.CreateDataFileResult;
import edu.harvard.iq.dataverse.util.json.JsonUtil;
import edu.harvard.iq.dataverse.util.json.NullSafeJsonBuilder;
import edu.kit.datamanager.ro_crate.RoCrate;
import jakarta.json.Json;
import jakarta.json.JsonReader;
import jakarta.json.JsonStructure;
import jakarta.json.JsonValue;
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

import static jakarta.ws.rs.core.Response.Status.*;
import static edu.harvard.iq.dataverse.api.ApiConstants.STATUS_ERROR;
import static edu.harvard.iq.dataverse.api.ApiConstants.STATUS_OK;

import java.net.URI;
import jakarta.ws.rs.core.HttpHeaders;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

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
    private static final String MISSING_CONTEXT_WARNING = "Missing @context URI added";
    private static final String MISSING_CONTEXT_FIXED_MESSAGE = "Valid RO-Crate, but missing @context URI-s were added automatically.";
    private static final String VALIDATION_FAILED_MESSAGE = "RO-Crate validation failed";

    private record PrepIssuesDecision(boolean ok, String bodyPrettyJson) {}

    private PrepIssuesDecision decidePrepIssues(RoCrateImportPrepResult result, boolean includeUpdatedRoCrate) {
        if (!result.hasIssues()) {
            return null;
        }
        if (result.hasOnlyWarningsWithMessage(MISSING_CONTEXT_WARNING)) {
            return new PrepIssuesDecision(true, buildRoCrateApiData(result, includeUpdatedRoCrate).toString());
        }
        if (!result.getErrors().isEmpty()) {
            result.removeWarningsWithMessage(MISSING_CONTEXT_WARNING);
        }
        return new PrepIssuesDecision(false, buildRoCrateApiData(result, includeUpdatedRoCrate).toString());
    }

    private jakarta.json.JsonObject buildRoCrateApiData(RoCrateImportPrepResult prepResult, boolean includeUpdatedRoCrate) {
        NullSafeJsonBuilder builder = NullSafeJsonBuilder.jsonObjectBuilder()
                .add("validation", prepResult.toJson());
        if (includeUpdatedRoCrate && prepResult.getRoCrate() != null) {
            builder.add("updatedRoCrate", coerceToJsonValueOrString(prepResult.getRoCrate().getJsonMetadata()));
        }
        return builder.build();
    }

    private JsonValue coerceToJsonValueOrString(String jsonString) {
        if (jsonString == null) {
            return JsonValue.NULL;
        }
        try (JsonReader reader = Json.createReader(new java.io.StringReader(jsonString))) {
            JsonStructure parsed = reader.read();
            return parsed;
        } catch (RuntimeException ex) {
            return Json.createValue(jsonString);
        }
    }

    private jakarta.json.JsonObject withMessage(String message, jakarta.json.JsonObject data) {
        var b = Json.createObjectBuilder();
        String payloadMessage = null;
        if (data != null && data.containsKey("message") && data.get("message") != null) {
            JsonValue m = data.get("message");
            payloadMessage = (m.getValueType() == JsonValue.ValueType.STRING) ? data.getString("message") : m.toString();
        }
        String finalMessage = message;
        if (finalMessage == null) {
            finalMessage = payloadMessage;
        } else if (payloadMessage != null && !payloadMessage.equals(finalMessage)) {
            finalMessage = (finalMessage + " " + payloadMessage).trim();
        }
        if (finalMessage != null) {
            b.add("message", finalMessage);
        }
        if (data != null) {
            for (Map.Entry<String, JsonValue> e : data.entrySet()) {
                if ("message".equals(e.getKey())) continue;
                b.add(e.getKey(), e.getValue());
            }
        }
        return b.build();
    }

    private Response roCrateOk(String message, jakarta.json.JsonObject data) {
        return Response.ok(Json.createObjectBuilder()
                        .add("status", STATUS_OK)
                        .add("data", withMessage(message, data))
                        .build())
                .type(MediaType.APPLICATION_JSON_TYPE)
                .build();
    }

    private Response roCrateOk(String message, jakarta.json.JsonObject data, Map<String, Object> headers) {
        Response.ResponseBuilder builder = Response.ok(Json.createObjectBuilder()
                        .add("status", STATUS_OK)
                        .add("data", withMessage(message, data))
                        .build())
                .type(MediaType.APPLICATION_JSON_TYPE);
        for (Map.Entry<String, Object> e : headers.entrySet()) {
            builder = builder.header(e.getKey(), e.getValue());
        }
        return builder.build();
    }

    private Response roCrateError(Response.Status httpStatus, String message, jakarta.json.JsonObject data) {
        return Response.status(httpStatus)
                .entity(NullSafeJsonBuilder.jsonObjectBuilder()
                        .add("status", STATUS_ERROR)
                        .add("data", withMessage(message, data))
                        .build())
                .type(MediaType.APPLICATION_JSON_TYPE)
                .build();
    }

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
    
    @Inject
    RoCrateUploadServiceBean roCrateUploadServiceBean;

    @EJB
    RoCrateImportMappingServiceBean roCrateImportMappingServiceBean;

    @EJB
    RoCrateImportMappingStoreBean roCrateImportMappingStoreBean;

    @EJB
    DatasetVersionServiceBean datasetVersionService;

    @EJB
    EjbDataverseEngine commandEngine;

    @EJB
    IngestServiceBean ingestService;

    @Inject
    LicenseServiceBean licenseServiceBean;

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
     * Checks whether a CEDAR resource is valid for use in or use as a Metadatablock.
     *
     * Requires no authentication.
     *
     * @param resourceJson
     * @return
     */
    @POST
    @Path("/checkCedarTemplate")
    @Consumes("application/json")
    public Response checkCedarResourceCall(String resourceJson) {
        CedarTemplateErrors errors;
        try {
            errors = arpService.validateCedarResource(resourceJson, true, false);
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

        return ok(NullSafeJsonBuilder.jsonObjectBuilder()
                .add("message", "Valid Resource")
                .add("warnings", errors.warningsAsJson())
                .build());
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
            if (!skipUpload) {
                String metadataBlockName = new ObjectMapper().readTree(templateJson).get("schema:identifier").textValue();
                arpService.updateMetadataBlockInNewTransaction(dvIdtf, metadataBlockName);
            }
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
     * Extracts TemplateElements from a CEDAR Template and uploads them to CEDAR.
     * Requires superuser authentication
     *
     * @param requestBody
     * containing the CEDAR Parameters and the CEDAR Template
     * @return
     */
    @POST
    @Path("/extractTemplateElements")
    @Consumes("application/json")
    @AuthRequired
    public Response extractTemplateElements(
            @Context ContainerRequestContext crc,
            String requestBody)
    {
        List<JsonNode> extractedElements;
        var mapper = new ObjectMapper();
        String extractedElementsJson = null;
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
            JsonNode extractParams = new ObjectMapper().readTree(requestBody);

            JsonNode cedarResource;
            if (extractParams.has("cedarResource") && !extractParams.get("cedarResource").isNull()) {
                cedarResource = extractParams.get("cedarResource");
            }  else {
                return error(Response.Status.BAD_REQUEST, "cedarResource is required");
            }

            ExportToCedarParams exportToCedarParams = arpService.getExtractParams(extractParams);
            extractedElements = arpService.extractTemplateElements(cedarResource, exportToCedarParams);
            extractedElementsJson = mapper.writeValueAsString(extractedElements);
        } catch (WrappedResponse ex) {
            ex.printStackTrace();
            return error(FORBIDDEN, "Authorized users only.");
        } catch (Exception e) {
            e.printStackTrace();
            return Response.serverError().entity(e.getMessage()).build();
        }

        return Response.ok(extractedElementsJson).build();
    }

    /**
     * Extracts TemplateFields from a CEDAR Resource and uploads them to CEDAR.
     * Requires superuser authentication
     *
     * @param requestBody
     * containing the CEDAR Parameters and the CEDAR Resource
     * @return
     */
    @POST
    @Path("/extractTemplateFields")
    @Consumes("application/json")
    @AuthRequired
    public Response extractTemplateFields(
            @Context ContainerRequestContext crc,
            String requestBody)
    {
        List<JsonNode> extractedFields;
        var mapper = new ObjectMapper();
        String extractedFieldsJson = null;
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
            JsonNode extractParams = new ObjectMapper().readTree(requestBody);
            
            JsonNode cedarResource;
            if (extractParams.has("cedarResource") && !extractParams.get("cedarResource").isNull()) {
                cedarResource = extractParams.get("cedarResource");
            }  else {
                return error(Response.Status.BAD_REQUEST, "cedarResource is required");
            }
            
            ExportToCedarParams exportToCedarParams = arpService.getExtractParams(extractParams);
            extractedFields = arpService.extractTemplateFields(cedarResource, exportToCedarParams);
            extractedFieldsJson = mapper.writeValueAsString(extractedFields);
        } catch (WrappedResponse ex) {
            ex.printStackTrace();
            return error(FORBIDDEN, "Authorized users only.");
        } catch (Exception e) {
            e.printStackTrace();
            return Response.serverError().entity(e.getMessage()).build();
        }

        return Response.ok(extractedFieldsJson).build();
    }

    /**
     * Extracts TemplateFields and TemplateElements from a CEDAR Resource and uploads them to CEDAR.
     * Requires superuser authentication
     *
     * @param requestBody
     * containing the CEDAR Parameters and the CEDAR Resource
     * @return
     */
    @POST
    @Path("/extractResources")
    @Consumes("application/json")
    @Produces("application/json")
    @AuthRequired
    public Response extractResources(
            @Context ContainerRequestContext crc,
            String requestBody)
    {
        List<JsonNode> extractedResources;
        var mapper = new ObjectMapper();
        String extractedResourcesJson = null;
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
            JsonNode extractParams = mapper.readTree(requestBody);

            JsonNode cedarResource;
            if (extractParams.has("cedarResource") && !extractParams.get("cedarResource").isNull()) {
                cedarResource = extractParams.get("cedarResource");
            }  else {
                return error(Response.Status.BAD_REQUEST, "cedarResource is required");
            }

            ExportToCedarParams exportToCedarParams = arpService.getExtractParams(extractParams);
            extractedResources = arpService.extractResources(cedarResource, exportToCedarParams);
            extractedResourcesJson = mapper.writeValueAsString(extractedResources);
        } catch (WrappedResponse ex) {
            ex.printStackTrace();
            return error(FORBIDDEN, "Authorized users only.");
        } catch (Exception e) {
            e.printStackTrace();
            return Response.serverError().entity(e.getMessage()).build();
        }

        return Response.ok(extractedResourcesJson).build();
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
            Response checkTemplateResponse = checkCedarResourceCall(templateJson);
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

    @POST
    @Path("/validateRoCrate")
    @Consumes("application/json")
    @Produces("application/json")
    public Response validateRoCrate(
            @QueryParam("strict") @DefaultValue("false") boolean isStrict,
            String roCrateJson)
    {
        try {
            RoCrateImportPrepResult roCrateImportPrepResult = roCrateImportManager.prepareRoCrateForDataverseImport(roCrateJson, null, isStrict);

            PrepIssuesDecision decision = decidePrepIssues(roCrateImportPrepResult, true);
            if (decision != null) {
                jakarta.json.JsonObject data = JsonUtil.getJsonObject(decision.bodyPrettyJson);
                String msg = data.getString("message", decision.ok ? MISSING_CONTEXT_FIXED_MESSAGE : VALIDATION_FAILED_MESSAGE);
                return decision.ok
                        ? roCrateOk(msg, data)
                        : roCrateError(Response.Status.BAD_REQUEST, msg, data);
            }
            return roCrateOk("OK - RO-Crate is valid", buildRoCrateApiData(roCrateImportPrepResult, false));
        } catch (RuntimeException e) {
            e.printStackTrace();
            return roCrateError(INTERNAL_SERVER_ERROR, e.getMessage(), null);
        }
    }
    
    @GET
    @Path("/rocrate/{persistentId : .+}")
    @Produces("application/json")
    @AuthRequired
    public Response getRoCrate(
            @Context ContainerRequestContext crc,
            @QueryParam("version") String version,
            @QueryParam("forceReload") @DefaultValue("false") boolean forceReload,
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
            if (version != null && !version.equals("DRAFT")) {
                var optionalVersion = dataset.getVersions().stream().filter(dsv -> dsv.getFriendlyVersionNumber().equals(version)).findFirst();
                if (optionalVersion.isPresent()) {
                    opened = optionalVersion.get();
                    if (!opened.isPublished() && (authenticatedUser == null || !permissionService.userOn(authenticatedUser, dataset).has(Permission.EditDataset))) {
                        if (!privateUrlUser) {
                            return error(FORBIDDEN, "Anonymous users can download RO-Crate from published versions only.");
//                            return roCrateError(FORBIDDEN, "Anonymous users can download RO-Crate from published versions only.", null);
                        }
                    }
                } else {
                    return error(FORBIDDEN, "The requested RO-Crate version is not available.");

//                    return roCrateError(FORBIDDEN, "The requested RO-Crate version is not available.", null);
                }
            } else {
                opened = execCommand(new GetLatestAccessibleDatasetVersionCommand(req, dataset));
                if (opened == null) {
                    return error(FORBIDDEN, "Insufficient permission.");
//                    return roCrateError(FORBIDDEN, "Insufficient permission.", null);
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
                var shouldForceReload = forceReload && authenticatedUser != null && authenticatedUser.isSuperuser();
                // Check whether something is missing or wrong with this ro crate, in which case we regenerate
                // or a superuser is requesting a force reload.
                if (needToRegenerate(roCrateJson) || shouldForceReload) {
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
//                    jakarta.json.JsonObject data = NullSafeJsonBuilder.jsonObjectBuilder()
//                            .add("roCrate", JsonUtil.getJsonObject(roCrateJson.toString()))
//                            .build();
//                    return roCrateOk("OK", data, Map.of(
//                            "X-Arp-RoCrate-Readonly", true,
//                            "Access-Control-Expose-Headers", "X-Arp-RoCrate-Readonly"
//                    ));
                } else {
                    // the editable version of the requested latest version
                    BufferedReader br = new BufferedReader(new FileReader(roCrateServiceBean.getDraftRoCrateJson(dataset)));
                    JsonObject draftRoCrateJson = gson.fromJson(br, JsonObject.class);
                    resp = Response.ok(draftRoCrateJson.toString());
                    
//                    jakarta.json.JsonObject data = NullSafeJsonBuilder.jsonObjectBuilder()
//                            .add("roCrate", JsonUtil.getJsonObject(draftRoCrateJson.toString()))
//                            .build();
//                    return roCrateOk("OK", data);
                }

                return resp.build();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
//                return roCrateError(INTERNAL_SERVER_ERROR, e.getMessage(), null);
                return Response.serverError().entity(e.getMessage()).build();
            } catch (WrappedResponse ex) {
                ex.printStackTrace();
//                return roCrateError(FORBIDDEN, "Authorized users only.", null);
                return error(FORBIDDEN, "Authorized users only.");
            } catch (Exception e) {
                e.printStackTrace();
//                return roCrateError(INTERNAL_SERVER_ERROR, e.getLocalizedMessage(), null);
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
        RoCrateImportPrepResult preProcessResult;
        RoCrate preProcessedRoCrate;
        AuthenticatedUser user;
        try {
            user = getRequestAuthenticatedUserOrDie(crc);
            dataset = datasetService.findByGlobalId(persistentId);
            if (dataset.isLocked()) {
                throw new RuntimeException(
                        BundleUtil.getStringFromBundle("dataset.message.locked.editNotAllowed"));
            }
            preProcessResult = roCrateImportManager.preProcessRoCrateFromAroma(dataset, roCrateJson, true);
            PrepIssuesDecision decision = decidePrepIssues(preProcessResult, true);
            if (decision != null && !decision.ok) {
                jakarta.json.JsonObject data = JsonUtil.getJsonObject(decision.bodyPrettyJson);
                String msg = data.getString("message", VALIDATION_FAILED_MESSAGE);
                return roCrateError(BAD_REQUEST, msg, data);
            }
            preProcessedRoCrate = preProcessResult.getRoCrate();
        } catch (IOException | RuntimeException e) {
            e.printStackTrace();
            return roCrateError(INTERNAL_SERVER_ERROR, e.getMessage(), null);
        } 
        catch (WrappedResponse ex) {
            ex.printStackTrace();
            return roCrateError(FORBIDDEN, "Authorized users only.", null);
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
            return roCrateError(CONFLICT, BundleUtil.getStringFromBundle("dataset.message.toua.invalid"), null);
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
                // Avoid triggering indexing from ARP here; Dataverse core will index as needed.
            }

            roCrateImportManager.postProcessRoCrateFromAroma(managedVersion.getDataset(), preProcessedRoCrate);
            String roCratePath = roCrateServiceBean.getRoCratePath(managedVersion);
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            BufferedReader bufferedReader = new BufferedReader(new FileReader(roCratePath));
            JsonObject updatedRoCrate = gson.fromJson(bufferedReader, JsonObject.class);

            ObjectMapper mapper = new ObjectMapper();
            jakarta.json.JsonObject data = NullSafeJsonBuilder.jsonObjectBuilder()
                    .add("roCrate", JsonUtil.getJsonObject(mapper.readTree(updatedRoCrate.toString()).toString()))
                    .build();
            return roCrateOk("RO-Crate updated", data);

        } catch (WrappedResponse ex) {
            ex.printStackTrace();
            return ex.getResponse();
        } catch (IOException ex ) {
            ex.printStackTrace();
            logger.severe("Error occurred during post processing RO-Crate from AROMA" + ex.getMessage());
            return roCrateError(BAD_REQUEST, "Error occurred during post processing RO-Crate from AROMA" + ex.getMessage(), null);
        }
    }

    @POST
    @Path("/uploadRoCrateJson")
    @Consumes("application/json")
    @Produces("application/json")
    @AuthRequired
    public Response uploadRoCrateJson(
            @QueryParam("ownerId") String ownerId,
            @Context ContainerRequestContext crc,
            String roCrateJson
    ) {
        AuthenticatedUser user;
        try {
            user = getRequestAuthenticatedUserOrDie(crc);
        } catch (RuntimeException e) {
            e.printStackTrace();
            return error(INTERNAL_SERVER_ERROR, e.getMessage());
        }
        catch (WrappedResponse ex) {
            ex.printStackTrace();
            return error(FORBIDDEN, "Authorized users only.");
        }

        DataverseRequest req = createDataverseRequest(user);
        try {
            RoCrateJsonUploadResult uploadResult = uploadRoCrateJson(roCrateJson, ownerId, req);
            jakarta.json.JsonObject data = NullSafeJsonBuilder.jsonObjectBuilder()
                    .add("roCrate", JsonUtil.getJsonObject(uploadResult.roCrate().toString()))
                    .build();
            return roCrateOk("RO-Crate uploaded", data);
        } catch (ArpException e) {
            e.printStackTrace();
            logger.severe(e.getMessage());
            try {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode parsed = mapper.readTree(e.getMessage());
                jakarta.json.JsonObject data = NullSafeJsonBuilder.jsonObjectBuilder()
                        .add("details", JsonUtil.getJsonObject(parsed.toString()))
                        .build();
                return roCrateError(BAD_REQUEST, "RO-Crate upload failed", data);
            } catch (RuntimeException | JsonProcessingException ex) {
                return roCrateError(BAD_REQUEST, e.getMessage(), null);
            }
        }
    }
    
    public RoCrateJsonUploadResult uploadRoCrateJson(String roCrateJsonString, String ownerId, DataverseRequest req) throws ArpException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode uploadedCrate;
        DatasetVersion managedVersion;
        try {
            var roCrateJson = mapper.readTree(roCrateJsonString);
            // whether the arpPid is present or not means it is a new ds or one already present in dv
            var graph = roCrateJson.get("@graph");
            if (graph == null || !graph.isArray() || graph.isEmpty() || graph.get(0) == null) {
                throw new ArpException("Invalid RO-Crate: missing or empty '@graph'");
            }
            var arpPid = graph.get(0).get("@arpPid");
            boolean alreadyPresentDs = arpPid != null;
            Dataset dataset;
            DatasetVersion newVersion;

            if (alreadyPresentDs) {
                dataset = datasetService.findByGlobalId(arpPid.textValue());
                if (dataset == null) {
                    throw new ArpException("Invalid '@arpPid': " + arpPid.textValue());
                }
                newVersion = dataset.getOrCreateEditVersion();
            } else {
                dataset = new Dataset();
                newVersion = dataset.getOrCreateEditVersion();
                // If the ownerId which can be the id or the name of the owner dv not provided, we use the root dv
                if (ownerId == null) {
                    dataset.setOwner(dataverseService.findRootDataverse());
                } else {
                    var ownerDv = findDataverseOrDie(ownerId);
                    if (ownerDv == null) {
                        throw new ArpException("Can't find dataverse with identifier='" + ownerId + "'");
                    } else {
                        dataset.setOwner(ownerDv);
                    }
                }

                // Using some default values for the new ds
                TermsOfUseAndAccess terms = new TermsOfUseAndAccess();
                terms.setDatasetVersion(newVersion);
                terms.setLicense(licenseServiceBean.getDefault());
                terms.setFileAccessRequest(true);
                newVersion.setTermsOfUseAndAccess(terms);
                newVersion.getTermsOfUseAndAccess().setDatasetVersion(newVersion);
                dataset.setVersions(List.of(newVersion));
            }

            RoCrateImportPrepResult preProcessResult = roCrateImportManager.preProcessRoCrateFromAroma(dataset, roCrateJsonString, true);
            PrepIssuesDecision decision = decidePrepIssues(preProcessResult, true);
            if (decision != null && !decision.ok) {
                throw new ArpException(decision.bodyPrettyJson);
            }

            RoCrate preProcessedRoCrate = preProcessResult.getRoCrate();

            if (alreadyPresentDs) {
                boolean updateDraft = dataset.getLatestVersion().isDraft();
                if (updateDraft) {
                    roCrateImportManager.importRoCrate(preProcessedRoCrate, newVersion);
                    var filesToBeDeleted = roCrateImportManager.updateFileMetadatas(newVersion.getDataset(), preProcessedRoCrate);
                    if (!filesToBeDeleted.isEmpty()) {
                        for (FileMetadata markedForDelete : filesToBeDeleted) {
                            if (markedForDelete.getId() != null) {
                                dataset.getOrCreateEditVersion().getFileMetadatas().remove(markedForDelete);
                            }
                        }
                        managedVersion = execCommand(new UpdateDatasetVersionCommand(dataset, req, filesToBeDeleted)).getOrCreateEditVersion();
                    } else {
                        managedVersion = execCommand(new UpdateDatasetVersionCommand(dataset, req)).getOrCreateEditVersion();
                    }
                } else {
                    newVersion = new DatasetVersion();
                    newVersion.setDataset(dataset);
                    newVersion.setVersionState(DatasetVersion.VersionState.DRAFT);
                    newVersion.setTermsOfUseAndAccess(dataset.getLatestVersion().getTermsOfUseAndAccess());
                    newVersion.getTermsOfUseAndAccess().setDatasetVersion(newVersion);
                    boolean hasValidTerms = TermsOfUseAndAccessValidator.isTOUAValid(newVersion.getTermsOfUseAndAccess(), null);
                    if (!hasValidTerms) {
                        throw new ArpException(BundleUtil.getStringFromBundle("dataset.message.toua.invalid"));
                    }
                    roCrateImportManager.importRoCrate(preProcessedRoCrate, newVersion);
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
                    // Avoid triggering indexing from ARP here; Dataverse core will index as needed.
                }
                
            } else {
                roCrateImportManager.importRoCrate(preProcessedRoCrate, newVersion);
                managedVersion = execCommand(new CreateNewDatasetCommand(dataset, req)).getOrCreateEditVersion();;
            }

            roCrateImportManager.postProcessRoCrateFromAroma(managedVersion.getDataset(), preProcessedRoCrate);
            String roCratePath = roCrateServiceBean.getRoCratePath(managedVersion);
            BufferedReader bufferedReader = new BufferedReader(new FileReader(roCratePath));
            uploadedCrate = mapper.readTree(bufferedReader);
            
        } catch (WrappedResponse | ArpException | IOException e) {
            e.printStackTrace();
            throw new ArpException("An error occurred during processing the uploaded RO-Crate: " + roCrateJsonString +
                    "Details: " + e.getMessage());
        }

        return new RoCrateJsonUploadResult(uploadedCrate, managedVersion);
    }

    private DatasetVersion findUploadedDatasetVersion(DatasetVersion versionFromUpload) throws ArpException {
        if (versionFromUpload == null) {
            throw new ArpException("Dataset version is not available after RO-Crate metadata import");
        }
        Long versionId = versionFromUpload.getId();
        if (versionId != null) {
            for (int attempt = 0; attempt < 10; attempt++) {
                try {
                    DatasetVersion version = datasetVersionService.findDeep(versionId);
                    if (version != null) {
                        return version;
                    }
                } catch (RuntimeException ignored) {
                    // Treat as "not visible yet" and retry.
                }
                try {
                    Thread.sleep(200L);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        if (versionFromUpload.getDataset() != null && versionFromUpload.getDataset().getGlobalId() != null) {
            Dataset dataset = datasetService.findByGlobalId(versionFromUpload.getDataset().getGlobalId().asString());
            if (dataset != null && dataset.getLatestVersion() != null) {
                try {
                    return datasetVersionService.findDeep(dataset.getLatestVersion().getId());
                } catch (RuntimeException ignored) {
                    return dataset.getLatestVersion();
                }
            }
        }
        throw new ArpException("Dataset version not found after RO-Crate metadata import (id=" + versionId + ")");
    }

    @POST
    @Path("/uploadRoCrateZip")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces("application/json")
    @AuthRequired
    public Response uploadRoCrateZip(
            @QueryParam("ownerId") String ownerId,
            @Context ContainerRequestContext crc,
            @FormDataParam("file") InputStream fileStream,
            @FormDataParam("file") FormDataContentDisposition fileDetail,
            @FormDataParam("file") FormDataBodyPart bodyPart) throws IOException {

        if (fileDetail == null) {
            return roCrateError(BAD_REQUEST, "No file uploaded", null);
        }
        
        AuthenticatedUser user;
        try {
            user = getRequestAuthenticatedUserOrDie(crc);
        } catch (RuntimeException e) {
            e.printStackTrace();
            return roCrateError(INTERNAL_SERVER_ERROR, e.getMessage(), null);
        }
        catch (WrappedResponse ex) {
            ex.printStackTrace();
            return roCrateError(FORBIDDEN, "Authorized users only.", null);
        }
        byte[] fileBytes = fileStream.readAllBytes();
        var roCrateFilesContent = roCrateUploadServiceBean.processRoCrateZip(fileBytes);

        DataverseRequest req = createDataverseRequest(user);
        try {
            var roCrateString = arpService.extractFileFromZip(new ByteArrayInputStream(fileBytes), ArpServiceBean.RO_CRATE_METADATA_JSON_NAME);
            RoCrateJsonUploadResult uploadResult = uploadRoCrateJson(roCrateString, ownerId, req);
            JsonNode uploadedRoCrate = uploadResult.roCrate();
            String filename = fileDetail.getFileName();
            String type = (bodyPart != null && bodyPart.getMediaType() != null)
                    ? bodyPart.getMediaType().toString()
                    : "unknown";

            DatasetVersion version = findUploadedDatasetVersion(uploadResult.version());
            Command<CreateDataFileResult> cmd = new CreateNewDataFilesCommand(req, version, roCrateFilesContent, filename, type, null, null, null, null, null, version.getDataset().getOwner());
            CreateDataFileResult createDataFilesResult = commandEngine.submit(cmd);
            List<DataFile> filesAdded = ingestService.saveAndAddFilesToDataset(version, createDataFilesResult.getDataFiles(), null, true, false);

            // Store the import mapping for the RO-Crate export triggered by the upcoming update command.
            // API requests have no HTTP session, so session-scoped upload state is unavailable.
            var importMapping = roCrateImportMappingServiceBean.createImportMapping((ArrayNode) uploadedRoCrate.get("@graph"), filesAdded);
            roCrateImportMappingStoreBean.put(version.getId(), importMapping);

            var updateDatasetVersionCommand = new UpdateDatasetVersionCommand(version.getDataset(), req);
            var updatedDataset = commandEngine.submit(updateDatasetVersionCommand);
            var latestVersion = updatedDataset.getLatestVersion();
            roCrateExportManager.finalizeRoCrateAfterZipUpload(latestVersion, importMapping);
            JsonNode roCrate = roCrateExportManager.readRoCrateJsonFromDisk(latestVersion);
            return roCrateOk("RO-Crate uploaded", NullSafeJsonBuilder.jsonObjectBuilder()
                    .add("roCrate", JsonUtil.getJsonObject(roCrate.toString()))
                    .build());
            
        } catch (ArpException | CommandException | IOException e) {
            e.printStackTrace();
            logger.severe(e.getMessage());
            return roCrateError(BAD_REQUEST, e.getMessage(), null);
        } catch (Exception e) {
            e.printStackTrace();
            logger.severe(e.getMessage());
            return roCrateError(BAD_REQUEST, e.getMessage(), null);
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

    public record RoCrateJsonUploadResult(JsonNode roCrate, DatasetVersion version) {}

}
