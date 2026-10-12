package edu.harvard.iq.dataverse.arp;

import edu.harvard.iq.dataverse.util.json.NullSafeJsonBuilder;

import edu.harvard.iq.dataverse.util.json.JsonUtil;
import jakarta.json.JsonArrayBuilder;
import java.util.ArrayList;
import java.util.HashMap;

public class CedarTemplateErrors {
    public ArrayList<String> unprocessableElements = new ArrayList<>();
    public ArrayList<String> invalidNames = new ArrayList<>();
    public HashMap<String, DatasetFieldTypeOverride> incompatiblePairs = new HashMap<>();

    public ArrayList<String> errors = new ArrayList<>();
    
    public ArrayList<String> warnings = new ArrayList<>();

    public CedarTemplateErrors() {
    }

    public jakarta.json.JsonObject toJson() {
        NullSafeJsonBuilder builder = NullSafeJsonBuilder.jsonObjectBuilder();

        if (!unprocessableElements.isEmpty()) {
            JsonArrayBuilder jsonArrayBuilder = JsonUtil.createArrayBuilder();
            unprocessableElements.forEach(jsonArrayBuilder::add);
            builder.add("unprocessableElements", jsonArrayBuilder);
        }

        if (!invalidNames.isEmpty()) {
            JsonArrayBuilder jsonArrayBuilder = JsonUtil.createArrayBuilder();
            invalidNames.forEach(jsonArrayBuilder::add);
            builder.add("invalidNames", jsonArrayBuilder);
        }

        if (!errors.isEmpty()) {
            JsonArrayBuilder jsonArrayBuilder = JsonUtil.createArrayBuilder();
            errors.forEach(jsonArrayBuilder::add);
            builder.add("errors", jsonArrayBuilder);
        }


        return builder.build();
    }
    
    public jakarta.json.JsonArray warningsAsJson() {
        JsonArrayBuilder jsonArrayBuilder = JsonUtil.createArrayBuilder();
        if (!warnings.isEmpty()) {
            warnings.forEach(jsonArrayBuilder::add);
        }

        return jsonArrayBuilder.build();
    }
}
