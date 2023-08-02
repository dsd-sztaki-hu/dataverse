package edu.harvard.iq.dataverse.arp;

import edu.harvard.iq.dataverse.util.json.NullSafeJsonBuilder;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import java.util.ArrayList;
import java.util.HashMap;

public class CedarTemplateErrors {
    public ArrayList<String> unprocessableElements = new ArrayList<>();
    public ArrayList<String> invalidNames = new ArrayList<>();
    public HashMap<String, DatasetFieldTypeOverride> incompatiblePairs = new HashMap<>();

    public ArrayList<String> errors = new ArrayList<>();

    public CedarTemplateErrors() {
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

        if (!errors.isEmpty()) {
            JsonArrayBuilder jsonArrayBuilder = Json.createArrayBuilder();
            errors.forEach(jsonArrayBuilder::add);
            builder.add("errors", jsonArrayBuilder);
        }


        return builder.build();
    }
}
