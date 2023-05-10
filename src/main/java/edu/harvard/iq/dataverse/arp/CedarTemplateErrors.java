package edu.harvard.iq.dataverse.arp;

import edu.harvard.iq.dataverse.arp.DatasetFieldTypeOverride;
import edu.harvard.iq.dataverse.util.json.NullSafeJsonBuilder;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import java.util.ArrayList;
import java.util.HashMap;

public class CedarTemplateErrors {
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
