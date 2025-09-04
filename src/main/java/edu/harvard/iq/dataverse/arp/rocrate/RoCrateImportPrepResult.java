package edu.harvard.iq.dataverse.arp.rocrate;

import edu.harvard.iq.dataverse.util.json.NullSafeJsonBuilder;
import edu.kit.datamanager.ro_crate.RoCrate;
import jakarta.json.Json;
import jakarta.json.JsonArrayBuilder;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RoCrateImportPrepResult {
    private RoCrate roCrate;
    
    public Set<String> errors;

    public RoCrateImportPrepResult() {
        this.errors = new HashSet<>();
    }

    public RoCrate getRoCrate() {
        return roCrate;
    }

    public void setRoCrate(RoCrate roCrate) {
        this.roCrate = roCrate;
    }
    
    public jakarta.json.JsonObject getErrorsJson() {
        NullSafeJsonBuilder builder = NullSafeJsonBuilder.jsonObjectBuilder();
        JsonArrayBuilder jsonArrayBuilder = Json.createArrayBuilder();
        errors.forEach(jsonArrayBuilder::add);
        builder.add("errors", jsonArrayBuilder);
        return builder.build();
    }
}