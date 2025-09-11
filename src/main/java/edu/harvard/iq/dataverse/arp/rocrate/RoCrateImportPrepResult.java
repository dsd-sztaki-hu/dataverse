package edu.harvard.iq.dataverse.arp.rocrate;

import edu.harvard.iq.dataverse.util.json.NullSafeJsonBuilder;
import edu.kit.datamanager.ro_crate.RoCrate;
import jakarta.json.*;
import java.util.*;

public class RoCrateImportPrepResult {
    private RoCrate roCrate;
    
    private final Map<String, Map<String, Set<String>>> warnings;
    private final Map<String, Map<String, Set<String>>> errors;
    private final boolean isStrict;
    

    public RoCrateImportPrepResult(boolean isStrict) {
        this.warnings = new HashMap<>();
        this.errors = new HashMap<>();
        this.isStrict = isStrict;
    }

    public RoCrate getRoCrate() {
        return roCrate;
    }

    public void setRoCrate(RoCrate roCrate) {
        this.roCrate = roCrate;
    }
    
    public Map<String, Map<String, Set<String>>> getWarnings() {
        return warnings;
    }
    
    public Map<String, Map<String, Set<String>>> getErrors() {
        return errors;
    }

    public void addWarning(String id, String fieldName, String message) {
        warnings
                .computeIfAbsent(id, k -> new HashMap<>())
                .computeIfAbsent(fieldName, k -> new LinkedHashSet<>()) // preserves insertion order
                .add(message);
    }

    public void addError(String id, String fieldName, String message) {
        errors
                .computeIfAbsent(id, k -> new HashMap<>())
                .computeIfAbsent(fieldName, k -> new LinkedHashSet<>())
                .add(message);
    }
    
    public void addGeneralError(String type, String message) {
        addError("general_ro_crate_errors", type, message);
    }

    public JsonObject toJson() {
        NullSafeJsonBuilder builder = NullSafeJsonBuilder.jsonObjectBuilder();
        builder.add("strict", isStrict);
        builder.add("warnings", buildIssuesJson(warnings));
        builder.add("errors", buildIssuesJson(errors));
        return builder.build();
    }

    private JsonObject buildIssuesJson(Map<String, Map<String, Set<String>>> issues) {
        JsonObjectBuilder root = Json.createObjectBuilder();
        for (Map.Entry<String, Map<String, Set<String>>> entry : issues.entrySet()) {
            String id = entry.getKey();
            JsonObjectBuilder fieldsBuilder = Json.createObjectBuilder();

            for (Map.Entry<String, Set<String>> fieldEntry : entry.getValue().entrySet()) {
                String fieldName = fieldEntry.getKey();
                Set<String> messages = fieldEntry.getValue();

                if (messages.size() == 1) {
                    fieldsBuilder.add(fieldName, messages.iterator().next());
                } else {
                    JsonArrayBuilder arr = Json.createArrayBuilder();
                    messages.forEach(arr::add);
                    fieldsBuilder.add(fieldName, arr);
                }
            }
            root.add(id, fieldsBuilder);
        }
        return root.build();
    }
    
    public void collectIssue(String id, String fieldName, String message) {
        if (isStrict) {
            addError(id, fieldName, message);
        } else {
            addWarning(id, fieldName, message);
        }
    }
}