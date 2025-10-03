package edu.harvard.iq.dataverse.arp.rocrate;

import edu.harvard.iq.dataverse.util.json.NullSafeJsonBuilder;
import edu.kit.datamanager.ro_crate.RoCrate;
import jakarta.json.*;
import java.util.*;

public class RoCrateImportPrepResult {
    private RoCrate roCrate;

    private final Map<String, Map<String, Set<IssueDetail>>> warnings;
    private final Map<String, Map<String, Set<IssueDetail>>> errors;
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

    public Map<String, Map<String, Set<IssueDetail>>> getWarnings() {
        return warnings;
    }

    public Map<String, Map<String, Set<IssueDetail>>> getErrors() {
        return errors;
    }

    public void addWarning(String id, String fieldName, String message) {
        addWarning(id, fieldName, message, null);
    }

    public void addWarning(String id, String fieldName, String message, String suggestion) {
        warnings
                .computeIfAbsent(id, k -> new HashMap<>())
                .computeIfAbsent(fieldName, k -> new LinkedHashSet<>())
                .add(new IssueDetail(fieldName, message, suggestion));
    }

    public void addError(String id, String fieldName, String message) {
        addError(id, fieldName, message, null);
    }

    public void addError(String id, String fieldName, String message, String suggestion) {
        errors
                .computeIfAbsent(id, k -> new HashMap<>())
                .computeIfAbsent(fieldName, k -> new LinkedHashSet<>())
                .add(new IssueDetail(fieldName, message, suggestion));
    }

    public JsonObject toJson() {
        NullSafeJsonBuilder builder = NullSafeJsonBuilder.jsonObjectBuilder();
        builder.add("strict", isStrict);
        builder.add("warnings", buildIssuesJson(warnings, "warning"));
        builder.add("errors", buildIssuesJson(errors, "error"));
        return builder.build();
    }

    private JsonArray buildIssuesJson(Map<String, Map<String, Set<IssueDetail>>> issues, String prefix) {
        JsonArrayBuilder arrayBuilder = Json.createArrayBuilder();

        for (Map.Entry<String, Map<String, Set<IssueDetail>>> entry : issues.entrySet()) {
            String id = entry.getKey();
            JsonArrayBuilder fieldIssuesArray = Json.createArrayBuilder();

            for (Set<IssueDetail> issueDetails : entry.getValue().values()) {
                for (IssueDetail detail : issueDetails) {
                    JsonObjectBuilder issueJson = Json.createObjectBuilder()
                            .add(prefix + "Field", detail.fieldName())
                            .add(prefix + "Message", detail.message());
                    if (detail.suggestion() != null) {
                        issueJson.add(prefix + "Suggestion", detail.suggestion());
                    }
                    fieldIssuesArray.add(issueJson);
                }
            }

            JsonObjectBuilder entityJson = Json.createObjectBuilder()
                    .add(prefix + "Entity", id)
                    .add(prefix + "s", fieldIssuesArray);

            arrayBuilder.add(entityJson);
        }

        return arrayBuilder.build();
    }

    public void collectIssue(String id, String fieldName, String message) {
        collectIssue(id, fieldName, message, null);
    }

    public void collectIssue(String id, String fieldName, String message, String suggestion) {
        if (isStrict) {
            addError(id, fieldName, message, suggestion);
        } else {
            addWarning(id, fieldName, message, suggestion);
        }
    }

    public record IssueDetail(String fieldName, String message, String suggestion) {}
}
