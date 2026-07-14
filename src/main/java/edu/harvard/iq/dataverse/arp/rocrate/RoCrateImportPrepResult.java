package edu.harvard.iq.dataverse.arp.rocrate;

import edu.harvard.iq.dataverse.util.json.NullSafeJsonBuilder;
import edu.kit.datamanager.ro_crate.RoCrate;
import jakarta.json.*;
import jakarta.json.stream.JsonGenerator;
import java.io.StringReader;
import java.io.StringWriter;
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
        if (!warnings.isEmpty()) {
            builder.add("warnings", buildIssuesJson(warnings, "warning"));
        }
        if (!errors.isEmpty()) {
            builder.add("errors", buildIssuesJson(errors, "error"));
        }
        return builder.build();
    }

    public JsonObject toApiResponseJson(String status, String message, boolean includeUpdatedRoCrate) {
        NullSafeJsonBuilder builder = NullSafeJsonBuilder.jsonObjectBuilder()
                .add("status", status)
                .add("message", message)
                .add("validation", toJson());

        if (includeUpdatedRoCrate && roCrate != null) {
            builder.add("updatedRoCrate", coerceToJsonValueOrString(roCrate.getJsonMetadata()));
        }

        return builder.build();
    }

    public JsonObject toApiResponseJson(String status, String message, String updatedRoCrateJsonString) {
        NullSafeJsonBuilder builder = NullSafeJsonBuilder.jsonObjectBuilder()
                .add("status", status)
                .add("message", message)
                .add("validation", toJson())
                .add("updatedRoCrate", coerceToJsonValueOrString(updatedRoCrateJsonString));
        return builder.build();
    }

    public String toApiResponseJsonPrettyString(String status, String message, boolean includeUpdatedRoCrate) {
        return prettyPrint(toApiResponseJson(status, message, includeUpdatedRoCrate));
    }

    public String toApiResponseJsonPrettyString(String status, String message, String updatedRoCrateJsonString) {
        return prettyPrint(toApiResponseJson(status, message, updatedRoCrateJsonString));
    }

    public boolean hasIssues() {
        return !errors.isEmpty() || !warnings.isEmpty();
    }

    public boolean hasOnlyWarningsWithMessage(String message) {
        if (!errors.isEmpty()) {
            return false;
        }
        if (warnings.isEmpty()) {
            return false;
        }
        return warnings.values().stream()
                .flatMap(warn -> warn.values().stream())
                .flatMap(Set::stream)
                .allMatch(issue -> message.equals(issue.message()));
    }

    public void removeWarningsWithMessage(String message) {
        warnings.entrySet().removeIf(entityEntry -> {
            var fields = entityEntry.getValue();
            fields.entrySet().removeIf(fieldEntry -> {
                fieldEntry.getValue().removeIf(issue -> message.equals(issue.message()));
                return fieldEntry.getValue().isEmpty();
            });
            return fields.isEmpty();
        });
    }

    private JsonValue coerceToJsonValueOrString(String jsonString) {
        if (jsonString == null) {
            return JsonValue.NULL;
        }
        try (JsonReader reader = Json.createReader(new StringReader(jsonString))) {
            JsonStructure parsed = reader.read();
            return parsed;
        } catch (RuntimeException ex) {
            return Json.createValue(jsonString);
        }
    }

    private String prettyPrint(JsonStructure structure) {
        Map<String, Object> config = Map.of(JsonGenerator.PRETTY_PRINTING, true);
        JsonWriterFactory writerFactory = Json.createWriterFactory(config);
        StringWriter out = new StringWriter();
        try (JsonWriter writer = writerFactory.createWriter(out)) {
            writer.write(structure);
        }
        return out.toString();
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
