package edu.harvard.iq.dataverse.api;

import com.google.gson.*;

public class ArpDatasetMetadataEditor {
    private JsonObject jsonObject;
    private Gson gson;

    public ArpDatasetMetadataEditor(String jsonString) {
        this.gson = new GsonBuilder().setPrettyPrinting().create();
        // We expect the jsonString to be a response from the native API so we need to unfold the data field
        this.jsonObject = gson.fromJson(jsonString, JsonObject.class).getAsJsonObject("data");
    }

    public ArpDatasetMetadataEditor(JsonObject jsonObject) {
        this.gson = new GsonBuilder().setPrettyPrinting().create();
        this.jsonObject = jsonObject.deepCopy();
        // In case the jsonObject is a response from the native API unfold the data field
        if (this.jsonObject.get("data") != null) {
            this.jsonObject = this.jsonObject.getAsJsonObject("data");
        }
    }

    public void editFieldLevelMetadata(String metadataBlockName, String fieldName, Object newValue) {
        JsonObject latestVersion = jsonObject.getAsJsonObject("latestVersion");
        JsonObject metadataBlocks = latestVersion.getAsJsonObject("metadataBlocks");
        JsonObject targetMetadataBlock = metadataBlocks.getAsJsonObject(metadataBlockName);

        if (targetMetadataBlock != null) {
            JsonArray fields = targetMetadataBlock.getAsJsonArray("fields");
            for (JsonElement fieldElement : fields) {
                JsonObject field = fieldElement.getAsJsonObject();
                if (field.get("typeName").getAsString().equals(fieldName)) {
                    updateFieldValue(field, newValue);
                    break;
                }
            }
        }
    }

    private void updateFieldValue(JsonObject field, Object newValue) {
        if (newValue instanceof String) {
            field.addProperty("value", (String) newValue);
        } else if (newValue instanceof JsonElement) {
            field.add("value", (JsonElement) newValue);
        } else {
            field.add("value", gson.toJsonTree(newValue));
        }
    }

    public String getCurrentJsonState() {
        return gson.toJson(jsonObject);
    }

    public JsonObject getCurrentJsonObject() {
        return jsonObject.deepCopy();
    }

    public String getCurrentJsonStateForUpdate() {
        JsonObject updateObject = new JsonObject();
        JsonObject latestVersion = jsonObject.getAsJsonObject("latestVersion");

        if (latestVersion.has("license")) {
            updateObject.add("license", latestVersion.get("license"));
        }
        if (latestVersion.has("metadataBlocks")) {
            updateObject.add("metadataBlocks", latestVersion.get("metadataBlocks"));
        }

        return gson.toJson(updateObject);
    }

    public JsonObject getCurrentJsonObjectForUpdate() {
        JsonObject updateObject = new JsonObject();
        JsonObject latestVersion = jsonObject.getAsJsonObject("latestVersion");

        if (latestVersion.has("license")) {
            updateObject.add("license", latestVersion.get("license").deepCopy());
        }
        if (latestVersion.has("metadataBlocks")) {
            updateObject.add("metadataBlocks", latestVersion.get("metadataBlocks").deepCopy());
        }

        return updateObject;
    }
}
