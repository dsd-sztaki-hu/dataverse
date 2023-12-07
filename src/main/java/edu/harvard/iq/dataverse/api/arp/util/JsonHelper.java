package edu.harvard.iq.dataverse.api.arp.util;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.*;

public class JsonHelper {
    /**
     * Returns a JSON sub-element from the given JsonElement and the given path
     *
     * @param json - a Gson JsonElement
     * @param path - a JSON path, e.g. a.b.c[2].d
     * @return - a sub-element of json according to the given path
     */
    public static JsonElement getJsonElement(JsonElement json, String path){

        String[] parts = path.split("\\.|\\[|\\]");
        JsonElement result = json;

        for (String key : parts) {

            key = key.trim();
            if (key.isEmpty())
                continue;

            if (result == null){
//                result = JsonNull.INSTANCE;
                break;
            }

            if (result.isJsonObject()){
                result = ((JsonObject)result).get(key);
            }
            else if (result.isJsonArray()){
                int ix = Integer.parseInt(key);
                result = ((com.google.gson.JsonArray)result).get(ix);
            }
            else break;
        }

        return result;
    }

    public static List<String> getStringList(JsonElement json, String path) {
        Gson gson = new Gson();
        Type type = new TypeToken<List<String>>(){}.getType();
        JsonElement jsonElement = getJsonElement(json, path);
        return jsonElement == null || jsonElement.isJsonNull() ? new ArrayList<>() : gson.fromJson(jsonElement, type);
    }


    public static JsonObject getJsonObject(JsonElement json, String path) {
        JsonElement jsonElement = getJsonElement(json, path);

        return jsonElement == null || jsonElement.isJsonNull() ? null : jsonElement.getAsJsonObject();
    }

    public static JsonArray getJsonArray(JsonElement json, String path) {
        JsonElement jsonElement = getJsonElement(json, path);

        return jsonElement == null || jsonElement.isJsonNull() ? null : jsonElement.getAsJsonArray();
    }


    public static boolean hasJsonElement(JsonElement json, String path) {
        try {
            return getJsonElement(json, path) != null;
        }
        catch (Exception ex) {
            return false;
        }
    }

    /**
     * Collect fields of a CEDAR template recursively, ie. it also collects fields inside CEDAR Elements as well,
     * assuming all field names are distinct at any level
     *
     * @param cedarTemplate
     * @return
     */
    public static Map<String, JsonElement> collectTemplateFields(JsonObject cedarTemplate) {
        Map<String, JsonElement> fields = new HashMap<>();
        collectFields(cedarTemplate.get("properties"), "", fields);
        return fields;
    }

    private static void collectFields(JsonElement json, String fieldName, Map<String, JsonElement> fields) {
        if (json.isJsonObject()) {
            JsonObject jsonObject = json.getAsJsonObject();

            if (jsonObject.has("_ui") ||
                    (jsonObject.has("type") && jsonObject.get("type").toString().equals("\"array\"")
                            && jsonObject.getAsJsonObject("items").has("_ui"))
            ) {
                fields.put(fieldName, jsonObject);
            }

            for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
                /*
                 * fieldnames can not contain dots in CEDAR, so we replace them with colons before exporting the template
                 * upon importing from CEDAR the colons are replaced with dots again
                 * */
                String key = entry.getKey().replace(':', '.');
                JsonElement value = entry.getValue();

                if (value.isJsonObject()) {
                    collectFields(value, key, fields);
                }
            }
        }
    }

}
