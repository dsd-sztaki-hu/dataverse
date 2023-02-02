package edu.harvard.iq.dataverse.api.arp.util;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;

import javax.json.Json;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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
}
