package edu.harvard.iq.dataverse.api;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class ArpJsonStructureComparator
{

    public static boolean compareJsonStructures(String json1, String json2) {
        Gson gson = new Gson();
        JsonObject obj1 = gson.fromJson(json1, JsonObject.class);
        JsonObject obj2 = gson.fromJson(json2, JsonObject.class);

        return compareObjects(obj1, obj2) && compareIdStructures(obj1, obj2);
    }

    private static boolean compareObjects(JsonElement elem1, JsonElement elem2) {
        if (!elem1.getClass().equals(elem2.getClass())) {
            System.out.println("Mismatch: Different element types - " + elem1.getClass() + " vs " + elem2.getClass());
            return false;
        }

        if (elem1.isJsonObject()) {
            JsonObject obj1 = elem1.getAsJsonObject();
            JsonObject obj2 = elem2.getAsJsonObject();

            if (!obj1.keySet().equals(obj2.keySet())) {
                System.out.println("Mismatch: Different key sets - " + obj1.keySet() + " vs " + obj2.keySet());
                return false;
            }

            for (String key : obj1.keySet()) {
                if (key.equals("@id") || key.equals("@arpPid") || key.equals("datePublished")) {
                    continue;
                }
                if (!compareObjects(obj1.get(key), obj2.get(key))) {
                    System.out.println("Mismatch in key: " + key);
                    return false;
                }
            }
            return true;
        } else if (elem1.isJsonArray()) {
            JsonArray arr1 = elem1.getAsJsonArray();
            JsonArray arr2 = elem2.getAsJsonArray();

            if (arr1.size() != arr2.size()) {
                System.out.println("Mismatch: Different array sizes - " + arr1.size() + " vs " + arr2.size());
                return false;
            }

            List<JsonElement> list1 = new ArrayList<>();
            List<JsonElement> list2 = new ArrayList<>();
            arr1.forEach(list1::add);
            arr2.forEach(list2::add);

            list1.sort(Comparator.comparing(JsonElement::toString));
            list2.sort(Comparator.comparing(JsonElement::toString));

            for (int i = 0; i < list1.size(); i++) {
                if (!compareObjects(list1.get(i), list2.get(i))) {
                    System.out.println("Mismatch in array element at index: " + i);
                    return false;
                }
            }
            return true;
        } else {
            if (!elem1.equals(elem2)) {
                System.out.println("Mismatch: Different primitive values - " + elem1 + " vs " + elem2);
                return false;
            }
            return true;
        }
    }

    private static boolean compareIdStructures(JsonElement elem1, JsonElement elem2) {
        Map<String, String> idMap1 = collectIds(elem1);
        Map<String, String> idMap2 = collectIds(elem2);

        if (idMap1.size() != idMap2.size()) {
            System.out.println("Mismatch: Different number of IDs - " + idMap1.size() + " vs " + idMap2.size());
            return false;
        }

        List<Map.Entry<String, String>> sortedIds1 = new ArrayList<>(idMap1.entrySet());
        List<Map.Entry<String, String>> sortedIds2 = new ArrayList<>(idMap2.entrySet());

        sortedIds1.sort(Comparator.comparing(Map.Entry::getKey));
        sortedIds2.sort(Comparator.comparing(Map.Entry::getKey));

        for (int i = 0; i < sortedIds1.size(); i++) {
            if (!sortedIds1.get(i).getValue().equals(sortedIds2.get(i).getValue())) {
                System.out.println("Mismatch: Different ID types - " + sortedIds1.get(i).getValue() + " vs " + sortedIds2.get(i).getValue());
                return false;
            }
        }

        return true;
    }

    private static Map<String, String> collectIds(JsonElement elem) {
        Map<String, String> idMap = new HashMap<>();

        if (elem.isJsonObject()) {
            JsonObject obj = elem.getAsJsonObject();
            // do not save '@id' multiple times from the parent objects too without a '@type'
            if (obj.has("@id") && obj.has("@type")) {
                idMap.put(obj.get("@id").getAsString(), obj.get("@type").getAsString());
            }
            for (Map.Entry<String, JsonElement> entry : obj.entrySet()) {
                idMap.putAll(collectIds(entry.getValue()));
            }
        } else if (elem.isJsonArray()) {
            for (JsonElement arrayElem : elem.getAsJsonArray()) {
                idMap.putAll(collectIds(arrayElem));
            }
        }

        return idMap;
    }
}
