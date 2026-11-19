package edu.harvard.iq.dataverse.arp;


import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.Json;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Decides which controlled vocabulary values of a field are removed when a
 * metadata block is imported from CEDAR. The CEDAR template is the source of
 * truth: a value whose label is not among the template's literals is removed,
 * unless a dataset, a dataset template or a collection still uses it. Used
 * values are kept and reported, so they can be dealt with by hand.
 * <p>
 * For example the ARP citation templates only list the ISO 639-2 languages,
 * while the upstream citation.tsv loads all of ISO 639-3.
 */
public final class ControlledVocabularyPruner {

    private ControlledVocabularyPruner() {
    }

    /**
     * A controlled vocabulary value as stored in the database.
     */
    public record Value(Long id, String label) {
    }

    /**
     * The ids to delete and the labels kept because they are in use.
     */
    public record Plan(List<Long> remove, List<String> keptInUse) {
    }

    /**
     * What the import did with one field.
     */
    public record Result(String field, int removed, List<String> keptInUse) {

        public boolean changedAnything() {
            return removed > 0 || !keptInUse.isEmpty();
        }

        public JsonObjectBuilder toJson() {
            JsonArrayBuilder kept = Json.createArrayBuilder();
            keptInUse.forEach(kept::add);
            return Json.createObjectBuilder()
                    .add("field", field)
                    .add("removed", removed)
                    .add("keptInUse", kept);
        }
    }

    public static Plan plan(Collection<Value> existing, Set<String> keptLabels, Set<Long> usedIds) {
        List<Long> remove = new ArrayList<>();
        List<String> keptInUse = new ArrayList<>();
        for (Value value : existing) {
            if (keptLabels.contains(value.label())) {
                continue;
            }
            if (usedIds.contains(value.id())) {
                keptInUse.add(value.label());
            } else {
                remove.add(value.id());
            }
        }
        keptInUse.sort(null);
        return new Plan(remove, keptInUse);
    }
}
