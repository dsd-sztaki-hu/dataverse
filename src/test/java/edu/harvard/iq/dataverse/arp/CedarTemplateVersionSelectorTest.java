package edu.harvard.iq.dataverse.arp;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CedarTemplateVersionSelectorTest {

    @Test
    public void selectsTheLatestTemplateWhenItsVersionMatches() throws Exception {
        String latest = template("https://example/templates/latest", "6.9", null);
        CedarTemplateVersionSelector.Selection selection = CedarTemplateVersionSelector.select(
                latest, "6.9", id -> {
                    throw new AssertionError("older version should not be fetched");
                });

        assertEquals("https://example/templates/latest", selection.selectedId());
        assertEquals("6.9", selection.version());
        assertEquals(latest, selection.templateJson());
    }

    @Test
    public void selectsTheSecondDerivedFromVersion() throws Exception {
        Map<String, String> templates = new HashMap<>();
        templates.put("https://example/templates/mid",
                template("https://example/templates/mid", "6.11", "\"https://example/templates/match\""));
        templates.put("https://example/templates/match",
                template("https://example/templates/match", "6.9", null));
        String latest = template("https://example/templates/latest", "6.12", "\"https://example/templates/mid\"");

        CedarTemplateVersionSelector.Selection selection = CedarTemplateVersionSelector.select(
                latest, "6.9", templates::get);

        assertEquals("https://example/templates/match", selection.selectedId());
        assertEquals("6.9", selection.version());
        assertEquals(templates.get("https://example/templates/match"), selection.templateJson());
    }

    @Test
    public void followsDerivedFromWhenItIsAnIdObject() throws Exception {
        Map<String, String> templates = new HashMap<>();
        templates.put("https://example/templates/match",
                template("https://example/templates/match", "6.9", null));
        String latest = template("https://example/templates/latest", "6.12",
                "{\"@id\":\"https://example/templates/match\"}");

        CedarTemplateVersionSelector.Selection selection = CedarTemplateVersionSelector.select(
                latest, "6.9", templates::get);

        assertEquals("https://example/templates/match", selection.selectedId());
        assertEquals("6.9", selection.version());
    }

    @Test
    public void matchesALeadingV() throws Exception {
        String latest = template("https://example/templates/latest", " v6.9 ", null);
        CedarTemplateVersionSelector.Selection selection = CedarTemplateVersionSelector.select(
                latest, "6.9", id -> {
                    throw new AssertionError("older version should not be fetched");
                });

        assertEquals("https://example/templates/latest", selection.selectedId());
        assertEquals("v6.9", selection.version());
    }

    // CEDAR stores three-part versions. Dataverse reports "6.10" for the same release.
    @Test
    public void matchesATrailingZeroPart() throws Exception {
        String latest = template("https://example/templates/latest", "6.10.0", null);
        CedarTemplateVersionSelector.Selection selection = CedarTemplateVersionSelector.select(
                latest, "6.10", id -> {
                    throw new AssertionError("older version should not be fetched");
                });

        assertEquals("https://example/templates/latest", selection.selectedId());
        assertEquals("6.10.0", selection.version());
    }

    @Test
    public void matchesEitherSideHavingTheTrailingZero() {
        assertTrue(CedarTemplateVersionSelector.versionsMatch("6.10.0", "6.10"));
        assertTrue(CedarTemplateVersionSelector.versionsMatch("6.10", "6.10.0"));
        assertTrue(CedarTemplateVersionSelector.versionsMatch("6.9.0", "v6.9"));
        assertTrue(CedarTemplateVersionSelector.versionsMatch("6.12.1", "6.12.1"));
        assertFalse(CedarTemplateVersionSelector.versionsMatch("6.12.1", "6.12"));
        assertFalse(CedarTemplateVersionSelector.versionsMatch("6.10.1", "6.10"));
    }

    // ARP Archive and Draft copies leave only pav:derivedFrom on the new resource
    @Test
    public void followsTheDerivedFromChainAcrossArchiveSteps() throws Exception {
        Map<String, String> templates = new HashMap<>();
        templates.put("https://example/templates/v610",
                template("https://example/templates/v610", "6.10.0", "\"https://example/templates/v69\""));
        templates.put("https://example/templates/v69",
                template("https://example/templates/v69", "6.9.0", null));
        String latest = template("https://example/templates/v611", "6.11.0",
                "\"https://example/templates/v610\"");

        CedarTemplateVersionSelector.Selection selection = CedarTemplateVersionSelector.select(
                latest, "6.9", templates::get);

        assertEquals("https://example/templates/v69", selection.selectedId());
        assertEquals("6.9.0", selection.version());
    }

    // pav:previousVersion is ignored: ARP workflows only set pav:derivedFrom
    @Test
    public void ignoresPreviousVersionWhenDerivedFromIsMissing() {
        String latest = "{"
                + "\"@id\":\"https://example/templates/latest\","
                + "\"pav:version\":\"6.10.0\","
                + "\"pav:previousVersion\":\"https://example/templates/older\""
                + "}";

        IllegalStateException error = assertThrows(IllegalStateException.class, () ->
                CedarTemplateVersionSelector.select(latest, "6.9", id -> {
                    throw new AssertionError("pav:previousVersion must not be followed: " + id);
                }));

        assertTrue(error.getMessage().contains("installation version 6.9"));
        assertTrue(error.getMessage().contains("saw pav:version 6.10.0"));
    }

    @Test
    public void failsWhenTheChainCycles() {
        Map<String, String> templates = new HashMap<>();
        String latest = template("https://example/templates/latest", "6.12", "\"https://example/templates/older\"");
        templates.put("https://example/templates/latest", latest);
        templates.put("https://example/templates/older",
                template("https://example/templates/older", "6.11", "\"https://example/templates/latest\""));

        IllegalStateException error = assertThrows(IllegalStateException.class, () ->
                CedarTemplateVersionSelector.select(latest, "6.9", templates::get));

        assertTrue(error.getMessage().contains("cycled at https://example/templates/latest"));
        assertTrue(error.getMessage().contains("saw pav:version 6.12, 6.11"));
    }

    @Test
    public void failsWhenNoVersionMatches() {
        Map<String, String> templates = new HashMap<>();
        templates.put("https://example/templates/older",
                template("https://example/templates/older", "6.11", null));
        String latest = template("https://example/templates/latest", "6.12", "\"https://example/templates/older\"");

        IllegalStateException error = assertThrows(IllegalStateException.class, () ->
                CedarTemplateVersionSelector.select(latest, "6.9", templates::get));

        assertTrue(error.getMessage().contains("installation version 6.9"));
        assertTrue(error.getMessage().contains("saw pav:version 6.12, 6.11"));
    }

    private static String template(String id, String version, String derivedJson) {
        StringBuilder json = new StringBuilder();
        json.append("{\"@id\":\"").append(id).append("\",\"pav:version\":\"").append(version).append("\"");
        if (derivedJson != null) {
            json.append(",\"pav:derivedFrom\":").append(derivedJson);
        }
        return json.append("}").toString();
    }
}
