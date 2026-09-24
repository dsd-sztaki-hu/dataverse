package edu.harvard.iq.dataverse.arp;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HarvestRegistryCheckServiceTest {

    private static final String SAMPLE_STATS_JSON = """
            [
              {
                "datasetCount": "112",
                "lastHarvestDate": "2026-09-10T05:03:26",
                "repo": "http://arptudasgraf.dsd.sztaki.hu/kg/repo/kdk",
                "link": "https://openarchive.tk.mta.hu/",
                "label": "Research Documentation Centre Data Repository (KDK)"
              },
              {
                "datasetCount": "11",
                "lastHarvestDate": "2026-09-10T05:05:57",
                "repo": "http://arptudasgraf.dsd.sztaki.hu/kg/repo/szte",
                "link": "https://datarepo.ek.szte.hu/",
                "label": "SZTE Adatrepozitórium"
              },
              {
                "datasetCount": "239",
                "lastHarvestDate": "2026-09-10T05:00:38",
                "repo": "http://search.researchdata.hu/kg/repo/arp",
                "link": "https://concorda2test.dsd.sztaki.hu/",
                "label": "ARP Repository"
              },
              {
                "datasetCount": "635",
                "lastHarvestDate": "2026-09-01T05:06:09",
                "repo": "http://arptudasgraf.dsd.sztaki.hu/kg/repo/zenodo",
                "link": "https://zenodo.org/",
                "label": "Zenodo"
              },
              {
                "datasetCount": "84",
                "lastHarvestDate": "2026-09-10T05:05:27",
                "repo": "http://arptudasgraf.dsd.sztaki.hu/kg/repo/debrecen",
                "link": "https://adattar.unideb.hu/",
                "label": "DE Adattár"
              }
            ]
            """;

    @Test
    public void extractLinksReadsLinkProperties() {
        List<String> links = HarvestRegistryCheckService.extractLinks(SAMPLE_STATS_JSON);
        assertEquals(5, links.size());
        assertTrue(links.contains("https://openarchive.tk.mta.hu/"));
        assertTrue(links.contains("https://zenodo.org/"));
        assertTrue(links.contains("https://adattar.unideb.hu/"));
    }

    @Test
    public void extractLinksReturnsEmptyForBlankJson() {
        assertTrue(HarvestRegistryCheckService.extractLinks(null).isEmpty());
        assertTrue(HarvestRegistryCheckService.extractLinks("  ").isEmpty());
    }

    @Test
    public void urlsMatchAgainstSampleStats() {
        List<String> links = HarvestRegistryCheckService.extractLinks(SAMPLE_STATS_JSON);
        assertTrue(HarvestRegistryCheckService.urlsMatch("https://openarchive.tk.mta.hu/", links));
        assertTrue(HarvestRegistryCheckService.urlsMatch("https://openarchive.tk.mta.hu", links));
        assertTrue(HarvestRegistryCheckService.urlsMatch("http://openarchive.tk.mta.hu/", links));
        assertFalse(HarvestRegistryCheckService.urlsMatch("http://localhost:8080", links));
        assertFalse(HarvestRegistryCheckService.urlsMatch("https://example.org/", links));
    }

    @Test
    public void normalizeIgnoresTrailingSlashSchemeAndDefaultPorts() {
        assertEquals(
                HarvestRegistryCheckService.normalizeRepoUrl("https://openarchive.tk.mta.hu/"),
                HarvestRegistryCheckService.normalizeRepoUrl("http://openarchive.tk.mta.hu"));
        assertEquals(
                HarvestRegistryCheckService.normalizeRepoUrl("https://example.org"),
                HarvestRegistryCheckService.normalizeRepoUrl("https://example.org:443/"));
        assertEquals(
                HarvestRegistryCheckService.normalizeRepoUrl("http://example.org"),
                HarvestRegistryCheckService.normalizeRepoUrl("http://example.org:80/"));
        assertEquals("example.org:8080", HarvestRegistryCheckService.normalizeRepoUrl("https://EXAMPLE.ORG:8080/"));
    }

    @Test
    public void isTruthyAcceptsCommonEnableFlags() {
        assertTrue(HarvestRegistryCheckService.isTruthy("true"));
        assertTrue(HarvestRegistryCheckService.isTruthy("TRUE"));
        assertTrue(HarvestRegistryCheckService.isTruthy("1"));
        assertTrue(HarvestRegistryCheckService.isTruthy("yes"));
        assertFalse(HarvestRegistryCheckService.isTruthy("false"));
        assertFalse(HarvestRegistryCheckService.isTruthy("0"));
        assertFalse(HarvestRegistryCheckService.isTruthy(null));
        assertFalse(HarvestRegistryCheckService.isTruthy(""));
    }
}
