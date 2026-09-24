package edu.harvard.iq.dataverse.arp;

/*
 * This class has been extracted from the ARP project (https://science-research-data.hu/en) in the frame of
 * FAIR-IMPACT's 1st Open Call "Enabling FAIR Signposting and RO-Crate for content/metadata discovery and consumption".
 *
 * @author Balázs E. Pataki <balazs.pataki@sztaki.hu>, SZTAKI, Department of Distributed Systems, https://dsd.sztaki.hu
 * @author Norbert Finta <norbert.finta@sztaki.hu>, SZTAKI, Department of Distributed Systems, https://dsd.sztaki.hu
 * @version 1.0
 */

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AromaBundleRewriterTest {

    private static final String FIXTURE = ""
            + "const vKe={"
            + "VITE_REACT_APP_DV_HOST:\"http://127.0.0.1:8080\","
            + "VITE_REACT_APP_CEDAR_DOMAIN:\"arp.orgx\","
            + "VITE_REACT_APP_W3ID_BASE:\"https://w3id.org/arp/localdev\","
            + "VITE_REACT_APP_CEDAR_PROXY:\"http://localhost:8080/api/arp/cedarResourceProxy/\""
            + "};globalThis.__AROMA_CONFIG__=vKe;const Ha=globalThis.__AROMA_CONFIG__;"
            + "fetch(`${Ha.VITE_REACT_APP_DV_HOST}/api`);"
            + "domainBase:\"arp.orgx\"";

    @Test
    public void rewriteSubstitutesConfigObjectOnly() {
        String rewritten = AromaBundleRewriter.rewrite(
                FIXTURE,
                "http://hunverse.example:8080/",
                "schema.researchdata.hu",
                "https://w3id.org/arp/dev/");

        assertTrue(rewritten.contains("VITE_REACT_APP_DV_HOST:\"http://hunverse.example:8080\""));
        assertTrue(rewritten.contains("VITE_REACT_APP_CEDAR_DOMAIN:\"schema.researchdata.hu\""));
        assertTrue(rewritten.contains("VITE_REACT_APP_W3ID_BASE:\"https://w3id.org/arp/dev\""));
        assertTrue(rewritten.contains("VITE_REACT_APP_CEDAR_PROXY:\"http://hunverse.example:8080/api/arp/cedarResourceProxy/\""));
        assertTrue(rewritten.contains("fetch(`${Ha.VITE_REACT_APP_DV_HOST}/api`)"));
        assertTrue(rewritten.contains("domainBase:\"arp.orgx\""));
    }

    @Test
    public void rewriteLeavesUnsafeValuesUntouched() {
        String rewritten = AromaBundleRewriter.rewrite(
                FIXTURE,
                "http://evil.example/${path}",
                "bad\"domain",
                "https://w3id.org/arp/dev`");

        assertEquals(FIXTURE, rewritten);
    }

    @Test
    public void shippedBundleKeepsValuesOnTheConfigObject() throws Exception {
        String html = Files.readString(Path.of("src/main/webapp/aroma/index.html"));
        int src = html.indexOf("/aroma/assets/index-");
        int end = html.indexOf(".js", src);
        String bundleName = html.substring(src + "/aroma/assets/".length(), end + 3);
        String javascript = Files.readString(Path.of("src/main/webapp/aroma/assets", bundleName));

        assertTrue(javascript.contains("globalThis.__AROMA_CONFIG__"));
        assertTrue(javascript.contains(AromaBundleRewriter.KEY_DV_HOST + ":\""));
        assertTrue(javascript.contains(AromaBundleRewriter.KEY_CEDAR_DOMAIN + ":\""));
        assertTrue(javascript.contains(AromaBundleRewriter.KEY_W3ID_BASE + ":\""));
        assertTrue(javascript.contains(AromaBundleRewriter.KEY_CEDAR_PROXY + ":\""));
    }
}
