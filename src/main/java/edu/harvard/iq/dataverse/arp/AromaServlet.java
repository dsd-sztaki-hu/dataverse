package edu.harvard.iq.dataverse.arp;

import edu.harvard.iq.dataverse.util.SystemConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

/**
 * Serves /aroma. JavaScript bundles have their baked Vite hosts replaced from the running Dataverse config.
 */
public class AromaServlet extends HttpServlet
{
    private String cachedJsPath;
    private long cachedJsModified;
    private String cachedJsSource;
    private String cachedRewriteKey;
    private byte[] cachedJsBody;

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        String basePath = getServletContext().getRealPath("/aroma");
        String requestedResource = request.getPathInfo();

        // Serve index.html for the base path of /aroma
        if (requestedResource == null || requestedResource.equals("/")) {
            requestedResource = "index.html";
        }

        File file = new File(basePath, requestedResource);

        if (!file.exists() || file.isDirectory()) {
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
            return;
        }

        String contentType = getServletContext().getMimeType(file.getName());
        if (contentType == null) {
            contentType = "application/octet-stream";
        }
        response.setContentType(contentType);

        if (file.getName().endsWith(".js")) {
            byte[] body = rewrittenJavaScript(file);
            response.setHeader("Cache-Control", "no-cache");
            response.setContentLength(body.length);
            response.getOutputStream().write(body);
            return;
        }

        response.setContentLengthLong(file.length());
        Files.copy(file.toPath(), response.getOutputStream());
    }

    private byte[] rewrittenJavaScript(File file) throws IOException {
        String siteUrl = SystemConfig.getDataverseSiteUrlStatic();
        String cedarDomain = null;
        String w3idBase = null;
        try {
            ArpConfig.ensureStaticInstance();
            if (ArpConfig.instance != null) {
                cedarDomain = ArpConfig.instance.get("arp.cedar.domain");
                w3idBase = ArpConfig.instance.get("arp.w3id.base");
            }
        } catch (RuntimeException ex) {
            // Keep the baked CEDAR domain and w3id base.
        }
        String key = siteUrl + "\n" + cedarDomain + "\n" + w3idBase;
        String path = file.getAbsolutePath();

        synchronized (this) {
            if (path.equals(cachedJsPath)
                    && file.lastModified() == cachedJsModified
                    && key.equals(cachedRewriteKey)
                    && cachedJsBody != null) {
                return cachedJsBody;
            }
            if (!path.equals(cachedJsPath) || file.lastModified() != cachedJsModified || cachedJsSource == null) {
                cachedJsSource = Files.readString(file.toPath());
                cachedJsPath = path;
                cachedJsModified = file.lastModified();
            }
            cachedJsBody = AromaBundleRewriter.rewrite(cachedJsSource, siteUrl, cedarDomain, w3idBase)
                    .getBytes(StandardCharsets.UTF_8);
            cachedRewriteKey = key;
            return cachedJsBody;
        }
    }
}
