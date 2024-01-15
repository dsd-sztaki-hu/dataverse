package edu.harvard.iq.dataverse.arp;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.*;
import java.nio.file.Files;

/**
 * Servlet to server /aroma/index.html when /aroma is requested
 */
public class AromaServlet extends HttpServlet
{
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

        if (!file.exists()) {
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
            return;
        }

        // Set content type and copy file contents to response
        String contentType = getServletContext().getMimeType(file.getName());
        if (contentType == null) {
            contentType = "application/octet-stream";
        }
        response.setContentType(contentType);
        response.setContentLengthLong(file.length());
        Files.copy(file.toPath(), response.getOutputStream());
    }
}
