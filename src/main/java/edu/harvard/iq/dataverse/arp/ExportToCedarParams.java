package edu.harvard.iq.dataverse.arp;

public class ExportToCedarParams
{
    // The domain where CEDAR is deployed
    public String cedarDomain;

    // CEDAR (admin) API key for authentication
    public String apiKey;

    // Folder in CEDAR to generate the template into
    public String folderId;
}
