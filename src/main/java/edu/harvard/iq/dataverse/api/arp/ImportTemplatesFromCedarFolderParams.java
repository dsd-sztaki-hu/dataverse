package edu.harvard.iq.dataverse.api.arp;

/**
 * Request body for POST /api/admin/arp/importTemplatesFromCedarFolder.
 * CEDAR domain and API key fall back to ArpConfig when omitted.
 */
public class ImportTemplatesFromCedarFolderParams
{
    public String folderId;
    public String dvIdtf;
    public String cedarDomain;
    public String apiKey;
}
