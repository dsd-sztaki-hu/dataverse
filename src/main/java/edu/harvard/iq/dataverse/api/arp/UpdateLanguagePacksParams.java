package edu.harvard.iq.dataverse.api.arp;

/**
 * Request body for POST /api/admin/langPacks/update.
 * Omitted fields fall back to ArpConfig ({@code arp.lang.packs.repoUrl}, {@code arp.lang.packs.ref}).
 */
public class UpdateLanguagePacksParams
{
    public String repoUrl;
    public String ref;
}
