package edu.harvard.iq.dataverse.api.arp;

import edu.harvard.iq.dataverse.arp.ExportToCedarParams;

import java.util.List;
import java.util.Map;

public class ArpInitialSetupParams
{
    // Default namespace URI-s for MDB-s. If a namespace URI is already set it will be updated with this one
    // Key is the MDB name, value is the URI
    public Map<String, String> mdbNamespaceUris;

    public SyncCedarData syncCedar;

    public static class SyncCedarData {
        public ExportToCedarParams cedarParams;
        public List<String> mdbs;
    }

    public ArpInitialSetupParams()
    {
    }

    public Map<String, String> getMdbNamespaceUris()
    {
        return mdbNamespaceUris;
    }

    public void setMdbNamespaceUris(Map<String, String> mdbNamespaceUris)
    {
        this.mdbNamespaceUris = mdbNamespaceUris;
    }
}
