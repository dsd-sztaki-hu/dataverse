package edu.harvard.iq.dataverse.api.arp;

import edu.harvard.iq.dataverse.arp.ExportToCedarParams;

import java.util.List;

public class ArpInitialSetupParams
{
    public static class MdbParam
    {
        // Name of the MDB to sync. Either name or id must be provided
        public String name;

        // ID of the MDB to sync. Either name or id must be provided.
        public Long id;

        // An optional namespace URI to set for the MDB in Dataverse
        public String namespaceUri;

        // Explicit CEDAR UUID to use when creating the template. If not provided one is automatically generated based
        // on the name of the MDB.
        public String cedarUuid;
    }

    public List<MdbParam> mdbParams;
    public ExportToCedarParams cedarParams;

    /**
     * When true, forces the use of namespaceUri (from mdbParams) for all property URIs,
     * overwriting any existing termUri on dataset fields. When false, existing termUri is preserved.
     */
    public Boolean forceNamespaceUri;

    public ArpInitialSetupParams()
    {
    }

    public List<MdbParam> getMdbParams()
    {
        return mdbParams;
    }

    public void setMdbParams(List<MdbParam> mdbParams)
    {
        this.mdbParams = mdbParams;
    }

    public ExportToCedarParams getCedarParams()
    {
        return cedarParams;
    }

    public void setCedarParams(ExportToCedarParams cedarParams)
    {
        this.cedarParams = cedarParams;
    }

    public Boolean getForceNamespaceUri() { return forceNamespaceUri; }

    public void setForceNamespaceUri(Boolean forceNamespaceUri)
    {
        this.forceNamespaceUri = forceNamespaceUri;
    }
}
