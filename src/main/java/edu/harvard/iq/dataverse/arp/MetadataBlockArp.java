package edu.harvard.iq.dataverse.arp;

import edu.harvard.iq.dataverse.DatasetFieldType;
import edu.harvard.iq.dataverse.MetadataBlock;

import javax.persistence.*;
import java.io.Serializable;

/**
 * ARP specific additional values associated with MetadataBlocks.
 */
@NamedQueries({
        // This should give a single result
        @NamedQuery(name = "MetadataBlockArp.findOneForMetadataBlock",
                query = "SELECT o FROM MetadataBlockArp o WHERE o.metadataBlock=:metadataBlock ORDER BY o.id"),
})
@Entity
@Table(indexes = {@Index(columnList="field_type_id")})
public class MetadataBlockArp implements Serializable
{
    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @OneToOne
    private MetadataBlock metadataBlock;

    private String roCrateConformsToId;

    @Column(columnDefinition="TEXT")
    private String cedarDefinition;

    public Long getId()
    {
        return id;
    }

    public void setId(Long id)
    {
        this.id = id;
    }

    public MetadataBlock getMetadataBlock()
    {
        return metadataBlock;
    }

    public void setMetadataBlock(MetadataBlock metadataBlock)
    {
        this.metadataBlock = metadataBlock;
    }

    public String getRoCrateConformsToId()
    {
        return roCrateConformsToId;
    }

    public void setRoCrateConformsToId(String roCrateConformsToId)
    {
        this.roCrateConformsToId = roCrateConformsToId;
    }

    public String getCedarDefinition()
    {
        return cedarDefinition;
    }

    public void setCedarDefinition(String cedarDefinition)
    {
        this.cedarDefinition = cedarDefinition;
    }
}
