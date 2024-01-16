package edu.harvard.iq.dataverse.arp;

import edu.harvard.iq.dataverse.DatasetFieldType;
import edu.harvard.iq.dataverse.MetadataBlock;

import jakarta.persistence.*;
import java.io.Serializable;

/**
 * ARP specific additional values to DatasetFieldTypes.
 */
@NamedQueries({
        // This should give a single result
        @NamedQuery(name = "DatasetFieldTypeArp.findOneForDatasetFieldType",
                query = "SELECT o FROM DatasetFieldTypeArp o WHERE o.fieldType=:fieldType ORDER BY o.id"),
        @NamedQuery(name = "DatasetFieldTypeArp.findAllByMetadataBlock",
                query = "SELECT o FROM DatasetFieldTypeArp o JOIN DatasetFieldType dft ON dft.id=o.id JOIN MetadataBlock mdb ON mdb.id=dft.id WHERE mdb=:metadataBlock ORDER BY mdb.id, dft.id")
})
@Entity
@Table(indexes = {@Index(columnList="field_type_id")})
public class DatasetFieldTypeArp implements Serializable
{
    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @OneToOne
    private DatasetFieldType fieldType;

    @OneToOne
    private DatasetFieldTypeOverride override;

    @Column(columnDefinition="TEXT")
    private String cedarDefinition;

    /**
     * Mark whether this field uses an OntoPortal provided external vocabulary
     */
    private boolean hasExternalValues;

    /**
     * Name of the field in a compound field to use as the display name for the compound. This is usually the
     * most descriptive field of the record. By default it should be the name of a field, but we may also support
     * and expression language here allowing more complex display name generation. eg. concatWithSpace(fiel1, field2),
     * which would result in the space concatenated version of field1 and field2.
     * Display names are used for RO-Crate "name" field generation for now, later we may use it for other purposes
     * as well
     */
    private String displayNameField;

    public Long getId()
    {
        return id;
    }

    public void setId(Long id)
    {
        this.id = id;
    }

    public DatasetFieldType getFieldType()
    {
        return fieldType;
    }

    public void setFieldType(DatasetFieldType fieldType)
    {
        this.fieldType = fieldType;
    }

    public DatasetFieldTypeOverride getOverride()
    {
        return override;
    }

    public void setOverride(DatasetFieldTypeOverride override)
    {
        this.override = override;
    }

    public String getCedarDefinition()
    {
        return cedarDefinition;
    }

    public void setCedarDefinition(String cedarDefinition)
    {
        this.cedarDefinition = cedarDefinition;
    }

    public boolean isHasExternalValues() {
        return hasExternalValues;
    }

    public void setHasExternalValues(boolean hasExternalValues) {
        this.hasExternalValues = hasExternalValues;
    }

    public String getDisplayNameField()
    {
        return displayNameField;
    }

    public void setDisplayNameField(String displayNameField)
    {
        this.displayNameField = displayNameField;
    }
}
