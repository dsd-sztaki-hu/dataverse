package edu.harvard.iq.dataverse.arp;

import edu.harvard.iq.dataverse.DatasetFieldType;
import edu.harvard.iq.dataverse.MetadataBlock;
import edu.harvard.iq.dataverse.util.BundleUtil;

import jakarta.persistence.*;
import java.io.Serializable;
import java.util.MissingResourceException;

/**
 * Maps to datasetfieldtypeoverride
 */
@NamedQueries({
        @NamedQuery(name = "DatasetFieldTypeOverride.findOneOverrideByOriginal",
                query = "SELECT o FROM DatasetFieldTypeOverride o WHERE o.original=:original ORDER BY o.id"),
        @NamedQuery(name = "DatasetFieldTypeOverride.findOverrides",
                query = "SELECT o FROM DatasetFieldTypeOverride o WHERE o.metadataBlock=:metadataBlock ORDER BY o.id")
})
@Entity
@Table(indexes = {@Index(columnList="metadatablock_id")})
public class DatasetFieldTypeOverride implements Serializable
{
    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne
    private DatasetFieldType original;

    @ManyToOne
    private MetadataBlock metadataBlock;

    @Column(columnDefinition = "TEXT")
    private String localName;


    @Column(columnDefinition = "TEXT")
    private String title;

    /**
     * A watermark to be displayed in the UI.
     */
    private String watermark;

    private int displayOrder;

    /**
     * Determines whether fields of this field type are always required. A
     * dataverse may set some fields required, but only if this is false.
     */
    private boolean required;

    @Column(columnDefinition = "TEXT")
    private String description;

    // Marks wether the original field already appears in the form. If generateOriginalField==false, then
    // the original field is generated for another MDB, if markAsOverride==true then we need to
    // generate field for the original but with labels from the override. When  markAsOverride==true, we
    // have to take care that the form field is ignored at Submit type, we only use it for displaying
    // in the form and syncing it with the original field's value.
    private transient boolean generateOriginalField;

    public Long getId()
    {
        return id;
    }

    public void setId(Long id)
    {
        this.id = id;
    }

    public DatasetFieldType getOriginal()
    {
        return original;
    }

    public void setOriginal(DatasetFieldType original)
    {
        this.original = original;
    }

    public MetadataBlock getMetadataBlock()
    {
        return metadataBlock;
    }

    public void setMetadataBlock(MetadataBlock metadataBlock)
    {
        this.metadataBlock = metadataBlock;
    }

    public String getTitle()
    {
        return title;
    }

    public void setTitle(String title)
    {
        this.title = title;
    }

    public String getLocalName()
    {
        return localName;
    }

    public void setLocalName(String localName)
    {
        this.localName = localName;
    }

    public String getWatermark()
    {
        return watermark;
    }

    public void setWatermark(String watermark)
    {
        this.watermark = watermark;
    }

    public int getDisplayOrder()
    {
        return displayOrder;
    }

    public void setDisplayOrder(int displayOrder)
    {
        this.displayOrder = displayOrder;
    }

    public boolean isRequired()
    {
        return required;
    }

    public void setRequired(boolean required)
    {
        this.required = required;
    }

    public String getDescription()
    {
        return description;
    }

    public void setDescription(String description)
    {
        this.description = description;
    }

    private String getLocalNameOrName() {
        if (localName != null) {
            return localName;
        }
        return getOriginal().getName();
    }

    public String getLocaleTitle() {
        if(getMetadataBlock()  == null) {
            return title;
        }
        else {
            try {
                return BundleUtil.getStringFromPropertyFile("datasetfieldtype." + getLocalNameOrName() + ".title", getMetadataBlock().getName());
            } catch (MissingResourceException e) {
                if (title != null) {
                    return title;
                }
                else {
                    return original.getTitle();
                }
            }
        }
    }

    public String getLocaleDescription() {
        if(getMetadataBlock()  == null) {
            return description;
        } else {
            try {
                return BundleUtil.getStringFromPropertyFile("datasetfieldtype." + getLocalNameOrName() + ".description", getMetadataBlock().getName());
            } catch (MissingResourceException e) {
                if (description != null) {
                    return description;
                }
                else {
                    return original.getDescription();
                }
            }
        }
    }

    public String getLocaleWatermark()    {
        if(getMetadataBlock()  == null) {
            return watermark;
        } else {
            try {
                return BundleUtil.getStringFromPropertyFile("datasetfieldtype." + getLocalNameOrName() + ".watermark", getMetadataBlock().getName());
            } catch (MissingResourceException e) {
                if (watermark != null) {
                    return watermark;
                }
                else {
                    return original.getWatermark();
                }
            }
        }
    }

    public boolean isGenerateOriginalField()
    {
        return generateOriginalField;
    }

    public void setGenerateOriginalField(boolean generateOriginalField)
    {
        this.generateOriginalField = generateOriginalField;
    }

    public String getName() {
        return getOriginal().getName();
    }
}
