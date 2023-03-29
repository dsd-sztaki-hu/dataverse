package edu.harvard.iq.dataverse.arp;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder({"name", "title", "description", "watermark", "fieldType", "displayOrder", "displayFormat", "advancedSearchField", "allowControlledVocabulary", "allowmultiples", "facetable", "displayoncreate", "required", "parent", "metadatablock_id", "termURI"})
class DataverseDatasetField
{
    // schema:name
    private String name;
    private String title;
    // schema:description
    private String description;
    private String watermark;
    private String fieldType;
    private int displayOrder;
    private String displayFormat;
    private String parent;
    private String metadatablock_id;
    private String termURI;
    private boolean advancedSearchField;
    private boolean allowControlledVocabulary;
    private boolean allowmultiples;
    private boolean facetable;
    private boolean displayoncreate;
    private boolean required;

    public DataverseDatasetField()
    {
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public String getTitle()
    {
        return title;
    }

    public void setTitle(String title)
    {
        this.title = title;
    }

    public String getDescription()
    {
        return description;
    }

    public void setDescription(String description)
    {
        this.description = description;
    }

    public String getWatermark()
    {
        return watermark;
    }

    public void setWatermark(String watermark)
    {
        this.watermark = watermark;
    }

    public String getFieldType()
    {
        return fieldType;
    }

    public void setFieldType(String fieldType)
    {
        this.fieldType = fieldType;
    }

    public int getDisplayOrder()
    {
        return displayOrder;
    }

    public void setDisplayOrder(int displayOrder)
    {
        this.displayOrder = displayOrder;
    }

    public String getDisplayFormat()
    {
        return displayFormat;
    }

    public void setDisplayFormat(String displayFormat)
    {
        this.displayFormat = displayFormat;
    }

    public String getParent()
    {
        return parent;
    }

    public void setParent(String parent)
    {
        this.parent = parent;
    }

    public String getmetadatablock_id()
    {
        return metadatablock_id;
    }

    public void setmetadatablock_id(String metadataBlockId)
    {
        this.metadatablock_id = metadataBlockId;
    }

    public String getTermURI()
    {
        return termURI;
    }

    public void setTermUri(String termUri)
    {
        this.termURI = termUri;
    }

    public boolean isAdvancedSearchField()
    {
        return advancedSearchField;
    }

    public void setAdvancedSearchField(boolean advancedSearchField)
    {
        this.advancedSearchField = advancedSearchField;
    }

    public boolean isAllowControlledVocabulary()
    {
        return allowControlledVocabulary;
    }

    public void setAllowControlledVocabulary(boolean allowControlledVocabulary)
    {
        this.allowControlledVocabulary = allowControlledVocabulary;
    }

    public boolean isAllowmultiples()
    {
        return allowmultiples;
    }

    public void setAllowmultiples(boolean allowmultiples)
    {
        this.allowmultiples = allowmultiples;
    }

    public boolean isFacetable()
    {
        return facetable;
    }

    public void setFacetable(boolean facetable)
    {
        this.facetable = facetable;
    }

    public boolean isDisplayoncreate()
    {
        return displayoncreate;
    }

    public void setDisplayoncreate(boolean displayoncreate)
    {
        this.displayoncreate = displayoncreate;
    }

    public boolean isRequired()
    {
        return required;
    }

    public void setRequired(boolean required)
    {
        this.required = required;
    }
}
