package edu.harvard.iq.dataverse.arp;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder({"DatasetField", "Value", "identifier", "displayOrder"})
class DataverseControlledVocabulary
{
    @JsonProperty("DatasetField")
    private String datasetField;
    @JsonProperty("Value")
    private String value;
    private String identifier;
    private int displayOrder;

    public DataverseControlledVocabulary()
    {
    }

    @JsonProperty("DatasetField")
    public String getDatasetField()
    {
        return datasetField;
    }

    @JsonProperty("DatasetField")
    public void setDatasetField(String DatasetField)
    {
        this.datasetField = DatasetField;
    }

    @JsonProperty("Value")
    public String getValue()
    {
        return value;
    }

    @JsonProperty("Value")
    public void setValue(String value)
    {
        this.value = value;
    }

    public String getIdentifier()
    {
        return identifier;
    }

    public void setIdentifier(String identifier)
    {
        this.identifier = identifier;
    }

    public int getDisplayOrder()
    {
        return displayOrder;
    }

    public void setDisplayOrder(int displayOrder)
    {
        this.displayOrder = displayOrder;
    }
}
