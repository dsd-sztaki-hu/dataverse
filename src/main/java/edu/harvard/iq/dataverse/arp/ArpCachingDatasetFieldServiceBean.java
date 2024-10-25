package edu.harvard.iq.dataverse.arp;

import edu.harvard.iq.dataverse.*;
import jakarta.ejb.EJB;
import jakarta.ejb.Stateless;
import jakarta.inject.Named;
import jakarta.json.JsonObject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A DatasetFieldServiceBean that caches the results of its find methods. We expect that MDB-s and fields rarely change,
 * and if they do we are aware of that and can invalidate the cache.
 */
@Stateless
@Named
public class ArpCachingDatasetFieldServiceBean {

    @EJB
    DatasetFieldServiceBean fieldService;

    List<DatasetFieldType> advancedSearchFieldTypes;
    List<DatasetFieldType> allFacetableFieldTypes;
    Map<Long, List<DatasetFieldType>> facetableFieldTypesByMetadataBlock;
    List<DatasetFieldType> allRequiredFields;
    List<DatasetFieldType> allOrderedById;
    List<DatasetFieldType> allOrderedByName;
    Map<Object, DatasetFieldType> fieldsById;
    Map<String, DatasetFieldType> fieldsByName;

   public void invalidateCaches() {
        advancedSearchFieldTypes = null;
        allFacetableFieldTypes = null;
        facetableFieldTypesByMetadataBlock = null;
        allRequiredFields = null;
        allOrderedById = null;
        allOrderedByName = null;
        fieldsById = null;
        fieldsByName = null;
    }

    public List<DatasetFieldType> findAllAdvancedSearchFieldTypes() {
        if (advancedSearchFieldTypes == null) {
            advancedSearchFieldTypes = fieldService.findAllAdvancedSearchFieldTypes();
        }
        return advancedSearchFieldTypes;
    }

    public List<DatasetFieldType> findAllFacetableFieldTypes() {
        if (allFacetableFieldTypes == null) {
            allFacetableFieldTypes = fieldService.findAllFacetableFieldTypes();
        }
        return allFacetableFieldTypes;
    }

    public List<DatasetFieldType> findFacetableFieldTypesByMetadataBlock(Long metadataBlockId) {
        if (facetableFieldTypesByMetadataBlock == null) {
            facetableFieldTypesByMetadataBlock = new HashMap<>();
        }
        if (!facetableFieldTypesByMetadataBlock.containsKey(metadataBlockId)) {
            facetableFieldTypesByMetadataBlock.put(metadataBlockId, fieldService.findFacetableFieldTypesByMetadataBlock(metadataBlockId));
        }
        return facetableFieldTypesByMetadataBlock.get(metadataBlockId);
    }

    public List<DatasetFieldType> findAllRequiredFields() {
        if (allRequiredFields == null) {
            allRequiredFields = fieldService.findAllRequiredFields();
        }
        return allRequiredFields;
    }

    public List<DatasetFieldType> findAllOrderedById() {
        if (allOrderedById == null) {
            allOrderedById = fieldService.findAllOrderedById();
        }
        return allOrderedById;
    }

    public List<DatasetFieldType> findAllOrderedByName() {
        if (allOrderedByName == null) {
            allOrderedByName = fieldService.findAllOrderedByName();
        }
        return allOrderedByName;
    }

    public DatasetFieldType find(Object pk) {
        if (fieldsById == null) {
            fieldsById = new HashMap<>();
        }
        if (!fieldsById.containsKey(pk)) {
            fieldsById.put(pk, fieldService.find(pk));
        }
        return fieldsById.get(pk);
    }

    public DatasetFieldType findByName(String name) {
        if (fieldsByName == null) {
            fieldsByName = new HashMap<>();
        }
        if (!fieldsByName.containsKey(name)) {
            fieldsByName.put(name, fieldService.findByName(name));
        }
        return fieldsByName.get(name);
    }

    // TODO: what's the difference between this and findByName? Should we cache both?
    public DatasetFieldType findByNameOpt(String name) {
        return fieldService.findByNameOpt(name);
    }

    public ForeignMetadataFieldMapping findFieldMapping(String formatName, String pathName) {
        return fieldService.findFieldMapping(formatName, pathName);
    }

    public ControlledVocabularyValue findControlledVocabularyValue(Object pk) {
        return fieldService.findControlledVocabularyValue(pk);
    }
    
    public ControlledVocabularyValue findControlledVocabularyValueByDatasetFieldTypeAndStrValue(DatasetFieldType dsft, String strValue, boolean lenient) {
        return fieldService.findControlledVocabularyValueByDatasetFieldTypeAndStrValue(dsft, strValue, lenient);
    }

    public ControlledVocabAlternate findControlledVocabAlternateByControlledVocabularyValueAndStrValue(ControlledVocabularyValue cvv, String strValue) {
        return fieldService.findControlledVocabAlternateByControlledVocabularyValueAndStrValue(cvv, strValue);
    }

    public ControlledVocabularyValue findControlledVocabularyValueByDatasetFieldTypeAndIdentifier(DatasetFieldType dsft, String identifier) {
        return fieldService.findControlledVocabularyValueByDatasetFieldTypeAndIdentifier(dsft, identifier);
    }

    public ControlledVocabularyValue findNAControlledVocabularyValue() {
        return fieldService.findNAControlledVocabularyValue();
    }

    public DatasetFieldType save(DatasetFieldType dsfType)
    {
        return fieldService.save(dsfType);
    }
    
    public MetadataBlock save(MetadataBlock mdb)
    {
        return fieldService.save(mdb);
    }

    public ControlledVocabularyValue save(ControlledVocabularyValue cvv)
    {
        return fieldService.save(cvv);
    }

    public ControlledVocabAlternate save(ControlledVocabAlternate alt)
    {
        return fieldService.save(alt);
    }

    public Map<Long, JsonObject> getCVocConf(boolean byTermUriField)
    {
        return fieldService.getCVocConf(byTermUriField);
    }

    public void registerExternalVocabValues(DatasetField df)
    {
        fieldService.registerExternalVocabValues(df);
    }

    public Set<String> getStringsFor(String termUri)
    {
        return fieldService.getStringsFor(termUri);
    }

    public JsonObject getExternalVocabularyValue(String termUri)
    {
        return fieldService.getExternalVocabularyValue(termUri);
    }

    public void registerExternalTerm(JsonObject cvocEntry, String term)
    {
        fieldService.registerExternalTerm(cvocEntry, term);
    }

    
    public boolean isValidCVocValue(DatasetFieldType dft, String value)
    {
        return fieldService.isValidCVocValue(dft, value);
    }

    public List<String> getVocabScripts(Map<Long, JsonObject> cvocConf)
    {
        return fieldService.getVocabScripts(cvocConf);
    }

    public String getFieldLanguage(String languages, String localeCode)
    {
        return fieldService.getFieldLanguage(languages, localeCode);
    }
}
