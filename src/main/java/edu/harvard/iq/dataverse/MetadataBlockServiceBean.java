package edu.harvard.iq.dataverse;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.util.List;
import java.util.StringJoiner;
import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.inject.Named;
import javax.persistence.EntityManager;
import javax.persistence.NoResultException;
import javax.persistence.PersistenceContext;

/**
 *
 * @author michael
 */
@Stateless
@Named
public class MetadataBlockServiceBean {
    @PersistenceContext(unitName = "VDCNet-ejbPU")
    private EntityManager em;
    
    
    
    public MetadataBlock save(MetadataBlock mdb) {
       return em.merge(mdb);
    }   
    
    
    public List<MetadataBlock> listMetadataBlocks() {
        return em.createNamedQuery("MetadataBlock.listAll", MetadataBlock.class).getResultList();
    }
    
    public MetadataBlock findById( Long id ) {
        return em.find(MetadataBlock.class, id);
    }
    
    public MetadataBlock findByName( String name ) {
        try {
            return em.createNamedQuery("MetadataBlock.findByName", MetadataBlock.class)
                        .setParameter("name", name)
                        .getSingleResult();
        } catch ( NoResultException nre ) {
            return null;
        }
    }

    public String exportMdbAsTsv(String mdbId) throws JsonProcessingException {
        MetadataBlock mdb = findById(Long.valueOf(mdbId));

        CsvSchema mdbSchema = CsvSchema.builder()
                .addColumn("#metadataBlock")
                .addColumn("name")
                .addColumn("dataverseAlias")
                .addColumn("displayName")
                .addColumn("blockURI")
                .build()
                .withHeader()
                .withColumnSeparator('\t')
                .withoutQuoteChar()
                .withoutEscapeChar();

        StringJoiner mdbRow = new StringJoiner("\t");
        mdbRow.add("");
        mdbRow.add(mdb.getName() != null ? mdb.getName() : "");
//      TODO: handle dataverseAlias?
        mdbRow.add("");
        mdbRow.add(mdb.getDisplayName());
        mdbRow.add(mdb.getNamespaceUri());

        CsvSchema datasetFieldSchema = CsvSchema.builder()
                .addColumn("#datasetField")
                .addColumn("name")
                .addColumn("title")
                .addColumn("description")
                .addColumn("watermark")
                .addColumn("fieldType")
                .addColumn("displayOrder")
                .addColumn("displayFormat")
                .addColumn("advancedSearchField")
                .addColumn("allowControlledVocabulary")
                .addColumn("allowmultiples")
                .addColumn("facetable")
                .addColumn("displayoncreate")
                .addColumn("required")
                .addColumn("parent")
                .addColumn("metadatablock_id")
                .addColumn("termURI")
                .build()
                .withHeader()
                .withColumnSeparator('\t')
                .withoutQuoteChar()
                .withoutEscapeChar();

        CsvSchema controlledVocabularySchema = CsvSchema.builder()
                .addColumn("#controlledVocabulary")
                .addColumn("DatasetField")
                .addColumn("Value")
                .addColumn("identifier")
                .addColumn("displayOrder")
                .build()
                .withHeader()
                .withColumnSeparator('\t')
                .withoutQuoteChar()
                .withoutEscapeChar();

        StringJoiner dsfRows = new StringJoiner("\n");
        StringJoiner cvRows = new StringJoiner("\n");
        mdb.getDatasetFieldTypes().forEach(dsf -> {
            StringJoiner dsfRowValues = new StringJoiner("\t");
            dsfRowValues.add("");
            dsfRowValues.add(dsf.getName());
            dsfRowValues.add(dsf.getTitle());
            dsfRowValues.add(dsf.getDescription());
            dsfRowValues.add(dsf.getWatermark());
            dsfRowValues.add(String.valueOf(dsf.getFieldType()));
            dsfRowValues.add(String.valueOf(dsf.getDisplayOrder()));
            dsfRowValues.add(dsf.getDisplayFormat());
            dsfRowValues.add(String.valueOf(dsf.isAdvancedSearchFieldType()));
            dsfRowValues.add(String.valueOf(dsf.isAllowControlledVocabulary()));
            dsfRowValues.add(String.valueOf(dsf.isAllowMultiples()));
            dsfRowValues.add(String.valueOf(dsf.isFacetable()));
            dsfRowValues.add(String.valueOf(dsf.isDisplayOnCreate()));
            dsfRowValues.add(String.valueOf(dsf.isRequired()));
            dsfRowValues.add(dsf.getParentDatasetFieldType() != null ? dsf.getParentDatasetFieldType().getName() : "");
            dsfRowValues.add(mdb.getName());
            dsfRowValues.add(dsf.getUri());
            dsfRows.add(dsfRowValues.toString().replace("null", ""));

            dsf.getControlledVocabularyValues().forEach(cvv -> {
                StringJoiner cvRowValues = new StringJoiner("\t");
                cvRowValues.add("");
                cvRowValues.add(dsf.getName());
                cvRowValues.add(cvv.getStrValue());
                cvRowValues.add(cvv.getIdentifier());
                cvRowValues.add(String.valueOf(cvv.getDisplayOrder()));
                cvRows.add(cvRowValues.toString().replace("null", ""));
            });
        });

        CsvMapper mapper = new CsvMapper();
        String metadataBlocks = mapper.writer(mdbSchema).writeValueAsString(mdbRow.toString().replace("null", "")).strip();
        String datasetFieldValues = mapper.writer(datasetFieldSchema).writeValueAsString(dsfRows.toString()).stripTrailing();
        String controlledVocabularyValues = mapper.writer(controlledVocabularySchema).writeValueAsString(cvRows.toString()).stripTrailing();

        return metadataBlocks + "\n" + datasetFieldValues + "\n" + controlledVocabularyValues;
    }
}
