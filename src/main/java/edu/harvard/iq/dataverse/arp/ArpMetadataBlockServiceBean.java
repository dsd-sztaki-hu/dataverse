package edu.harvard.iq.dataverse.arp;

import edu.harvard.iq.dataverse.DatasetFieldType;
import edu.harvard.iq.dataverse.DatasetServiceBean;
import edu.harvard.iq.dataverse.MetadataBlock;

import javax.ejb.Stateless;
import javax.inject.Named;
import javax.persistence.*;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Handles DatasetFieldTypeOverride records.
 */
@Stateless
@Named
public class ArpMetadataBlockServiceBean implements java.io.Serializable
{
    private static final Logger logger = Logger.getLogger(DatasetServiceBean.class.getCanonicalName());

    @PersistenceContext(unitName = "VDCNet-ejbPU")
    private EntityManager em;


    public List<DatasetFieldTypeOverride> findOverrides(MetadataBlock mdb) {
        Query query = em.createNamedQuery("DatasetFieldTypeOverride.findOverrides");
        query.setParameter("metadataBlock", mdb);
        return query.getResultList();
    }

    public DatasetFieldTypeOverride findOverrideByOriginal(DatasetFieldType original) {
        var query = em.createNamedQuery("DatasetFieldTypeOverride.findOneOverrideByOriginal", DatasetFieldTypeOverride.class);
        query.setParameter("original", original);
        var res = query.getResultList();
        if (res.size() == 0) {
            return null;
        }
        return res.get(0);
    }


    public DatasetFieldTypeOverride save(DatasetFieldTypeOverride override) {
        return em.merge(override);
    }

    public List<DatasetFieldTypeOverride> save(List<DatasetFieldTypeOverride> overrides) {
        return overrides.stream().map(ov -> em.merge(ov)).collect(Collectors.toList());
    }

    public void delete(DatasetFieldTypeOverride override) {
         em.remove(override);
    }

    public void delete(List<DatasetFieldTypeOverride> overrides) {
        overrides.forEach(ov -> em.remove(ov));
    }

    public DatasetFieldTypeArp findDatasetFieldTypeArpForFieldType(DatasetFieldType fieldType) {
        var query = em.createNamedQuery("DatasetFieldTypeArp.findOneForDatasetFieldType", DatasetFieldTypeArp.class);
        query.setParameter("fieldType", fieldType);
        var res = query.getResultList();
        if (res.size() == 0) {
            return null;
        }
        return res.get(0);
    }

    public List<DatasetFieldTypeArp> findDatasetFieldTypesForMetadataBlock(MetadataBlock metadataBlock) {
        var query = em.createNamedQuery("DatasetFieldTypeArp.findAllByMetadataBlock", DatasetFieldTypeArp.class);
        query.setParameter("metadataBlock",metadataBlock);
        var res = query.getResultList();
        return res;
    }

    public DatasetFieldTypeArp save(DatasetFieldTypeArp fieldTypeArp) {
        return em.merge(fieldTypeArp);
    }

    public void delete(DatasetFieldTypeArp fieldTypeArp) {
        em.remove(fieldTypeArp);
    }

    public MetadataBlockArp findMetadataBlockArpForMetadataBlock(MetadataBlock metadataBlock) {
        var query = em.createNamedQuery("MetadataBlockArp.findOneForMetadataBlock", MetadataBlockArp.class);
        query.setParameter("metadataBlock", metadataBlock);
        var res = query.getResultList();
        if (res.size() == 0) {
            return null;
        }
        return res.get(0);
    }

    public MetadataBlockArp save(MetadataBlockArp metadataBlockArp) {
        return em.merge(metadataBlockArp);
    }

    public void delete(MetadataBlockArp metadataBlockArp) {
        em.remove(metadataBlockArp);
    }

}
