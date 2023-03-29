package edu.harvard.iq.dataverse.arp;

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
public class DatasetFieldTypeOverrideServiceBean implements java.io.Serializable
{
    private static final Logger logger = Logger.getLogger(DatasetServiceBean.class.getCanonicalName());

    @PersistenceContext(unitName = "VDCNet-ejbPU")
    private EntityManager em;


    public List<DatasetFieldTypeOverride> findOverrides(MetadataBlock mdb) {
        Query query = em.createNamedQuery("DatasetFieldTypeOverride.findOverrides");
        query.setParameter("metadataBlock", mdb);
        return query.getResultList();
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

}
