package edu.harvard.iq.dataverse;

import edu.harvard.iq.dataverse.search.IndexServiceBean;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.inject.Named;
import javax.persistence.*;
import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;
import java.util.stream.Collectors;

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

    /**
     * Sets @{@link DatasetFieldTypeOverride#generateOriginalField} value to @{code true } for each field in
     * @{code overrides}, when the original field associated with the override cannot be found in any of @{code allMdbs}
     * @param overrides dataset field overrides to process
     * @param allMdbs all MDB-s to be presented to the user, including the MDB of the overrides
     * @param dv the Dataverse the dataset with overriden fields belongs to. Used for calculating hidden fields.
     * @return
     */
    public void calcGenerateOriginalFieldValues(
            List<DatasetFieldTypeOverride> overrides,
            List<MetadataBlock> allMdbs,
            Dataverse dv
    ) {
        if (overrides.isEmpty()) {
            return;
        }
        // Field that may be excluded from MDB-s
        List<DataverseFieldTypeInputLevel> inputLevels = dv.getDataverseFieldTypeInputLevels();

        MetadataBlock overrideMdb = overrides.get(0).getMetadataBlock();

        // MDB-s other than that of overrides
        List<MetadataBlock> otherMdbs = allMdbs.stream().filter(mdb -> mdb.getId() != overrideMdb.getId()).collect(Collectors.toList());

        // Now set the generateOriginalField based on whether the original field is included in any of the otherMdbs
        // and is not contained in the inputLevels with include==false value.
        overrides.stream().forEach(ov -> {
            boolean otherMdbContains = otherMdbs.contains(ov.getOriginal().getMetadataBlock());
            // Even if other MDB contains, it may be hidden
            if (otherMdbContains) {
                // If the original field is listed in the Dataverse as not included then actually no other MDB will
                // contain the field, ie. no input field will be generated for it
                Optional<DataverseFieldTypeInputLevel> ilFound = inputLevels.stream().filter(il ->
                        il.getDatasetFieldType().getId() == ov.getOriginal().getId()
                ).findFirst();
                if (ilFound.isPresent() && ilFound.get().isInclude() == false) {
                    otherMdbContains = false;
                }
            }
            // If no other mdb contains the original field, then we have to generate it when displaying overrideMdb
            ov.setGenerateOriginalField(!otherMdbContains);
        });
    }
}
