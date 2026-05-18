package edu.harvard.iq.dataverse.arp;

import edu.harvard.iq.dataverse.DatasetFieldType;
import edu.harvard.iq.dataverse.DatasetServiceBean;
import edu.harvard.iq.dataverse.MetadataBlock;

import jakarta.ejb.Stateless;
import jakarta.inject.Named;
import jakarta.persistence.*;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import com.google.gson.JsonParser;

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

    public MetadataBlockArp findMetadataBlockArpForMetadataBlockById(Long id) {
        var query = em.createNamedQuery("MetadataBlockArp.findOneForMetadataBlockById", MetadataBlockArp.class);
        query.setParameter("metadataBlockId", id);
        var res = query.getResultList();
        if (res.size() == 0) {
            return null;
        }
        return res.get(0);
    }
    
    public MetadataBlockArp findByRoCrateConformsToId(String roCrateConformsToId) {
        try {
            return em.createNamedQuery("MetadataBlockArp.findByRoCrateConformsToId", MetadataBlockArp.class)
                    .setParameter("roCrateConformsToId", roCrateConformsToId)
                    .getSingleResult();
        } catch (NoResultException nre) {
            return null;
        }
    }

    public MetadataBlockArp save(MetadataBlockArp metadataBlockArp) {
        return em.merge(metadataBlockArp);
    }

    public void delete(MetadataBlockArp metadataBlockArp) {
        em.remove(metadataBlockArp);
    }

    /**
     * Checks if there is a MetadataBlockArp with the same schema:identifier but different @id
     * and verifies that the provided version is greater than or equal to the existing version.
     * @param schemaIdentifier The schema:identifier to check
     * @param id The @id to compare against
     * @param providedVersion The pav:version of the schema being checked (e.g., "0.0.1")
     * @return true if a duplicate is found and provided version is smaller than existing version, false otherwise
     */
    public boolean isDuplicateSchemaIdentifier(String schemaIdentifier, String id, String providedVersion) {
        try {
            // Get all MetadataBlockArp records
            var query = em.createQuery("SELECT mdbArp FROM MetadataBlockArp mdbArp", MetadataBlockArp.class);
            var results = query.getResultList();
            
            // Parse each result's cedarDefinition to check schema:identifier and @id
            for (var mdbArp : results) {
                var cedarDef = mdbArp.getCedarDefinition();
                if (cedarDef != null) {
                    var jsonObject = JsonParser.parseString(cedarDef).getAsJsonObject();
                    var existingIdentifier = jsonObject.get("schema:identifier");
                    if (existingIdentifier != null && existingIdentifier.getAsString().equals(schemaIdentifier)) {
                        var existingId = jsonObject.get("@id");
                        if (existingId != null && !existingId.getAsString().equals(id)) {
                            // Check version comparison if both versions exist
                            var existingVersion = jsonObject.get("pav:version");
                            if (existingVersion != null && providedVersion != null) {
                                var existingVersionString = existingVersion.getAsString();
                                // Only return true if provided smaller than existing version
                                if (compareVersions(providedVersion, existingVersionString) < 0) {
                                    return true;
                                }
                            } else if (existingVersion != null && providedVersion == null) {
                                // If existing has version but provided doesn't, treat as a duplicate
                                return true;
                            }
                        }
                    }
                }
            }
            return false;
        } catch (Exception e) {
            logger.warning("Error checking for duplicate schema:identifier: " + e.getMessage());
            return false;
        }
    }

    /**
     * Compares two semantic version strings (e.g., "0.0.1").
     * @param version1 First version to compare
     * @param version2 Second version to compare
     * @return negative if version1 < version2, zero if equal, positive if version1 > version2
     */
    private int compareVersions(String version1, String version2) {
        try {
            String[] parts1 = version1.split("\\.");
            String[] parts2 = version2.split("\\.");
            int maxLength = Math.max(parts1.length, parts2.length);
            
            for (int i = 0; i < maxLength; i++) {
                int part1 = i < parts1.length ? Integer.parseInt(parts1[i]) : 0;
                int part2 = i < parts2.length ? Integer.parseInt(parts2[i]) : 0;
                
                if (part1 != part2) {
                    return part1 - part2;
                }
            }
            return 0;
        } catch (Exception e) {
            logger.warning("Error comparing versions: " + e.getMessage());
            // If version parsing fails, treat as equal to avoid false positives
            return 0;
        }
    }

}
