package edu.harvard.iq.dataverse.arp.rocrate;

import edu.harvard.iq.dataverse.Dataset;
import edu.harvard.iq.dataverse.MetadataBlock;
import edu.harvard.iq.dataverse.arp.ArpMetadataBlockServiceBean;
import edu.kit.datamanager.ro_crate.entities.data.RootDataEntity;
import jakarta.ejb.EJB;
import jakarta.ejb.Stateless;
import jakarta.inject.Named;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Manages RO-Crate conformsTo values based on Dataverse metadatablocks.
 *
 * This class has been extracted from the ARP project (https://science-research-data.hu/en) in the frame of
 * FAIR-IMPACT's 1st Open Call "Enabling FAIR Signposting and RO-Crate for content/metadata discovery and consumption".
 *
 * @author Balázs E. Pataki <balazs.pataki@sztaki.hu>, SZTAKI, Department of Distributed Systems, https://dsd.sztaki.hu
 * @author Norbert Finta <norbert.finta@sztaki.hu>, SZTAKI, Department of Distributed Systems, https://dsd.sztaki.hu
 * @version 1.0
 */
@Stateless
@Named
public class DefaultRoCrateConformsToIdProvider implements RoCrateConformsToIdProvider
{
    @EJB
    protected ArpMetadataBlockServiceBean arpMetadataBlockSvc;
    
    @Override
    public List<String> generateConformsToIds(Dataset dataset, RootDataEntity rootDataEntity)
    {
        return dataset.getOwner().getMetadataBlocks().stream()
                .map(mdb -> {
                    var mdbArp = arpMetadataBlockSvc.findMetadataBlockArpForMetadataBlock(mdb);
                    if (mdbArp == null) {
                        throw new Error("No ARP metadatablock found for metadatablock '"+mdb.getName()+
                                "'. You need to upload the metadatablock to CEDAR and back to Dataverse to connect it with its CEDAR template representation");
                    }
                    return mdbArp.getRoCrateConformsToId();
                }).collect(Collectors.toList());
    }

    @Override
    public List<MetadataBlock> findMetadataBlockForConformsToIds(List<String> ids)
    {
        return ids.stream()
                .map(id -> {
                    // If not found, map to null, filter it later
                    var mdbArp = arpMetadataBlockSvc.findByRoCrateConformsToId(id);
                    if (mdbArp == null) {
                        return null;
                    }
                    return mdbArp.getMetadataBlock();
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }
}
