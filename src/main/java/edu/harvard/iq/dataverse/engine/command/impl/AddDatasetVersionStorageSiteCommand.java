package edu.harvard.iq.dataverse.engine.command.impl;

import edu.harvard.iq.dataverse.*;
import edu.harvard.iq.dataverse.authorization.Permission;
import edu.harvard.iq.dataverse.engine.command.*;
import edu.harvard.iq.dataverse.engine.command.exception.*;
import edu.harvard.iq.dataverse.util.BundleUtil;
import java.util.logging.Logger;

/**
 *
 * @author pallinger
 */
@RequiredPermissions(Permission.EditDataset)
public class AddDatasetVersionStorageSiteCommand  extends AbstractCommand<DatasetVersion> {

	private static final Logger logger = Logger.getLogger(AddDatasetVersionStorageSiteCommand.class.getCanonicalName());
	private final DatasetVersion datasetVersion;
	private final DatasetVersionStorageSite newStorageSite;
    
	public AddDatasetVersionStorageSiteCommand(DatasetVersion dsv, DataverseRequest req, DatasetVersionStorageSite storageSite) {
		super(req,dsv.getDataset());
		this.datasetVersion=dsv;
		this.newStorageSite=storageSite;
	}
	
	@Override
	public DatasetVersion execute(CommandContext ctxt) throws CommandException {
		if(!ctxt.permissions().requestOn(getRequest(),datasetVersion.getDataset()).has(Permission.ManageDatasetPermissions)
				&& newStorageSite.getStatus().allowedInitially())
			throw new IllegalCommandException(BundleUtil.getStringFromBundle("datasetVersion.message.addStorageSiteNotAllowed"),this);
		
		if(datasetVersion.getStorageSites().stream().filter(s -> s.matches(newStorageSite)).findAny().isPresent())
			throw new IllegalCommandException(BundleUtil.getStringFromBundle("datasetVersion.message.addStorageSiteAlreadyExists"),this);
		
		datasetVersion.getStorageSites().add(newStorageSite);
		return datasetVersion;
	}
}
