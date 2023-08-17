package edu.harvard.iq.dataverse.engine.command.impl;

import edu.harvard.iq.dataverse.*;
import edu.harvard.iq.dataverse.authorization.Permission;
import edu.harvard.iq.dataverse.authorization.users.AuthenticatedUser;
import edu.harvard.iq.dataverse.engine.command.*;
import edu.harvard.iq.dataverse.engine.command.exception.*;
import edu.harvard.iq.dataverse.util.BundleUtil;
import java.util.*;
import java.util.logging.Logger;

/**
 *
 * @author pallinger
 */
public class EditDatasetVersionStorageSiteCommand  extends AbstractCommand<DatasetVersion> {

	private static final Logger logger = Logger.getLogger(EditDatasetVersionStorageSiteCommand.class.getCanonicalName());
	private final DatasetVersion datasetVersion;
	private final DatasetVersionStorageSite updatedStorageSite;
	private final DatasetVersionStorageSite originalStorageSite;
	
	public EditDatasetVersionStorageSiteCommand(DatasetVersion dsv, DataverseRequest req, DatasetVersionStorageSite updatedStorageSite) {
		super(req,dsv.getDataset());
		this.datasetVersion = dsv;
		this.updatedStorageSite = updatedStorageSite;
		this.originalStorageSite = datasetVersion.getStorageSites().stream().
				filter(s -> s.matches(updatedStorageSite)).findFirst().orElseThrow();
	}
	
	@Override
	public DatasetVersion execute(CommandContext ctxt) throws CommandException {
		if(!ctxt.permissions().requestOn(getRequest(),datasetVersion.getDataset()).has(Permission.ManageDatasetPermissions))
			throw new IllegalCommandException(BundleUtil.getStringFromBundle("datasetVersion.message.editStorageSiteNotAllowed"),this);

		if(updatedStorageSite.getStatus().onlyAllowedForSuperAdmin() && (!(getUser() instanceof AuthenticatedUser) || !getUser().isSuperuser()))
			throw new IllegalCommandException(BundleUtil.getStringFromBundle("datasetVersion.message.editStorageSiteNotAllowed"),this);
		
		if(!updatedStorageSite.getStatus().allowedAfter(originalStorageSite.getStatus()))
			throw new IllegalCommandException(BundleUtil.getStringFromBundle("datasetVersion.message.storageSiteStatusChangeNotAllowed", Arrays.asList(originalStorageSite.getStatus().toString(), updatedStorageSite.getStatus().toString())),this);
		
		datasetVersion.getStorageSites().set(datasetVersion.getStorageSites().indexOf(originalStorageSite), updatedStorageSite);

		return datasetVersion;
	}
}
