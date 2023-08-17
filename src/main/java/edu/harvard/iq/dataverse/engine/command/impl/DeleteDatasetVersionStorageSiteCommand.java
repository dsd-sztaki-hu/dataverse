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
public class DeleteDatasetVersionStorageSiteCommand  extends AbstractCommand<DatasetVersion> {

	private static final Logger logger = Logger.getLogger(DeleteDatasetVersionStorageSiteCommand.class.getCanonicalName());
	private final DatasetVersion datasetVersion;
	private final DatasetVersionStorageSite updatedStorageSite;
	private final DatasetVersionStorageSite originalStorageSite;
	
	public DeleteDatasetVersionStorageSiteCommand(DatasetVersion dsv, DataverseRequest req, DatasetVersionStorageSite updatedStorageSite) {
		super(req,dsv.getDataset());
		this.datasetVersion = dsv;
		this.updatedStorageSite = updatedStorageSite;
		this.originalStorageSite = datasetVersion.getStorageSites().stream().
				filter(s -> s.matches(updatedStorageSite)).findFirst().orElseThrow();
	}
	
	@Override
	public DatasetVersion execute(CommandContext ctxt) throws CommandException {
		if(originalStorageSite.getStatus().deleteAllowed() && (!(getUser() instanceof AuthenticatedUser) || !getUser().isSuperuser()))
			throw new IllegalCommandException(BundleUtil.getStringFromBundle("datasetVersion.message.deleteStorageSiteNotAllowed"),this);
		
		datasetVersion.getStorageSites().remove(originalStorageSite);

		return datasetVersion;
	}
}
