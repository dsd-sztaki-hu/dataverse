package edu.harvard.iq.dataverse.api.arp.util;

import edu.harvard.iq.dataverse.Dataset;
import edu.harvard.iq.dataverse.dataaccess.DataAccess;
import edu.harvard.iq.dataverse.dataaccess.FileAccessIO;
import edu.harvard.iq.dataverse.dataaccess.S3AccessIO;

import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

public class StorageUtils
{
    private static String SZTAKI_STORAGE_ID = "sztaki";
    private static String WIGNER_STORAGE_ID = "wigner";
    private static String LOCAL_STORAGE_ID = "local";

    private static final Logger logger = Logger.getLogger(StorageUtils.class.getCanonicalName());

    /**
     * Synchronizes RO-Crate related files from local filesystem to the storage of the Dataset.
     *
     * If allVersions is true, all versions of the RO-Crate is synced with the storage taking
     * possible deletions in the local version into account. If allVersions is false only the latest
     * RO-Crate related files will be synchronized.
     *
     * Note: Every RO-Crate related operation works on local storage because the RO-Crate handling library
     * is bound to filesystem. In case of file based storage we work right in the filesystem, in which case this
     * method does nothing. However, if storage is of type other than "file", then we do the synchronization.
     * Specifically we have S3 storage besides filesystem.
     * @param dataset the dataset the RO-Crate is to be synced with
     * @param allVersions
     */
    public static void syncRoCrateToStorage(Dataset dataset, boolean allVersions) {

    }

    /**
     * Returns a directory where RO-Crate related files can be stored. In case the dataset uses file storage this
     * will return the directory of the file storage, so we can work alongside all the other dataset related files
     * (data files). In case the dataset uses remote storage, ie. S3 then it will return the value of the system
     * config value of "dataverse.files.directory" + dataset ID related subdirectory.
     * @param dataset
     */
    public static String getLocalRoCrateDir(Dataset dataset) {
        try {
            // In case it is already FileAccessIO just return the dataset's dir
            var storageIO = DataAccess.getStorageIO(dataset);
            if (storageIO instanceof FileAccessIO) {
                return ((FileAccessIO)storageIO).getDatasetDirectory();
            }
            // Otherwise if it is a well known storage (SZTAKI or Wigner), work with the directory configure for the
            // storage.
            else {
                String dir = null;
                var driverId = dataset.getEffectiveStorageDriverId();
                if (storageIO instanceof S3AccessIO) {
                    dir = System.getProperty("dataverse.files." + driverId + "." + "directory", null);
                    if (dir == null) {
                        throw new RuntimeException("Value for 'dataverse.files." + driverId + ".directory' property has not been specified. "+
                                "Use JVM_ARGS or 'asadmin $ASADMIN_OPTS create-jvm-options' with value '-Ddataverse.files." + driverId + ".directory'");
                    }
                    // TODO: these needs to be tuned to the actual S3 filemapping solution. it may not actually store in this
                    // dir!
                    return String.join(File.separator, dir, dataset.getAuthorityForFileStorage(), dataset.getIdentifierForFileStorage());
                }
                else {
                    throw new RuntimeException("Unknown storage drive id: '"+driverId+"'");
                }
            }
        } catch (IOException e) {
            logger.severe("Unable to get RO-Crate dir: "+e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
