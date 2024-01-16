package edu.harvard.iq.dataverse;

import edu.harvard.iq.dataverse.locality.StorageSite;
import java.util.*;
import jakarta.persistence.*;

/**
 * This is to track alternative storage sites for a given dataset version.
 * The dataset version is stored normally in the storage designated by DvObject.storageIdentifier.
 * This junction table contains the secondary and tertiary locations that the data is copied to.
 * @author pallinger
 */
//@Entity
@Table(	name = "dvobjectremotestoragelocation",
		indexes = {@Index(columnList="dvobject_id")},
		uniqueConstraints = {@UniqueConstraint(columnNames = {"datasetversion_id"}),@UniqueConstraint(columnNames = {"storagesite_id"})})
public class DatasetVersionStorageSite implements java.io.Serializable {
	@Id
	@ManyToOne
    @JoinColumn(name = "datasetversion_id")
	private DatasetVersion datasetVersion;
	public DatasetVersion getDatasetVersion() {
		return datasetVersion;
	}
	public void setDvobject(DatasetVersion dsv) {
		this.datasetVersion = dsv;
	}
	
	@Id
	private StorageSite site;
	public StorageSite getSite() {
		return site;
	}
	public void setStorageSite(StorageSite site) {
		this.site = site;
	}

    @Column(nullable = false, length = 4)
    @Enumerated(EnumType.STRING)
	private StorageStatusEnum status;
	public StorageStatusEnum getStatus() {
		return status;
	}
	public void setStatus(StorageStatusEnum status) {
		this.status = status;
	}
	
	/**
	 * This is for searching in the versions storagesite list at edit time.
	 * @param other 
	 */
	public boolean matches(DatasetVersionStorageSite other) {
		return datasetVersion.equals(other.datasetVersion)
				&& site.equals(other.site);
	}

	public static enum StorageStatusEnum {
		NONE("NONE"),
		COPY_REQUESTED("CPRQ"),
		COPY_APPROVED("CPAP"),
		COPY_IN_PROGRESS("CPIN"),
		COPY_DONE("CPDN"), 
		//COPY_REJECTED("CPRJ"), // would we need this?
		//COPY_CHANGED("CPCH"), // this is not needed, as only draft can change, and we do not sync drafts // copy was done before, but meanwhile the dvobject was changed 
		DELETE_REQUESTED("DLRQ"),
		DELETE_APPROVED("DLAP"),
		DELETE_IN_PROGRESS("DLIN"),
		//DELETED("DLDN") // CHECKME: Is this necessary or not? Is this the same as NONE?
		;
		
		private final String text;

		private final static EnumSet<StorageStatusEnum> onlyAllowedForSuperAdmin = EnumSet.of(NONE,COPY_APPROVED,COPY_IN_PROGRESS,COPY_DONE,DELETE_APPROVED,DELETE_IN_PROGRESS);
		private final static EnumSet<StorageStatusEnum> allowedInitially = EnumSet.of(COPY_REQUESTED);
		private final static EnumSet<StorageStatusEnum> deleteAllowed = EnumSet.of(NONE, COPY_REQUESTED);
				
		private StorageStatusEnum(final String text) {
			this.text = text;
		}
        public static StorageStatusEnum fromString(String text) {
            if (text != null) {
                for (StorageStatusEnum sse : StorageStatusEnum.values()) {
                    if (text.equals(sse.text)) {
                        return sse;
                    }
                }
            }
            throw new IllegalArgumentException(StorageStatusEnum.class.toString()+" must be one of these values: " + Arrays.asList(StorageStatusEnum.values()) + ".");
        }

		@Override
		public String toString() {
			return "StorageStatusEnum{" + "text=" + text + '}';
		}
		
		/**
		 * Whether the current status is allowed to be selected after the status in the parameter.
		 * @param beforeStatus
		 * @return 
		 */
		public boolean allowedAfter(StorageStatusEnum beforeStatus) {
			if(beforeStatus==this)
				return true;
			switch (this) {
				case NONE:
					return beforeStatus==DELETE_IN_PROGRESS;
				case COPY_REQUESTED:
					return beforeStatus==NONE;
				case COPY_APPROVED:
					return beforeStatus==COPY_REQUESTED;
				case COPY_IN_PROGRESS:
					return beforeStatus==COPY_APPROVED;
				case COPY_DONE:
					return beforeStatus==COPY_IN_PROGRESS;
				case DELETE_REQUESTED:
					return EnumSet.of(COPY_APPROVED, COPY_IN_PROGRESS, COPY_DONE).contains(beforeStatus);
				case DELETE_APPROVED:
					return beforeStatus==DELETE_REQUESTED;
				case DELETE_IN_PROGRESS:
					return beforeStatus==DELETE_APPROVED;
				default:
					throw new AssertionError();
			}
		}
		
		public boolean onlyAllowedForSuperAdmin() {
			return onlyAllowedForSuperAdmin.contains(this);
		}
		
		public boolean allowedInitially() {
			return allowedInitially.contains(this);
		}

		public boolean deleteAllowed() {
			return deleteAllowed.contains(this);
		}
	}
}
