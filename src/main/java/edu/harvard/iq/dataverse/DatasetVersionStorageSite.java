/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package edu.harvard.iq.dataverse;

import edu.harvard.iq.dataverse.locality.StorageSite;
import java.util.Arrays;
import java.util.EnumSet;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

/**
 *
 * @author pallinger
 */
@Entity
@Table(	name = "dvobjectremotestoragelocation",
		indexes = {@Index(columnList="dvobject_id")},
		uniqueConstraints = {@UniqueConstraint(columnNames = {"dsversion_id"}),@UniqueConstraint(columnNames = {"storagesite_id"})})
public class DatasetVersionStorageSite implements java.io.Serializable {
	@Id
	@ManyToOne
    @JoinColumn(name = "datasetversion_id")
	private DatasetVersion datasetVersion;
	public DatasetVersion getDatasetVersion() {
		return datasetVersion;
	}
	public void setDvobject(DatasetVersion dvobject) {
		this.datasetVersion = dvobject;
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

	public static enum StorageStatusEnum {
		NONE("NONE"),
		COPY_REQUEST("CPRQ"),
		COPY_APPROVED("CPAP"),
		COPY_IN_PROGRESS("CPIN"),
		COPY_DONE("CPDN"),
		//COPY_CHANGED("CPCH"), // this is not needed, as only draft can change, and we do not sync drafts // copy was done before, but meanwhile the dvobject was changed 
		DELETE_REQUEST("DLRQ"),
		DELETE_APPROVED("DLAP"),
		DELETE_IN_PROGRESS("DLIN")
		;
		
		private final String text;

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
				case COPY_REQUEST:
					return beforeStatus==NONE;
				case COPY_APPROVED:
					return beforeStatus==COPY_REQUEST;
				case COPY_IN_PROGRESS:
					return beforeStatus==COPY_APPROVED;
				case COPY_DONE:
					return beforeStatus==COPY_IN_PROGRESS;
				case DELETE_REQUEST:
					return EnumSet.of(COPY_APPROVED, COPY_IN_PROGRESS, COPY_DONE).contains(beforeStatus);
				case DELETE_APPROVED:
					return beforeStatus==DELETE_REQUEST;
				case DELETE_IN_PROGRESS:
					return beforeStatus==DELETE_APPROVED;
				default:
					throw new AssertionError();
			}
		}
	}
}
