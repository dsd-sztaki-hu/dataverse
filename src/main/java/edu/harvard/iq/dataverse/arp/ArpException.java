package edu.harvard.iq.dataverse.arp;

import jakarta.ejb.ApplicationException;

@ApplicationException(rollback=true)
public class ArpException extends Exception {

    public ArpException(String s) {
        super(s);
    }
}
