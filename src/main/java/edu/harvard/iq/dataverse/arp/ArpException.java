package edu.harvard.iq.dataverse.arp;

import javax.ejb.ApplicationException;

@ApplicationException(rollback=true)
public class ArpException extends Exception {

    public ArpException(String s) {
        super(s);
    }
}
