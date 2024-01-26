package edu.harvard.iq.dataverse.arp;

import edu.kit.datamanager.ro_crate.RoCrate;

import java.util.ArrayList;
import java.util.List;

public class RoCrateImportPrepResult {
    private RoCrate roCrate;
    
    public List<String> errors;

    public RoCrateImportPrepResult() {
        this.errors = new ArrayList<>();
    }

    public RoCrate getRoCrate() {
        return roCrate;
    }

    public void setRoCrate(RoCrate roCrate) {
        this.roCrate = roCrate;
    }
}