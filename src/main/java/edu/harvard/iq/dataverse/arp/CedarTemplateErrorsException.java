package edu.harvard.iq.dataverse.arp;

public class CedarTemplateErrorsException extends Exception
{
    CedarTemplateErrors errors;

    public CedarTemplateErrorsException(String message, CedarTemplateErrors errors)
    {
        super(message);
        this.errors = errors;
    }

    public CedarTemplateErrorsException(CedarTemplateErrors errors)
    {
        super(errors.toJson().toString());
        this.errors = errors;
    }


    public CedarTemplateErrors getErrors()
    {
        return errors;
    }
}
