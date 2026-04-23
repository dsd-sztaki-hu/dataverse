package edu.harvard.iq.dataverse.util;

import java.util.logging.Level;
import java.util.logging.Logger;
import jakarta.faces.application.FacesMessage;
import jakarta.faces.context.FacesContext;

/**
 * Utility class for common JSF tasks.
 * @author michael
 */
public class JsfHelper {
	private static final Logger logger = Logger.getLogger(JsfHelper.class.getName());
	
	public static final JsfHelper JH = new JsfHelper();

        public static void addSuccessMessage(String message) {
              FacesContext facesContext = FacesContext.getCurrentInstance();
              if (facesContext == null) {
                  logger.fine("FacesContext is null. Skipping JSF success message: " + message);
                  return;
              }
              facesContext.getExternalContext().getFlash().put("successMsg", message);
      
        } 
        public static void addFlashMessage(String message) {
            addSuccessMessage(message);
        }
        public static void addErrorMessage(String message) {
              FacesContext facesContext = FacesContext.getCurrentInstance();
              if (facesContext == null) {
                  logger.fine("FacesContext is null. Skipping JSF error message: " + message);
                  return;
              }
              facesContext.getExternalContext().getFlash().put("errorMsg", message);      
        } 
        public static void addInfoMessage(String message) {
              FacesContext facesContext = FacesContext.getCurrentInstance();
              if (facesContext == null) {
                  logger.fine("FacesContext is null. Skipping JSF info message: " + message);
                  return;
              }
              facesContext.getExternalContext().getFlash().put("infoMsg", message);      
        } 
        public static void addWarningMessage(String message) {
              FacesContext facesContext = FacesContext.getCurrentInstance();
              if (facesContext == null) {
                  logger.fine("FacesContext is null. Skipping JSF warning message: " + message);
                  return;
              }
              facesContext.getExternalContext().getFlash().put("warningMsg", message);      
        } 
	public void addMessage( FacesMessage.Severity s, String summary, String details ) {
		FacesContext facesContext = FacesContext.getCurrentInstance();
		if (facesContext == null) {
			logger.fine("FacesContext is null. Skipping FacesMessage: " + summary);
			return;
		}
		facesContext.addMessage(null, new FacesMessage(s, summary, details));
	}
	public void addMessage( FacesMessage.Severity s, String summary ) {
		addMessage(s, summary, "");
	}
	
	public <T extends Enum<T>> T enumValue( String param, Class<T> enmClass, T defaultValue ) {
		if ( param == null ) return defaultValue;
		param = param.trim();
		try {
			return Enum.valueOf(enmClass, param);
		} catch ( IllegalArgumentException iar ) {
			logger.log(Level.WARNING, "Illegal value for enum {0}: ''{1}''", new Object[]{enmClass.getName(), param});
			return defaultValue;
		}
	}
}
