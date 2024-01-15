
package edu.harvard.iq.dataverse.arp;

import edu.harvard.iq.dataverse.ControlledVocabularyValue;
import edu.harvard.iq.dataverse.DatasetField;
import edu.harvard.iq.dataverse.DatasetFieldServiceBean;

import jakarta.enterprise.inject.spi.CDI;
import jakarta.faces.component.UIComponent;
import jakarta.faces.context.FacesContext;
import jakarta.faces.convert.Converter;
import jakarta.faces.convert.FacesConverter;
import java.util.Objects;

@FacesConverter("externalVocabularyValueConverter")
public class ExternalVocabularyValueConverter implements Converter {

    
    DatasetFieldServiceBean datasetFieldSvc = CDI.current().select(DatasetFieldServiceBean.class).get();

    public Object getAsObject(FacesContext facesContext, UIComponent component, String submittedValue) {
        if (submittedValue == null || submittedValue.equals("")) {
            return "";
        } else {
            var requestMap = facesContext.getExternalContext().getRequestMap();
            var dsf = requestMap.containsKey("subdsf") ? (DatasetField) requestMap.get("subdsf") : (DatasetField) requestMap.get("dsf");
            ControlledVocabularyValue cvv = datasetFieldSvc.findControlledVocabularyValueByDatasetFieldTypeAndStrValue(dsf.getDatasetFieldType(), submittedValue, false);
            return Objects.requireNonNullElseGet(cvv, () -> dsf.getDatasetFieldType().getExternalVocabularyValues().stream().filter(evv -> evv.getStrValue().equals(submittedValue)).findFirst().get());
        }
    }

    public String getAsString(FacesContext facesContext, UIComponent component, Object value) {
        if (value == null || value.equals("")) {
            return "";
        } else {
            return ((ControlledVocabularyValue) value).getStrValue();
        }
    }
}
