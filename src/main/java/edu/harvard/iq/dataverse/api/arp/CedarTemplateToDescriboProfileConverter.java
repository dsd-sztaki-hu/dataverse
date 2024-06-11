package edu.harvard.iq.dataverse.api.arp;

import com.google.gson.*;
import edu.harvard.iq.dataverse.api.arp.util.JsonHelper;
import edu.harvard.iq.dataverse.arp.ArpServiceBean;
import org.apache.commons.lang3.tuple.Pair;

import java.io.*;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static edu.harvard.iq.dataverse.api.arp.util.JsonHelper.*;
import static edu.harvard.iq.dataverse.api.arp.util.JsonHelper.getJsonElement;

public class CedarTemplateToDescriboProfileConverter {
    private static final Logger logger = Logger.getLogger(ArpApi.class.getCanonicalName());

    private String language;
    private ArpServiceBean arpService;


    public CedarTemplateToDescriboProfileConverter(String language, ArpServiceBean arpService) {
        if (language == null) {
            this.language = "en";
        }
        else {
            this.language = language;
        }
        this.arpService = arpService;
    }

    // TODO: Pass override/inherit values for the classes, maybe store the profile in a seperated file
    public String processCedarTemplate(String cedarTemplate) throws IOException {
        String describoProfileTemplate = "{\n  \"metadata\": {\n    \"name\": \"Cedar to Describo generated profile\",\n    \"version\": 1.0,\n    \"description\": \"Generated Describo schema from a Cedar template\",\n    \"warnMissingProperty\": true\n  },\n  \"classes\": {\n    \"Dataset\": {\n      \"definition\": \"override\",\n      \"subClassOf\": [],\n      \"inputs\":[] \n    }\n  },\n  \"enabledClasses\": [\n    \"Dataset\"\n  ]\n}";
        String classTemplate = "{\n  \"definition\": \"override\",\n  \"subClassOf\": [],\n  \"inputs\": []\n}";
        
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        JsonObject describoProfile = gson.fromJson(describoProfileTemplate, JsonObject.class);
        JsonObject cedarTemplateJson = gson.fromJson(cedarTemplate, JsonObject.class);

        processProfileMetadata(cedarTemplateJson, describoProfile);
        ProcessedDescriboProfileValues profValues = processTemplate(cedarTemplateJson, new ProcessedDescriboProfileValues(new ArrayList<>()), "Dataset");

        // Add default values for bultin types
        if (language.equals("hu")) {
            profValues.classLocalizations.put("Dataset", new ClassLocalization("Adatcsomag", "Fájlok és metaadataik adatcsomagja"));
            profValues.classLocalizations.put("File", new ClassLocalization("Fájl", "Adatfájl"));
            profValues.classLocalizations.put("Text", new ClassLocalization("Szöveg", "Szöveg"));
            profValues.classLocalizations.put("Number", new ClassLocalization("Szám", "Szám"));
            profValues.classLocalizations.put("Select", new ClassLocalization("Kiválasztás", "Kiválasztás"));
            profValues.classLocalizations.put("TextArea", new ClassLocalization("Hosszú szöveg", "Hosszú szöveg"));
            profValues.classLocalizations.put("Date", new ClassLocalization("Dátum", "Dátum"));
        }
        else  {
            profValues.classLocalizations.put("Dataset", new ClassLocalization("Dataset", "A collection of files with metadata"));
            profValues.classLocalizations.put("File", new ClassLocalization("File", "Data file"));
        }

        JsonObject classes = describoProfile.getAsJsonObject("classes");

        for (var input : profValues.inputs) {
            String className = input.getKey();
            if (!classes.has(className)) {
                classes.add(className, gson.fromJson(classTemplate, JsonObject.class));
            }
            var classJson = classes.getAsJsonObject(className);
            classJson.getAsJsonArray("inputs").add(gson.toJsonTree(input.getValue()));

            var classLoc = profValues.classLocalizations.get(className);
            if (classLoc != null) {
                // This is our custom localisation at class level
                classJson.addProperty("label", classLoc.label);
                classJson.addProperty("help", classLoc.help);
            }
        }

        // This is the Describo supported way of localisation
        JsonObject localisation = new JsonObject();
        describoProfile.add("localisation", localisation);
        profValues.classLocalizations.entrySet().forEach(e -> localisation.addProperty(e.getKey(), e.getValue().label));

        JsonArray enabledClasses = new JsonArray();
        profValues.inputs.stream().map(Pair::getKey).collect(Collectors.toSet()).forEach(enabledClasses::add);
        
        describoProfile.add("enabledClasses", enabledClasses);

        return gson.toJson(describoProfile);
    }

    public ProcessedDescriboProfileValues processTemplate(JsonObject cedarTemplate, ProcessedDescriboProfileValues processedDescriboProfileValues, String parentName) {
        for (String propertyName : getStringList(cedarTemplate, "_ui.order")) {
            // Note: cannot use getJsonObject here because the propertyName may contain ".", eg:
            // coverage.Spectral.CentralWavelength, which would result in properties.coverage.Spectral.CentralWavelength
            // JsonObject property = getJsonObject(cedarTemplate, "properties." + propertyName);
            JsonObject property = cedarTemplate.getAsJsonObject("properties").getAsJsonObject(propertyName);
            String propertyType = Optional.ofNullable(property.get("@type")).map(JsonElement::getAsString).orElse(null);
            //String inputId = getStringList(cedarTemplate, "properties.@context.properties." + propertyName + ".enum").get(0);
            
            // richtext type can not be used in Describo, leave it out from the profile
            if (Optional.ofNullable(getJsonElement(property, "_ui.inputType")).map(JsonElement::getAsString).orElse("").equals("richtext")) {
                continue;
            }
            String inputId = getJsonElement(cedarTemplate, "properties.@context.properties")
                    .getAsJsonObject()
                    .getAsJsonObject(propertyName)
                    .getAsJsonArray("enum").get(0).getAsString();

            if (propertyType != null) {
                String actPropertyType = propertyType.substring(propertyType.lastIndexOf("/") + 1);
                boolean isHidden = Optional.ofNullable(property.getAsJsonObject("_ui").get("hidden")).map(JsonElement::getAsBoolean).orElse(false);
                if (!isHidden && (actPropertyType.equals("TemplateField") || actPropertyType.equals("StaticTemplateField"))) {
                    JsonObject valueConstraints = property.getAsJsonObject("_valueConstraints");
                    boolean allowMultiple =
                            (valueConstraints.has("multipleChoice") && valueConstraints.get("multipleChoice").getAsBoolean())
                            || (property.has("minItems") || property.has("maxItems"));
                    processTemplateField(property, allowMultiple, inputId, processedDescriboProfileValues, parentName);
                } else if (actPropertyType.equals("TemplateElement")) {
                    processTemplateElement(property, processedDescriboProfileValues, false, inputId, parentName, getJsonObject(cedarTemplate, "_ui.propertyLabels"));
                }
            } else {
                String actPropertyType = property.get("type").getAsString();
                if (actPropertyType.equals("array")) {
                    processArray(property, processedDescriboProfileValues, inputId, parentName, getJsonObject(cedarTemplate, "_ui.propertyLabels"));
                }
            }
        }

        return processedDescriboProfileValues;
    }

    public void processTemplateField(JsonObject templateField, boolean allowMultiple, String inputId, ProcessedDescriboProfileValues processedDescriboProfileValues, String parentName) {
        DescriboInput describoInput = new DescriboInput();
        String fieldType = Optional.ofNullable(getJsonElement(templateField, "_ui.inputType")).map(JsonElement::getAsString).orElse(null);

        if (JsonHelper.hasJsonElement(templateField, "_valueConstraints.branches")
                && JsonHelper.getJsonArray(templateField, "_valueConstraints.branches").isEmpty()) {
            logger.warning("Invalid OntoPortal based values defined in field. Expecting terms in _valueConstraints.branches[0]:  "+templateField.toString());
        }

        String path = "_valueConstraints.branches[0]";
        JsonObject externalVocab = JsonHelper.hasJsonElement(templateField, path)
                ? JsonHelper.getJsonObject(templateField, "_valueConstraints.branches[0]")
                : null;
        if (externalVocab != null) {
            fieldType = "list";
        }

        describoInput.setId(inputId);
        // Replace the ":" with "." upon generating the Describo Profile from the CEDAR Template
        describoInput.setName(Objects.requireNonNull(Optional.ofNullable(templateField.get("schema:name")).map(JsonElement::getAsString).orElse(null)).replace(":","."));
        String label = getLocalizedLabel(templateField);
        describoInput.setLabel(label);
        String help = getLocalizedHelp(templateField);
        describoInput.setHelp(help);
        describoInput.setType(getDescriboType(fieldType));
        describoInput.setRequired(Optional.ofNullable(getJsonElement(templateField, "_valueConstraints.requiredValue")).map(JsonElement::getAsBoolean).orElse(false));
        describoInput.setMinValue(Optional.ofNullable(getJsonElement(templateField, "_valueConstraints.minValue")).map(JsonElement::getAsInt).orElse(null));
        describoInput.setMaxValue(Optional.ofNullable(getJsonElement(templateField, "_valueConstraints.maxValue")).map(JsonElement::getAsInt).orElse(null));
        describoInput.setMinLength(Optional.ofNullable(getJsonElement(templateField, "_valueConstraints.minLength")).map(JsonElement::getAsInt).orElse(null));
        describoInput.setMaxLength(Optional.ofNullable(getJsonElement(templateField, "_valueConstraints.maxLength")).map(JsonElement::getAsInt).orElse(null));
        describoInput.setRegex(Optional.ofNullable(getJsonElement(templateField, "_valueConstraints.regex")).map(JsonElement::getAsString).orElse(null));
        describoInput.setPlaceholder(Optional.ofNullable(getJsonElement(templateField, "_arp.dataverse.watermark")).map(JsonElement::getAsString).orElse(null));
        describoInput.setMultiple(allowMultiple);

        List<String> literalValues;
        if (fieldType != null && (fieldType.equals("list") || fieldType.equals("radio"))) {
            if (externalVocab != null) {
                literalValues = arpService.getExternalVocabValues(templateField);
                if (!literalValues.isEmpty()) {
                    describoInput.setValues(literalValues);
                }
            }
            else {
                JsonElement jsonElement = getJsonElement(templateField, "_valueConstraints.literals");
                JsonArray literals = jsonElement == null ? new JsonArray() : jsonElement.getAsJsonArray();
                literalValues = new ArrayList<>();
                literals.forEach(literal -> literalValues.add(literal.getAsJsonObject().get("label").getAsString()));
            }
            if (!literalValues.isEmpty()) {
                describoInput.setValues(literalValues);
            }
        }
        
        if (Objects.equals(fieldType, "numeric")) {
            // minValue and maxValue always handled as an int, even for long, double and float numbers
            describoInput.setMinValue(Optional.ofNullable(getJsonElement(templateField, "_valueConstraints.minValue")).map(JsonElement::getAsInt).orElse(null));
            describoInput.setMaxValue(Optional.ofNullable(getJsonElement(templateField, "_valueConstraints.maxValue")).map(JsonElement::getAsInt).orElse(null));
            Optional.ofNullable(getJsonElement(templateField, "_valueConstraints.numberType"))
                    .map(JsonElement::getAsString)
                    .ifPresent(cedarNumberType -> describoInput.setNumberType(cedarDescriboNumberTypes.get(cedarNumberType)));
        }

        if (Objects.equals(fieldType, "temporal")) {
            Optional.ofNullable(getJsonElement(templateField, "_valueConstraints.temporalType"))
                    .map(JsonElement::getAsString)
                    .ifPresent(cedarDateType -> describoInput.setType(cedarDescriboDateTypes.get(cedarDateType)));
        }
        
        // hard-coded regexes
        if (Objects.equals(fieldType, "email")) {
            describoInput.setRegex("^((?!\\.)[\\w\\-_.]*[^.])(@\\w+)(\\.\\w+(\\.\\w+)?[^.\\W])$");
        }

        if (Objects.equals(fieldType, "phone-number")) {
            describoInput.setRegex("(?:([+]\\d{1,4})[-.\\s]?)?(?:[(](\\d{1,3})[)][-.\\s]?)?(\\d{1,4})[-.\\s]?(\\d{1,4})[-.\\s]?(\\d{1,9})");
        }
        

        processedDescriboProfileValues.inputs.add(Pair.of(parentName, describoInput));
    }

    // "dataverseFile" is a special Template Element in CEDAR that is used to represent file relations
    // this property needs to be handled differently
    private void processTemplateElement(JsonObject templateElement, ProcessedDescriboProfileValues processedDescriboProfileValues, boolean allowMultiple, String inputId, String parentName, JsonObject propertyLabels) {
        DescriboInput describoInput = new DescriboInput();
        
        String elementName = templateElement.get("schema:name").getAsString();
        String elementNameReplaced = templateElement.get("schema:name").getAsString().replace(":", ".");

        // Replace the ":" with "." upon generating the Describo Profile from the CEDAR Template 
        String propName = propertyLabels != null ? propertyLabels.get(elementName).getAsString().replace(":", ".") : elementNameReplaced;
        String type = templateElement.has("schema:identifier") ? templateElement.get("schema:identifier").getAsString().replace(":", ".") : elementNameReplaced;
        String actualType = type.equals("dataverseFile") ? "File" : type.equals("dataverseDataset") ? "Dataset" : propName;
        
        describoInput.setId(inputId);
        describoInput.setName(propName);
        String label = getLocalizedLabel(templateElement); //Optional.ofNullable(templateElement.get("skos:prefLabel")).map(JsonElement::getAsString).orElse(propName);
        describoInput.setLabel(label);
        String help = getLocalizedHelp(templateElement); // Optional.ofNullable(templateElement.get("schema:description")).map(JsonElement::getAsString).orElse(null);
        describoInput.setHelp(help);
        describoInput.setType(List.of(actualType));
        describoInput.setRequired(Optional.ofNullable(getJsonElement(templateElement, "_valueConstraints.requiredValue")).map(JsonElement::getAsBoolean).orElse(false));
        boolean allowsMultiple = allowMultiple || templateElement.keySet().contains("minItems") || templateElement.keySet().contains("maxItems");
        describoInput.setMultiple(allowsMultiple);

        processedDescriboProfileValues.classLocalizations.put(propName, new ClassLocalization(label, help));
        processedDescriboProfileValues.inputs.add(Pair.of(parentName, describoInput));
        
        processTemplate(templateElement, processedDescriboProfileValues, propName);
    }

    public void processArray(JsonObject array, ProcessedDescriboProfileValues processedDescriboProfileValues, String inputId, String parentName, JsonObject propertyLabels) {
        JsonObject items = array.getAsJsonObject("items");
        String inputType = Optional.ofNullable(getJsonElement(items, "_ui.inputType")).map(JsonElement::getAsString).orElse(null);
        if (inputType != null) {
            processTemplateField(items, true, inputId, processedDescriboProfileValues, parentName);
        } else {
            processTemplateElement(items, processedDescriboProfileValues, true, inputId, parentName, propertyLabels);
        }
    }
    
    private void processProfileMetadata(JsonObject cedarTemplate, JsonObject describoProfile) {
        String name = cedarTemplate.get("schema:name").getAsString();
        if (language.equals("hu")) {
            name = cedarTemplate.has("hunName")
                    ? cedarTemplate.get("hunName").getAsString()
                    : name;
        }
        describoProfile.getAsJsonObject("metadata").addProperty("name", name);
        // TODO: MDB-s have no description. We have the actual cedar template in metadatablockarp, so we could use it.

        if (language.equals("hu")) {
            name = cedarTemplate.has("hunName")
                    ? cedarTemplate.get("hunName").getAsString()
                    : name;
        }

        String desc =  cedarTemplate.get("schema:description").getAsString();
        if (language.equals("hu")) {
            desc = cedarTemplate.has("hunDescription")
                    ? cedarTemplate.get("hunDescription").getAsString()
                    : desc;

        }
        describoProfile.getAsJsonObject("metadata").addProperty("description", desc);
        
    }

    Map<String, List<String>> cedarDescriboFieldTypes = Map.of(
            "textfield", List.of("Text"),
            "temporal", List.of("Date"),
            "numeric", List.of("Number"),
            "richtext", List.of("TextArea"),
            "textarea", List.of("TextArea"),
            "link", List.of("URL"),
            "list", List.of("Select"),
            "radio", List.of("Select"),
            "phone-number", List.of("Text"),
            "email", List.of("Text")
    );

    Map<String, List<String>> cedarDescriboNumberTypes = Map.of(
            "xsd:decimal", List.of("Any"),
            "xsd:long", List.of("Long"),
            "xsd:int", List.of("Int"),
            "xsd:double", List.of("Double"),
            "xsd:float", List.of("Float")
    );

    Map<String, List<String>> cedarDescriboDateTypes = Map.of(
            "xsd:dateTime", List.of("DateTime"),
            "xsd:date", List.of("Date"),
            "xsd:time", List.of("Time")
    );

    public List<String> getDescriboType(JsonObject templateField) {

        List<String> dataverseFieldType = null;
        String fieldType = Optional.ofNullable(getJsonElement(templateField, "_ui.inputType")).map(JsonElement::getAsString).orElse(null);

        if (fieldType != null && cedarDescriboFieldTypes.containsKey(fieldType)) {
            dataverseFieldType = cedarDescriboFieldTypes.get(fieldType);
        }

        return dataverseFieldType;
    }

    public List<String> getDescriboType(String cedarFieldType)
    {
        List<String> describoType = null;
        if (cedarFieldType == null) {
            return describoType;
        }

        return cedarDescriboFieldTypes.get(cedarFieldType);
    }

    public String getLocalizedLabel(JsonObject obj) {
        String label = "";
        String engLabel = obj.get("schema:name").getAsString();
        // Absolute fallback: the field name
        var prefLabel = obj.get("skos:prefLabel");
        // If we have an english name in skos:prefLabel, use that
        if (prefLabel != null && !prefLabel.getAsString().isEmpty()) {
            engLabel = prefLabel.getAsString();
        }
        // If we have an hunLabel, use that for hunLabel, otherwise fall back to engLabel
        if (language.equals("hu")) {
            var hunLabel = obj.get("hunLabel");
            if (hunLabel == null || hunLabel.getAsString().isEmpty()) {
                label = engLabel +" (magyarul)";
            }
            else {
                label = hunLabel.getAsString();
            }
        }
        else {
            label = engLabel;
        }
        return label;
    }

    public String getLocalizedHelp(JsonObject obj) {
        String help = "";
        if (language.equals("hu")) {
            help = Optional.ofNullable(obj.get("hunDescription")).map(JsonElement::getAsString).orElse(null);
        }
        else {
            help = Optional.ofNullable(obj.get("schema:description")).map(JsonElement::getAsString).orElse(null);
        }
        return help;
    }


    private static class DescriboInput {
        private String id;
//        schema:name
        private String name;
        private String label;
//        schema:description
        private String help;
        private List<String> type;
        private List<String> values;
        private boolean required;
        private boolean multiple;
        private Integer minValue;
        private Integer maxValue;
        private Integer minLength;
        private Integer maxLength;
        private String regex;
        private String placeholder;
        private List<String> numberType;

        public DescriboInput() {
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getLabel() {
            return label;
        }

        public void setLabel(String label) {
            this.label = label;
        }

        public String getHelp() {
            return help;
        }

        public void setHelp(String help) {
            this.help = help;
        }

        public List<String> getType() {
            return type;
        }

        public void setType(List<String> type) {
            this.type = type;
        }

        public boolean isRequired() {
            return required;
        }

        public void setRequired(boolean required) {
            this.required = required;
        }

        public boolean isMultiple() {
            return multiple;
        }

        public void setMultiple(boolean multiple) {
            this.multiple = multiple;
        }

        public List<String> getValues() {
            return values;
        }

        public void setValues(List<String> values) {
            this.values = values;
        }

        public Integer getMinValue()
        {
            return minValue;
        }

        public void setMinValue(Integer minValue)
        {
            this.minValue = minValue;
        }

        public Integer getMaxValue()
        {
            return maxValue;
        }

        public void setMaxValue(Integer maxValue)
        {
            this.maxValue = maxValue;
        }

        public Integer getMinLength()
        {
            return minLength;
        }

        public void setMinLength(Integer minLength)
        {
            this.minLength = minLength;
        }

        public Integer getMaxLength()
        {
            return maxLength;
        }

        public void setMaxLength(Integer maxLength)
        {
            this.maxLength = maxLength;
        }

        public String getRegex()
        {
            return regex;
        }

        public void setRegex(String regex)
        {
            this.regex = regex;
        }

        public String getPlaceholder() {
            return placeholder;
        }

        public void setPlaceholder(String placeholder) {
            this.placeholder = placeholder;
        }

        public List<String> getNumberType() {
            return numberType;
        }

        public void setNumberType(List<String> numberType) {
            this.numberType = numberType;
        }
    }


    private static class ProcessedDescriboProfileValues {
        protected List<Pair<String, DescriboInput>> inputs;

        protected Map<String, ClassLocalization> classLocalizations = new HashMap<>();

        public ProcessedDescriboProfileValues(List<Pair<String, DescriboInput>> inputs) {
            this.inputs = inputs;
        }

        public List<Pair<String, DescriboInput>> getInputs() {
            return inputs;
        }

        public void setInputs(List<Pair<String, DescriboInput>> inputs) {
            this.inputs = inputs;
        }
    }

    private static class ClassLocalization {
        protected String label;
        protected String help;

        public ClassLocalization(String label, String help)
        {
            this.label = label;
            this.help = help;
        }
    }
}
