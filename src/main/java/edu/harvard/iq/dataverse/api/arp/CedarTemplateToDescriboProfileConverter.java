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


    public CedarTemplateToDescriboProfileConverter(String language) {
        if (language == null) {
            this.language = "en";
        }
        else {
            this.language = language;
        }
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
                classJson.addProperty("label", classLoc.label);
                classJson.addProperty("help", classLoc.help);
            }
        }
        
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
            String inputId = getJsonElement(cedarTemplate, "properties.@context.properties")
                    .getAsJsonObject()
                    .getAsJsonObject(propertyName)
                    .getAsJsonArray("enum").get(0).getAsString();

            if (propertyType != null) {
                String actPropertyType = propertyType.substring(propertyType.lastIndexOf("/") + 1);
                boolean isHidden = Optional.ofNullable(property.getAsJsonObject("_ui").get("hidden")).map(JsonElement::getAsBoolean).orElse(false);
                if (!isHidden && (actPropertyType.equals("TemplateField") || actPropertyType.equals("StaticTemplateField"))) {
                    JsonObject valueConstraints = property.getAsJsonObject("_valueConstraints");
                    boolean allowMultiple = valueConstraints.has("multipleChoice") && valueConstraints.get("multipleChoice").getAsBoolean();
                    processTemplateField(property, allowMultiple, inputId, processedDescriboProfileValues, parentName);
                } else if (actPropertyType.equals("TemplateElement")) {
                    processTemplateElement(property, processedDescriboProfileValues, false, inputId, parentName);
                }
            } else {
                String actPropertyType = property.get("type").getAsString();
                if (actPropertyType.equals("array")) {
                    processArray(property, processedDescriboProfileValues, inputId, parentName);
                }
            }
        }

        return processedDescriboProfileValues;
    }

    public void processTemplateField(JsonObject templateField, boolean allowMultiple, String inputId, ProcessedDescriboProfileValues processedDescriboProfileValues, String parentName) {
        DescriboInput describoInput = new DescriboInput();
        String fieldType = Optional.ofNullable(getJsonElement(templateField, "_ui.inputType")).map(JsonElement::getAsString).orElse(null);
        JsonObject externalVocab = JsonHelper.getJsonObject(templateField, "_valueConstraints.branches[0]");
        if (externalVocab != null) {
            fieldType = "list";
        }

        describoInput.setId(inputId);
        describoInput.setName(Optional.ofNullable(templateField.get("schema:name")).map(JsonElement::getAsString).orElse(null));
        String label = getLocalizedLabel(templateField);
        describoInput.setLabel(label);
        String help = getLocalizedHelp(templateField);
        describoInput.setHelp(help);
        describoInput.setType(getDescriboType(fieldType));
        describoInput.setRequired(Optional.ofNullable(getJsonElement(templateField, "_valueConstraints.requiredValue")).map(JsonElement::getAsBoolean).orElse(false));
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

        processedDescriboProfileValues.inputs.add(Pair.of(parentName, describoInput));
    }

    private void processTemplateElement(JsonObject templateElement, ProcessedDescriboProfileValues processedDescriboProfileValues, boolean allowMultiple, String inputId, String parentName) {
        DescriboInput describoInput = new DescriboInput();

        String propName = templateElement.get("schema:name").getAsString();
        
        describoInput.setId(inputId);
        describoInput.setName(propName);
        String label = getLocalizedLabel(templateElement); //Optional.ofNullable(templateElement.get("skos:prefLabel")).map(JsonElement::getAsString).orElse(propName);
        describoInput.setLabel(label);
        String help = getLocalizedHelp(templateElement); // Optional.ofNullable(templateElement.get("schema:description")).map(JsonElement::getAsString).orElse(null);
        describoInput.setHelp(help);
        describoInput.setType(List.of(propName));
        describoInput.setRequired(Optional.ofNullable(getJsonElement(templateElement, "_valueConstraints.requiredValue")).map(JsonElement::getAsBoolean).orElse(false));
        boolean allowsMultiple = allowMultiple || templateElement.keySet().contains("minItems") || templateElement.keySet().contains("maxItems");
        describoInput.setMultiple(allowsMultiple);

        processedDescriboProfileValues.classLocalizations.put(propName, new ClassLocalization(label, help));
        processedDescriboProfileValues.inputs.add(Pair.of(parentName, describoInput));
        
        processTemplate(templateElement, processedDescriboProfileValues, propName);
    }

    public void processArray(JsonObject array, ProcessedDescriboProfileValues processedDescriboProfileValues, String inputId, String parentName) {
        JsonObject items = array.getAsJsonObject("items");
        String inputType = Optional.ofNullable(getJsonElement(items, "_ui.inputType")).map(JsonElement::getAsString).orElse(null);
        if (inputType != null) {
            processTemplateField(items, true, inputId, processedDescriboProfileValues, parentName);
        } else {
            processTemplateElement(items, processedDescriboProfileValues, true, inputId, parentName);
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
        describoProfile.getAsJsonObject("metadata").addProperty("description", cedarTemplate.get("schema:description").getAsString());
        
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
            "attribute-value", List.of("Text"),
            "email", List.of("Text")
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
        if (language.equals("hu")) {
            label = Optional.ofNullable(obj.get("hunLabel")).map(JsonElement::getAsString).orElse(obj.get("schema:name").getAsString());
        }
        else {
            label = Optional.ofNullable(obj.get("skos:prefLabel")).map(JsonElement::getAsString).orElse(obj.get("schema:name").getAsString());
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

        public DescriboInput() {
        }

        public DescriboInput(String id, String name, String label, String help, List<String> type, boolean required, boolean multiple) {
            this.id = id;
            this.name = name;
            this.label = label;
            this.help = help;
            this.type = type;
            this.required = required;
            this.multiple = multiple;
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
