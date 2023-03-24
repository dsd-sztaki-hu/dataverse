package edu.harvard.iq.dataverse.api.arp;

import com.google.gson.*;
import edu.harvard.iq.dataverse.DatasetPage;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.harvard.iq.dataverse.api.arp.util.JsonHelper.*;
import static edu.harvard.iq.dataverse.api.arp.util.JsonHelper.getJsonElement;

public class CedarTemplateToDescriboProfileConverter {
    private static final Logger logger = Logger.getLogger(ArpApi.class.getCanonicalName());

    public CedarTemplateToDescriboProfileConverter() {
    }

    // TODO: Update profile metadata, pass override/inherit values for the classes, maybe store the profile in a seperated file
    public String processCedarTemplate(String cedarTemplate) throws IOException {
        String describoProfileTemplate = "{\n  \"metadata\": {\n    \"name\": \"Cedar to Describo generated profile\",\n    \"version\": 1.0,\n    \"description\": \"Generated Describo schema from a Cedar template\",\n    \"warnMissingProperty\": true\n  },\n  \"layouts\": {\n    \"Dataset\": \"layouts\"\n  },\n  \"classes\": {\n    \"Dataset\": {\n      \"definition\": \"override\",\n      \"subClassOf\": [],\n      \"inputs\": \"inputs\"\n    }\n  },\n  \"enabledClasses\": [\n    \"Dataset\"\n  ]\n}";

        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        JsonObject describoProfile = gson.fromJson(describoProfileTemplate, JsonObject.class);
        JsonObject cedarTemplateJson = gson.fromJson(cedarTemplate, JsonObject.class);

        processProfileMetadata(cedarTemplateJson, describoProfile);
        ProcessedDescriboProfileValues processedDescriboProfileValues = processTemplate(cedarTemplateJson, new ProcessedDescriboProfileValues(new ArrayList<>(), new ArrayList<>()));
        describoProfile.getAsJsonObject("layouts").add("Dataset", gson.toJsonTree(processedDescriboProfileValues.layouts));
        describoProfile.getAsJsonObject("classes").getAsJsonObject("Dataset").add("inputs", gson.toJsonTree(processedDescriboProfileValues.inputs));

        String resultProfile = gson.toJson(describoProfile);
        System.out.println(resultProfile);
        return resultProfile;
    }

    public ProcessedDescriboProfileValues processTemplate(JsonObject cedarTemplate, ProcessedDescriboProfileValues processedDescriboProfileValues) {
        for (String propertyName : getStringList(cedarTemplate, "_ui.order")) {
            JsonObject property = getJsonObject(cedarTemplate, "properties." + propertyName);
            String propertyType = Optional.ofNullable(property.get("@type")).map(JsonElement::getAsString).orElse(null);
            String inputId = getStringList(cedarTemplate, "properties.@context.properties." + propertyName + ".enum").get(0);

            if (propertyType != null) {
                String actPropertyType = propertyType.substring(propertyType.lastIndexOf("/") + 1);
                boolean isHidden = Optional.ofNullable(property.getAsJsonObject("_ui").get("hidden")).map(JsonElement::getAsBoolean).orElse(false);
                if (!isHidden && (actPropertyType.equals("TemplateField") || actPropertyType.equals("StaticTemplateField"))) {
                    processTemplateField(property, false, inputId, processedDescriboProfileValues);
                } else if (actPropertyType.equals("TemplateElement")) {
                    processTemplateElement(property, processedDescriboProfileValues);
                }
            } else {
                String actPropertyType = property.get("type").getAsString();
                if (actPropertyType.equals("array")) {
                    processArray(property, processedDescriboProfileValues, inputId);
                }
            }
        }

        return processedDescriboProfileValues;
    }

    public void processTemplateField(JsonObject templateField, boolean allowMultiple, String inputId, ProcessedDescriboProfileValues processedDescriboProfileValues) {
        DescriboInput describoInput = new DescriboInput();
        String fieldType = Optional.ofNullable(getJsonElement(templateField, "_ui.inputType")).map(JsonElement::getAsString).orElse(null);

        describoInput.setId(inputId);
        describoInput.setName(Optional.ofNullable(templateField.get("schema:name")).map(JsonElement::getAsString).orElse(null));
        String label = Optional.ofNullable(templateField.get("skos:prefLabel")).map(JsonElement::getAsString).orElse(templateField.get("schema:name").getAsString());
        describoInput.setLabel(label);
        describoInput.setHelp(Optional.ofNullable(templateField.get("schema:description")).map(JsonElement::getAsString).orElse(null));
        describoInput.setType(getDescriboType(templateField));
        describoInput.setRequired(Optional.ofNullable(getJsonElement(templateField, "_valueConstraints.requiredValue")).map(JsonElement::getAsBoolean).orElse(false));
        describoInput.setMultiple(allowMultiple);

        if (fieldType != null && (fieldType.equals("list") || fieldType.equals("radio"))) {
            JsonElement jsonElement = getJsonElement(templateField, "_valueConstraints.literals");
            JsonArray literals = jsonElement == null ? new JsonArray() : jsonElement.getAsJsonArray();
            List<String> literalValues = new ArrayList<>();
            literals.forEach(literal -> literalValues.add(literal.getAsJsonObject().get("label").getAsString()));
            if (!literalValues.isEmpty()) {
                describoInput.setValues(literalValues);
            }
        }

        processedDescriboProfileValues.inputs.add(describoInput);
    }

    private void processTemplateElement(JsonObject templateElement, ProcessedDescriboProfileValues processedDescriboProfileValues) {
        processLayouts(templateElement, processedDescriboProfileValues);
        processTemplate(templateElement, processedDescriboProfileValues);
    }

    public void processArray(JsonObject array, ProcessedDescriboProfileValues processedDescriboProfileValues, String inputId) {
        JsonObject items = array.getAsJsonObject("items");
        String inputType = Optional.ofNullable(getJsonElement(items, "_ui.inputType")).map(JsonElement::getAsString).orElse(null);
        if (inputType != null) {
            processTemplateField(items, true, inputId, processedDescriboProfileValues);
        } else {
            processTemplateElement(items, processedDescriboProfileValues);
        }
    }

    private void processLayouts(JsonObject templateElement, ProcessedDescriboProfileValues processedDescriboProfileValues) {
        DescriboLayout layout = new DescriboLayout();
        layout.setName(Optional.ofNullable(templateElement.get("schema:name")).map(JsonElement::getAsString).orElse(null));
        layout.setDescription(Optional.ofNullable(templateElement.get("schema:description")).map(JsonElement::getAsString).orElse(null));
        layout.setInputs(getStringList(templateElement, "_ui.order"));
        if (!layout.getInputs().isEmpty()) {
            processedDescriboProfileValues.layouts.add(layout);
        }
    }
    
    private void processProfileMetadata(JsonObject cedarTemplate, JsonObject describoProfile) {
        describoProfile.getAsJsonObject("metadata").addProperty("name", cedarTemplate.get("schema:name").getAsString());
        describoProfile.getAsJsonObject("metadata").addProperty("description", cedarTemplate.get("schema:description").getAsString());
        
    }

    public List<String> getDescriboType(JsonObject templateField) {
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

        List<String> dataverseFieldType = null;
        String fieldType = Optional.ofNullable(getJsonElement(templateField, "_ui.inputType")).map(JsonElement::getAsString).orElse(null);

        if (fieldType != null && cedarDescriboFieldTypes.containsKey(fieldType)) {
            dataverseFieldType = cedarDescriboFieldTypes.get(fieldType);
        }

        return dataverseFieldType;
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

    private static class DescriboLayout {
        private String name;
        private String description;
        private List<String> inputs;

        public DescriboLayout() {
        }

        public DescriboLayout(String name, String description, List<String> inputs) {
            this.name = name;
            this.description = description;
            this.inputs = inputs;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        public List<String> getInputs() {
            return inputs;
        }

        public void setInputs(List<String> inputs) {
            this.inputs = inputs;
        }
    }

    private static class ProcessedDescriboProfileValues {
        private List<DescriboInput> inputs;
        private List<DescriboLayout> layouts;

        public ProcessedDescriboProfileValues(List<DescriboInput> inputs, List<DescriboLayout> layouts) {
            this.inputs = inputs;
            this.layouts = layouts;
        }

        public List<DescriboInput> getInputs() {
            return inputs;
        }

        public void setInputs(List<DescriboInput> inputs) {
            this.inputs = inputs;
        }

        public List<DescriboLayout> getLayouts() {
            return layouts;
        }

        public void setLayouts(List<DescriboLayout> layouts) {
            this.layouts = layouts;
        }
    }
}
