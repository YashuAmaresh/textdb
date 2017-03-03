package edu.uci.textdb.framework.api;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Schema {
    
    private List<Attribute> attributes;
    private Map<String, Integer> fieldNameVsIndex;

    public Schema(Attribute... attributes) {
        this.attributes = Arrays.asList(attributes);
        populateFieldNameVsIndexMap();
    }

    private void populateFieldNameVsIndexMap() {
        fieldNameVsIndex = new HashMap<String, Integer>();
        for (int count = 0; count < attributes.size(); count++) {
            String fieldName = attributes.get(count).getFieldName();
            fieldNameVsIndex.put(fieldName.toLowerCase(), count);
        }
    }

    public List<Attribute> getAttributes() {
        return attributes;
    }
    
    public List<String> getAttributeNames() {
        return attributes.stream().map(attr -> attr.getFieldName()).collect(Collectors.toList());
    }

    public Integer getIndex(String fieldName) {
        return fieldNameVsIndex.get(fieldName.toLowerCase());
    }
    
    public Attribute getAttribute(String fieldName) {
        Integer attrIndex = getIndex(fieldName);
        if (attrIndex == null) {
            return null;
        }
        return attributes.get(attrIndex);
    }

    public boolean containsField(String fieldName) {
        return fieldNameVsIndex.keySet().contains(fieldName.toLowerCase());
    }

}
