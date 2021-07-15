package com.example.storageformat.orc;

import com.example.storageformat.parquet.Fields;
import org.apache.orc.TypeDescription;

import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName Schema
 * @Description TODO
 * @Author rey
 * @Date 2019/8/16 下午3:27
 */
public class Schema {

    private Map<String, Field> fieldType;

    protected void add(String name, TypeDescription.Category type){
        fieldType.put(name, new Field(name, type));
    }

    public Schema(){
        fieldType = new HashMap<String, Field>();
        //添加可能需要的列
        add(Fields.NAME, TypeDescription.Category.STRING);
        add(Fields.AGE, TypeDescription.Category.INT);
        add(Fields.SEX, TypeDescription.Category.STRING);
        add(Fields.PHONE, TypeDescription.Category.LIST);
    }

    public String getSchema(){
        String[] fieldNames = fieldType.keySet().toArray(new String[fieldType.size()]);
        return getSchema(fieldNames);
    }

    public String getSchema(String[] fieldNames){
        StringBuilder schema = new StringBuilder();
        for(String field : fieldNames){
            if(fieldType.containsKey(field)){
                schema.append(fieldType.get(field).toString());
            }
        }

        StringBuilder schemaend = new StringBuilder("struct<");
        schemaend.append(schema.substring(0, schema.lastIndexOf(","))).append(">");
        return schema.toString();
    }

    class Field {
        String name;
        TypeDescription.Category type;

        public Field(String name, TypeDescription.Category type){
            this.name = name;
            this.type = type;
        }

        public String toString(){
            return String.format("%s:%s,", name, type);
        }
    }
}