package com.example.storageformat.parquet;


import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author:
 * @Description: 将需要的所有字段写到map中，然后需要什么字段合成的schema直接传入相应字段即可的到对应的schema声明
 * 在Parquet中需要定义schema来描述存储结构，可以分为量两种(主要是为了表示各种嵌套)
 * group:      复杂类型，可以包含其它子字段，在树结构中为中间节点，并不存储数据，为虚拟节点
 * primitive: 基本类型，如int32、string等，在树结构中为叶子节点，只有这种类型才存储具体数据

 * 为了表示复杂的数据结构， Parquet定义了如下属性来描述字段。
 * required 表示该域必须出现1次
 * repeated 表示该域可以出现0次或多次
 * optional 表示该域可以出现0次或1次

 * @Date: Created in 10:36 AM 8/23/18
 * @Modified by:
 */
public class Schema {
    private static final Logger LOGGER = LoggerFactory.getLogger(Schema.class);
    private Map<String, Field> fieldType;

    protected void add(String name,  PrimitiveType.PrimitiveTypeName type, Type.Repetition repetitionType){
        fieldType.put(name, new Field(name, type, repetitionType));
    }

    public Schema(){
        fieldType = new HashMap<String, Field>();
        //添加可能需要的列
        add(Fields.NAME, PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED);
        add(Fields.AGE, PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL);
        add(Fields.SEX, PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL);
        add(Fields.PHONE, PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REPEATED);
    }

    public String getSchema(){
        String[] fieldNames = fieldType.keySet().toArray(new String[fieldType.size()]);
        return getSchema(fieldNames);
    }

    public String getSchema(String[] fieldNames){
        StringBuilder schema = new StringBuilder("message Msg { \n");
        for(String field : fieldNames){
            if(fieldType.containsKey(field)){
                schema.append(fieldType.get(field).toString());
            }else{
                LOGGER.warn(" unknow field : " + field);
            }
        }
        schema.append("}");
        return schema.toString();
    }
    class Field {
        String name;
        PrimitiveType.PrimitiveTypeName type;
        Type.Repetition repetitionType;

        public Field(String name, PrimitiveType.PrimitiveTypeName type, Type.Repetition repetitionType){
            this.name = name;
            this.type = type;
            this.repetitionType = repetitionType;
        }

        public String toString(){
            return String.format("\t %s %s %s;\n", repetitionType, type, name);
        }
    }

    public static void main(String[] args){
        System.out.println(new Schema().getSchema());
    }
}
