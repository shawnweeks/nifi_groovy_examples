/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import groovy.json.JsonSlurper
import org.apache.nifi.controller.AbstractControllerService
import org.apache.nifi.controller.ConfigurationContext
import org.apache.nifi.logging.ComponentLog
import org.apache.nifi.schema.access.SchemaNotFoundException
import org.apache.nifi.serialization.MalformedRecordException
import org.apache.nifi.serialization.RecordReader
import org.apache.nifi.serialization.RecordReaderFactory
import org.apache.nifi.serialization.SimpleRecordSchema
import org.apache.nifi.serialization.record.MapRecord
import org.apache.nifi.serialization.record.Record
import org.apache.nifi.serialization.record.RecordField
import org.apache.nifi.serialization.record.RecordFieldType
import org.apache.nifi.serialization.record.RecordSchema


class GroovyXmlRecordReader implements RecordReader {

    def hasNext
    def CDF
    def uuid
    def attributeSchema = new SimpleRecordSchema(
        [
            new RecordField('entry_num',RecordFieldType.INT.dataType),
            new RecordField('entry_type',RecordFieldType.STRING.dataType),
            new RecordField('entry_value',RecordFieldType.STRING.dataType),
        ]
    )
    def recordSchema = new SimpleRecordSchema(
            [
                new RecordField('id', RecordFieldType.STRING.dataType),
                new RecordField('name', RecordFieldType.STRING.dataType),
                new RecordField('file_format', RecordFieldType.STRING.dataType),
                new RecordField('majority', RecordFieldType.STRING.dataType),
                new RecordField('neg_to_pos_fp0', RecordFieldType.STRING.dataType),
                new RecordField('checksum', RecordFieldType.STRING.dataType),
                new RecordField(
                    'global_attributes',
                    RecordFieldType.MAP.getMapDataType(
                        RecordFieldType.ARRAY.getArrayDataType(
                            RecordFieldType.RECORD.getRecordDataType(attributeSchema))))
                ]
    )

    GroovyXmlRecordReader(final String uuid, final InputStream inputStream) {
        this.hasNext = true
        this.uuid = uuid
        this.CDF = new XmlSlurper().parse(inputStream)
        
    }

    Record nextRecord(boolean coerceTypes, boolean dropUnknown) throws IOException, MalformedRecordException {
        if(!hasNext){
            return null
        } else {
            hasNext = false
            def record
            def globalAttributes = [:]
            CDF.cdfGAttributes.attribute.each{ attribute ->
                def entries = []
                attribute.entry.each{ entry ->
                    entries << new MapRecord(attributeSchema,['entry_num':entry.@entryNum as String, 'entry_type':entry.@cdfDatatype as String, 'entry_value':entry as String])
                }
                globalAttributes[attribute.@name as String] = entries as Object []
            }
            record = new MapRecord(
                recordSchema,
                [
                    'id':uuid,
                    'name':CDF.@name,
                    'file_format':CDF.cdfFileInfo.@fileFormat,
                    'majority':CDF.cdfFileInfo.@majority,
                    'encoding':CDF.cdfFileInfo.@encoding,
                    'neg_to_pos_fp0':CDF.cdfFileInfo.@negToPosFp0,
                    'checksum':CDF.cdfFileInfo.@checksum,
                    'global_attributes':globalAttributes
                ]
                )
            return record
        }
    }

    RecordSchema getSchema() throws MalformedRecordException {
        return recordSchema
    }

    void close() throws IOException {
    }
}

class GroovyXmlRecordReaderFactory extends AbstractControllerService implements RecordReaderFactory {

    // Will be set by the ScriptedRecordReaderFactory
    ConfigurationContext configurationContext

    RecordReader createRecordReader(Map<String, String> variables, InputStream inputStream, ComponentLog logger) throws MalformedRecordException, IOException, SchemaNotFoundException {
        return new GroovyXmlRecordReader(variables.'uuid',inputStream)
    }

}

// Create an instance of RecordReaderFactory called "writer", this is the entry point for ScriptedReader
reader = new GroovyXmlRecordReaderFactory()