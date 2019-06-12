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
package nifi.Plc4xModbusProcessor.processors.Plc4xModbusProcessor;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.plc4x.java.api.PlcConnection;
import org.apache.plc4x.java.api.messages.PlcReadRequest;
import org.apache.plc4x.java.api.messages.PlcReadResponse;
import org.apache.plc4x.java.base.util.HexUtil;
import org.json.simple.JSONObject;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"plc4x-modbus"})
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("Processor able to read data using Apache PLC4X by modbus tcp/ip..")
@WritesAttributes({@WritesAttribute(
        attribute = "value"
)})
public class MyProcessor extends BasePlc4xProcessor {

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        //establish a connection to .
        PlcConnection connection = getConnection();
        if (!connection.getMetadata().canRead()) {
            throw new ProcessException("Writing not supported by connection");
        }

        //create a session to receive file flow.
        FlowFile flowFile = session.create();
        final AtomicReference<JSONObject> results = new AtomicReference<>();
        session.append(flowFile, out -> {
            try {
                PlcReadRequest.Builder builder = connection.readRequestBuilder();
                getFields().forEach(field -> {
                    String address = getAddress(field);
                    if(address != null) {
                        builder.addItem(field, address);
                    }
                });
                PlcReadRequest readRequest = builder.build();
                PlcReadResponse response = readRequest.execute().get();
                //value type is json.
                JSONObject obj = new JSONObject();

                //bool register and numerical register have different process methods.
                String[] bools={"coil","readdiscreteinputs"};
                for (String fieldName : response.getFieldNames()) {
                    if (!Arrays.asList(bools).contains(fieldName)) {
                        Integer[] value = response.getAllByteArrays(fieldName)
                                .stream()
                                .map(HexUtil::toHex)
                                .map(s -> Integer.parseInt(StringUtils.deleteWhitespace(s.substring(0, 15)), 16))
                                .toArray(Integer[]::new);
                        obj.put(fieldName, Arrays.asList(value));
                    } else {
                        for (int i = 0; i < response.getNumberOfValues(fieldName); i++) {
                            Object value = response.getObject(fieldName, i);
                            obj.put(fieldName, value);
                        }
                    }
                }

                obj.writeJSONString(new OutputStreamWriter(out));
                //get the value after process
                results.set(obj);

            } catch (InterruptedException | ExecutionException e) {
                throw new IOException(e);
            }
        });

        JSONObject result = results.get();
        if(result != null && !result.isEmpty()){
            flowFile = session.putAttribute(flowFile, "match", String.valueOf(results));
        }
        //write reading result to flowfile
        flowFile = session.write(flowFile, out -> {
            out.write(results.get().toJSONString().getBytes());
        });
        //output the flowfile when successfully match to other processor.
        session.transfer(flowFile, SUCCESS);
    }
}
