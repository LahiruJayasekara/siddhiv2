// Copyright (c) 2018 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
//
// WSO2 Inc. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerina/runtime;
import ballerina/io;
import streams;
import ballerina/reflect;

type InputRecord record {
    string id;
    string category;
    int intVal;
    float floatVal;
};

type OutputRecord record {
    string id;
    string category;
    int iSum;
    float fSum;
    int count;
    float iAvg;
    float fAvg;
};

stream<InputRecord> inputStream;
stream<OutputRecord> outputStream;
int index = 0;
OutputRecord[] outputDataArray = [];

function main(string... args) {

    InputRecord[] records = [];
    records[0] = { id: "ANX_1", category: "ANX", intVal: 2, floatVal: 2.5 };
    records[1] = { id: "BMX_1", category: "BMX", intVal: 1, floatVal: 1.5 };
    records[2] = { id: "ANX_2", category: "ANX", intVal: 4, floatVal: 4.5 };
    records[3] = { id: "BMX_2", category: "BMX", intVal: 3, floatVal: 3.5 };

    streamFunc();

    outputStream.subscribe(printInputRecords);
    foreach r in records {
        inputStream.publish(r);
    }
    runtime:sleep(1000);
    io:println("outputDataArray: ", outputDataArray);
}

function streamFunc() {
    
    function (OutputRecord[]) outputFunc = (OutputRecord[] o) => {
        io:println(o);
        outputStream.publish(o);
    };

    // register output function
    streams:OutputProcess outputProcess = streams:createOutputProcess(outputFunc);

    // create aggregators
    streams:Sum sumAggregator = new();
    streams:Count countAggregator = new();
    streams:Average avgAggregator = new();

    streams:Aggregator[] aggregators = [];
    aggregators[0] = sumAggregator;
    aggregators[1] = countAggregator;
    aggregators[2] = avgAggregator;

    // create selector
    streams:Select select = streams:createSelect(
        outputProcess.process,
        aggregators,
        (streams:StreamEvent e) => string {
            InputRecord i = check <InputRecord>e.eventObject;
            return i.category;
        },
        (streams:StreamEvent e, streams:Aggregator[] aggregatorArray) => any {
            InputRecord i = check <InputRecord> e.eventObject;
            streams:Sum sumAggregator1 = check <streams:Sum>aggregatorArray[0];
            streams:Count countAggregator1 = check <streams:Count>aggregatorArray[1];
            streams:Average avgAggregator1 = check <streams:Average>aggregatorArray[2];
            OutputRecord o = {
                id: i.id,
                category: i.category,
                iSum: check <int>sumAggregator1.process(i.intVal, e.eventType),
                fSum: check <float>sumAggregator1.process(i.floatVal, e.eventType),
                count: check <int>countAggregator1.process((), e.eventType),
                iAvg: check <float>countAggregator1.process(i.intVal, e.eventType),
                fAvg: check <float>countAggregator1.process(i.floatVal, e.eventType)
            };
            return o;
        }
    );

    streams:Filter filter = streams:createFilter(
        select.process,
        (any o) => boolean {
            InputRecord i = check <InputRecord>o;
            io:println("Filter: ", i);
            return i.intVal > getValue();
        }
    );

    inputStream.subscribe((InputRecord i) => {
            streams:StreamEvent[] eventArr = streams:buildStreamEvent(i);
            io:println("eventArr: ", eventArr);
            filter.process(eventArr);
        }
    );
}

function getValue() returns int {
    return 0;
}

function printInputRecords(OutputRecord e) {
    addToOutputDataArray(e);
}

function addToOutputDataArray(OutputRecord e) {
    outputDataArray[index] = e;
    index = index + 1;
}