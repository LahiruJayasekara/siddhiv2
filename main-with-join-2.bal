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
import ballerina/reflect;
import streams;
import ballerina/math;

type InputRecordA record {
    string id;
    string category;
};

type InputRecordB record {
    string id;
    string symbol;
    int intVal;
};

type OutputRecord record {
    string? id;
    string? category;
    string? symbol;
    string? const;
    int? sum;
};

public int streamLock = 0;

stream<InputRecordA> inputStreamA;
stream<InputRecordB> inputStreamB;
stream<OutputRecord> outputStream;

int index = 0;
OutputRecord[] outputDataArray = [];

public function main(string... args) {
    InputRecordA[] recordsA = [];
    recordsA[0] = { id: "ANX_2", category: "ANX" };
    recordsA[1] = { id: "ANX_1", category: "ANX" };

    InputRecordB[] recordsB = [];
    recordsB[0] = { id: "ANX_2", symbol: "ANX", intVal: 1 };
    recordsB[1] = { id: "ANX_1", symbol: "ANX", intVal: 2 };

    joinFunc();

    outputStream.subscribe(printInputRecords);

    foreach i, r in recordsA {
        inputStreamA.publish(r);
        inputStreamB.publish(recordsB[i]);
    }

    runtime:sleep(1000);

    io:println("output: ", outputDataArray);
}

//  ------------- Query to be implemented -------------------------------------------------------
//  from inputStreamA where inputStreamA.category == "ANX" window length(1)
//  join inputStreamB where inputStreamB.intVal > getValue() window length(1)
//  select
//      inputStreamA.id,
//      inputStreamA.category,
//      inputRecordB.symbol,
//      "some text" as const,
//      sum (inputStreamB.intVal) as sum
//  group by inputStreamA.category
//  => (OutputRecord [] o) {
//      outputStream.publish(o);
//  }
//
function joinFunc() {

    function (map[]) outputFunc = function (map[] events) {
        foreach m in events {
            // just cast input map into the output type
            OutputRecord o = check <OutputRecord>m;
            outputStream.publish(o);
        }
    };

    // Output processor
    streams:OutputProcess outputProcess = streams:createOutputProcess(outputFunc);

    // Prepare Aggregators
    streams:Sum sumAggregator = new();
    streams:Aggregator[] aggregatorArr = [];
    aggregatorArr[0] = sumAggregator;

    // Selector
    streams:Select select = streams:createSelect(outputProcess.process, aggregatorArr,
        [function (streams:StreamEvent e) returns string { // groupBy
            return <string>e.data["inputStreamA.category"];
        }],
        function (streams:StreamEvent e, streams:Aggregator[] aggregatorArr1) returns map { // seclectFunction
            streams:Sum sumAggregator1 = check <streams:Sum>aggregatorArr1[0];
            // got rid of type casting
            return {
                "id": e.data["inputStreamA.id"],
                "category": e.data["inputStreamA.category"],
                "symbol": e.data["inputStreamB.symbol"],
                "const": "some text",
                "sum": sumAggregator1.process(e.data["inputStreamB.intVal"], e.eventType)
            };
        }
    );

    // On condition
    function (map, map) returns boolean conditionFunc =
    function (map lsh, map rhs) returns boolean {
        return lsh["inputStreamA.category"] == rhs["inputStreamB.symbol"];
    };

    // Join processor
    streams:StreamJoinProcessor joinProcessor = streams:createStreamJoinProcessor(select.process, "JOIN", conditionFunc=conditionFunc);

    // Window processors
    streams:Window lengthWindowA = streams:lengthWindow([1], nextProcessPointer = joinProcessor.process);
    streams:Window lengthWindowB = streams:lengthWindow([1], nextProcessPointer = joinProcessor.process);

    // Set the window processors to the join processor
    joinProcessor.setLHS("inputStreamA", lengthWindowA);
    joinProcessor.setRHS("inputStreamB", lengthWindowB);

    // Per stream filters
    streams:Filter filterA = streams:createFilter(lengthWindowA.process, function (map m) returns boolean {
            // simplify filter
            return <string>m["inputStreamA.category"] == "ANX";
        }
    );
    streams:Filter filterB = streams:createFilter(lengthWindowB.process, function (map m) returns boolean {
            // simplify filter
            return check <int>m["inputStreamB.intVal"] > getValue();
        }
    );

    // Subscribe to input streams
    inputStreamA.subscribe(function (InputRecordA i) {
            lock {
                streamLock += 1;
                io:println(i);
                // make it type unaware and proceed
                map keyVal = <map>i;
                streams:StreamEvent[] eventArr = streams:buildStreamEvent(keyVal, "inputStreamA");
                filterA.process(eventArr);
            }
        }
    );
    inputStreamB.subscribe(function (InputRecordB i) {
            lock {
                streamLock += 1;
                io:println(i);
                // make it type unaware and proceed
                map keyVal = <map>i;
                streams:StreamEvent[] eventArr = streams:buildStreamEvent(keyVal, "inputStreamB");
                filterB.process(eventArr);
            }
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
    index += 1;
}