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
import ballerina/time;

type Teacher record {
    string name;
    int age;
    string status;
    string batch;
    string school;
    int timeStamp;
};

type TeacherOutput record {
    string name;
    int age;
    //int sumAge;
};

int index = 0;
stream<Teacher> inputStream;
stream<TeacherOutput> outputStream;

TeacherOutput[] globalEmployeeArray = [];

public function main(string... args) {

    Teacher[] teachers = [];
    Teacher t1 = { name: "Mohan", age: 30, status: "single", batch: "LK2014", school: "Hindu College"};
    Teacher t2 = { name: "Raja", age: 30, status: "single", batch: "LK2014", school: "Hindu College"};
    Teacher t3 = { name: "Naveen", age: 35, status: "single", batch: "LK2014", school: "Hindu College"};
    Teacher t4 = { name: "Amal", age: 50, status: "single", batch: "LK2014", school: "Hindu College"};
    Teacher t5 = { name: "Nimal", age: 50, status: "single", batch: "LK2014", school: "Hindu College" };
    Teacher t6 = { name: "Kamal", age: 60, status: "single", batch: "LK2014", school: "Hindu College" };

    teachers[0] = t1;
    teachers[1] = t2;
    teachers[2] = t3;
    teachers[3] = t4;
    teachers[4] = t5;
    teachers[5] = t6;
    foo();

    outputStream.subscribe(printTeachers);
    foreach t in teachers {
        inputStream.publish(t);
    }

    runtime:sleep(2000);

    io:println("output: ", globalEmployeeArray);
}


//  ------------- Query to be implemented -------------------------------------------------------
//  from teacherStream window delayWindow(1000)
//        select name, age
//        => (TeacherOutput[] t) {
//            outPutStream.publish(t);
//        }
//

function foo() {

    function (map[]) outputFunc = function(map[] m) {
        foreach x in m {
            TeacherOutput t = check <TeacherOutput>x;
            outputStream.publish(t);
        }
    };

    streams:OutputProcess outputProcess = streams:createOutputProcess(outputFunc);

    //streams:Sum iSumAggregator = new();
    //
    //streams:Aggregator[] aggregators = [];
    //aggregators[0] = iSumAggregator;
    //
    //streams:Select select = streams:createSelect(outputProcess.process, aggregators,
    //    (),
    //    function(streams:StreamEvent e, streams:Aggregator[] aggregatorArray) returns map {
    //        streams:Sum iSumAggregator1 = check <streams:Sum>aggregatorArray[0];
    //        return {
    //            "name": e.data["inputStream.name"],
    //            "age": e.data["inputStream.age"],
    //            "sumAge": iSumAggregator.process(e.data["inputStream.age"], e.eventType)
    //        };
    //    });

    streams:SimpleSelect select = streams:createSimpleSelect(outputProcess.process,
        function (streams:StreamEvent e) returns map {
            return {
                "name": e.data["inputStream.name"],
                "age": e.data["inputStream.age"]
            };
        }
    );

    streams:DelayWindow tmpWindow = streams:delayWindow(1000, nextProcessPointer = select.process);

    inputStream.subscribe(function(Teacher t) {
            map keyVal = <map>t;
            streams:StreamEvent[] eventArr = streams:buildStreamEvent(keyVal, "inputStream");
            tmpWindow.process(eventArr);
        });
}

function printTeachers(TeacherOutput e) {
    addToGlobalEmployeeArray(e);
}

function addToGlobalEmployeeArray(TeacherOutput e) {
    globalEmployeeArray[index] = e;
    index = index + 1;
}