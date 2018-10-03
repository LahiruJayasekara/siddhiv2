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
    int sumAge;
};

int index = 0;
stream<Teacher> inputStream;
stream<TeacherOutput> outputStream;

TeacherOutput[] globalEmployeeArray = [];

public function main() {

    Teacher[] teachers = [];
    Teacher t1 = { name: "Mohan", age: 30, status: "single", batch: "LK2014", school: "Hindu College", timeStamp: 1000 };
    Teacher t2 = { name: "Raja", age: 45, status: "single", batch: "LK2014", school: "Hindu College", timeStamp: 1400 };
    Teacher t3 = { name: "Naveen", age: 35, status: "single", batch: "LK2014", school: "Hindu College", timeStamp: 3000      };
    Teacher t4 = { name: "Amal", age: 50, status: "single", batch: "LK2014", school: "Hindu College", timeStamp: 3200 };
    teachers[0] = t1;
    teachers[1] = t2;
    teachers[2] = t3;
    teachers[3] = t4;

    foo();

    outputStream.subscribe(printTeachers);
    foreach t in teachers {
        inputStream.publish(t);
    }

    runtime:sleep(1000);

    io:println("output: ", globalEmployeeArray);
}


//  ------------- Query to be implemented -------------------------------------------------------
//  from teacherStream window externalTime(Teacher.timeStamp, 1000)
//        select name, age, sum(age) as sumAge
//        => (TeacherOutput[] t) {
//            outPutStream.publish(t);
//        }
//

function foo() {

    function (map) outputFunc = function (map m) {
        // just cast input map into the output type
        TeacherOutput t = check <TeacherOutput>m;
        outputStream.publish(t);
    };

    streams:OutputProcess outputProcess = streams:createOutputProcess(outputFunc);

    streams:Sum sumAggregator = new();

    streams:SimpleSelect simpleSelect = streams:createSimpleSelect(outputProcess.process,
        function (streams:StreamEvent e) returns map {
            // got rid of type casting
            return {
                "name": e.data["inputStream.name"],
                "age": e.data["inputStream.age"],
                "sumAge": sumAggregator.process(e.data["inputStream.age"], e.eventType)
            };
        }
    );

    streams:ExternalTimeWindow tmpWindow = streams:externalTimeWindow(simpleSelect.process, 1000, "inputStream.timeStamp");

    inputStream.subscribe(function (Teacher t) {
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