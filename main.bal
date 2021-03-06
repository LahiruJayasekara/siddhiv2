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
};

type TeacherOutput record {
    string name;
    int age;
};

int index = 0;
stream<Teacher> inputStream;
stream<TeacherOutput> outputStream;

TeacherOutput[] globalEmployeeArray = [];

public function main(string... args) {

    Teacher[] teachers = [];
    Teacher t1 = { name: "Raja", age: 25, status: "single", batch: "LK2014", school: "Hindu College" };
    Teacher t2 = { name: "Raja", age: 45, status: "single", batch: "LK2014", school: "Hindu College" };
    teachers[0] = t1;
    teachers[1] = t2;
    teachers[2] = t2;
    //teachers[3] = t1;
    //teachers[4] = t2;
    //teachers[5] = t1;
    //teachers[6] = t2;
    //teachers[7] = t1;
    //teachers[8] = t2;
    //teachers[9] = t1;

    foo();

    outputStream.subscribe(printTeachers);
    foreach t in teachers {
        inputStream.publish(t);
    }

    runtime:sleep(1000);

    io:println("output: ", globalEmployeeArray);
}


//  ------------- Query to be implemented -------------------------------------------------------
//  from inputStream where inputStream.age > 25
//  select inputStream.name, inputStream.age
//      => (TeacherOutput [] o) {
//            outputStream.publish(o);
//      }
//

function foo() {

    function (map[]) outputFunc = function (map[] events) {
        foreach m in events {
            // just cast input map into the output type
            TeacherOutput t = check <TeacherOutput>m;
            outputStream.publish(t);
        }
    };

    streams:OutputProcess outputProcess = streams:createOutputProcess(outputFunc);

    streams:SimpleSelect simpleSelect = streams:createSimpleSelect(outputProcess.process,
        function (streams:StreamEvent e) returns map {
            // got rid of type casting
            return {
                "name": e.data["inputStream.name"],
                "age": e.data["inputStream.age"]
            };
        }
    );

    streams:Filter filter = streams:createFilter(simpleSelect.process, function (map m) returns boolean {
            // simplify filter
            return check <int>m["inputStream.age"] > 25;
        }
    );

    inputStream.subscribe(function (Teacher t) {
            // make it type unaware and proceed
            map keyVal = <map>t;
            streams:StreamEvent[] eventArr = streams:buildStreamEvent(keyVal, "inputStream");
            filter.process(eventArr);
        }
    );
}

function printTeachers(TeacherOutput e) {
    addToGlobalEmployeeArray(e);
}

function addToGlobalEmployeeArray(TeacherOutput e) {
    globalEmployeeArray[index] = e;
    index = index + 1;
}