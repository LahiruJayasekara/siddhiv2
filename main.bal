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

type Teacher {
    string name;
    int age;
    string status;
    string batch;
    string school;
};

type TeacherOutput {
    string name;
    int age;
};

int index = 0;
stream<Teacher> inputStream;
stream<TeacherOutput> outputStream;

TeacherOutput[] globalEmployeeArray = [];

function main(string... args) {

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

function foo() {

    //function (TeacherOutput) outputFunc = (TeacherOutput t) => {
    //    outputStream.publish(t);
    //};
    //
    //streams:OutputProcess outputProcess = streams:createOutputProcess(outputFunc);

    streams:SimpleSelect simpleSelect = streams:createSimpleSelect((streams:StreamEvent[] t) => {
            io:println("WWWWW");
        } ,
        (any o)  => any {
            Teacher t = check <Teacher>o;
            TeacherOutput teacherOutput = {name: t.name, age: t.age};
            return teacherOutput;
        });

    streams:Filter filter = streams:createFilter(simpleSelect.process1, (any o) => boolean {
            Teacher teacher = check <Teacher> o;
            io:println("Filter: ", teacher);
            return teacher.age > 25;
        });


    inputStream.subscribe((Teacher t) => {
            streams:StreamEvent[] eventArr = streams:buildStreamEvent(t);
            io:println("eventArr: ", eventArr);
            filter.process(eventArr);
        });
}

function printTeachers(TeacherOutput e) {
    addToGlobalEmployeeArray(e);
}

function addToGlobalEmployeeArray(TeacherOutput e) {
    globalEmployeeArray[index] = e;
    index = index + 1;
}