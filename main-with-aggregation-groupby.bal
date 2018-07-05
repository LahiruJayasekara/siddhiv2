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
    int sumAge;
};

int index = 0;
stream<Teacher> inputStream;
stream<TeacherOutput> outputStream;

TeacherOutput[] globalEmployeeArray = [];

function main(string... args) {

    Teacher[] teachers = [];
    Teacher t1 = { name: "Mohan", age: 30, status: "single", batch: "LK2014", school: "Hindu College" };
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
//  select inputStream.name, inputStream.age, sum (inputStream.age) as sumAge
//  group by inputStream.name
//      => (TeacherOutput [] o) {
//            outputStream.publish(o);
//      }
//

function foo() {

    //forever {
    //    from inputStream where inputStream.age > 25
    //    select inputStream.name, inputStream.age
    //    => (Teacher[] emp) {
    //        outputStream.publish(emp);
    //    }
    //}


    function (TeacherOutput []) outputFunc = (TeacherOutput [] t) => {
        outputStream.publish(t);
    };

    streams:OutputProcess outputProcess = streams:createOutputProcess(outputFunc);


    streams:Sum sumAggregator = new();
    streams:Aggregator [] aggregatorArr = [];
    aggregatorArr[0] = sumAggregator;

    streams:Select select = streams:createSelect(outputProcess.process, aggregatorArr,
        (streams:StreamEvent e) => string {
            Teacher t = check <Teacher> e.eventObject;
            return t.name;
        },
        (streams:StreamEvent e, streams:Aggregator [] aggregatorArr1)  => any {
            Teacher t = check <Teacher> e.eventObject;
            streams:Sum sumAggregator1 = check <streams:Sum> aggregatorArr1[0];
            TeacherOutput teacherOutput = {name: t.name, age: t.age, sumAge : sumAggregator1.process(t.age, e.eventType) };
            return teacherOutput;
        });


    streams:SimpleSelect simpleSelect = streams:createSimpleSelect(outputProcess.process,
        (streams:StreamEvent o)  => any {
            Teacher t = check <Teacher>o.eventObject;
            TeacherOutput teacherOutput = {name: t.name, age: t.age};
            return teacherOutput;
        });


    streams:Filter filter = streams:createFilter(select.process, (any o) => boolean {
            Teacher teacher = check <Teacher> o;
            io:println("Filter: ", teacher);
            return teacher.age > getValue();
        });

    inputStream.subscribe((Teacher t) => {
            streams:StreamEvent[] eventArr = streams:buildStreamEvent(t);
            io:println("eventArr: ", eventArr);
            filter.process(eventArr);
        });
}

function getValue() returns int  {
    return 25;
}

function printTeachers(TeacherOutput e) {
    addToGlobalEmployeeArray(e);
}

function addToGlobalEmployeeArray(TeacherOutput e) {
    globalEmployeeArray[index] = e;
    index = index + 1;
}