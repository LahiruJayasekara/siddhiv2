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
    int sumAge;
    int count;
};

int index = 0;
stream<Teacher> inputStream;
stream<TeacherOutput> outputStream;

TeacherOutput[] globalEmployeeArray = [];

public function main(string... args) {

    Teacher[] teachers = [];
    Teacher t1 = { name: "Mohan", age: 30, status: "single", batch: "LK2014", school: "Hindu College" };
    Teacher t2 = { name: "Raja", age: 45, status: "single", batch: "LK2014", school: "Royal College" };
    Teacher t3 = { name: "Naveen", age: 30, status: "single", batch: "LK2014", school: "Hindu College"};
    Teacher t4 = { name: "Amal", age: 50, status: "single", batch: "LK2014", school: "Hindu College"};


    teachers[0] = t1;
    teachers[1] = t2;
    teachers[2] = t3;
    teachers[3] = t4;

    foo();

    outputStream.subscribe(printTeachers);
    foreach t in teachers {
        runtime:sleep(1);
        inputStream.publish(t);
    }

    runtime:sleep(1000);

    io:println("output: ", globalEmployeeArray);
}


//  ------------- Query to be implemented -------------------------------------------------------
//  from inputStream where inputStream.age > getValue()
//  select inputStream.name, inputStream.age, sum (inputStream.age) as sumAge, count() as count
//  group by inputStream.school, inputStream.age
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

    streams:Sum sumAggregator = new();
    streams:Count countAggregator = new();
    streams:Aggregator [] aggregatorArr = [];
    aggregatorArr[0] = sumAggregator;
    aggregatorArr[1] = countAggregator;

    streams:Select select = streams:createSelect(outputProcess.process, aggregatorArr,
        [function (streams:StreamEvent e) returns string {
            return <string>e.data["inputStream.school"];
        },
        function (streams:StreamEvent e) returns string {
            return <string>e.data["inputStream.age"];
        }],
        function (streams:StreamEvent e, streams:Aggregator[] aggregatorArr1) returns map {
            streams:Sum sumAggregator1 = check <streams:Sum>aggregatorArr1[0];
            streams:Count countAggregator1 = check <streams:Count>aggregatorArr1[1];
            // got rid of type casting
            return {
                "name": e.data["inputStream.name"],
                "age": e.data["inputStream.age"],
                "sumAge": sumAggregator1.process(e.data["inputStream.age"], e.eventType),
                "count": countAggregator1.process((), e.eventType)
            };
        }
    );

    //   streams:Window tmpWindow = streams:lengthWindow(select.process, 5);

    //   streams:Window tmpWindow = streams:lengthBatchWindow(select.process, 5);

    streams:Window tmpWindow = streams:timeWindow([1000], nextProcessPointer = select.process);

    //   streams:Window tmpWindow = streams:timeBatchWindow(select.process, 1000);

    streams:Filter filter = streams:createFilter(tmpWindow.process, function (map m) returns boolean {
            // simplify filter
            return check <int>m["inputStream.age"] > getValue();
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