import ballerina/runtime;
import ballerina/io;
import streams;
import ballerinax/docker;


type Teacher record {
    string name;
    int age;
    string status;
    string batch;
    string school;
};

type Student record {
    string name;
    int age;
};

type TeacherOutput record {
    string name;
    int age;
    int sumAge;
    int count;
};

int index = 0;
stream<Teacher> teacherStream;
stream<Student> studentStream;
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

    Student[] students = [];
    Student s1 = {name: "Kavindu", age: 15};
    Student s2 = {name: "Lasith", age: 35};


    foo();

    outputStream.subscribe(printTeachers);
    teacherStream.publish(t1);
    studentStream.publish(s1);
    teacherStream.publish(t2);
    studentStream.publish(s2);
    teacherStream.publish(t3);
    teacherStream.publish(t4);
    //foreach t in teachers {
    //    runtime:sleep(1);
    //    teacherStream.publish(t);
    //}

    runtime:sleep(2000);

    //io:println("output: ", globalEmployeeArray);
}

//  ------------- Query to be implemented -------------------------------------------------------
//from every teacherStream where teacherStream.age > 10
//followed by every studentStream where studentStream.age > teacherStream.age
//

function foo() {

    streams:PatternProcessor patternProcessor = streams:createPatternProcessor(["teacherStream", "studentStream"], [
            function (map m) returns boolean {
                return check <int>m["teacherStream.age"] > 10;
            },
            function (map m) returns boolean {
                return check <int>m["studentStream.age"] > check <int>m["teacherStream.age"];
            }
        ]);

    teacherStream.subscribe(function (Teacher t) {
            // make it type unaware and proceed
            map keyVal = <map>t;
            streams:StreamEvent[] eventArr = streams:buildStreamEvent(keyVal, "teacherStream");
            patternProcessor.process(eventArr, "teacherStream");
        }
    );

    studentStream.subscribe(function (Student t) {
            // make it type unaware and proceed
            map keyVal = <map>t;
            streams:StreamEvent[] eventArr = streams:buildStreamEvent(keyVal, "studentStream");
            patternProcessor.process(eventArr, "studentStream");
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
