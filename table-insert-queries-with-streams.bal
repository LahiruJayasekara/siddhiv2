import ballerina/io;
import ballerina/jdbc;
import ballerina/runtime;
import ballerina/streams;

// Client endpoint for MySQL database. This client endpoint can be used with any jdbc
// supported database by providing the corresponding jdbc url.
endpoint jdbc:Client testDB {
    url: "jdbc:mysql://localhost:3306/testdb",
    username: "root",
    password: "root",
    poolOptions: { maximumPoolSize: 5 },
    dbOptions: { useSSL: false }
};

// This is the type created to represent data row.
type Student record {
    int id;
    int age;
    string name;
};

stream<Student> studentInputStream;

function insertEvents() {


    forever {
        from studentInputStream
        select studentInputStream.id, studentInputStream.age, studentInputStream.name
        => (Student [] students) {

            foreach s in students {
                io:println(s);
                io:println("&&&&");
                var ret = testDB->update("INSERT INTO student(age, name) values (?, ?)",
                    s.age, s.name);
                handleUpdate(ret, "Insert to student table");
            }


        }
    }

}

public function main(string... args) {
    // Creates a table using the update operation. If the DDL
    // statement execution is successful, the `update` operation returns 0.

    io:println("The update operation - Creating a table:");
    var ret = testDB->update("CREATE TABLE student(id INT AUTO_INCREMENT,
                         age INT, name VARCHAR(255), PRIMARY KEY (id))");
    handleUpdate(ret, "Create student table");


    //
    Student[] students = [];
    Student s1 = { id: 12, age: 30, name: "Mohan"};
    Student s2 = { id: 14, age: 22, name: "Raja"};
    students[0] = s1;
    students[1] = s2;

    insertEvents();
    foreach s in students {
        studentInputStream.publish(s);
    }


    //Drop the table and procedures.
    //io:println("\nThe update operation - Drop the table");
    //ret = testDB->update("DROP TABLE student");
    //handleUpdate(ret, "Drop table student");


    runtime:sleep(5000);
    // Finally, close the connection pool.
    testDB.stop();

}

function printStudent (Student[] s){
    io:println(s);
}

// Function to handle return of the update operation.
function handleUpdate(int|error returned, string message) {
    match returned {
        int retInt => io:println(message + " status: " + retInt);
        error e => io:println(message + " failed: " + e.message);
    }
}