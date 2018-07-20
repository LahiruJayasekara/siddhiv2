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

public type QNode object {

    public any data;
    public QNode? nextNode;


    new(data) {

    }
};

public type Queue object {

    private QNode? front;
    private QNode? rear;

    public function isEmpty() returns boolean {
        match front {
            QNode value => {
                return false;
            }
            () => {
                return true;
            }
        }
    }

    public function peekRear() returns any {
        match rear {
            QNode value => {
                return value.data;
            }
            () => {
                return ();
            }
        }
    }

    public function peekFront() returns any {
        match front {
            QNode value => {
                return value.data;
            }
            () => {
                return ();
            }
        }
    }

    public function enqueue(any data) {
        QNode temp = new(data);
        match rear {
            QNode value => {
                value.nextNode = temp;
                rear = temp;
            }
            () => {
                rear = temp;
                front = rear;
            }
        }
    }

    public function dequeue() returns any? {
        match front {
            QNode value => {
                front = value.nextNode;
                match front {
                    QNode nextValue => {
                        // do nothing
                    }
                    () => {
                        rear = ();
                    }
                }
                return value.data;
            }
            () => {
                return ();
            }
        }
    }

    public function asArray() returns any[] {
        any[] anyArray = [];
        QNode? temp;
        int i;
        if (!isEmpty()) {
            match front {
                QNode value => {
                    temp = value;
                    while (temp != ()) {
                        anyArray[i] = temp.data;
                        i = i + 1;
                        temp = temp.nextNode;
                    }
                }
                () => {

                }
            }
        }
        return anyArray;
    }
};