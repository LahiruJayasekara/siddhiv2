import ballerina/io;

public type Filter object {

    private function (StreamEvent[]) nextProcessorPointer;
    private function (map) returns boolean conditionFunc;

    new(nextProcessorPointer, conditionFunc) {

    }

    public function process(StreamEvent[] streamEvents) {
        StreamEvent[] newStreamEventArr = [];
        int index = 0;
        foreach event in streamEvents {
            if (conditionFunc(event.data)) {
                newStreamEventArr[index] = event;
                index += 1;
            }
        }
        if (index > 0) {
            nextProcessorPointer(newStreamEventArr);
        }
    }
};

public function createFilter(function(StreamEvent[]) nextProcPointer, function (map o) returns boolean conditionFunc)
                    returns Filter {
    Filter filter = new(nextProcPointer, conditionFunc);
    return filter;
}


