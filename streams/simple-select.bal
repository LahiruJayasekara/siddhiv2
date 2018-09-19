import ballerina/io;

public type SimpleSelect object {

    private function (StreamEvent[]) nextProcessorPointer;
    private function(StreamEvent o) returns map selectFunc;


    new(nextProcessorPointer, selectFunc) {
    }

    public function process(StreamEvent[] streamEvents) {
        StreamEvent[] newStreamEventArr = [];
        int index = 0;
        foreach event in streamEvents {
            StreamEvent streamEvent = new((OUTPUT, selectFunc(event)), event.eventType, event.timestamp);
            newStreamEventArr[index] = streamEvent;
            index += 1;
        }
        if (index > 0) {
            nextProcessorPointer(newStreamEventArr);
        }

    }
};

public function createSimpleSelect(function (StreamEvent[]) nextProcPointer, function(StreamEvent o) returns map selectFunc)
        returns SimpleSelect {
    SimpleSelect simpleSelect = new(nextProcPointer, selectFunc);
    return simpleSelect;
}