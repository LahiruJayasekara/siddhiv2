import ballerina/io;

public type OutputProcess object {
    private {
        function (any) outputFunc;
    }

    new(outputFunc) {

    }

    public function process(StreamEvent[] streamEvents) {
        any[] newEventArr = [];
        int index = 0;
        foreach event in streamEvents {
            newEventArr[index] = event.eventObject;

        }
        outputFunc(newEventArr);
    }
};

public function createOutputProcess(function (any) outputFunc) returns OutputProcess {
    OutputProcess outputProcess = new(outputFunc);
    return outputProcess;
}