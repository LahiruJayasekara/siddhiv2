import ballerina/io;

public type OutputProcess object {

    private function (map[]) outputFunc;

    public new (outputFunc) {
    }

    public function process(StreamEvent[] streamEvents) {
        int index = 0;
        map[] events;
        int i = 0;
        foreach event in streamEvents {
            if (event.eventType == "CURRENT") {
                map outputData;
                foreach k, v in event.data {
                    string[] s = k.split("\\.");
                    if (OUTPUT.equalsIgnoreCase(s[0])) {
                        outputData[s[1]] = v;
                    }
                }
                events[i] = outputData;
                i += 1;
            }
        }
        outputFunc(events);
    }
};

public function createOutputProcess(function (map[]) outputFunc) returns OutputProcess {
    OutputProcess outputProcess = new(outputFunc);
    return outputProcess;
}