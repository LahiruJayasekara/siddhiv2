import ballerina/io;

public type OutputProcess object {

    private function (map) outputFunc;

    public new (outputFunc) {
    }

    public function process(StreamEvent[] streamEvents) {
        int index = 0;
        foreach event in streamEvents {
            map outputData;
            foreach k, v in event.data {
                string[] s = k.split("\\.");
                if (OUTPUT.equalsIgnoreCase(s[0])) {
                    outputData[s[1]] = v;
                }
            }
            outputFunc(outputData);
        }
    }
};

public function createOutputProcess(function (map) outputFunc) returns OutputProcess {
    OutputProcess outputProcess = new(outputFunc);
    return outputProcess;
}