import ballerina/io;

public type PatternProcessor object {

    public string[] streamNames;
    public (function (map o) returns boolean)[] conditionFuncArray;
    LinkedList matchedEventArrayList;

    public new(streamNames, conditionFuncArray) {
        matchedEventArrayList = new;
    }

    public function process(StreamEvent[] streamEvents, string currentStreamName) {
        //io:println(streamEvents);
        foreach event in streamEvents {
            if(currentStreamName == streamNames[0]) {
                function (map o) returns boolean conditionFunction = conditionFuncArray[0];
                if(conditionFunction(event.data)) {
                    StreamEvent[] matchedEventArray = [event];
                    matchedEventArrayList.addLast(matchedEventArray);
                }
            } else {
                foreach i in 1 ..< lengthof streamNames {
                    if(streamNames[i] != currentStreamName) {
                        continue;
                    }
                    matchedEventArrayList.resetToFront();
                    while(matchedEventArrayList.hasNext()) {
                        StreamEvent[] matchedEventArray = check <StreamEvent[]>matchedEventArrayList.next();
                        if(lengthof matchedEventArray == i) {
                            //creating the map argument for filter function
                            map appendedMap;
                            //appending past matched events
                            foreach matchedEvent in matchedEventArray {
                                appendedMap = appendToMap(appendedMap, matchedEvent.data);
                            }
                            //appending current matched event
                            appendedMap = appendToMap(appendedMap, event.data);

                            function (map o) returns boolean conditionFunction = conditionFuncArray[i];
                            if(conditionFunction(appendedMap)) {
                                matchedEventArray[lengthof matchedEventArray] = event;
                                if(lengthof matchedEventArray == lengthof streamNames) {
                                    io:println("final", matchedEventArray);
                                    matchedEventArrayList.removeCurrent();
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    public function appendToMap (map firstMap, map secondMap) returns map {
        string[] secondMapKeys = secondMap.keys();
        foreach key in secondMapKeys {
            firstMap[key] = secondMap[key];
        }
        return firstMap;
    }
};

public function createPatternProcessor(string[] streamNamesArray, (function (map o) returns boolean)[]
    conditionFuncArray)
                    returns PatternProcessor {
    PatternProcessor patternProcessor = new(streamNamesArray, conditionFuncArray);
    return patternProcessor;
}
