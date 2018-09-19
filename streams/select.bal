import ballerina/io;

public type Select object {

    private function (StreamEvent[]) nextProcessorPointer;
    private Aggregator [] aggregatorArr;
    private (function(StreamEvent o) returns string)? groupbyFunc;
    private function(StreamEvent o, Aggregator []  aggregatorArr1) returns map selectFunc;
    private map<Aggregator[]> aggregatorsCloneMap;


    new(nextProcessorPointer, aggregatorArr, groupbyFunc, selectFunc) {
    }

    public function process(StreamEvent[] streamEvents) {
        StreamEvent[] outputStreamEvents = [];
        if (lengthof aggregatorArr > 0) {
            map<StreamEvent> groupedEvents;
            foreach event in streamEvents {

                if (event.eventType == RESET) {
                    aggregatorsCloneMap.clear();
                }

                string groupbyKey = groupbyFunc but {
                    (function(StreamEvent o) returns string) groupbyFunction => groupbyFunction(event),
                    () => DEFAULT
                };
                Aggregator[] aggregatorsClone;
                match (aggregatorsCloneMap[groupbyKey]) {
                    Aggregator[] aggregators => {
                        aggregatorsClone = aggregators;
                    }
                    () => {
                        int i = 0;
                        foreach aggregator in aggregatorArr {
                            aggregatorsClone[i] = aggregator.clone();
                            i++;
                        }
                        aggregatorsCloneMap[groupbyKey] = aggregatorsClone;
                    }
                }
                StreamEvent e = new ((OUTPUT, selectFunc(event, aggregatorsClone)), event.eventType, event.timestamp);
                groupedEvents[groupbyKey] = e;
            }
            foreach key in groupedEvents.keys() {
                match groupedEvents[key] {
                    StreamEvent e => {
                        outputStreamEvents[lengthof outputStreamEvents] = e;
                    }
                    () => {}
                }
            }
        } else {
            foreach event in streamEvents {
                StreamEvent e = new ((OUTPUT, selectFunc(event, aggregatorArr)), event.eventType, event.timestamp);
                outputStreamEvents[lengthof outputStreamEvents] = e;
            }
        }
        if (lengthof outputStreamEvents > 0) {
            nextProcessorPointer(outputStreamEvents);
        }
    }
};

public function createSelect(function (StreamEvent[]) nextProcPointer,
                                Aggregator [] aggregatorArr,
                                (function(StreamEvent o) returns string)? groupbyFunc,
                                function(StreamEvent o, Aggregator [] aggregatorArr1) returns map selectFunc)
        returns Select {

    Select select = new(nextProcPointer, aggregatorArr, groupbyFunc, selectFunc);
    return select;
}