import ballerina/io;

public type Select object {

    private function (StreamEvent[]) nextProcessorPointer;
    private Aggregator [] aggregatorArr;
    private (function(StreamEvent o) returns string)? groupbyFunc;
    private function(StreamEvent o, Aggregator []  aggregatorArr1) returns any selectFunc;
    private map<Aggregator[]> aggregatorsCloneMap;


    new(nextProcessorPointer, aggregatorArr, groupbyFunc, selectFunc) {
    }

    public function process(StreamEvent[] streamEvents) {
        StreamEvent[] outputStreamEvents = [];

        if (lengthof aggregatorArr > 0) {
            map<StreamEvent> groupedEvents;
            foreach event in streamEvents {
                string groupbyKey = groupbyFunc but {
                    (function(StreamEvent o) returns string) groupbyFunction => groupbyFunction(event),
                    () => "DEFAULT"
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
                groupedEvents[groupbyKey] = {
                    eventType: event.eventType,
                    timestamp: event.timestamp,
                    eventObject: selectFunc(event, aggregatorsClone)
                };
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
                outputStreamEvents[lengthof outputStreamEvents] = {
                    eventType: event.eventType,
                    timestamp: event.timestamp,
                    eventObject: selectFunc(event, aggregatorArr)
                };
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
                                function(StreamEvent o, Aggregator [] aggregatorArr1) returns any selectFunc)
        returns Select {

    Select select = new(nextProcPointer, aggregatorArr, groupbyFunc, selectFunc);
    return select;
}