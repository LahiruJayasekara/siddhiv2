import ballerina/io;

public type Select object {


    private function (StreamEvent[]) nextProcessorPointer;
    private Aggregator [] aggregatorArr;
    private (function(StreamEvent o) returns string)? groupbyFunc;
    private function(StreamEvent o, Aggregator []  aggregatorArr1) returns any selectFunc;
    private map <Aggregator []> aggregatorMap;


    new(nextProcessorPointer, aggregatorArr, groupbyFunc, selectFunc) {
    }

    public function process(StreamEvent[] streamEvents) {
                StreamEvent[] newStreamEventArr = [];
                int index =0;

                foreach event in streamEvents {
                    string groupbyKey = groupbyFunc but {
                        (function(StreamEvent o) returns string) groupbyFunction => groupbyFunction(event),
                        () => "DEFAULT"
                    };
                    Aggregator[] aggregatorArray;
                    if(aggregatorMap.hasKey(groupbyKey)) {
                        match(aggregatorMap[groupbyKey]) {
                            Aggregator[] aggregators => {
                                aggregatorArray = aggregators;
                            }
                            () => {
                            }
                        }
                    } else {
                        int i = 0;
                        foreach aggregator in aggregatorArr {
                            aggregatorArray[i] = aggregator.clone();
                            i++;
                        }
                        aggregatorMap[groupbyKey] = aggregatorArray;
                    }

                    StreamEvent streamEvent = {eventType: event.eventType, timestamp: event.timestamp,
                                                                    eventObject: selectFunc(event, aggregatorArray)};

                    newStreamEventArr[index] = streamEvent;
                    index +=1;
                }

                if (index >0) {
                    nextProcessorPointer(newStreamEventArr);
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