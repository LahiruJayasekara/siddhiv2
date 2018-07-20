import collections;

public type EventType "CURRENT"|"EXPIRED"|"ALL"|"RESET";

public type StreamEvent record {
    EventType eventType;
    any eventObject;
    int timestamp;
};

public type LengthWindow object {

    private int counter;
    private int size;
    private EventType eventType = "ALL";



    private StreamEvent[] events = [];
    private function (StreamEvent[]) nextProcessorPointer;


    new(nextProcessorPointer, size, eventType) {

    }

    public function add(StreamEvent event) {

        StreamEvent? streamEvent = getEventToBeExpired();
        match streamEvent {
            StreamEvent value => {
                StreamEvent[] streamEvents = [];
                streamEvents[0] = value;
                nextProcessorPointer(streamEvents);
            }
            () => {
                //do nothing
            }
        }
        events[counter % size] = event;
        counter = counter + 1;
        nextProcessorPointer(getCurrentEvents());
    }

    function getCurrentEvents() returns (StreamEvent[]) {
        return events;
    }

    public function getEventToBeExpired() returns (StreamEvent?) {
        StreamEvent? eventToBeExpired;
        if (counter > size) {
            eventToBeExpired = events[counter % size];
        }
        match eventToBeExpired {
            StreamEvent value => {
                EventType evType = "EXPIRED";
                StreamEvent event = {eventType : evType, eventObject : value.eventObject, timestamp : value.timestamp};
                return event;
            }
            () => {
                return ();
            }
        }
    }
};

public function lengthWindow(int length, EventType eventType, function (StreamEvent[]) nextProcessorPointer)
                    returns LengthWindow {
    LengthWindow lengthWindow1 = new(nextProcessorPointer, length, eventType);
    return lengthWindow1;
}

public type TimeWindow object {

    public int counter;
    public int timeLength;
    public EventType eventType = "ALL";


    private collections:Queue eventQueue;
    private function (StreamEvent[]) nextProcessorPointer;


    new(timeLength, eventType, nextProcessorPointer) {
        eventQueue = new;
    }

    public function startEventRemovalWorker() {

        StreamEvent frontEvent = check <StreamEvent>eventQueue.peekFront();
        StreamEvent rearEvent = check <StreamEvent>eventQueue.peekRear();
        StreamEvent[] expiredEvents = [];
        int index = 0;
        while (!eventQueue.isEmpty() && rearEvent.timestamp > frontEvent.timestamp + timeLength) {
            if (!eventQueue.isEmpty()) {
                StreamEvent streamEvent = check <StreamEvent>eventQueue.dequeue();
                EventType evType = "EXPIRED";
                StreamEvent event = {eventType : evType, eventObject : streamEvent.eventObject,
                    timestamp : streamEvent.timestamp};
                expiredEvents[index] = event;
                index += 1;
                frontEvent = check <StreamEvent>eventQueue.peekFront();
                rearEvent = check <StreamEvent>eventQueue.peekRear();
            }
        }
        if (lengthof expiredEvents > 0) {
            nextProcessorPointer(expiredEvents);
        }
    }

    public function add(StreamEvent event) {
        if (!eventQueue.isEmpty()) {
            StreamEvent rearEvent = check <StreamEvent>eventQueue.peekRear();
            if (rearEvent.timestamp <= event.timestamp) {
                eventQueue.enqueue(event);
            }
        } else {
            eventQueue.enqueue(event);
        }
        startEventRemovalWorker();
    }

    public function returnContent() returns StreamEvent[] {
        StreamEvent [] events = [];
        int i = 0;
        foreach item in eventQueue.asArray() {
            StreamEvent event = check <StreamEvent>item;
            events[i] = event;
            i += 1;
        }
        return events;
    }
};

public function timeWindow(int timeLength, EventType  eventType, function(StreamEvent[]) nextProcessPointer)
                    returns TimeWindow {
    TimeWindow timeWindow1 = new(timeLength, eventType, nextProcessPointer);
    return timeWindow1;
}
