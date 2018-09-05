import ballerina/io;
import ballerina/time;
import ballerina/task;

public type StreamEvent object {
    public EventType eventType;
    public int timestamp;
    public map data;

    public new((string, map) | map eventData, eventType, timestamp) {
        match eventData {
            (string, map) t => {
                foreach k, v in t[1] {
                    data[t[0] + DELIMITER + k] = v;
                }
            }
            map m => {
                data = m;
            }
        }
    }

    public function clone() returns StreamEvent {
        StreamEvent clone = new(cloneData(), eventType, timestamp);
        return clone;
    }

    public function addData(map eventData) {
        foreach k, v in eventData {
            data[k] = v;
        }
    }

    function cloneData() returns map {
        map dataClone;
        foreach k, v in data {
            dataClone[k] = v;
        }
        return dataClone;
    }
};

public type LengthWindow object {

    public int size;
    public LinkedList linkedList;
    public function (StreamEvent[]) nextProcessorPointer;

    public new(nextProcessorPointer, size) {
        linkedList = new;
    }

    public function process(StreamEvent[] streamEvents) {
        StreamEvent[] outputEvents = [];
        foreach event in streamEvents {
            if (linkedList.getSize() == size) {
                match linkedList.removeFirst() {
                    StreamEvent streamEvent => {
                        outputEvents[lengthof outputEvents] = streamEvent;
                    }

                    () => {
                        // do nothing
                    }

                    any anyValue => {
                        // do nothing
                    }
                }
            }

            outputEvents[lengthof outputEvents] = event;
            StreamEvent expiredVeresionOfEvent = event.clone();
            expiredVeresionOfEvent.eventType = "EXPIRED";
            linkedList.addLast(expiredVeresionOfEvent);
        }
        nextProcessorPointer(outputEvents);
    }

    public function getCurrentEvents() returns StreamEvent[]{
        StreamEvent[] events = [];
        int i = 0;
        foreach e in linkedList.asArray() {
            match e {
                StreamEvent s => {
                    events[i] = s;
                    i++;
                }
                any a => {
                }
            }
        }
        return events;
    }

    public function getCandidateEvents(
                        StreamEvent originEvent,
                        function (StreamEvent e1, StreamEvent e2) returns boolean conditionFunc,
                        boolean isLHSTrigger = true)
                        returns (StreamEvent, StreamEvent)[] {
        (StreamEvent, StreamEvent)[] events;
        int i = 0;
        foreach e in linkedList.asArray() {
            match e {
                StreamEvent s => {
                    StreamEvent lshEvent = (isLHSTrigger) ? originEvent : s;
                    StreamEvent rhsEvent = (isLHSTrigger) ? s : originEvent;
                    if (conditionFunc(lshEvent, rhsEvent)) {
                        events[i] = (lshEvent, rhsEvent);
                        i++;
                    }
                }
                any a => {
                }
            }
        }
        return events;
    }
};

public function lengthWindow(function (StreamEvent[]) nextProcessorPointer, int length)
        returns LengthWindow {
    LengthWindow lengthWindow1 = new(nextProcessorPointer, length);
    return lengthWindow1;
}

public type TimeWindow object {

    public int counter;
    public int timeLength;

    private LinkedList linkedList;
    private function (StreamEvent[]) nextProcessorPointer;

    public new (nextProcessorPointer, timeLength) {
        linkedList = new;
    }

    function startEventRemovalWorker() {

        StreamEvent frontEvent = check <StreamEvent>linkedList.getFirst();
        StreamEvent rearEvent = check <StreamEvent>linkedList.getLast();
        StreamEvent[] expiredEvents = [];
        int index = 0;

        while (!linkedList.isEmpty() && rearEvent.timestamp > frontEvent.timestamp + timeLength) {
            if (!linkedList.isEmpty()) {
                StreamEvent streamEvent = check <StreamEvent>linkedList.removeFirst();
                StreamEvent event = streamEvent.clone();
                event.eventType = "EXPIRED";
                expiredEvents[index] = event;
                index += 1;
                frontEvent = check <StreamEvent>linkedList.getFirst();
                rearEvent = check <StreamEvent>linkedList.getLast();
            }
        }

        if (lengthof expiredEvents > 0) {
            nextProcessorPointer(expiredEvents);
        }
    }

    public function add(StreamEvent event) {
        if (!linkedList.isEmpty()) {
            StreamEvent rearEvent = check <StreamEvent>linkedList.getLast();
            if (rearEvent.timestamp <= event.timestamp) {
                linkedList.addLast(event);
            }
        } else {
            linkedList.addLast(event);
        }
        startEventRemovalWorker();
    }

    public function getCurrentEvents() returns StreamEvent[]{
        StreamEvent[] events = [];
        int i = 0;
        foreach e in linkedList.asArray() {
            match e {
                StreamEvent s => {
                    events[i] = s;
                    i++;
                }
                any a => {

                }
            }
        }
        return events;
    }

    public function getCandidateEvents(
                        StreamEvent originEvent,
                        function (StreamEvent e1, StreamEvent e2) returns boolean conditionFunc,
                        boolean isLHSTrigger = true)
                        returns (StreamEvent, StreamEvent)[] {
        (StreamEvent, StreamEvent)[] events = [];
        int i = 0;
        foreach e in linkedList.asArray() {
            match e {
                StreamEvent s => {
                    StreamEvent lshEvent = (isLHSTrigger) ? originEvent : s;
                    StreamEvent rhsEvent = (isLHSTrigger) ? s : originEvent;
                    if (conditionFunc(lshEvent, rhsEvent)) {
                        events[i] = (lshEvent, rhsEvent);
                        i++;
                    }
                }
                any a => {
                }
            }
        }
        return events;
    }
};

public function timeWindow(function(StreamEvent[]) nextProcessPointer, int timeLength)
                    returns TimeWindow {
    TimeWindow timeWindow1 = new(nextProcessPointer, timeLength);
    return timeWindow1;
}

public type LengthBatchWindow object {
    private int length;
    private int count;
    private StreamEvent? resetEvent;
    private LinkedList currentEventQueue;
    private LinkedList? expiredEventQueue;
    private function (StreamEvent[]) nextProcessorPointer;

    public new (nextProcessorPointer, length) {
        currentEventQueue = new();
        expiredEventQueue = ();
    }

    public function process(StreamEvent[] streamEvents) {
        LinkedList streamEventChunks = new();
        LinkedList outputStreamEventChunk = new();
        int currentTime = time:currentTime().time;

        foreach event in streamEvents {
            StreamEvent clonedStreamEvent = event.clone();
            currentEventQueue.addLast(clonedStreamEvent);
            count++;
            if (count == length) {
                //if (expiredEventQueue.getFirst() != ()) {
                //    expiredEventQueue.clear();
                //}
                if (currentEventQueue.getFirst() != ()) {
                    if (resetEvent != ()) {
                        outputStreamEventChunk.addLast(resetEvent);
                        resetEvent = ();
                    }
                    //if (expiredEventQueue != ()) {
                    //    currentEventQueue.resetToFront();
                    //    while (currentEventQueue.hasNext()) {
                    //        StreamEvent currentEvent = check <StreamEvent> currentEventQueue.next();
                    //        StreamEvent toBeExpired = {eventType: "EXPIRED", eventMap: currentEvent.eventMap,
                    //            timestamp: currentEvent.timestamp};
                    //        expiredEventQueue.addLast(toBeExpired);
                    //    }
                    //}
                    StreamEvent firstInCurrentEventQueue = check <StreamEvent> currentEventQueue.getFirst();
                    resetEvent = createResetStreamEvent(firstInCurrentEventQueue);
                    foreach currentEvent in currentEventQueue.asArray() {
                        outputStreamEventChunk.addLast(currentEvent);
                    }
                }
                currentEventQueue.clear();
                count = 0;
                if (outputStreamEventChunk.getFirst() != ()) {
                    streamEventChunks.addLast(outputStreamEventChunk);
                }
            }
        }

        streamEventChunks.resetToFront();
        while streamEventChunks.hasNext() {
            StreamEvent[] events = [];
            LinkedList streamEventChunk = check <LinkedList> streamEventChunks.next();
            streamEventChunk.resetToFront();
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = check <StreamEvent> streamEventChunk.next();
                events[lengthof events] = streamEvent;
            }
            nextProcessorPointer(events);
        }
    }

    public function getCurrentEvents() returns StreamEvent[]{
        StreamEvent[] events = [];
        int i = 0;
        foreach e in currentEventQueue.asArray() {
            match e {
                StreamEvent s => {
                    events[i] = s;
                    i++;
                }
                any a => {

                }
            }
        }
        return events;
    }

    public function getCandidateEvents(
                        StreamEvent originEvent,
                        function (StreamEvent e1, StreamEvent e2) returns boolean conditionFunc,
                        boolean isLHSTrigger = true)
                        returns (StreamEvent, StreamEvent)[] {
        (StreamEvent, StreamEvent)[] events = [];
        int i = 0;
        foreach e in currentEventQueue.asArray() {
            match e {
                StreamEvent s => {
                    StreamEvent lshEvent = (isLHSTrigger) ? originEvent : s;
                    StreamEvent rhsEvent = (isLHSTrigger) ? s : originEvent;
                    if (conditionFunc(lshEvent, rhsEvent)) {
                        events[i] = (lshEvent, rhsEvent);
                        i++;
                    }
                }
                any a => {
                }
            }
        }
        return events;
    }
};

public function lengthBatchWindow(function(StreamEvent[]) nextProcessPointer, int length)
                    returns LengthBatchWindow {
    LengthBatchWindow lengthBatch = new(nextProcessPointer, length);
    return lengthBatch;
}


public type TimeBatchWindow object {
    private int timeInMilliSeconds;
    private int nextEmitTime = -1;
    private LinkedList currentEventQueue;
    private LinkedList? expiredEventQueue;
    private StreamEvent? resetEvent;
    private task:Timer? timer;
    private function (StreamEvent[]) nextProcessorPointer;

    public new(nextProcessorPointer, timeInMilliSeconds) {
        currentEventQueue = new();
        expiredEventQueue = ();
    }

    function invokeProcess() returns error? {
        StreamEvent timerEvent = new (("timer", {}), "TIMER", time:currentTime().time);
        StreamEvent[] timerEventWrapper = [];
        timerEventWrapper[0] = timerEvent;
        process(timerEventWrapper);
        return ();
    }
    public function process(StreamEvent[] streamEvents) {
        LinkedList outputStreamEvents = new();
        if (nextEmitTime == -1) {
            nextEmitTime = time:currentTime().time + timeInMilliSeconds;
            timer = new task:Timer(self.invokeProcess, self.handleError, timeInMilliSeconds, delay =
                timeInMilliSeconds);
            _ = timer.start();
        }

        int currentTime = time:currentTime().time;
        boolean sendEvents = false;

        if (currentTime >= nextEmitTime) {
            nextEmitTime += timeInMilliSeconds;
            timer.stop();
            timer = new task:Timer(self.invokeProcess, self.handleError, timeInMilliSeconds, delay =
                timeInMilliSeconds);
            _ = timer.start();
            sendEvents = true;
        } else {
            sendEvents = false;
        }

        foreach event in streamEvents {
            if (event.eventType != "CURRENT") {
                continue;
            }
            StreamEvent clonedEvent = event.clone();
            currentEventQueue.addLast(clonedEvent);
        }
        if (sendEvents) {
            if (currentEventQueue.getFirst() != ()) {
                if (resetEvent != ()) {
                    outputStreamEvents.addLast(resetEvent);
                    resetEvent = ();
                }
                resetEvent = createResetStreamEvent(check <StreamEvent> currentEventQueue.getFirst());
                currentEventQueue.resetToFront();
                while (currentEventQueue.hasNext()) {
                    StreamEvent streamEvent = check <StreamEvent> currentEventQueue.next();
                    outputStreamEvents.addLast(streamEvent);
                }
            }
            currentEventQueue.clear();
        }
        if (outputStreamEvents.getSize() != 0) {
            StreamEvent[] events = [];
            outputStreamEvents.resetToFront();
            while (outputStreamEvents.hasNext()) {
                StreamEvent streamEvent = check <StreamEvent> outputStreamEvents.next();
                events[lengthof events] = streamEvent;
            }
            nextProcessorPointer(events);
        }
    }

    public function getCurrentEvents() returns StreamEvent[]{
        StreamEvent[] events = [];
        int i = 0;
        foreach e in currentEventQueue.asArray() {
            match e {
                StreamEvent s => {
                    events[i] = s;
                    i++;
                }
                any a => {

                }
            }
        }
        return events;
    }

    public function getCandidateEvents(
                        StreamEvent originEvent,
                        function (StreamEvent e1, StreamEvent e2) returns boolean conditionFunc,
                        boolean isLHSTrigger = true)
                        returns (StreamEvent, StreamEvent)[] {
        (StreamEvent, StreamEvent)[] events = [];
        int i = 0;
        foreach e in currentEventQueue.asArray() {
            match e {
                StreamEvent s => {
                    StreamEvent lshEvent = (isLHSTrigger) ? originEvent : s;
                    StreamEvent rhsEvent = (isLHSTrigger) ? s : originEvent;
                    if (conditionFunc(lshEvent, rhsEvent)) {
                        events[i] = (lshEvent, rhsEvent);
                        i++;
                    }
                }
                any a => {
                }
            }
        }
        return events;
    }

    function handleError(error e) {
        io:println("Error occured", e);
    }
};

public function timeBatchWindow(function(StreamEvent[]) nextProcessPointer, int time)
                    returns TimeBatchWindow {
    TimeBatchWindow timeBatch = new(nextProcessPointer, time);
    return timeBatch;
}
