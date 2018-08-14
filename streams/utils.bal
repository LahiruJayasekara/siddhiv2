import ballerina/time;

public function buildStreamEvent(any o) returns StreamEvent[] {
    EventType evntType = "CURRENT";
    StreamEvent[] streamEvents = [{eventType: evntType, eventObject: o,
        timestamp: time:currentTime().time }];
    return streamEvents;
}

public function cloneStreamEvent(StreamEvent event) returns StreamEvent {
    StreamEvent clonedStreamEvent = {eventType: event.eventType, eventObject: event.eventObject, timestamp:
    event.timestamp};
    return clonedStreamEvent;
}

public function createResetStreamEvent(StreamEvent event) returns StreamEvent {
    StreamEvent resetStreamEvent = {eventType: "RESET", eventObject: event.eventObject, timestamp:
    event.timestamp};
    return resetStreamEvent;
}