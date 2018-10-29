// Copyright (c) 2018 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
//
// WSO2 Inc. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerina/io;

public type StreamJoinProcessor object {
    private (function (map e1Data, map e2Data) returns boolean)? onConditionFunc;
    private function (any) nextProcessor;
    public Window? lhsWindow;
    public Window? rhsWindow;
    public string? lhsStream;
    public string? rhsStream;
    public string? unidirectionalStream;
    public JoinType joinType;

    public new(nextProcessor, joinType, onConditionFunc) {
        lhsWindow = ();
        rhsWindow = ();
        lhsStream = ();
        rhsStream = ();
        unidirectionalStream = ();
    }

    public function process(StreamEvent[] streamEvents) {
        StreamEvent?[] joinedEvents;
        int i = 0;
        foreach event in streamEvents {
            string originStream = event.data.keys()[0].split("\\.")[0];
            // resolve trigger according to join direction
            boolean triggerJoin = false;
            match unidirectionalStream {
                string s => {
                    // unidirectional
                    if (s.equalsIgnoreCase(originStream)) {
                        triggerJoin = true;
                    }
                }
                () => {
                    // bidirectional
                    triggerJoin = true;
                }
            }
            if (triggerJoin) {
                (StreamEvent?, StreamEvent?)[] candidateEvents;
                // join events according to the triggered side
                if (self.lhsStream.equalsIgnoreCase(originStream) ?: false) {
                    // triggered from LHS and type is `left outer join` or a `full outer join`
                    if (joinType == LEFT_OUTER_JOIN || joinType == FULL_OUTER_JOIN || joinType == INNER_JOIN) {
                        match rhsWindow.getCandidateEvents(event, onConditionFunc) {
                            (StreamEvent?, StreamEvent?)[] evtArr => {
                                candidateEvents = evtArr;
                                // left/full joins have to emit an event even there's no candidate events in rhs.
                                if (lengthof candidateEvents == 0 && (joinType == LEFT_OUTER_JOIN
                                        || joinType == FULL_OUTER_JOIN)) {
                                    candidateEvents[0] = (event, ());
                                }
                            }
                            () => {
                                if (joinType == LEFT_OUTER_JOIN || joinType == FULL_OUTER_JOIN) {
                                    candidateEvents[0] = (event, ());
                                }
                            }
                        }
                    } else {
                        match rhsWindow.getCandidateEvents(event, returnTrue) {
                            (StreamEvent?, StreamEvent?)[] tmpEvtArr => {
                                int k = 0;
                                (StreamEvent?, StreamEvent?)[] tmpCandidateEvents;
                                foreach rEvt in tmpEvtArr {
                                    // match rhs event of the tuple
                                    match rEvt[1] {
                                        StreamEvent evt => {
                                            // clone the event from the window (usually the type of this is expired).
                                            StreamEvent clonedEvt = evt.clone();
                                            // so, set the event type to triggered event's eventType
                                            clonedEvt.eventType = event.eventType;
                                            match lhsWindow.getCandidateEvents(clonedEvt, onConditionFunc,
                                                isLHSTrigger = false) {
                                                (StreamEvent?, StreamEvent?)[] evtArr => {
                                                    tmpCandidateEvents = evtArr;
                                                    // right join have to emit an event even there's
                                                    // no candidate events in lhs.
                                                    if (lengthof tmpCandidateEvents == 0) {
                                                        tmpCandidateEvents[0] = ((), clonedEvt);
                                                    }
                                                }
                                                () => {
                                                    tmpCandidateEvents[0] = ((), clonedEvt);
                                                }
                                            }
                                            foreach tEvt in tmpCandidateEvents {
                                                candidateEvents[k] = tEvt;
                                                k += 1;
                                            }
                                        }
                                        () => {
                                            // do nothing.
                                        }
                                    }
                                }
                            }
                            () => {
                                // do nothing.
                            }
                        }
                    }
                    foreach evtTuple in candidateEvents {
                        joinedEvents[i] = joinEvents(evtTuple[0], evtTuple[1]);
                        i += 1;
                    }
                } else {
                    // triggered from RHS and type is `right outer join` or `full outer join` or `join`
                    if (joinType == RIGHT_OUTER_JOIN || joinType == FULL_OUTER_JOIN || joinType == INNER_JOIN) {
                        match lhsWindow.getCandidateEvents(event, onConditionFunc, isLHSTrigger = false) {
                            (StreamEvent?, StreamEvent?)[] evtArr => {
                                candidateEvents = evtArr;
                                // right/full joins have to emit an event even there's no candidate events in lhs.
                                if (lengthof candidateEvents == 0 && (joinType == RIGHT_OUTER_JOIN
                                        || joinType == FULL_OUTER_JOIN)) {
                                    candidateEvents[0] = ((), event);
                                }
                            }
                            () => {
                                if (joinType == RIGHT_OUTER_JOIN || joinType == FULL_OUTER_JOIN) {
                                    candidateEvents[0] = ((), event);
                                }
                            }
                        }

                    } else {
                        match lhsWindow.getCandidateEvents(event, returnTrue, isLHSTrigger = false) {
                            (StreamEvent?, StreamEvent?)[] tmpEvtArr => {
                                int k = 0;
                                (StreamEvent?, StreamEvent?)[] tmpCandidateEvents;
                                foreach lEvt in tmpEvtArr {
                                    // match lhs event of the tuple
                                    match lEvt[0] {
                                        StreamEvent evt => {
                                            // clone the event from the window (usually the type of this is expired).
                                            StreamEvent clonedEvt = evt.clone();
                                            // so, set the event type to triggered event's eventType
                                            clonedEvt.eventType = event.eventType;
                                            match rhsWindow.getCandidateEvents(clonedEvt, onConditionFunc) {
                                                (StreamEvent?, StreamEvent?)[] evtArr => {
                                                    tmpCandidateEvents = evtArr;
                                                    // right join have to emit an event even there's
                                                    // no candidate events in lhs.
                                                    if (lengthof tmpCandidateEvents == 0) {
                                                        tmpCandidateEvents[0] = (clonedEvt, ());
                                                    }
                                                }
                                                () => {
                                                    tmpCandidateEvents[0] = (clonedEvt, ());
                                                }
                                            }
                                            foreach tEvt in tmpCandidateEvents {
                                                candidateEvents[k] = tEvt;
                                                k += 1;
                                            }
                                        }
                                        () => {
                                            // do nothing.
                                        }
                                    }
                                }
                            }
                            () => {
                                // do nothing.
                            }
                        }
                    }
                    foreach evtTuple in candidateEvents {
                        joinedEvents[i] = joinEvents(evtTuple[0], evtTuple[1], lhsTriggered = false);
                        i += 1;
                    }
                }
            }
        }

        StreamEvent[] outputEvents;
        i = 0;
        foreach e in joinedEvents {
            match e {
                StreamEvent s => {
                    outputEvents[i] = s;
                    i += 1;
                }
                () => {
                }
            }
        }
        nextProcessor(outputEvents);
    }

    public function setLHS(string streamName, Window windowInstance) {
        self.lhsStream = streamName;
        self.lhsWindow = windowInstance;
    }

    public function setRHS(string streamName, Window windowInstance) {
        self.rhsStream = streamName;
        self.rhsWindow = windowInstance;
    }

    public function setUnidirectionalStream(string streamName) {
        self.unidirectionalStream = streamName;
    }

    function joinEvents(StreamEvent? lhsEvent, StreamEvent? rhsEvent, boolean lhsTriggered = true)
                 returns StreamEvent? {
        StreamEvent? joined = ();
        if (joinType == LEFT_OUTER_JOIN) {
            // Left outer join: Returns all the events of left stream
            // even if there are no matching events in the right stream.
            match lhsEvent {
                StreamEvent lhs => {
                    joined = lhs.clone();
                    match rhsEvent {
                        StreamEvent rhs => {
                            joined.addData(rhs.data);
                        }
                        () => {
                            // nothing to do.
                        }
                    }
                }
                () => {
                    // nothing to do.
                }
            }
        } else if (joinType == RIGHT_OUTER_JOIN) {
            // Right outer join: Returns all the events of the right stream
            // even if there are no matching events in the left stream.
            match rhsEvent {
                StreamEvent rhs => {
                    joined = rhs.clone();
                    match lhsEvent {
                        StreamEvent lhs => {
                            joined.addData(lhs.data);
                        }
                        () => {
                            // nothing to do.
                        }
                    }
                }
                () => {
                    // nothing to do.
                }
            }
        } else if (joinType == FULL_OUTER_JOIN) {
            // Full outer join: output event are generated for each incoming
            // event even if there are no matching events in the other stream.
            if (lhsTriggered) {
                match lhsEvent {
                    StreamEvent lhs => {
                        joined = lhs.clone();
                        match rhsEvent {
                            StreamEvent rhs => {
                                joined.addData(rhs.data);
                            }
                            () => {
                                // nothing to do.
                            }
                        }
                    }
                    () => {
                        // nothing to do.
                    }
                }
            } else {
                match rhsEvent {
                    StreamEvent rhs => {
                        joined = rhs.clone();
                        match lhsEvent {
                            StreamEvent lhs => {
                                joined.addData(lhs.data);
                            }
                            () => {
                                // nothing to do.
                            }
                        }
                    }
                    () => {
                        // nothing to do.
                    }
                }
            }
        } else {
            // Inner join (join): The output is generated only if
            // there is a matching event in both the streams.
            StreamEvent lEvt = lhsEvent but {
                () => new StreamEvent({}, CURRENT, 1)
            };
            StreamEvent rEvt = rhsEvent but {
                () => new StreamEvent({}, CURRENT, 1)
            };
            if (lhsTriggered) {
                joined = lEvt.clone();
                joined.addData(rEvt.data);
            } else {
                joined = rEvt.clone();
                joined.addData(lEvt.data);
            }
        }
        return joined;
    }

    function returnTrue(map e1Data, map e2Data) returns boolean {
        return true;
    }
};

public function createStreamJoinProcessor(function (any) nextProcessor, JoinType joinType,
            (function (map e1Data, map e2Data) returns boolean)? conditionFunc = ())
                    returns StreamJoinProcessor {
    StreamJoinProcessor joinProcesor = new(nextProcessor, joinType, conditionFunc);
    return joinProcesor;
}