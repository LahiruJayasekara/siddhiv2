import ballerina/io;

public type JoinProcesor object {

    private function (any) nextProcessor;
    public LengthWindow? lhsWindow;
    public LengthWindow? rhsWindow;
    public string lhsStream;
    public string rhsStream;

    public new (nextProcessor) {
        lhsWindow = ();
        rhsWindow = ();
        lhsStream = "lhs";
        rhsStream = "rhs";
    }

    public function process(StreamEvent[] streamEvents) {
        io:println("before join: ", streamEvents);
        io:println("lhs join: ", lhsWindow.getCurrentEvents());
        io:println("rhs join: ", rhsWindow.getCurrentEvents());
        io:println("after join: ", streamEvents);

        nextProcessor(streamEvents);
    }

    public function setLHSWindow(LengthWindow lhsWindsow) {
        self.lhsWindow = lhsWindsow;
    }

    public function setRHSWindow(LengthWindow rhsWindsow) {
        self.rhsWindow = rhsWindsow;
    }

    //public function setLHSStream(string lhsStream) {
    //    self.lhsStream = lhsStream;
    //}
    //
    //public function setRHSStream(string rhsStream) {
    //    self.rhsStream = rhsStream;
    //}
};

public function createJoinProcesor(function (any) nextProcessor) returns JoinProcesor {
    JoinProcesor joinProcesor = new(nextProcessor);
    return joinProcesor;
}