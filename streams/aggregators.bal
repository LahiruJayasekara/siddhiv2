import ballerina/reflect;

public type Sum object {

    public int iSum = 0;
    public float fSum = 0.0;

    public new() {

    }

    public function process(any value, EventType eventType) returns any {
        match value {
            int i => {
                if (eventType == "CURRENT") {
                    iSum += i;
                } else if (eventType == "EXPIRED"){
                    iSum -= i;
                } else if (eventType == "RESET"){
                    iSum = 0;
                }
                return iSum;
            }
            float f => {
                if (eventType == "CURRENT") {
                    fSum += f;
                } else if (eventType == "EXPIRED"){
                    fSum -= f;
                } else if (eventType == "RESET"){
                    fSum = 0.0;
                }
                return fSum;
            }
            any a => {
                error e = { message : "Unsupported attribute type found" };
                return e;
            }
        }
    }

    public function clone() returns Aggregator {
        Sum sumAggregator = new ();
        return sumAggregator;
    }

};

public type Count object {

    public int count = 0;

    public new() {

    }

    public function process(any value, EventType eventType) returns any {
        if (eventType == "CURRENT") {
            count++;
        } else if (eventType == "EXPIRED"){
            count--;
        } else if (eventType == "RESET"){
            count = 0;
        }
        return count;
    }

    public function clone() returns Aggregator {
        Count countAggregator = new ();
        return countAggregator;
    }

};

public function createSumAggregator() returns Sum {
    Sum sumAggregator = new ();
    return sumAggregator;
}

public type Aggregator object {

    public new () {

    }

    public function clone() returns Aggregator {
        Aggregator aggregator = new ();
        return aggregator;
    }

    public function process(any value, EventType eventType) returns any {
        match value {
            int i => {
                return 0;
            }
            float f => {
                return 0.0;
            }
            any a => {
                error e = { message : "Unsupported attribute type found" };
                return e;
            }
        }
    }
};

