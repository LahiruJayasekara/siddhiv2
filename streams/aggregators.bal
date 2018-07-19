import ballerina/reflect;
import ballerina/crypto;

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

public type Average object {

    public int count = 0;
    public float sum = 0.0;

    public new() {

    }

    public function process(any value, EventType eventType) returns any {
        match value {
            int i => {
                if (eventType == "CURRENT") {
                    sum += i;
                    count++;
                } else if (eventType == "EXPIRED"){
                    sum -= i;
                    count--;
                } else if (eventType == "RESET"){
                    sum = 0;
                    count = 0;
                }
            }
            float f => {
                if (eventType == "CURRENT") {
                    sum += f;
                    count++;
                } else if (eventType == "EXPIRED"){
                    sum -= f;
                    count--;
                } else if (eventType == "RESET"){
                    sum = 0.0;
                    count = 0;
                }
            }
            any a => {
                error e = { message : "Unsupported attribute type found" };
                return e;
            }
        }
        return (count > 0) ? (sum/count) : 0.0;
    }

    public function clone() returns Aggregator {
        Average avgAggregator = new ();
        return avgAggregator;
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

public type DistinctCount object {

    public map<int> distinctValues;

    public new() {

    }

    public function process(any value, EventType eventType) returns any {
        string key = crypto:crc32(value);
        if (eventType == "CURRENT") {
            int preVal = distinctValues[key] ?: 0;
            preVal++;
            distinctValues[key] = preVal;
        } else if (eventType == "EXPIRED"){
            int preVal = distinctValues[key] ?: 1;
            preVal--;
            if (preVal <= 0) {
                _ = distinctValues.remove(key);
            } else {
                distinctValues[key] = preVal;
            }
        } else if (eventType == "RESET"){
            distinctValues.clear();
        }
        return lengthof distinctValues;
    }

    public function clone() returns Aggregator {
        DistinctCount distinctCountAggregator = new ();
        return distinctCountAggregator;
    }
};

public function createSumAggregator() returns Sum {
    Sum sumAggregator = new ();
    return sumAggregator;
}



