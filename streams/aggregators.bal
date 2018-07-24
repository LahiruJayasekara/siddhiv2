import ballerina/reflect;
import ballerina/crypto;
import ballerina/math;
import collections;

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


public type Max object {

    public collections:Queue maxQueue;
    public int iMax = 0;
    public float fMax = 0.0;

    public new() {

    }

    public function process(any value, EventType eventType) returns any {
        match value {
            int i => {
                if (eventType == "CURRENT") {
                    iMax = i;
                } else if (eventType == "EXPIRED"){
                    iMax = i;
                } else if (eventType == "RESET"){
                    iMax = 0;
                }
                return iMax;
            }
            float f => {
                if (eventType == "CURRENT") {
                    fMax = f;
                } else if (eventType == "EXPIRED"){
                    fMax = f;
                } else if (eventType == "RESET"){
                    fMax = 0.0;
                }
                return fMax;
            }
            any a => {
                error e = { message : "Unsupported attribute type found" };
                return e;
            }
        }
    }

    public function clone() returns Aggregator {
        Max maxAggregator = new ();
        return maxAggregator;
    }

};



public type StdDev object {

    public float mean = 0.0;
    public float stdDeviation = 0.0;
    public float sum = 0.0;
    public int count = 0;

    public new() {

    }

    public function process(any value, EventType eventType) returns any {
        float fVal;
        match value {
            int i => {
                fVal = <float> i;
            }
            float f => {
                fVal = f;
            }
            any a => {
                error e = { message: "Unsupported attribute type found" };
                return e;
            }
        }

        if (eventType == "CURRENT") {
            // See here for the algorithm: http://www.johndcook.com/blog/standard_deviation/
            count++;
            if (count == 0) {
                return ();
            } else if (count == 1) {
                sum = fVal;
                mean = fVal;
                stdDeviation = 0.0;
                return 0.0;
            } else {
                float oldMean = mean;
                sum += fVal;
                mean = sum / count;
                stdDeviation += (fVal - oldMean) * (fVal - mean);
                return math:sqrt(stdDeviation / count);
            }
        } else if (eventType == "EXPIRED") {
            count--;
            if (count == 0) {
                sum = 0.0;
                mean = 0.0;
                stdDeviation = 0.0;
                return ();
            } else if (count == 1) {
                return 0.0;
            } else {
                float oldMean = mean;
                sum -= fVal;
                mean = sum / count;
                stdDeviation -= (fVal - oldMean) * (fVal - mean);
                return math:sqrt(stdDeviation / count);
            }
        } else if (eventType == "RESET") {
            mean = 0.0;
            stdDeviation = 0.0;
            sum = 0.0;
            count = 0;
            return 0.0;
        } else {
            return ();
        }
    }

    public function clone() returns Aggregator {
        StdDev stdDevAggregator = new ();
        return stdDevAggregator;
    }

};

public type MaxForever object {

    public int? iMax = ();
    public float? fMax = ();

    public new() {

    }

    public function process(any value, EventType eventType) returns any {
        match value {
            int i => {
                if (eventType == "CURRENT" || eventType == "EXPIRED") {
                    match iMax {
                        int max => {
                            iMax = (max < i) ? i : max;
                        }
                        () => {
                            iMax = i;
                        }
                    }
                }
                return iMax;
            }
            float f => {
                if (eventType == "CURRENT" || eventType == "EXPIRED") {
                    match fMax {
                        float max => {
                            fMax = (max < f) ? f : max;
                        }
                        () => {
                            fMax = f;
                        }
                    }
                }
                return fMax;
            }
            any a => {
                error e = { message : "Unsupported attribute type found" };
                return e;
            }
        }
    }

    public function clone() returns Aggregator {
        MaxForever maxForeverAggregator = new ();
        return maxForeverAggregator;
    }

};

public type MinForever object {

    public int? iMin = ();
    public float? fMin = ();

    public new() {

    }

    public function process(any value, EventType eventType) returns any {
        match value {
            int i => {
                if (eventType == "CURRENT" || eventType == "EXPIRED") {
                    match iMin {
                        int min => {
                            iMin = (min > i) ? i : min;
                        }
                        () => {
                            iMin = i;
                        }
                    }
                }
                return iMin;
            }
            float f => {
                if (eventType == "CURRENT" || eventType == "EXPIRED") {
                    match fMin {
                        float min => {
                            fMin = (min > f) ? f : min;
                        }
                        () => {
                            fMin = f;
                        }
                    }
                }
                return fMin;
            }
            any a => {
                error e = { message : "Unsupported attribute type found" };
                return e;
            }
        }
    }

    public function clone() returns Aggregator {
        MinForever minForeverAggregator = new ();
        return minForeverAggregator;
    }

};


public function createSumAggregator() returns Sum {
    Sum sumAggregator = new ();
    return sumAggregator;
}



