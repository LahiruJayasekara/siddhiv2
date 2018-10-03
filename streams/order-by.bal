public type OrderBy object {

    public function (StreamEvent[]) nextProcessorPointer;
    public string[] sortFieldMetadata;

    public new(nextProcessorPointer, sortFieldMetadata) {

    }

    public function process(StreamEvent[] streamEvents) {
        io:println(streamEvents);
        topDownMergeSort(streamEvents, sortFieldMetadata);
        nextProcessorPointer(streamEvents);
    }
};

public function createOrderBy(function (StreamEvent[]) nextProcessorPointer, string[] sortFieldMetadata)
                    returns OrderBy {
    return new(nextProcessorPointer, sortFieldMetadata);
}

function topDownMergeSort(StreamEvent[] a, string[] sortFieldMetadata) {
    int index = 0;
    int n = lengthof a;
    StreamEvent[] b;
    while ( index < n) {
        b[index] = a[index];
        index++;
    }
    topDownSplitMerge(b, 0, n, a, sortFunc, sortFieldMetadata);
}

function topDownSplitMerge(StreamEvent[] b, int iBegin, int iEnd, StreamEvent[] a,
                           function (StreamEvent, StreamEvent, string[], int) returns int sortFunc,
                           string[] sortFieldMetadata) {

    if (iEnd - iBegin < 2) {
        return;
    }
    int iMiddle = (iEnd + iBegin) / 2;
    topDownSplitMerge(a, iBegin, iMiddle, b, sortFunc, sortFieldMetadata);
    topDownSplitMerge(a, iMiddle, iEnd, b, sortFunc, sortFieldMetadata);
    topDownMerge(b, iBegin, iMiddle, iEnd, a, sortFunc, sortFieldMetadata);
}

function topDownMerge(StreamEvent[] a, int iBegin, int iMiddle, int iEnd, StreamEvent[] b,
                      function (StreamEvent, StreamEvent, string[], int) returns int sortFunc,
                      string[] sortFieldMetadata) {
    int i = iBegin;
    int j = iMiddle;

    int k = iBegin;
    while (k < iEnd) {
        if (i < iMiddle && (j >= iEnd || sortFunc(a[i], a[j], sortFieldMetadata, 0) < 0)) {
            b[k] = a[i];
            i = i + 1;
        } else {
            b[k] = a[j];
            j = j + 1;
        }
        k++;
    }
}

function numberSort(int|float x, int|float y) returns int {
    match x {
        int ix => {
            match y {
                int iy => {
                    return ix - iy;
                }
                float fy => {
                    return <float>ix < fy ? -1 : <float> ix == fy ? 0 : 1;
                }
            }
        }

        float fx => {
            match y {
                int iy => {
                    return fx < (<float>iy) ? -1 : fx == <float>iy ? 0 : 1;
                }
                float fy => {
                    return fx < fy ? -1 : fx == fy ? 0 : 1;
                }
            }
        }
    }
}

function stringSort(string x, string y) returns int {

    byte[] v1 = x.toByteArray("UTF-8");
    byte[] v2 = y.toByteArray("UTF-8");

    int len1 = lengthof v1;
    int len2 = lengthof v2;
    int lim = len1 < len2 ? len1 : len2;
    int k = 0;
    while (k < lim) {
        int c1 =<int>v1[k];
        int c2 = <int>v2[k];
        if (c1 != c2) {
            return c1 - c2;
        }
        k++;
    }
    return len1 - len2;
}

function sortFunc(StreamEvent x, StreamEvent y, string[] sortFieldMetadata, int fieldIndex) returns int {
    match x.data[OUTPUT + "." + sortFieldMetadata[fieldIndex]] { //even indices contain the field name
        string sx => {
            match y.data[OUTPUT + "." + sortFieldMetadata[fieldIndex]] {
                string sy => {
                    int c;
                    //odd indices contain the sort type (ascending/descending)
                    if (sortFieldMetadata[fieldIndex + 1].equalsIgnoreCase("ascending")) {
                        c = stringSort(sx, sy);
                    } else {
                        c = stringSort(sy, sx);
                    }
                    // if c == 0 then check for the next sort field
                    return callNextSortFunc(x, y, c, sortFieldMetadata, 2 * (fieldIndex + 1));
                }
                any a => {
                    error err = {message: "Values to be orderred contain non-string values in field: " +
                        sortFieldMetadata[fieldIndex]};
                    throw err;
                }
            }
        }
        int|float ax => {
            match y.data[OUTPUT + "." + sortFieldMetadata[fieldIndex]] {
                int|float ay => {
                    int c;
                    if (sortFieldMetadata[fieldIndex + 1].equalsIgnoreCase("ascending")) {
                        c = numberSort(ax, ay);
                    } else {
                        c = numberSort(ay, ax);
                    }
                    return callNextSortFunc(x, y, c, sortFieldMetadata, 2 * (fieldIndex + 1));
                }
                any aa => {
                    error err = {message: "Values to be orderred contain non-number values in field: " +
                        sortFieldMetadata[fieldIndex]};
                    throw err;
                }
            }

        }
        any a => {
            error err = {message: "Values of types other than strings and numbers cannot be sorted in field: " +
                sortFieldMetadata[fieldIndex]};
            throw err;
        }
    }
}

function callNextSortFunc(StreamEvent x, StreamEvent y, int c, string[] sortFieldMetadata, int fieldIndex) returns int {
    int result = c;
    if (result == 0 && (lengthof sortFieldMetadata > fieldIndex)) {
        result = sortFunc(x, y, sortFieldMetadata, fieldIndex);
    }
    return result;
}