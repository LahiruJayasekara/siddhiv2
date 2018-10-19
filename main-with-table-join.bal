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

import ballerina/runtime;
import ballerina/io;
import ballerina/reflect;
import streams;
import ballerina/math;

public type Stock record {
    string symbol;
    float price;
    int volume;
};

type Twitter record {
    string user;
    string tweet;
    string company;
};

type StockWithPrice record {
    string? symbol;
    string? tweet;
    float? price;
};

StockWithPrice[] globalEventsArray = [];
int index = 0;

stream<Twitter> twitterStream;
stream<StockWithPrice> stockWithPriceStream;

table<Stock> stocksTable = table {
    { symbol, price, volume },
    [
        {"WSO2", 55.6, 100},
        {"IBM", 58.6, 70}
    ]
};

public function main(string... args) {

    tableJoinFunc();

    Twitter t1 = { user: "User1", tweet: "Hello WSO2, happy to be a user.", company: "WSO2" };
    Twitter t2 = { user: "User2", tweet: "Hello IBM, happy to be a user.", company: "IBM" };

    stockWithPriceStream.subscribe(printCompanyStockPrice);

    twitterStream.publish(t1);
    runtime:sleep(1000);
    twitterStream.publish(t2);
    runtime:sleep(1000);

    io:println(globalEventsArray);
}


//    forever {
//        from twitterStream window lengthWindow(1)
//        join queryStocksTable(stockStream.symbol, 1) as tb
//        select tb.symbol, stockStream.tweet, tb.price
//        => (StockWithPrice[] stocks) {
//            foreach s in stocks {
//                stockWithPriceStream.publish(s)
//            }
//        }
//    }
function tableJoinFunc() {

    function (map[]) outputFunc = function (map[] events) {
        foreach m in events {
            // just cast input map into the output type
            StockWithPrice o = check <StockWithPrice>m;
            stockWithPriceStream.publish(o);
        }
    };

    // Output processor
    streams:OutputProcess outputProcess = streams:createOutputProcess(outputFunc);

    // Selector
    streams:SimpleSelect select = streams:createSimpleSelect(outputProcess.process,
        function (streams:StreamEvent e) returns map {
            return {
                "symbol": e.data["tb.symbol"],
                "tweet": e.data["twitterStream.tweet"],
                "price": e.data["tb.price"]
            };
        }
    );

    // Join processor
    streams:TableJoinProcessor tableJoinProcessor = streams:createTableJoinProcessor(select.process, "JOIN",
        function (streams:StreamEvent s) returns map[] {
            map[] result;
            foreach i, r in queryStocksTable(<string> s.data["twitterStream.company"], 1) {
                result[i] = <map>r;
            }
            return result;
        });

    // Window processor
    streams:LengthWindow lengthWindow = streams:lengthWindow(1, nextProcessPointer = tableJoinProcessor.process);

    // Set the tableName, streamName, windowProcessors to the table join processor
    tableJoinProcessor.setJoinProperties("tb", "twitterStream", lengthWindow);

    twitterStream.subscribe(function (Twitter i) {
            map keyVal = <map>i;
            streams:StreamEvent[] eventArr = streams:buildStreamEvent(keyVal, "twitterStream");
            lengthWindow.process(eventArr);
        }
    );
}

public function queryStocksTable(string symbol, int volume) returns table<Stock> {
    table<Stock> result = table {
        { symbol, price, volume }, []
    };
    foreach stock in stocksTable {
        if (stock.symbol == symbol && stock.volume > volume) {
            var ret = result.add(stock);
        }
    }
    return result;
}

function printCompanyStockPrice(StockWithPrice s) {
    addToGlobalEventsArray(s);
}

function addToGlobalEventsArray(StockWithPrice s) {
    globalEventsArray[index] = s;
    index = index + 1;
}