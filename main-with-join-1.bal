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

type Stock record {
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

stream<Stock> stockStream;
stream<Twitter> twitterStream;
stream<StockWithPrice> stockWithPriceStream;

public function main(string... args) {
    joinFunc();

    Stock s1 = { symbol: "WSO2", price: 55.6, volume: 100 };
    Stock s2 = { symbol: "MBI", price: 74.6, volume: 100 };
    Stock s3 = { symbol: "WSO2", price: 58.6, volume: 100 };

    Twitter t1 = { user: "User1", tweet: "Hello WSO2, happy to be a user.", company: "WSO2" };

    stockWithPriceStream.subscribe(printCompanyStockPrice);

    stockStream.publish(s1);
    runtime:sleep(1000);
    twitterStream.publish(t1);
    runtime:sleep(1000);
    stockStream.publish(s2);
    runtime:sleep(1000);
    stockStream.publish(s3);
    runtime:sleep(1000);

    io:println(globalEventsArray);
}


//  forever {
//      from stockStream window lengthWindow(1)
//      join twitterStream window lengthWindow(1)
//      on stockStream.symbol == twitterStream.company
//          select stockStream.symbol as symbol, twitterStream.tweet as tweet, stockStream.price as price
//          => (StockWithPrice[] emp) {
//                  foreach e in emp {
//                      stockWithPriceStream.publish(e);
//          }
//      }
//  }
//

function joinFunc() {

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
                "symbol": e.data["stockStream.symbol"],
                "tweet": e.data["twitterStream.tweet"],
                "price": e.data["stockStream.price"]
            };
        }
    );

    // On condition
    function (map, map) returns boolean conditionFunc =
    function (map lsh, map rhs) returns boolean {
        return lsh["stockStream.symbol"] == rhs["twitterStream.company"];
    };

    // Join processor
    streams:StreamJoinProcessor joinProcessor = streams:createStreamJoinProcessor(select.process, "RIGHTOUTERJOIN",
        conditionFunc=conditionFunc);

    // Window processors
    streams:Window lengthWindowA = streams:lengthWindow(1, nextProcessPointer = joinProcessor.process);
    streams:Window lengthWindowB = streams:lengthWindow(1, nextProcessPointer = joinProcessor.process);

    // Set the window processors to the join processor
    joinProcessor.setLHS("stockStream", lengthWindowA);
    joinProcessor.setRHS("twitterStream", lengthWindowB);


    // Subscribe to input streams
    stockStream.subscribe(function (Stock i) {
            map keyVal = <map>i;
            streams:StreamEvent[] eventArr = streams:buildStreamEvent(keyVal, "stockStream");
            lengthWindowA.process(eventArr);
        }
    );
    twitterStream.subscribe(function (Twitter i) {
            map keyVal = <map>i;
            streams:StreamEvent[] eventArr = streams:buildStreamEvent(keyVal, "twitterStream");
            lengthWindowB.process(eventArr);
        }
    );
}

function printCompanyStockPrice(StockWithPrice s) {
    addToGlobalEventsArray(s);
}

function addToGlobalEventsArray(StockWithPrice s) {
    globalEventsArray[index] = s;
    index = index + 1;
}