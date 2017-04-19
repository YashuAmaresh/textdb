import { Injectable } from '@angular/core';
import { Subject }    from 'rxjs/Subject';
import { Response, Http } from '@angular/http';
import { Headers } from '@angular/http';

import 'rxjs/add/operator/catch';
import 'rxjs/add/operator/map';
import 'rxjs/add/operator/toPromise';


import { Data } from './data';

declare var jQuery: any;


let clean_data = {
    top: 20,
    left: 20,
    properties: {
        title: 'Operator',
        inputs: {},
        outputs: {}
    }
}

@Injectable()
export class CurrentDataService {
    currentData : Data;
    TEXTDBJSON: any;

    private newAddition = new Subject<any>();
    newAddition$ = this.newAddition.asObservable();

    private checkPressed = new Subject<any>();
    checkPressed$ = this.checkPressed.asObservable();


    private textdbUrl = 'http://localhost:8080/newqueryplan/execute';

    constructor(private http: Http) { }



    getData(): any {
        return this.currentData;
    }

    setData(data : any): void {
        this.currentData = {id: 1, jsonData: data};
    }

    addData(operatorData : any, operatorNum: number, allData : any): void {
        this.newAddition.next({operatorNum: operatorNum, operatorData: operatorData});
        this.setData(allData);
    }

    selectData(operatorNum : number): void {
      var data_now = jQuery("#the-flowchart").flowchart("getOperatorData",operatorNum);
      this.newAddition.next({operatorNum: operatorNum, operatorData: data_now});
      this.setData(jQuery("#the-flowchart").flowchart("getData"));
    }

    clearData() : void {
      this.newAddition.next({operatorNum : 0, operatorData: clean_data});
    }
    processData(): void {
        this.TEXTDBJSON = {operators: {}, links: {}};
        var operators = [];
        var links = [];
        
        var listAttributes : string[] = ["attributes", "dictionaryEntries"]

        for (var operatorIndex in this.currentData.jsonData.operators) {
            var currentOperator = this.currentData.jsonData['operators'];
            if (currentOperator.hasOwnProperty(operatorIndex)) {
                var attributes = {};
                attributes["operatorID"] = operatorIndex;
                for (var attribute in currentOperator[operatorIndex]['properties']['attributes']) {
                    if (currentOperator[operatorIndex]['properties']['attributes'].hasOwnProperty(attribute)) {
                        attributes[attribute] = currentOperator[operatorIndex]['properties']['attributes'][attribute];
                        // if attribute is an array property, and it's not an array
                        if (jQuery.inArray(attribute, listAttributes) != -1 && ! Array.isArray(attributes[attribute])) {
                          attributes[attribute] = attributes[attribute].split(",").map((item) => item.trim());
                        }
                        // if the value is a string and can be converted to a boolean value
                        if (attributes[attribute] instanceof String && Boolean(attributes[attribute])) {
                          attributes[attribute] = (attributes[attribute].toLowerCase() === 'true')
                        }
                    }
                }
                operators.push(attributes);
            }
        }
        for(var link in this.currentData.jsonData.links){
            var destination = {};
            var currentLink = this.currentData.jsonData['links'];
            if (currentLink[link].hasOwnProperty("fromOperator")){
                destination["origin"] = currentLink[link]['fromOperator'].toString();
                destination["destination"] = currentLink[link]['toOperator'].toString();
                links.push(destination);
            }
        }

        this.TEXTDBJSON.operators = operators;
        this.TEXTDBJSON.links = links;
        console.log("about to send request")
        this.sendRequest();
    }

    private sendRequest(): void {
        let headers = new Headers({ 'Content-Type': 'application/json' });
        console.log("TextDB JSON is:");
        console.log(JSON.stringify(this.TEXTDBJSON));
        this.http.post(this.textdbUrl, JSON.stringify(this.TEXTDBJSON), {headers: headers})
            .subscribe(
                data => {
                    console.log("OKAY in getting server data");
                    this.checkPressed.next(data.json());
                },
                err => {
                    console.log("Error in getting server data");
                    console.log(err);
                    this.checkPressed.next(err.json());
                }
            );
    }

    private handleError(error: any): Promise<any> {
        console.error('An error occurred', error); // for demo purposes only
        return Promise.reject(error.message || error);
    }

    getDataSlowly(): Promise<Data[]> {
        return new Promise(resolve => {
            // Simulate server latency with 2 second delay
            setTimeout(() => resolve(this.getData()), 2000);
        });
    }
}
