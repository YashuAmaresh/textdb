import { Component , ViewChild} from '@angular/core';

import { CurrentDataService } from './current-data-service';
import { ModalComponent } from 'ng2-bs3-modal/ng2-bs3-modal';


declare var jQuery: any;
declare var Backbone : any;

declare var PrettyJSON : any;

@Component({
    moduleId: module.id,
    selector: 'side-bar-container',
    templateUrl: './side-bar.component.html',
    styleUrls: ['style.css']
})
export class SideBarComponent {
    data: any;
    attributes: string[] = [];
    operator = "Operator";
    submitted = false;
    operatorId: number;


    tempSubmitted = false;

    hiddenList : string[] = ["operatorType"];
    selectorList : string[] = ["matchingType","nlpEntityType","splitType","sampleType"].concat(this.hiddenList);
    matcherList : string[] = ["conjunction","phrase","substring"];
    nlpEntityList : string[] = ["noun","verb","adjective","adverb","ne_all","number","location","person","organization","money","percent","date","time"];
    regexSplitList : string[] = ["left", "right", "standalone"];
    samplerList : string[] = ["random", "firstk"];

    @ViewChild('MyModal')
    modal: ModalComponent;
    ModalOpen() {
        this.modal.open();
    }
    ModalClose() {
        this.modal.close();
    }

    checkInHidden(name : string){
      return jQuery.inArray(name,this.hiddenList);
    }
    checkInSelector(name: string){
      return jQuery.inArray(name,this.selectorList);
    }

    constructor(private currentDataService: CurrentDataService) {
        currentDataService.newAddition$.subscribe(
            data => {
                this.submitted = false;
                // this.tempSubmitted = false;
                this.data = data.operatorData;
                this.operatorId = data.operatorNum;
                this.operator = data.operatorData.properties.title;
                this.attributes = [];
                for(var attribute in data.operatorData.properties.attributes){
                    this.attributes.push(attribute);
                }
            });

        currentDataService.checkPressed$.subscribe(
            data => {
                console.log(data);
                this.submitted = false;
                
                if (data.code === 0) {
                  // this.tempSubmitted = true;
                  var node = new PrettyJSON.view.Node({
                    el: jQuery("#elem"),
                    data: JSON.parse(data.message)
                  });
                } else {
                  // this.tempSubmitted = true;
                  var node = new PrettyJSON.view.Node({
                    el: jQuery("#elem"),
                    data: JSON.parse(data.message)
                  });
                }

                this.ModalOpen();
            });
    }

    humanize(name: string): string{
        var frags = name.split('_');
        for (var i=0; i<frags.length; i++) {
            frags[i] = frags[i].charAt(0).toUpperCase() + frags[i].slice(1);
        }
        return frags.join(' ');
    }

    onSubmit() {
        this.submitted = true;
        jQuery('#the-flowchart').flowchart('setOperatorData', this.operatorId, this.data);
        this.currentDataService.setData(jQuery('#the-flowchart').flowchart('getData'));
    }

    onDelete(){
          this.submitted = false;
          this.tempSubmitted = false;
          this.operator = "Operator";
          this.attributes = [];
          jQuery("#the-flowchart").flowchart("deleteOperator", this.operatorId);
          this.currentDataService.setData(jQuery('#the-flowchart').flowchart('getData'));
    }
}
