import { Component} from '@angular/core';

import { SideBarComponent }   from './side-bar.component';


declare var jQuery: any;

@Component({
	moduleId: module.id,
	selector: 'flowchart-container',
	template: `
		<div id="flow-chart-container">
			<div id="the-flowchart"></div>
		</div>
	`,
	styleUrls: ['style.css']
})
export class TheFlowchartComponent {
	theSideBar : SideBarComponent;
	initialize(data: any) {
		var current = this;
		jQuery('#the-flowchart').flowchart({
			data: data,
    	multipleLinksOnOutput: true,
			onOperatorSelect : function (operatorId){
				var data = jQuery('#the-flowchart').flowchart('getData');
				console.log("Selected Operator = " + operatorId);
				console.log("HELLO Selected");
				return true;
			}
		});
	}



}
