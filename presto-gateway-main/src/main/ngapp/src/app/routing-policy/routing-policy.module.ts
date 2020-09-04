import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule, Routes } from '@angular/router';
import { NgxDatatableModule } from '@swimlane/ngx-datatable';
import { ModalModule } from 'ngx-bootstrap/modal';
import { ReactiveFormsModule } from '@angular/forms';
import { RoutingRulesComponent } from './routing-rules.component';
import { AddRuleComponent } from './add-rule/add-rule.component';
import { AddPolicyComponent } from './add-policy/add-policy.component';

const routes: Routes = [
  {
    path: '',
    pathMatch: 'full',
    component: RoutingRulesComponent,
    data: {
      title: 'Routing Policy'
    }
  }
];
@NgModule({
  declarations: [RoutingRulesComponent, AddRuleComponent, AddPolicyComponent],
  imports: [
    CommonModule,
    NgxDatatableModule,
    ReactiveFormsModule,
    ModalModule.forRoot(),
    RouterModule.forChild(routes)
  ],
  entryComponents : [AddRuleComponent,AddPolicyComponent]
})
export class RoutingPolicyModule { }
