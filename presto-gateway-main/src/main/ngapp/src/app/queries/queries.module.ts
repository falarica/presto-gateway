import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { QueryComponent } from './query.component';
import { Routes, RouterModule } from '@angular/router';
import { NgxDatatableModule } from '@swimlane/ngx-datatable';
import { HighlightJsModule } from 'ngx-highlight-js';
import { FormsModule } from '@angular/forms';
import { TooltipModule } from 'ngx-bootstrap/tooltip';

const routes: Routes = [
  {
    path: '',
    pathMatch: 'full',
    component: QueryComponent,
    data: {
      title: 'Forms Works'
    }
  }
];

@NgModule({
  declarations: [QueryComponent],
  imports: [
    CommonModule,
    NgxDatatableModule,
    HighlightJsModule,
    TooltipModule.forRoot(),
    FormsModule,
    RouterModule.forChild(routes)
  ]
})
export class QueriesModule { }
