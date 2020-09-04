import { NgModule } from '@angular/core';
import { DashboardComponent } from './dashboard.component';
import { DashboardRoutingModule } from './dashboard-routing/dashboard-routing.module';
import { NgxDatatableModule } from '@swimlane/ngx-datatable';
import { HighlightJsModule } from 'ngx-highlight-js';
import { TooltipModule } from 'ngx-bootstrap/tooltip';
import { FormsModule } from '@angular/forms';
import { CommonModule } from '@angular/common';
import { FileSizePipe } from './filesize.pipe';

@NgModule({
  imports: [
    CommonModule,
    DashboardRoutingModule,
    NgxDatatableModule,
    HighlightJsModule,
    FormsModule,
    TooltipModule.forRoot(),
  ],
  declarations: [ DashboardComponent,FileSizePipe],
  providers: [
  ]
})
export class DashboardModule { }
