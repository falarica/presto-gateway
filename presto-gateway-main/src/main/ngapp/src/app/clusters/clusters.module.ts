import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ClusterComponent } from './cluster.component';
import { RouterModule, Routes } from '@angular/router';
import { AddClusterComponent } from './add-cluster/add-cluster.component';
import { NgxDatatableModule } from '@swimlane/ngx-datatable';
import { ModalModule } from 'ngx-bootstrap/modal';
import { ReactiveFormsModule } from '@angular/forms';
import { ClusterDetailComponent } from './cluster-detail/cluster-detail.component';
import { EditClusterComponent } from './edit-cluster/edit-cluster.component';

const routes: Routes = [
  {
    path: '',
    pathMatch: 'full',
    component: ClusterComponent,
    data: {
      title: 'Forms Works'
    }
  }
];
@NgModule({
  declarations: [ClusterComponent, AddClusterComponent, ClusterDetailComponent, EditClusterComponent],
  imports: [
    CommonModule,
    NgxDatatableModule,
    ReactiveFormsModule,
    ModalModule.forRoot(),
    RouterModule.forChild(routes)
  ],
  entryComponents : [AddClusterComponent,ClusterDetailComponent,EditClusterComponent]
})
export class ClustersModule { }
