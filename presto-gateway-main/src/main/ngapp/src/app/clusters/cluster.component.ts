import { Component, OnInit, TemplateRef } from '@angular/core';
import { PrestInfoService } from 'app/services/presto-info.service';
import { ToastrService } from 'ngx-toastr';
import { BsModalService } from 'ngx-bootstrap/modal';
import { BsModalRef } from 'ngx-bootstrap/modal/bs-modal-ref.service';
import { AddClusterComponent } from './add-cluster/add-cluster.component';
import { ClusterDetailComponent } from './cluster-detail/cluster-detail.component';
import { EditClusterComponent } from './edit-cluster/edit-cluster.component';

@Component({
  selector: 'app-cluster',
  templateUrl: './cluster.component.html',
  styleUrls: ['./cluster.component.css']
})
export class ClusterComponent implements OnInit {
  clusterList = []
  columns = [];
  loadingIndicator: boolean = true;
  modalRef: BsModalRef;
  idToBeDeleted;

  constructor(
    private prestoData: PrestInfoService,
    private modalService: BsModalService,
    private toastr: ToastrService) { }

  ngOnInit(): void {
    // Initial Load
    this.getClusterList();

    // Columns settings
    this.columns = [
      { prop: 'name' },
      { name: 'clusterUrl' },
      { name: 'location' },
      { prop:'adminName' ,name: 'adminName' },
      { prop:'adminPassword' ,name: 'adminPassword' }
    ];
  }

  // Open Modal for add cluster form
  openModal()
  {
    this.modalRef = this.modalService.show(AddClusterComponent,  {
      initialState: {
        title: 'Add Cluster',
        addData: {}
      }
    });
    this.modalRef.content.onClose.subscribe(result => {
      if(result.isSubmit)
      {
        this.addCluster(result.payload);
      }
  })
  }

  // Open Modal for Edit cluster form
  openEditModal(row)
  {
    this.modalRef = this.modalService.show(EditClusterComponent,  {
      initialState: {
        title: 'Edit Cluster',
        data: row
      }
    });
    this.modalRef.content.onClose.subscribe(result => {
      if(result.isSubmit)
      {
        this.editCluster(result.payload);
      }
  })
  }

// Open Modal for Edit cluster form
  openDetailModal(row)
  {
    this.modalRef = this.modalService.show(ClusterDetailComponent,  {
      initialState: {
        title: 'Cluster Detail',
        data: row
      }
    });
    this.modalRef.content.onClose.subscribe(result => {
      if(result.isSubmit)
      {
      }
  })
  }

  // Add cluster function
  addCluster(payload)
  {
    this.prestoData.addCluster(payload)
    .subscribe(response => {
      this.getClusterList();
      this.toastr.success('Add Cluster', 'Cluster added successfully...')
    }, error => {
      this.toastr.error(error.devMessage, 'Error', {
        timeOut: 5000,
      });
    });
  }

  // Add cluster function
  editCluster(payload)
  {
    this.prestoData.editCluster(payload)
    .subscribe(response => {
      this.getClusterList();
      this.toastr.success('Edit Cluster', 'Cluster updated successfully...')
    }, error => {
      this.toastr.error(error.devMessage, 'Error', {
        timeOut: 5000,
      });
    });
  }

   // Get cluster list function
  getClusterList()
  {
    this.prestoData.getClusters()
      .subscribe(response => {
        this.clusterList = response;
      }, error => {
        this.toastr.error(error.devMessage, 'Error', {
          timeOut: 5000,
        });
      });
  }

  // Delete cluster function
  deleteCluster(cluster)
  {
    this.prestoData.deleteCluster(cluster.name)
      .subscribe(response => {
        this.getClusterList();
        this.toastr.success('Cluster Deletion', 'Cluster ' + cluster.name + ' deleted successfully ..')
      }, error => {
        this.toastr.error(error.devMessage, 'Error', {
          timeOut: 5000,
        });
      });
  }
  //deleteCluster(row)
  openDelModal(template: TemplateRef<any>, id: any) {
    this.modalRef = this.modalService.show(template, { class: 'modal-sm' });
    this.idToBeDeleted = id;
  }
  confirm(): void {
    this.modalRef.hide();
    this.deleteCluster(this.idToBeDeleted);
  }

  decline(): void {
    this.modalRef.hide();
  }
}
