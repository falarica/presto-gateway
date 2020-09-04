import { Component, OnInit, ViewEncapsulation } from '@angular/core';
import { BsModalRef } from 'ngx-bootstrap';
import { Subject } from 'rxjs';
import { PrestInfoService } from 'app/services/presto-info.service';

@Component({
  selector: 'app-cluster-detail',
  templateUrl: './cluster-detail.component.html',
  styleUrls: ['./cluster-detail.component.css'],
  encapsulation: ViewEncapsulation.None
})
export class ClusterDetailComponent implements OnInit {
  data;
  title;
  cluster;
  public onClose: Subject<any>;
  
  constructor( private prestoData: PrestInfoService,private _bsModalRef: BsModalRef) { }

  ngOnInit(): void {
    this.onClose = new Subject();
    this.getClustrDetail();
  }

  getClustrDetail()
  {
    this.prestoData.getInfo()
    .subscribe(response => {
      this.cluster = response;
      this.cluster.filter(c=>{
        return c.clusterName=this.data.name;
      })
    }, error => {
      console.log(error);
    });   
   
  }

  onCancel() {
    this.onClose.next({ isSubmit: false });
    this._bsModalRef.hide();
  }

}
