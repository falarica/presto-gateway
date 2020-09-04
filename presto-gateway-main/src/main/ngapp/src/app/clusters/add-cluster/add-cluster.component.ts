import { Component, OnInit } from '@angular/core';
import { BsModalRef } from 'ngx-bootstrap';
import { Subject } from 'rxjs';
import { FormGroup, FormControl, FormArray, Validators } from '@angular/forms';

@Component({
  selector: 'app-add-cluster',
  templateUrl: './add-cluster.component.html',
  styleUrls: ['./add-cluster.component.css']
})
export class AddClusterComponent implements OnInit {
  public onClose: Subject<any>;
  model: any = {};
  title;
  AddClusterForm: FormGroup;
  submitted = false;
  

  constructor(private _bsModalRef: BsModalRef) { }

  ngOnInit(): void {
    
    this.onClose = new Subject();
    this.AddClusterForm = new FormGroup({
      'name': new FormControl(null, [Validators.required]),
      'clusterUrl': new FormControl(null, [Validators.required]),
      'active': new FormControl(true, [Validators.required]),
      'location': new FormControl(null, [Validators.required]),
      'adminName' :new FormControl(null),
      'adminPassword' :new FormControl(null),
    });
  }

  public onConfirm(): void {
    this.submitted = true;
    // stop here if form is invalid
    if (this.AddClusterForm.invalid) {
      return;
    }
   
    this.onClose.next({ isSubmit: true, payload: this.AddClusterForm.value });
    this._bsModalRef.hide();
  }

  get f() { return this.AddClusterForm.controls; }

  onCancel() {
    this.onClose.next({ isSubmit: false });
    this._bsModalRef.hide();
  }

  onAddCatalog() {
    const control = new FormControl(null, Validators.required);
    (<FormArray>this.AddClusterForm.get('catalogs')).push(control);
  }

}
