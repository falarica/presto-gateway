import { Component, OnInit } from '@angular/core';
import { BsModalRef } from 'ngx-bootstrap';
import { Subject } from 'rxjs';
import { FormGroup, FormControl, FormArray, Validators } from '@angular/forms';

@Component({
  selector: 'app-edit-cluster',
  templateUrl: './edit-cluster.component.html',
  styleUrls: ['./edit-cluster.component.css']
})
export class EditClusterComponent implements OnInit {

  public onClose: Subject<any>;
  data: any;
  title;
  EditClusterForm: FormGroup;
  submitted = false;

  constructor(private _bsModalRef: BsModalRef) { }

  ngOnInit(): void {

    this.onClose = new Subject();
    this.EditClusterForm = new FormGroup({
      'name': new FormControl(this.data.name, [Validators.required]),
      'clusterUrl': new FormControl(this.data.clusterUrl, [Validators.required]),
      'active': new FormControl(true, [Validators.required]),
      'location': new FormControl(this.data.location, [Validators.required]),
      'adminName' :new FormControl(this.data.adminName),
      'adminPassword' :new FormControl(this.data.adminPassword),
    });
  }

  public onConfirm(): void {
    this.submitted = true;
    // stop here if form is invalid
    if (this.EditClusterForm.invalid) {
      return;
    }

    this.onClose.next({ isSubmit: true, payload: this.EditClusterForm.value });
    this._bsModalRef.hide();
  }

  get f() { return this.EditClusterForm.controls; }

  onCancel() {
    this.onClose.next({ isSubmit: false });
    this._bsModalRef.hide();
  }

  onAddCatalog() {
    const control = new FormControl(null, Validators.required);
    (<FormArray>this.EditClusterForm.get('catalogs')).push(control);
  }

}
