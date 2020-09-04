import { Component, OnInit, AfterViewInit } from '@angular/core';
import { BsModalRef } from 'ngx-bootstrap';
import { Subject } from 'rxjs';
import { FormGroup, FormControl, FormArray, Validators } from '@angular/forms';

@Component({
  selector: 'app-add-policy',
  templateUrl: './add-policy.component.html',
  styleUrls: ['./add-policy.component.css']
})
export class AddPolicyComponent implements OnInit, AfterViewInit {
  public onClose: Subject<any>;
  model: any = {};
  title;
  rules;
  formType;
  data;
  policyForm: FormGroup;
  submitted = false;
  message = '';
  controlArray = [];

  constructor(private _bsModalRef: BsModalRef) { }

  ngOnInit(): void {
    this.onClose = new Subject();
    this.controlArray = this.rules.map(c => new FormControl(false));

    this.policyForm = new FormGroup({
      'name': new FormControl(this.data == undefined ? null : this.data.name, [Validators.required]),
      'rule' : new FormControl(this.data == undefined ? null : this.data.routingRules[0], [Validators.required]),
      'routingRules': new FormArray(this.controlArray)
    });

    if (this.formType == 'edit') {
      this.rules.forEach((element, index) => {
        if (this.data.routingRules.indexOf(element.name) > -1)
          this.controlArray[index].setValue(true);
      });
    }
  }

  ngAfterViewInit() {

  }

  public onConfirm(): void {
    this.submitted = true;
    let selectedOrderIds = [];

    // stop here if form is invalid
    if (this.policyForm.invalid) {
      return;
    }

    this.onClose.next({ isSubmit: true, payload: { name: this.policyForm.value.name, routingRules: [this.policyForm.value.rule] } });
    this._bsModalRef.hide();
  }

  get f() {
    return this.policyForm.controls;
  }

  onCancel() {
    this.onClose.next({ isSubmit: false });
    this._bsModalRef.hide();
  }

  onAddCatalog() {
  }

}
