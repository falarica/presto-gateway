import { Component, OnInit } from '@angular/core';
import { BsModalRef } from 'ngx-bootstrap';
import { Subject } from 'rxjs';
import { FormGroup, FormControl, FormArray, Validators } from '@angular/forms';

@Component({
  selector: 'app-add-rule',
  templateUrl: './add-rule.component.html',
  styleUrls: ['./add-rule.component.css']
})
export class AddRuleComponent implements OnInit {
  public onClose: Subject<any>;
  model: any = {};
  title;
  AddRuleForm: FormGroup;
  submitted = false;
  myFormValueChanges;

  constructor(private _bsModalRef: BsModalRef) { }

  ngOnInit(): void {
    this.onClose = new Subject();
    this.AddRuleForm = new FormGroup({
      'name': new FormControl(null, [Validators.required]),
      'type': new FormControl('QUEUEDQUERY', [Validators.required]),
      'properties': new FormGroup({
        "numQueuedQueries" : new FormControl(null, [Validators.required])
      })
    });

    this.myFormValueChanges = this.AddRuleForm.controls['type'].valueChanges;

    this.myFormValueChanges.subscribe(tp => {
      if(tp=='ROUNDROBIN' || tp=='RANDOMCLUSTER')
      {
        this.AddRuleForm.controls.properties['controls']['numQueuedQueries'].setValidators(null);
      }
      else{
        this.AddRuleForm.controls.properties['controls']['numQueuedQueries'].setValidators([Validators.required]);
      }
      this.AddRuleForm.controls.properties['controls']['numQueuedQueries'].updateValueAndValidity();
    });
  }

 public onConfirm(): void {
    this.submitted = true;
    // stop here if form is invalid
    if (this.AddRuleForm.invalid) {
      return;
    }
    let payload = {...this.AddRuleForm.value};

    if(this.AddRuleForm.value.type=='RUNNINGQUERY')
    {
      payload['properties'].numRunningQueries=payload['properties'].numQueuedQueries;
      delete payload['properties'].numQueuedQueries;
    }

    this.onClose.next({ isSubmit: true, payload: this.AddRuleForm.value });
    this._bsModalRef.hide();
  }

  get f() {
    return this.AddRuleForm.controls;
  }

  onCancel() {
    this.onClose.next({ isSubmit: false });
    this._bsModalRef.hide();
  }

  onAddCatalog() {
  }
}
