import { Component, OnInit, TemplateRef } from '@angular/core';
import { PrestInfoService } from 'app/services/presto-info.service';
import { ToastrService } from 'ngx-toastr';
import { BsModalService } from 'ngx-bootstrap/modal';
import { BsModalRef } from 'ngx-bootstrap/modal/bs-modal-ref.service';
import { AddRuleComponent } from './add-rule/add-rule.component';
import { AddPolicyComponent } from './add-policy/add-policy.component';

@Component({
  selector: 'app-routing-rules',
  templateUrl: './routing-rules.component.html',
  styleUrls: ['./routing-rules.component.css']
})
export class RoutingRulesComponent implements OnInit {
  loadingIndicator: boolean = true;
  modalRef: BsModalRef;
  idToBeDeleted;
  columns = [];
  rules=[]  ;
  policy=[]  ;
  formType='add'
  title='Edit Policy';

  constructor(private prestoData: PrestInfoService,
    private modalService: BsModalService,
    private toastr: ToastrService) { }

  ngOnInit(): void {
    // Columns settings
    this.columns = [
      { prop: 'name' },
      { prop:'type',name: 'type' },
      { prop:'properties.numQueuedQueries',name: 'numQueuedQueries' }
    ];

    this.getPolicyList();
    this.getRulesList();
  }

  // Open Modal for add rule form
  openModal()
  {
    this.modalRef = this.modalService.show(AddRuleComponent,  {
      initialState: {
        title: 'Add Rule',
        addData: {}
      }
    });
    this.modalRef.content.onClose.subscribe(result => {
      if(result.isSubmit)
      {
        this.addRule(result.payload);
      }
  })
  }

  // Open Modal for add policy form
  openPolicyModal()
  {

    if(this.policy.length>0)
    {
      this.title = "Edit Policy";
      this.formType='edit';
    }
    else
    {
      this.title = "Edit Policy";
      this.formType='add';
    }

    this.modalRef = this.modalService.show(AddPolicyComponent,  {
      initialState: {
        title: this.title,
        formType:this.formType,
        rules: this.rules,
        data: this.policy[0]
      }
    });
    this.modalRef.content.onClose.subscribe(result => {
      if(result.isSubmit)
      {
        this.addUpdatePolicy(result.payload);
      }
  })
  }

  // Get cluster list function
  getRulesList()
  {
    this.prestoData.getRoutingRules()
      .subscribe(response => {
        this.rules = response;
      }, error => {
        this.toastr.error(error.devMessage, 'Error', {
          timeOut: 5000,
        });
      });
  }


  // Get policy listing
  getPolicyList()
  {
    this.prestoData.getRoutingPolicies()
      .subscribe(response => {
        this.policy = response;
        if(this.policy.length>0)
        {
          this.title = "Edit Policy";
        }
        else
        {
          this.title = "Add Policy";
        }
      }, error => {
        this.toastr.error(error.devMessage, 'Error', {
          timeOut: 5000,
        });
      });
  }

  //add or update policy
  addUpdatePolicy(payload)
  {
    this.prestoData.addUpdatePolicy(payload,this.formType)
     .subscribe(response => {
      this.getPolicyList();
      this.getRulesList();
       this.toastr.success('Add / Update Policy', 'Policy Updated Successfully..')
     }, error => {
       this.toastr.error(error.devMessage, 'Error', {
         timeOut: 5000,
       });
     });
  }

   // Add Rule function
   addRule(payload)
   {
     this.prestoData.addRule(payload,payload.type)
     .subscribe(response => {
      this.getPolicyList();
      this.getRulesList();
       this.toastr.success('Add Rule', 'Rule added successfully...')
     }, error => {
       this.toastr.error(error.devMessage, 'Error', {
         timeOut: 5000,
       });
     });
   }

   // Delete Rule function
   deleteRule(ruleName)
  {
    this.prestoData.deleteRoutingRule(ruleName)
      .subscribe(response => {
        this.getRulesList();
        this.toastr.success('Rule Deletion', 'Rule ' + ruleName + ' deleted successfully ..')
      }, error => {
        this.toastr.error(error.devMessage, 'Error', {
          timeOut: 5000,
        });
      });
  }

   //delete Rule(row)
  openDelModal(template: TemplateRef<any>, row: any) {
    this.modalRef = this.modalService.show(template, { class: 'modal-sm' });
    this.idToBeDeleted = row.name;
  }
  confirm(): void {
    this.modalRef.hide();
    this.deleteRule(this.idToBeDeleted);
  }

  decline(): void {
    this.modalRef.hide();
  }

}
