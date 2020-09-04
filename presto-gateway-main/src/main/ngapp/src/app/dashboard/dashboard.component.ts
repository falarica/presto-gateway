import { Component, OnInit, ViewChild, ViewEncapsulation, OnDestroy } from '@angular/core';
import { PrestInfoService } from 'app/services/presto-info.service';
import { map,debounceTime } from 'rxjs/operators';
import { interval, Subscription , fromEvent } from 'rxjs';


@Component({
  selector: 'app-dashboard',
  templateUrl: './dashboard.component.html',
  styleUrls: ['./dashboard.component.scss'],
  encapsulation: ViewEncapsulation.None
})
export class DashboardComponent implements OnInit, OnDestroy {
  list = [];
  stats={"runningQueries":0,"blockedQueries":0,"queuedQueries":0,"activeCoordinators":0,"activeWorkers":0,"runningDrivers":0,"totalAvailableProcessors":0,"reservedMemory":0.0,"totalInputRows":0,"totalInputBytes":0,"totalCpuTimeSecs":0};
 
  queryList=[];
  isCheck: boolean = true;
  columns = [];
  loadingIndicator: boolean = true;
  source = interval(5000);
  sourceQuery = interval(10000);
  subscription: Subscription;
  subscriptionQuery: Subscription;

  @ViewChild('mydatatable', { static: true }) table: any;
  @ViewChild('search', { static: false }) search: any;
  public temp: Array<any> = [];
  
  constructor(private prestoData:PrestInfoService) { }

  ngOnInit() {
      
    this.getQueryClusterStats();
    this.getQueriesList();
    this.temp = this.queryList;
    // Columns settings
    this.columns = [
      { prop: 'queryText', name: 'Query',sortable: true },
      { prop: 'captureTime', name: 'Create Time',sortable: true },      
      { prop: 'queryId', name: 'QueryId',sortable: true },
      { prop: 'basicQueryInfo.state', name: 'State',sortable: true }     
    ];      

    this.getClusterStats();
    this.getQueryAtInterval();
  }

  ngOnDestroy() {
    this.subscription.unsubscribe();
    this.subscriptionQuery.unsubscribe();
  }

  
  // Getting query & cluster at regular interval
  getQueryClusterStats()
  {
      this.subscription = this.source.subscribe(val => this.prestoData.getInfo()
      .subscribe(response=> {
        this.list=response;
         this.createStats();
      })
    );
  }

   //Getting query at regular interval
   getQueryAtInterval()
   {
     this.subscriptionQuery = this.sourceQuery.subscribe(val => this.prestoData.getQueries()
     .subscribe(response=> {
       this.queryList=response;
       this.temp = this.queryList;
     })
     );
   }

  //Getting initial cluster stats
  getClusterStats()
  {
    this.prestoData.getInfo()
      .subscribe(response=> {
        this.list=response;
         this.createStats();
      })
  }
 
  // Creating stats based on query stats we get from getQueryStats()
  createStats()
  {
    if(this.list != null)
    {
      this.stats={"runningQueries":0,"blockedQueries":0,"queuedQueries":0,"activeCoordinators":0,"activeWorkers":0,"runningDrivers":0,"totalAvailableProcessors":0,"reservedMemory":0.0,"totalInputRows":0,"totalInputBytes":0,"totalCpuTimeSecs":0};
    this.list.forEach(element => {
    if(element.prestoClusterStats)
    {
      this.stats.activeWorkers=this.stats.activeWorkers+element.prestoClusterStats.activeWorkers;
      this.stats.runningDrivers=this.stats.runningDrivers+element.prestoClusterStats.runningDrivers;
      this.stats.reservedMemory=this.stats.reservedMemory+element.prestoClusterStats.reservedMemory;

      this.stats.queuedQueries=this.stats.queuedQueries+element.prestoClusterStats.queuedQueries;
      this.stats.blockedQueries=this.stats.blockedQueries+element.prestoClusterStats.blockedQueries;
      this.stats.runningQueries=this.stats.runningQueries+element.prestoClusterStats.runningQueries;
    }
    });
  }
  }

  // Getting query list 
  getQueriesList()
  {
   
    this.prestoData.getQueries()
    .subscribe(response=> {
      this.queryList=response;
      this.temp = this.queryList;
    });
  }

  // Expand Row
  toggleExpandRow(row) {
    this.table.rowDetail.toggleExpandRow(row);
  }

  onDetailToggle(event) {}

  // Return resourceGroupId array as string
   getResourceGroup(resourceGroupId)
   {
     return resourceGroupId.toString()
   }

   // Return cluster name from cluster URL
   getClusterUrl(url)
   {
    var hostname = (new URL(url)).hostname;
    return hostname;
   }

   // After view initialize call settings for filtering queries
   ngAfterViewInit(): void {
    // Called after ngAfterContentInit when the component's view has been initialized. Applies to components only.
    // Add 'implements AfterViewInit' to the class.
    fromEvent(this.search.nativeElement, 'keydown')
      .pipe(
        debounceTime(550),
        map(x => x['target']['value'])
      )
      .subscribe(value => {
        this.updateFilter(value);
      });
  }

  // Sorting function for queries
  sortData(event) {
    //console.log(event);  
  }

  // filtering queries list
  updateFilter(val: any) {
    if(val!=='')
      this.subscriptionQuery.unsubscribe();
    else
      this.getQueryAtInterval();

    const value = val.toString().toLowerCase().trim();
    // get the amount of columns in the table
    //const count = this.columns.length;
    // get the key names of each column in the dataset
    const keys = ["queryId","queryText","user","source","clusterUrl","captureTime","basicQueryInfo"]; 
    // assign filtered matches to the active datatable
    this.queryList = this.temp.filter(item => {
      // iterate through each row's column data
      for (let i = 0; i < keys.length; i++) {
        if(item['basicQueryInfo']==undefined && (val.toString().toLowerCase()=='running' || val.toString().toLowerCase()=='queued'))
        {
          return true;
        }

        // check for a match
        if ((item[keys[i]] && item[keys[i]].toString().toLowerCase().indexOf(value) !== -1) || !value) {
          // found match, return true to add to result set
          return true;
        }
        
        if(keys[i]=='basicQueryInfo' && 'basicQueryInfo' in item)
        {
              let tempCnt = Object.keys(item[keys[i]]).length;
              let k =  Object.keys(item[keys[i]]);
              for (let j = 0; j < tempCnt; j++) {
                if ((item[keys[i]][k[j]] && item[keys[i]][k[j]].toString().toLowerCase().indexOf(value) !== -1) || !value) {
                  // found match, return true to add to result set
                  return true;
                }
              } 

              // tempCnt = Object.keys(item[keys[i]]).length;
              let l =  Object.keys(item[keys[i]]['session']);
              for (let j = 0; j < tempCnt; j++) {
                if ((item[keys[i]]['session'][l[j]] && item[keys[i]]['session'][l[j]].toString().toLowerCase().indexOf(value) !== -1) || !value) {
                  // found match, return true to add to result set
                  return true;
                }
              } 
           
              
               //tempCnt = Object.keys(item[keys[i]]).length;
               let m =  Object.keys(item[keys[i]]['queryStats']);
              for (let j = 0; j < tempCnt; j++) {
                if ((item[keys[i]]['queryStats'][m[j]] && item[keys[i]]['queryStats'][m[j]].toString().toLowerCase().indexOf(value) !== -1) || !value) {
                  // found match, return true to add to result set
                  return true;
                }
              }
        }
        
      }
    });

  }

}
