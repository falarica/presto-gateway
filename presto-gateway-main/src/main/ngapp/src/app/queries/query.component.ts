import { Component, OnInit, ViewEncapsulation, ViewChild, AfterViewInit } from '@angular/core';
import { PrestInfoService } from 'app/services/presto-info.service';
import { fromEvent } from 'rxjs';
import { map,debounceTime } from 'rxjs/operators';

@Component({
  selector: 'app-query',
  templateUrl: './query.component.html',
  styleUrls: ['./query.component.scss'],
  encapsulation: ViewEncapsulation.None
})
export class QueryComponent implements OnInit, AfterViewInit {
  queryList=[{
    "queryId": "20200812_103857_00003_ca7uu",
    "session": {
      "queryId": "20200812_103857_00003_ca7uu",
      "transactionId": "a1b7c3b2-9380-4d6a-a0d3-a9dd128860f2",
      "clientTransactionSupport": true,
      "user": "admin",
      "groups": [],
      "source": "presto-cli",
      "catalog": "tpch",
      "path": {},
      "timeZoneKey": 1953,
      "locale": "en_IN",
      "remoteUserAddress": "172.20.0.1",
      "userAgent": "[Presto Proxy] StatementClientV1/333-153-ge0204be-dirty",
      "clientTags": [],
      "clientCapabilities": ["PATH"],
      "resourceEstimates": {},
      "start": "2020-08-12T10:38:57.233141Z",
      "systemProperties": {},
      "catalogProperties": {},
      "unprocessedCatalogProperties": {},
      "roles": {},
      "preparedStatements": {}
    },
    "resourceGroupId": ["global"],
    "state": "FAILED",
    "memoryPool": "general",
    "scheduled": false,
    "self": "http://172.20.0.4:8080/v1/query/20200812_103857_00003_ca7uu",
    "query": "create schema ab1",
    "updateType": "CREATE SCHEMA",
    "queryStats": {
      "createTime": "2020-08-12T10:38:57.257Z",
      "endTime": "2020-08-12T10:38:59.536Z",
      "queuedTime": "1.25ms",
      "elapsedTime": "2.28s",
      "executionTime": "2.26s",
      "totalDrivers": 0,
      "queuedDrivers": 0,
      "runningDrivers": 0,
      "completedDrivers": 0,
      "rawInputDataSize": "0B",
      "rawInputPositions": 0,
      "cumulativeUserMemory": 0.0,
      "userMemoryReservation": "0B",
      "totalMemoryReservation": "0B",
      "peakUserMemoryReservation": "0B",
      "peakTotalMemoryReservation": "0B",
      "totalCpuTime": "0.00ns",
      "totalScheduledTime": "0.00ns",
      "fullyBlocked": false,
      "blockedReasons": []
    },
    "errorType": "USER_ERROR",
    "errorCode": {
      "code": 13,
      "name": "NOT_SUPPORTED",
      "type": "USER_ERROR"
    }
  }, {
    "queryId": "20200812_103722_00002_ca7uu",
    "session": {
      "queryId": "20200812_103722_00002_ca7uu",
      "transactionId": "7cb64ded-86cc-4128-a68d-3e96c7649e00",
      "clientTransactionSupport": true,
      "user": "admin",
      "groups": [],
      "source": "presto-cli",
      "catalog": "tpch",
      "path": {},
      "timeZoneKey": 1953,
      "locale": "en_IN",
      "remoteUserAddress": "172.20.0.1",
      "userAgent": "[Presto Proxy] StatementClientV1/333-153-ge0204be-dirty",
      "clientTags": [],
      "clientCapabilities": ["PATH"],
      "resourceEstimates": {},
      "start": "2020-08-12T10:37:22.641365Z",
      "systemProperties": {},
      "catalogProperties": {},
      "unprocessedCatalogProperties": {},
      "roles": {},
      "preparedStatements": {}
    },
    "resourceGroupId": ["global"],
    "state": "FINISHED",
    "memoryPool": "general",
    "scheduled": true,
    "self": "http://172.20.0.4:8080/v1/query/20200812_103722_00002_ca7uu",
    "query": "select 2",
    "queryStats": {
      "createTime": "2020-08-12T10:37:22.647Z",
      "endTime": "2020-08-12T10:37:23.010Z",
      "queuedTime": "45.63ms",
      "elapsedTime": "363.46ms",
      "executionTime": "312.54ms",
      "totalDrivers": 1,
      "queuedDrivers": 0,
      "runningDrivers": 0,
      "completedDrivers": 1,
      "rawInputDataSize": "0B",
      "rawInputPositions": 0,
      "cumulativeUserMemory": 0.0,
      "userMemoryReservation": "0B",
      "totalMemoryReservation": "0B",
      "peakUserMemoryReservation": "0B",
      "peakTotalMemoryReservation": "0B",
      "totalCpuTime": "1.00ms",
      "totalScheduledTime": "1.00ms",
      "fullyBlocked": true,
      "blockedReasons": [],
      "progressPercentage": 100.0
    }
  }, {
    "queryId": "20200812_103207_00001_ca7uu",
    "session": {
      "queryId": "20200812_103207_00001_ca7uu",
      "transactionId": "77055482-8872-4858-bcd0-6570d0748bc5",
      "clientTransactionSupport": true,
      "user": "admin",
      "groups": [],
      "source": "presto-cli",
      "catalog": "tpch",
      "path": {},
      "timeZoneKey": 1953,
      "locale": "en_IN",
      "remoteUserAddress": "172.20.0.1",
      "userAgent": "[Presto Proxy] StatementClientV1/333-153-ge0204be-dirty",
      "clientTags": [],
      "clientCapabilities": ["PATH"],
      "resourceEstimates": {},
      "start": "2020-08-12T10:32:07.728819Z",
      "systemProperties": {},
      "catalogProperties": {},
      "unprocessedCatalogProperties": {},
      "roles": {},
      "preparedStatements": {}
    },
    "resourceGroupId": ["global"],
    "state": "FINISHED",
    "memoryPool": "general",
    "scheduled": true,
    "self": "http://172.20.0.4:8080/v1/query/20200812_103207_00001_ca7uu",
    "query": "select 1",
    "queryStats": {
      "createTime": "2020-08-12T10:32:07.730Z",
      "endTime": "2020-08-12T10:32:07.988Z",
      "queuedTime": "2.88ms",
      "elapsedTime": "258.34ms",
      "executionTime": "255.06ms",
      "totalDrivers": 1,
      "queuedDrivers": 0,
      "runningDrivers": 0,
      "completedDrivers": 1,
      "rawInputDataSize": "0B",
      "rawInputPositions": 0,
      "cumulativeUserMemory": 0.0,
      "userMemoryReservation": "0B",
      "totalMemoryReservation": "0B",
      "peakUserMemoryReservation": "0B",
      "peakTotalMemoryReservation": "0B",
      "totalCpuTime": "2.00ms",
      "totalScheduledTime": "12.00ms",
      "fullyBlocked": true,
      "blockedReasons": [],
      "progressPercentage": 100.0
    }
  }, {
    "queryId": "20200812_103110_00000_ca7uu",
    "session": {
      "queryId": "20200812_103110_00000_ca7uu",
      "transactionId": "40478735-e7df-47e7-99ac-a9bd975e1a54",
      "clientTransactionSupport": true,
      "user": "admin",
      "groups": [],
      "source": "presto-cli",
      "catalog": "tpch",
      "path": {},
      "timeZoneKey": 1953,
      "locale": "en_IN",
      "remoteUserAddress": "172.20.0.1",
      "userAgent": "[Presto Proxy] StatementClientV1/333-153-ge0204be-dirty",
      "clientTags": [],
      "clientCapabilities": ["PATH"],
      "resourceEstimates": {},
      "start": "2020-08-12T10:31:13.263563Z",
      "systemProperties": {},
      "catalogProperties": {},
      "unprocessedCatalogProperties": {},
      "roles": {},
      "preparedStatements": {}
    },
    "resourceGroupId": ["global"],
    "state": "FINISHED",
    "memoryPool": "general",
    "scheduled": true,
    "self": "http://172.20.0.4:8080/v1/query/20200812_103110_00000_ca7uu",
    "query": "select 1",
    "queryStats": {
      "createTime": "2020-08-12T10:31:15.656Z",
      "endTime": "2020-08-12T10:31:30.644Z",
      "queuedTime": "547.99ms",
      "elapsedTime": "14.99s",
      "executionTime": "13.19s",
      "totalDrivers": 1,
      "queuedDrivers": 0,
      "runningDrivers": 0,
      "completedDrivers": 1,
      "rawInputDataSize": "0B",
      "rawInputPositions": 0,
      "cumulativeUserMemory": 0.0,
      "userMemoryReservation": "0B",
      "totalMemoryReservation": "0B",
      "peakUserMemoryReservation": "0B",
      "peakTotalMemoryReservation": "122B",
      "totalCpuTime": "17.00ms",
      "totalScheduledTime": "185.00ms",
      "fullyBlocked": true,
      "blockedReasons": [],
      "progressPercentage": 100.0
    }
  }, {
    "queryId": "20200812_104244_00005_ca7uu",
    "session": {
      "queryId": "20200812_104244_00005_ca7uu",
      "transactionId": "25659868-26d6-48c3-b815-7be6a5de1501",
      "clientTransactionSupport": true,
      "user": "admin",
      "groups": [],
      "source": "presto-cli",
      "catalog": "tpch",
      "path": {},
      "timeZoneKey": 1953,
      "locale": "en_IN",
      "remoteUserAddress": "172.20.0.1",
      "userAgent": "[Presto Proxy] StatementClientV1/333-153-ge0204be-dirty",
      "clientTags": [],
      "clientCapabilities": ["PATH"],
      "resourceEstimates": {},
      "start": "2020-08-12T10:42:45.130197Z",
      "systemProperties": {},
      "catalogProperties": {},
      "unprocessedCatalogProperties": {},
      "roles": {},
      "preparedStatements": {}
    },
    "resourceGroupId": ["global"],
    "state": "FINISHED",
    "memoryPool": "general",
    "scheduled": true,
    "self": "http://172.20.0.4:8080/v1/query/20200812_104244_00005_ca7uu",
    "query": "select count(*) from sf1.customer",
    "queryStats": {
      "createTime": "2020-08-12T10:42:45.145Z",
      "endTime": "2020-08-12T10:42:53.295Z",
      "queuedTime": "10.88ms",
      "elapsedTime": "8.15s",
      "executionTime": "7.93s",
      "totalDrivers": 21,
      "queuedDrivers": 0,
      "runningDrivers": 0,
      "completedDrivers": 21,
      "rawInputDataSize": "0B",
      "rawInputPositions": 150000,
      "cumulativeUserMemory": 13082.0,
      "userMemoryReservation": "0B",
      "totalMemoryReservation": "0B",
      "peakUserMemoryReservation": "0B",
      "peakTotalMemoryReservation": "0B",
      "totalCpuTime": "4.80s",
      "totalScheduledTime": "22.36s",
      "fullyBlocked": true,
      "blockedReasons": [],
      "progressPercentage": 100.0
    }
  }, {
    "queryId": "20200812_103900_00004_ca7uu",
    "session": {
      "queryId": "20200812_103900_00004_ca7uu",
      "transactionId": "6beea26a-0c6a-4ff9-ac54-1cddbb2486ab",
      "clientTransactionSupport": true,
      "user": "admin",
      "groups": [],
      "source": "presto-cli",
      "catalog": "tpch",
      "path": {},
      "timeZoneKey": 1953,
      "locale": "en_IN",
      "remoteUserAddress": "172.20.0.1",
      "userAgent": "[Presto Proxy] StatementClientV1/333-153-ge0204be-dirty",
      "clientTags": [],
      "clientCapabilities": ["PATH"],
      "resourceEstimates": {},
      "start": "2020-08-12T10:39:00.219549Z",
      "systemProperties": {},
      "catalogProperties": {},
      "unprocessedCatalogProperties": {},
      "roles": {},
      "preparedStatements": {}
    },
    "resourceGroupId": ["global"],
    "state": "FAILED",
    "memoryPool": "general",
    "scheduled": false,
    "self": "http://172.20.0.4:8080/v1/query/20200812_103900_00004_ca7uu",
    "query": "CREATE TABLE hive.ab1.ppxy1 (id varchar) WITH (format = \u0027CSV\u0027, external_location = \u0027file:///tmp/filedata/a1\u0027)",
    "updateType": "CREATE TABLE",
    "queryStats": {
      "createTime": "2020-08-12T10:39:00.280Z",
      "endTime": "2020-08-12T10:39:33.586Z",
      "queuedTime": "23.78ms",
      "elapsedTime": "33.31s",
      "executionTime": "33.27s",
      "totalDrivers": 0,
      "queuedDrivers": 0,
      "runningDrivers": 0,
      "completedDrivers": 0,
      "rawInputDataSize": "0B",
      "rawInputPositions": 0,
      "cumulativeUserMemory": 0.0,
      "userMemoryReservation": "0B",
      "totalMemoryReservation": "0B",
      "peakUserMemoryReservation": "0B",
      "peakTotalMemoryReservation": "0B",
      "totalCpuTime": "0.00ns",
      "totalScheduledTime": "0.00ns",
      "fullyBlocked": false,
      "blockedReasons": []
    },
    "errorType": "EXTERNAL",
    "errorCode": {
      "code": 16777216,
      "name": "HIVE_METASTORE_ERROR",
      "type": "EXTERNAL"
    }
  }];
  //queryList=[];
  isCheck: boolean = true;
  columns = [];
  loadingIndicator: boolean = true;
  @ViewChild('mydatatable', { static: true }) table: any;
  constructor(private prestoData:PrestInfoService) { }
  @ViewChild('search', { static: false }) search: any;
  public temp: Array<any> = [];

  ngOnInit(): void {
      this.prestoData.getQueries()
       .subscribe(response=> {
         this.temp = this.queryList;
         this.queryList=response;
       });

    // Columns settings
    this.columns = [
      { prop: 'state' }, 
      { prop: 'createTime', name: 'createTime' }, 
      { prop: 'query', name: 'query' },
      { prop: 'session', name: 'queryId' },
      { prop: 'queryStats', name: 'queryStats' }
    ];  

   
  }
  toggleExpandRow(row) {
    this.table.rowDetail.toggleExpandRow(row);
  }

  onDetailToggle(event) {
   }

   getResourceGroup(resourceGroupId)
   {
     return resourceGroupId.toString()
   }

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

  updateFilter(val: any) {
    const value = val.toString().toLowerCase().trim();
    // get the amount of columns in the table
    const count = this.columns.length;
    // get the key names of each column in the dataset
    const keys = Object.keys(this.temp[0]);
    // assign filtered matches to the active datatable
    this.queryList = this.temp.filter(item => {
      // iterate through each row's column data
      for (let i = 0; i < count; i++) {
        // check for a match
        if ((item[keys[i]] && item[keys[i]].toString().toLowerCase().indexOf(value) !== -1) || !value) {
          // found match, return true to add to result set
          return true;
        }
        else if(keys[i]=='session')
        {
          let tempCnt = Object.keys(item[keys[i]]).length;
          let k =  Object.keys(item[keys[i]]);
          for (let j = 0; j < tempCnt; j++) {
            if ((item[keys[i]][k[j]] && item[keys[i]][k[j]].toString().toLowerCase().indexOf(value) !== -1) || !value) {
              // found match, return true to add to result set
              return true;
            }
          }
        }
        else if(keys[i]=='queryStats')
        {
          let tempCnt = Object.keys(item[keys[i]]).length;
          let k =  Object.keys(item[keys[i]]);
          for (let j = 0; j < tempCnt; j++) {
            if ((item[keys[i]][k[j]] && item[keys[i]][k[j]].toString().toLowerCase().indexOf(value) !== -1) || !value) {
              // found match, return true to add to result set
              return true;
            }
          }
        }
      }
    });

    // Whenever the filter changes, always go back to the first page
    // this.table.offset = 0;
  }
}
