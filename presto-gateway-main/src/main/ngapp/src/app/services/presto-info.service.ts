import { Injectable } from '@angular/core';
import { HttpClient,HttpHeaders } from '@angular/common/http';
import { throwError } from 'rxjs';
import { map, catchError } from "rxjs/operators";
import { CookieService } from 'ngx-cookie-service';

@Injectable({
  providedIn: 'root'
})
export class PrestInfoService {
  // Setting API endpoint link
  url:string = window.location.origin;

  // Setting Http Header information
  httpOptions = {
    headers: new HttpHeaders({
      "withCredentials" : 'true',
    })
  }

  // Constructor function to initialize
  constructor(
    private httpClient: HttpClient,
    private cookieService: CookieService
  )
  {
  }

  getpost():any{

  }

  // Service method to call statistics api
  getInfo(): any {
    return this.httpClient.get<any>(this.url + '/stats/clusterstats' , this.httpOptions)
    .pipe(
      map(res => res) ,
      catchError(error=>this.handleError(error))
    )
  }

  // Service method to call get cluster  api
  getCluster():any {
    return
  }

  // Service method to call get cluster list api
  getClusters():any {
    return this.httpClient.get<any>(this.url + '/clusters', this.httpOptions)
    .pipe(
      map(res => res) ,
      catchError(error=>this.handleError(error))
    )
  }

  // Service method to call add cluster api
  addCluster(payload):any {
    return this.httpClient.post<any>(this.url + '/add?entityType=CLUSTER',payload, this.httpOptions)
    .pipe(
      map(res => res) ,
      catchError(error=>this.handleError(error))
    )
  }

  // Service method to call add cluster api
  editCluster(payload):any {
    return this.httpClient.post<any>(this.url + '/update?entityType=CLUSTER',payload, this.httpOptions)
    .pipe(
      map(res => res) ,
      catchError(error=>this.handleError(error))
    )
  }

  // Service method to call delete cluster api
  deleteCluster(cluster):any {
    return this.httpClient.delete<any>(this.url + '/delete/cluster/' + cluster, this.httpOptions)
    .pipe(
      map(res => res) ,
      catchError(error=>this.handleError(error))
    )
  }

  // Service method to call Get queries list api
  getQueries():any {
    return this.httpClient.get<any>(this.url + '/querydetails2', this.httpOptions)
    .pipe(
      map(res => res) ,
      catchError(error=>this.handleError(error))
    )
  }

  // Service method to call add or update Policy api
  addUpdatePolicy(payload,formType):any {
    let urlType='/add?entityType=ROUTINGPOLICY';

    if(formType=='edit')
      urlType='/update?entityType=ROUTINGPOLICY';

    return this.httpClient.post<any>(this.url + urlType,payload, this.httpOptions)
    .pipe(
      map(res => res) ,
      catchError(error=>this.handleError(error))
    )
  }

   // Service method to call add rules api
   addRule(payload,type):any {
    return this.httpClient.post<any>(this.url + '/add?entityType=' + type,payload, this.httpOptions)
    .pipe(
      map(res => res) ,
      catchError(error=>this.handleError(error))
    )
  }

  // Service method to call Get Routing Rules list api
  getRoutingRules():any {
    return this.httpClient.get<any>(this.url + '/routingrules', this.httpOptions)
    .pipe(
      map(res => res) ,
      catchError(error=>this.handleError(error))
    )
  }

  // Service method to call Get Routing Policies list api
  getRoutingPolicies():any {
    return this.httpClient.get<any>(this.url + '/routingpolicy', this.httpOptions)
    .pipe(
      map(res => res) ,
      catchError(error=>this.handleError(error))
    )
  }

  // Service method to call delete rule api
  deleteRoutingRule(ruleName):any {
    return this.httpClient.delete<any>(this.url + '/delete/routingrule/' + ruleName, this.httpOptions)
    .pipe(
      map(res => res) ,
      catchError(error=>this.handleError(error))
    )
  }

  // Function to handle error
  handleError(error)
  {
    return throwError(error.error);
  }
}
