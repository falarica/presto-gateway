import { Injectable } from '@angular/core';
import {
  HttpInterceptor,
  HttpRequest,
  HttpHandler,
  HttpEvent,
  HttpResponseBase
} from '@angular/common/http';

import { Observable } from 'rxjs/Observable';
import 'rxjs/add/operator/do';

@Injectable()
export class MyHttpInterceptor implements HttpInterceptor {
  intercept(request: HttpRequest<any>, next: HttpHandler): Observable<HttpEvent<any>> {
    return next.handle(request).do(event => {
        if (event instanceof HttpResponseBase) {
          const response = event as HttpResponseBase;
          if (response.status === 303) {
            window.location.href = '/ui/login.html';
            return;
          }
        }
      });
  }
}