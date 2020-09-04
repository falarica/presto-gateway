import { TestBed } from '@angular/core/testing';

import { PrestInfoService } from './presto-info.service';

describe('PrestInfoService', () => {
  let service: PrestInfoService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(PrestInfoService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
