import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { RoutingRulesComponent } from './routing-rules.component';

describe('RoutingRulesComponent', () => {
  let component: RoutingRulesComponent;
  let fixture: ComponentFixture<RoutingRulesComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ RoutingRulesComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(RoutingRulesComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
