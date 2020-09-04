import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { EditClusterComponent } from './edit-cluster.component';

describe('EditClusterComponent', () => {
  let component: EditClusterComponent;
  let fixture: ComponentFixture<EditClusterComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ EditClusterComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(EditClusterComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
