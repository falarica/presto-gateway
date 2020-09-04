import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import { LeftNavTemplateComponent } from './template/left-nav-template.component';
import { PageNotFoundComponent } from './page-not-found/page-not-found.component';

export const routes: Routes = [{
  path: '',
  redirectTo: 'dashboard',
  pathMatch: 'full'
}, {
  path: '',
  component: LeftNavTemplateComponent,
  data: {
    title: 'Presto Gateway'
  },
  children: [
    {
      path: 'dashboard',
      loadChildren: () => import('./dashboard/dashboard.module').then(m => m.DashboardModule),
      data: {
        title: 'Dashboard Page'
      }
    },
    {
      path: 'clusters',
      loadChildren: () => import('./clusters/clusters.module').then(m => m.ClustersModule),
      data: {
        title: 'Clusters Page'
      }
    },
    {
      path: 'routing-rules',
      loadChildren: () => import('./routing-policy/routing-policy.module').then(m => m.RoutingPolicyModule),
      data: {
        title: 'Routing Policy'
      }
    },
    {
      path: 'queries',
      loadChildren: () => import('./queries/queries.module').then(m => m.QueriesModule),
      data: {
        title: 'Query Page'
      },
    }
  ]
}, {
  path: '**',
  component: PageNotFoundComponent
}];

@NgModule({
  imports: [
    RouterModule.forRoot(routes)
  ],
  exports: [RouterModule],
  declarations: []
})
export class AppRoutingModule {
}
