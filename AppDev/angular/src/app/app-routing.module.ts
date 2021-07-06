import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { AuthGuard } from './auth/auth.guard';
import { HeaderComponent } from './header/header.component';
import { HomeComponent } from './home/home.component';
import { LoginComponent } from './login/login.component';

const routes: Routes = [
  {
    path: '',
    //component: HeaderComponent,
    // canActivateChild:[AuthGuard],
    // children: [
    //   {
    //     path: 'login',
    //     component: LoginComponent
    //   },
    //   {
    //     path: 'home',
    //     component: HomeComponent,
    //     canActivate:[AuthGuard]
    //   }
    // ]
    redirectTo:'home',
    pathMatch:'full'
  },
  {
    path:'home',
    component: HomeComponent,
    //canActivate:[AuthGuard]
  },
  {
    path:'login',
    component: LoginComponent
  }

];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }
