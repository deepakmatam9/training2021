import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { AuthService } from '../auth/auth.service';
import { take } from 'rxjs/operators';


@Component({
  selector: 'app-header',
  templateUrl: './header.component.html',
  styleUrls: ['./header.component.css']
})
export class HeaderComponent implements OnInit {
  isLoggedIn: boolean = false;

  constructor(private router: Router, private authService: AuthService) { }

  ngOnInit(): void {
    if(localStorage.getItem('userData')!==null){
      this.isLoggedIn = true;
    }
    this.authService.userSubject.pipe(take(1)).subscribe(
      (status) => {
        if(status=='valid') {
          console.log('User state is valid')
          this.isLoggedIn = true;
        }
        else {
          this.isLoggedIn = false;
        }
      },
      (error) => {
        this.isLoggedIn = false;
      }
    )
  }

  onLoginClicked() {
    this.router.navigate(['/login']);

  }

  OnLogoutClicked() {
    if(localStorage.getItem('userData')!==null) {
      localStorage.removeItem('userData');
      this.isLoggedIn =false;
      this.router.navigate(['/login']);
    }
    else{
      this.isLoggedIn =false;
      this.router.navigate(['/login']);
    }
  }

}
