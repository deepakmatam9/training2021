import { Component, OnChanges, OnInit } from '@angular/core';
import { FormControl, FormGroup, Validators } from '@angular/forms';
import { Router } from '@angular/router';
import { AuthService } from '../auth/auth.service';

@Component({
  selector: 'app-login',
  templateUrl: './login.component.html',
  styleUrls: ['./login.component.css']
})
export class LoginComponent implements OnInit {

  signUpFailed: boolean = false;
  signUpSuccess: boolean = false;
  actionSuccess: boolean = false;
  actionFailed: boolean = false;
  actionMessage: string = ''

  loginFormGroup = new FormGroup({
    email: new FormControl('', [Validators.required]),
    password: new FormControl('', [Validators.required])
  });

  signupFormGroup = new FormGroup({
    email: new FormControl('', [Validators.required]),
    password: new FormControl('', [Validators.required])
  });

  constructor(private authService: AuthService, private router: Router) { }

  ngOnInit(): void {
    this.loginFormGroup.reset();
    this.signupFormGroup.reset();
  }

  onLoginClicked() {
    console.log("LOGIN CLICKED");
    //console.log(this.loginFormGroup.value);
    if(this.loginFormGroup.valid) {
      const email: string = this.loginFormGroup.value['email'];
      const password: string = this.loginFormGroup.value['password'];
      this.authService.LogIn(email, password).then(
        resp => {
          console.log(resp.user?.email);
          localStorage.setItem('userData', JSON.stringify({'email':resp.user?.email,'uid':resp.user?.uid}));
          setTimeout(
            () => {
              if(localStorage.getItem('userData')!==null){
                localStorage.removeItem('userData');
                console.log("USER DATA DELETED!!!");
              }
            }, 120000
          );
          this.authService.changeUserAuthStatus('valid');
          this.router.navigate(['/home']);
        },
        error => {
          this.actionFailed=true;
          this.actionMessage= error.message;
          console.log(error);
        }
      )
    }

  }

  onSignupClicked() {
    console.log("SignUp CLICKED");
    console.log(this.signupFormGroup.value);
    var email: string = this.signupFormGroup.value['email'];
    var password: string = this.signupFormGroup.value['password'];
    if(email && password) {
      this.authService.SignUp(email, password).then(
        resp => {
          console.log(resp);
          this.router.navigate(['/login']);
         // this.signUpSuccess = true;
          this.actionSuccess = true;
          this.actionMessage = 'Signup successful!'
          this.loginFormGroup.reset();
          this.signupFormGroup.reset();
        }
      ).catch(
        err => {
          console.log(err);
          this.router.navigate(['/login']);
          //this.signUpFailed = true;
          this.actionFailed = true;
          this.actionMessage = err.message;
          this.loginFormGroup.reset();
          this.signupFormGroup.reset();
        }
      )
    }
    else {
      console.log("UNABLE TO SIGNUP");
    }
  }

}
