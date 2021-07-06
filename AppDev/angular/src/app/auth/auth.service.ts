import { Injectable } from "@angular/core";
import { AngularFireAuth } from "@angular/fire/auth";
import { Observable, Subject } from "rxjs";

@Injectable({
    providedIn:'root'
})
export class AuthService {
    is_auth_passed: boolean = false;
    userSubject = new Subject<string>();

    constructor(private afAuth: AngularFireAuth) {}

    SignUp(email: string, password: string) {
        return this.afAuth.createUserWithEmailAndPassword(email, password);
    }

    LogIn(email: string, password: string) {
        return this.afAuth.signInWithEmailAndPassword(email, password);
    }

    isAuthenticated() {
        // this.afAuth.authState.subscribe(
        //     (user) => {
        //         if(user!==null) {
        //             console.log("User is already logged in");
        //             this.is_auth_passed = true;
        //         }
        //         else {
        //             console.log("User is not logged in");
        //             this.is_auth_passed = false;
        //         }
        //     },
        //     (error) => {
        //         console.log("Error occured while checking for user authentication");
        //         this.is_auth_passed = false;

        //     }
        // )
        // return this.is_auth_passed;
        return this.afAuth.authState;
    }

    LogOut() {
        return this.afAuth.signOut;
    }

    changeUserAuthStatus(status:string) {
        this.userSubject.next(status);
        console.log("User status changed to " + status);
    }



}