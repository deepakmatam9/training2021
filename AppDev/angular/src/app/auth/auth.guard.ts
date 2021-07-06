import { Injectable } from '@angular/core';
import { CanActivate, ActivatedRouteSnapshot, RouterStateSnapshot, UrlTree, Router, CanActivateChild } from '@angular/router';
import { Observable } from 'rxjs';
import { AuthService } from './auth.service';
@Injectable({
    providedIn: 'root'
})
export class AuthGuard implements CanActivate {

    constructor(private authService: AuthService, private router: Router) {}
    
    
    canActivate(next: ActivatedRouteSnapshot, state: RouterStateSnapshot): Observable<boolean> | Promise<boolean> | boolean {
        var isAuthenticated: boolean = false;

        // if(this.authService.isAuthenticated()) {
        //     return true;
        // }
        // else{
        //     this.router.navigate(['/login']);
        //     return false;
        // }
        this.authService.isAuthenticated().subscribe(
            (user) => {
                if(user!==null) {
                    console.log("User is already authenticated");
                    isAuthenticated = true;
                }
                else {
                    console.log("User is not authenticated");
                    this.router.navigate(['/login']);
                    isAuthenticated = false;
                }
            },
            (error) => {
                console.log("Error occured in fetching Authentication state");
                this.router.navigate(['/login']);
                isAuthenticated = false;
            }
        );
        return isAuthenticated;
    }

    // canActivateChild(next: ActivatedRouteSnapshot, state: RouterStateSnapshot): Observable<boolean> | Promise<boolean> | boolean {
    //     var isAuthenticated: boolean = false;

    //     this.authService.isAuthenticated().subscribe(
            
    //         (user) => {
    //             if(user!==null) {
    //                 console.log("User is already authenticated");
    //                 isAuthenticated = true;
    //             }
    //             else {
    //                 console.log("User is not authenticated");
    //                 isAuthenticated = false;
    //             }
    //         },
    //         (error) => {
    //             console.log("Error occured in fetching Authentication state");
    //             isAuthenticated = false;
    //         }
    //     )
    //     return isAuthenticated;
    // }
    
}