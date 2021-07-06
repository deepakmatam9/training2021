import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { APIService } from '../api/api.service';
import { AuthService } from '../auth/auth.service';

@Component({
  selector: 'app-home',
  templateUrl: './home.component.html',
  styleUrls: ['./home.component.css']
})
export class HomeComponent implements OnInit {
  userName: string = '';
  userEmail: string = '';
  showFileList: boolean = true;
  file: File | null = null;

  constructor(private router: Router, private authService: AuthService, private apiService: APIService) { }

  ngOnInit(): void {
    if (localStorage.getItem('userData')!==null) {
      var userData: any = localStorage.getItem('userData');
      var user = JSON.parse(userData!==null ? userData : "{}");
      if(Object.keys(user).length !== 0) {
        this.userEmail = user['email'];
        this.userName = this.userEmail.split('@')[0];
      }    
    }
    else {
      this.router.navigate(['/login']);

    }
  }

  onUploadJSONClicked() {
    this.showFileList = false;
  }
  onShowFileListClicked() {
    this.showFileList = true;
  }

  onChange(event: Event) {
    if(event.target) {
      var file_elem = (event.target as HTMLInputElement);
      if(file_elem.files && file_elem.files.length > 0) {
        this.file = file_elem.files[0];
        console.log(this.file);
      }
    }
  }

  onUpload() {
    const formData = new FormData();
    //formData.append('json_file',this.file,this.file.name)
    if(this.file!==null) {
      formData.append('json_file',this.file,this.file.name);
      console.log('FormData: ',formData);
      this.apiService.uploadFile(formData).subscribe(
        (resp) => {
          console.log(resp);
        },
        (error) => {
          console.log(error);
        }
      )

    }


  }

}
