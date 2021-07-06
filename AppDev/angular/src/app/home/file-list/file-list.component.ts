import { Component, OnInit } from '@angular/core';
import { APIService } from 'src/app/api/api.service';

export interface File {
  'filename':string,
  'file_content':{}
}

@Component({
  selector: 'app-file-list',
  templateUrl: './file-list.component.html',
  styleUrls: ['./file-list.component.css']
})

export class FileListComponent implements OnInit {
  //fileList:{'filename':string,'file_content':{}}[] = []
  fileList:File[] = []
  filename: any = []
  filecontent: any = []

  constructor(private apiService: APIService) { }

  ngOnInit(): void {
    this.apiService.getAllFiles().subscribe(
      (resp:any) => {
        this.fileList = resp;
        if(this.fileList.length > 0) {
          for(let file of this.fileList) {
            this.filename.push(file['filename']);
            this.filecontent.push(file['file_content']);
          }
        }    
      },
      (error) => {
        console.log(error);
      }
    );

  }

}
