import { HttpClient } from "@angular/common/http";
import { Injectable } from "@angular/core";

@Injectable({
    providedIn:'root'
})
export class APIService {
    //BASE_URL = 'http://127.0.0.1:8080';
    BASE_URL = '';
    API_URL = `${this.BASE_URL}/api`;
    FILE_UPLOAD_URL = `${this.API_URL}/upload-file`;
    GET_ALL_FILES_LIST = `${this.API_URL}/list-all-files`;

    constructor(private http: HttpClient){}

    uploadFile(formData:any) {
        return this.http.post(this.FILE_UPLOAD_URL, formData);
    }

    getAllFiles() {
        return this.http.get(this.GET_ALL_FILES_LIST);
    }



}