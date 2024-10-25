# API Endpoints Documentation

This document outlines the available API endpoints for the application.

## Drive Routes

### Get Shared Drive Info
- **Route**: `/get_shared_drive_info`
- **Method**: POST
- **Payload**: 
  ```json
  {
    "drive_name": "string"
  }
  ```
- **Return Type**: 
  ```json
  {
    "status": "success" | "error",
    "content": {
      "id": "string",
      "name": "string"
    } | string
  }
  ```

### Get Folder Info
- **Route**: `/get_folder_info`
- **Method**: POST
- **Payload**: 
  ```json
  {
    "parent_id": "string",
    "folder_name": "string"
  }
  ```
- **Return Type**: 
  ```json
  {
    "status": "success" | "error",
    "content": {
      "id": "string",
      "name": "string",
      "parents": ["string"]
    } | string
  }
  ```

### Get Files in Folder
- **Route**: `/get_files_in_folder`
- **Method**: POST
- **Payload**: 
  ```json
  {
    "parent_id": "string"
  }
  ```
- **Return Type**: 
  ```json
  {
    "status": "success" | "error",
    "content": [
      {
        "id": "string",
        "name": "string",
        "parents": ["string"],
        "mimeType": "string",
        "size": "string",
        "modifiedTime": "string",
        "createdTime": "string"
      }
    ] | string
  }
  ```

### Reset Folder
- **Route**: `/reset_folder`
- **Method**: POST
- **Payload**: 
  ```json
  {
    "folder_id": "string"
  }
  ```
- **Return Type**: 
  ```json
  {
    "status": "success" | "error",
    "content": "string"
  }
  ```

### Delete File
- **Route**: `/delete_file`
- **Method**: POST
- **Payload**: 
  ```json
  {
    "fileId": "string"
  }
  ```
- **Return Type**: 
  ```json
  {
    "status": "success" | "error",
    "content": null | {
      "content": "string",
      "file_id": "string"
    }
  }
  ```

### Move File
- **Route**: `/move_file`
- **Method**: POST
- **Payload**: 
  ```json
  {
    "file": "object",
    "new_parent_id": "string"
  }
  ```
- **Return Type**: 
  ```json
  {
    "status": "success" | "error",
    "content": {
      "id": "string",
      "parents": ["string"],
      "name": "string",
      "mimeType": "string",
      "size": "string",
      "modifiedTime": "string",
      "createdTime": "string"
    } | string
  }
  ```

### Download File
- **Route**: `/download_file`
- **Method**: POST
- **Payload**: 
  ```json
  {
    "file_id": "string"
  }
  ```
- **Return Type**: 
  ```json
  {
    "status": "success" | "error",
    "content": "bytes" | string
  }
  ```

### Rename File
- **Route**: `/rename_file`
- **Method**: POST
- **Payload**: 
  ```json
  {
    "fileId": "string",
    "newName": "string"
  }
  ```
- **Return Type**: 
  ```json
  {
    "status": "success" | "error",
    "content": {
      "id": "string",
      "name": "string",
      "parents": ["string"],
      "mimeType": "string",
      "size": "string",
      "modifiedTime": "string",
      "createdTime": "string"
    } | string
  }
  ```

### Upload File
- **Route**: `/upload_file`
- **Method**: POST
- **Payload**: 
  ```json
  {
    "file": "base64_encoded_string",
    "fileName": "string",
    "mimeType": "string",
    "parentFolderId": "string"
  }
  ```
- **Return Type**: 
  ```json
  {
    "status": "success" | "error",
    "content": {
      "id": "string",
      "name": "string",
      "parents": ["string"],
      "mimeType": "string",
      "size": "string",
      "modifiedTime": "string",
      "createdTime": "string"
    } | string
  }
  ```

## Database Routes

### Clear Collection
- **Route**: `/clear_collection`
- **Method**: POST
- **Payload**: 
  ```json
  {
    "path": "string"
  }
  ```
- **Return Type**: 
  ```json
  {
    "status": "success" | "error",
    "content": "string"
  }
  ```

### Upload Collection
- **Route**: `/upload_collection`
- **Method**: POST
- **Payload**: 
  ```json
  {
    "path": "string",
    "data": "object"
  }
  ```
- **Return Type**: 
  ```json
  {
    "status": "success" | "error",
    "content": "string"
  }
  ```

### List Subcollections
- **Route**: `/list_subcollections`
- **Method**: POST
- **Payload**: 
  ```json
  {
    "path": "string"
  }
  ```
- **Return Type**: 
  ```json
  {
    "status": "success" | "error",
    "content": [
      {
        "collection_id": "string",
        "documents": [
          {
            "id": "string",
            // other document fields
          }
        ]
      }
    ] | string
  }
  ```

### Create Document
- **Route**: `/create`
- **Method**: POST
- **Payload**: 
  ```json
  {
    "data": "object",
    "path": "string",
    "id": "string"
  }
  ```
- **Return Type**: 
  ```json
  {
    "status": "success" | "error",
    "content": "string"
  }
  ```

### Read Document(s)
- **Route**: `/read`
- **Method**: POST
- **Payload**: 
  ```json
  {
    "path": "string",
    "query": "object (optional)"
  }
  ```
- **Return Type**: 
  ```json
  {
    "status": "success" | "error",
    "content": [
      {
        "id": "string",
        // other document fields
      }
    ] | string
  }
  ```

### Update Document
- **Route**: `/update`
- **Method**: POST
- **Payload**: 
  ```json
  {
    "path": "string",
    "data": "object"
  }
  ```
- **Return Type**: 
  ```json
  {
    "status": "success" | "error",
    "content": "string"
  }
  ```

### Delete Document
- **Route**: `/delete`
- **Method**: POST
- **Payload**: 
  ```json
  {
    "path": "string"
  }
  ```
- **Return Type**: 
  ```json
  {
    "status": "success" | "error",
    "content": "string"
  }
  ```

## Email Routes

### Send Client Email
- **Route**: `/send_client_email`
- **Method**: POST
- **Payload**: 
  ```json
  {
    "data": "object",
    "client_email": "string",
    "subject": "string"
  }
  ```
- **Return Type**: 
  ```json
  {
    "status": "success" | "error",
    "content": {
      "emailId": "string"
    } | string
  }
  ```
