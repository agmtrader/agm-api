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

### Send Email
- **Route**: `/email/send_email`
- **Method**: POST
- **Scope Required**: `email/send_email`
- **Payload**: 
  ```json
  {
    "content": "object",
    "client_email": "string",
    "subject": "string",
    "email_template": "string"
  }
  ```
- **Return Type**: Response from Gmail.send_email function

## Users Routes

### Create User
- **Route**: `/users/create`
- **Method**: POST
- **Scope Required**: `users/create`
- **Payload**: 
  ```json
  {
    "data": "object",
    "id": "string"
  }
  ```
- **Return Type**: Response from create_user function
- **Notes**: Requires user filter enforcement on 'id' field

### Read Users
- **Route**: `/users/read`
- **Method**: POST
- **Scope Required**: `users/read`
- **Payload**: 
  ```json
  {
    "query": "object"
  }
  ```
- **Return Type**: Response from read_users function
- **Notes**: Requires user filter enforcement on 'id' field

### Update User
- **Route**: `/users/update`
- **Method**: POST
- **Scope Required**: `users/update`
- **Payload**: 
  ```json
  {
    "data": "object",
    "query": "object"
  }
  ```
- **Return Type**: Response from update_user function
- **Notes**: Requires user filter enforcement on 'id' field

## Trade Tickets Routes

### Generate Trade Ticket
- **Route**: `/trade_tickets/generate_trade_ticket`
- **Method**: POST
- **Scope Required**: `trade_tickets/generate_trade_ticket`
- **Payload**: 
  ```json
  {
    "indices": "string", // Comma-separated indices that will be converted to integers
    "flex_query_dict": "object"
  }
  ```
- **Return Type**: Response from generate_trade_ticket function

### Generate Client Confirmation Message
- **Route**: `/trade_tickets/generate_client_confirmation_message`
- **Method**: POST
- **Scope Required**: `trade_tickets/generate_client_confirmation_message`
- **Payload**: 
  ```json
  {
    "trade_data": "object"
  }
  ```
- **Return Type**: Response from generate_client_confirmation_message function

## Tickets Routes

### Create Ticket
- **Route**: `/tickets/create`
- **Method**: POST
- **Scope Required**: `tickets/create`
- **Payload**: 
  ```json
  {
    "data": "object",
    "id": "string"
  }
  ```
- **Return Type**: Response from create_ticket function
- **Notes**: Requires user filter enforcement

### Read Tickets
- **Route**: `/tickets/read`
- **Method**: POST
- **Scope Required**: `tickets/read`
- **Payload**: 
  ```json
  {
    "query": "object" // Optional, defaults to empty object
  }
  ```
- **Return Type**: Response from read_tickets function
- **Notes**: Requires user filter enforcement

### Update Ticket
- **Route**: `/tickets/update`
- **Method**: POST
- **Scope Required**: `tickets/update`
- **Payload**: 
  ```json
  {
    "data": "object",
    "query": "object" // Optional, defaults to empty object
  }
  ```
- **Return Type**: Response from update_ticket function
- **Notes**: Requires user filter enforcement

## Risk Profiles Routes

### Create Risk Profile
- **Route**: `/risk_profiles/create`
- **Method**: POST
- **Scope Required**: `risk_profiles/create`
- **Payload**: 
  ```json
  {
    "data": "object",
    "id": "string"
  }
  ```
- **Return Type**: Response from create_risk_profile function

### Read Risk Profiles
- **Route**: `/risk_profiles/read`
- **Method**: GET
- **Scope Required**: `risk_profiles/read`
- **Payload**: None
- **Return Type**: Response from read_risk_profiles function

## Accounts Routes

### Create Account
- **Route**: `/accounts/create`
- **Method**: POST
- **Scope Required**: `accounts/create`
- **Payload**: 
  ```json
  {
    "data": "object",
    "id": "string"
  }
  ```
- **Return Type**: Response from create_account function

### Read Accounts
- **Route**: `/accounts/read`
- **Method**: POST
- **Scope Required**: `accounts/read`
- **Payload**: 
  ```json
  {
    "query": "object"
  }
  ```
- **Return Type**: Response from read_accounts function

### Update Account
- **Route**: `/accounts/update`
- **Method**: POST
- **Scope Required**: `accounts/update`
- **Payload**: 
  ```json
  {
    "data": "object",
    "query": "object"
  }
  ```
- **Return Type**: Response from update_account function

## Notifications Routes

### Create Notification
- **Route**: `/notifications/create`
- **Method**: POST
- **Scope Required**: `notifications/create`
- **Payload**: 
  ```json
  {
    "notification": "object",
    "type": "string"
  }
  ```
- **Return Type**: Response from create_notification function

### Read All Notifications
- **Route**: `/notifications/read`
- **Method**: GET
- **Scope Required**: `notifications/read`
- **Payload**: None
- **Return Type**: Response from read_all_notifications function

### Read Notifications by Type
- **Route**: `/notifications/read_by_type`
- **Method**: POST
- **Scope Required**: `notifications/read_by_type`
- **Payload**: 
  ```json
  {
    "type": "string"
  }
  ```
- **Return Type**: Response from read_notifications_by_type function

## Reporting Routes

### Extract Data
- **Route**: `/reporting/extract`
- **Method**: GET
- **Scope Required**: `reporting/extract`
- **Payload**: None
- **Return Type**: Response from extract function

### Transform Data
- **Route**: `/reporting/transform`
- **Method**: GET
- **Scope Required**: `reporting/transform`
- **Payload**: None
- **Return Type**: Response from transform function

### Get Clients Report
- **Route**: `/reporting/get_clients_report`
- **Method**: GET
- **Scope Required**: `reporting/get_clients_report`
- **Payload**: None
- **Return Type**: Response from get_clients_report function

### Get Accrued Interest Report
- **Route**: `/reporting/get_accrued_interest`
- **Method**: GET
- **Scope Required**: `reporting/get_accrued_interest`
- **Payload**: None
- **Return Type**: Response from get_accrued_interest_report function

## Investment Proposals Routes

### Read Investment Proposals
- **Route**: `/investment_proposals/read`
- **Method**: GET
- **Scope Required**: `investment_proposals/read`
- **Payload**: None
- **Return Type**: Response from read function

### Backup Investment Proposals
- **Route**: `/investment_proposals/backup_investment_proposals`
- **Method**: GET
- **Scope Required**: `investment_proposals/backup_investment_proposals`
- **Payload**: None
- **Return Type**: Response from backup_investment_proposals function

## Flex Query Routes

### Fetch Flex Queries
- **Route**: `/flex_query/fetch`
- **Method**: POST
- **Scope Required**: `flex_query/fetch`
- **Payload**: 
  ```json
  {
    "queryIds": "array"
  }
  ```
- **Return Type**: Response from fetchFlexQueries function

## Document Center Routes

### Get Folder Dictionary
- **Route**: `/document_center/get_folder_dictionary`
- **Method**: GET
- **Scope Required**: `document_center/get_folder_dictionary`
- **Payload**: None
- **Return Type**: Response from DocumentCenter.get_folder_dictionary function

### Read Files
- **Route**: `/document_center/read`
- **Method**: POST
- **Scope Required**: `document_center/read`
- **Payload**: 
  ```json
  {
    "query": "object"
  }
  ```
- **Return Type**: Response from DocumentCenter.read_files function

### Delete File
- **Route**: `/document_center/delete`
- **Method**: POST
- **Scope Required**: `document_center/delete`
- **Payload**: 
  ```json
  {
    "document": "object",
    "parent_folder_id": "string"
  }
  ```
- **Return Type**: Response from DocumentCenter.delete_file function

### Upload File
- **Route**: `/document_center/upload`
- **Method**: POST
- **Scope Required**: `document_center/upload`
- **Payload**: 
  ```json
  {
    "file_name": "string",
    "mime_type": "string",
    "file_data": "string",
    "parent_folder_id": "string",
    "document_info": "object",
    "uploader": "string"
  }
  ```
- **Return Type**: Response from DocumentCenter.upload_file function

## Advisors Routes

### Read Commissions
- **Route**: `/advisors/commissions`
- **Method**: GET
- **Scope Required**: `advisors/commissions`
- **Payload**: None
- **Return Type**: Response from read_commissions function

### Read Advisors
- **Route**: `/advisors/read`
- **Method**: POST
- **Scope Required**: `advisors/read`
- **Payload**: 
  ```json
  {
    "query": "object"
  }
  ```
- **Return Type**: Response from read_advisors function

## Account Management Routes

### Get Accounts
- **Route**: `/account_management/accounts`
- **Method**: GET
- **Scope Required**: `account_management/accounts`
- **Payload**: None
- **Return Type**: Response from AccountManagement.get_accounts function
