import base64
import time
from typing import Dict, List, Tuple

import boto3
import requests

from .base import FaxService
from .config import IFaxConfig

class IFaxService(FaxService):
    config: IFaxConfig
    base_url = 'https://api.ifaxapp.com/v1'
    send_fax_url = f'{base_url}/customer/fax-send'
    resend_fax_url = f'{base_url}/customer/fax-resend'
    get_fax_status_url = f'{base_url}/customer/fax-status'

    def __call__(self, config: IFaxConfig, *args, **kwargs):
        super().__call__(config)
        self.source_name = self.config.FAX_SOURCE_NAME
        self.source_number = self.config.FAX_SOURCE_NUMBER
        return self

    def send_fax(self, message: Dict, retry_count: int = 3) -> None:
        event_data = self.config.get_event(message.get('event', 'DEFAULT'))
        request_data = self._prepare_send_fax_request_data(message, event_data)

        fax_job_id = self.send_fax_request(request_data)
        fax_result, fax_result_message = self.wait_for_fax_result(fax_job_id)
        if fax_result == 'failed':
            if retry_count == 0:
                raise ValueError(f'Sending fax failed: "{fax_result_message}"')
            return self.send_fax(message, retry_count - 1)

    def wait_for_fax_result(self, job_id: str) -> Tuple[str, str]:
        fax = self.get_fax_status(job_id)

        while fax['faxStatus'] == 'sending':
            time.sleep(3)
            fax = self.get_fax_status(job_id)

        return fax['faxStatus'], fax['message']

    def send_fax_request(self, request_data: Dict) -> str:
        data = self._send_request(self.send_fax_url, request_data)
        return data['jobId']

    def get_fax_status(self, job_id: str) -> Dict:
        return self._send_request(self.get_fax_status_url, {'jobId': job_id})

    def _prepare_send_fax_request_data(self, message: Dict, event_data: Dict) -> Dict:
        recipient = message.get('recipient')
        recipient_number = recipient.get('number')
        if not recipient_number:
            raise ValueError('"recipient.number" is required')
        
        return {
            'callerId': self.source_number,
            'from_name': self.source_name,
            'faxNumber': recipient_number,
            'to_name': recipient.get('name'),
            'message': message.get('message') or event_data.get('message'),
            'subject': message.get('subject') or event_data.get('subject'),
            'send_at': message.get('send_at'),
            'faxQuality': message.get('fax_quality') or event_data.get('fax_quality'),
            'faxData': self._prepare_fax_data(message.get('faxes')),
        }

    def _prepare_fax_data(self, fax_files: List[Dict]) -> List[Dict]:
        fax_data = []
        
        for idx, fax_file in enumerate(fax_files):
            filename = fax_file.get('filename', f'document_{idx + 1}.pdf')
            filetype = fax_file.get('type')

            if filetype == 's3':
                fax_data.append(self._prepare_s3_file_data(filename, fax_file))
            elif filetype == 'local':
                fax_data.append(self._prepare_local_file_data(filename, fax_file))
            else:
                raise ValueError(f'Invalid value for \'faxes[{idx}]["type"]\' in message: {filetype}')
            
        return fax_data

    def _prepare_s3_file_data(self, filename: str, fax_file: Dict) -> Dict:
        required_fields = ['aws_key', 'aws_secret_key', 'aws_region', 'path']
        for field in required_fields:
            if field not in fax_file:
                raise ValueError(f'Missing required field "{field}" for S3 file')
        
        s3_path = fax_file['path']
        if not s3_path.startswith('s3://'):
            raise ValueError('"path" field should be valid S3 path for S3 file')

        path_parts = s3_path[5:].split('/', 1)
        if len(path_parts) != 2:
            raise ValueError('"path" field should be valid S3 path for S3 file')

        bucket_name = path_parts[0]
        object_key = path_parts[1] if len(path_parts) > 1 else ''

        s3_client = boto3.client(
            's3',
            aws_access_key_id=fax_file['aws_key'],
            aws_secret_access_key=fax_file['aws_secret_key'],
            region_name=fax_file['aws_region']
        )
            
        signed_url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket_name, 'Key': object_key},
            ExpiresIn=3600  # 1 hour
        )
            
        return {
            'fileName': filename,
            'fileUrl': signed_url
        }

    def _prepare_local_file_data(self, filename: str, fax_file: Dict) -> Dict:
        if 'path' not in fax_file:
            raise ValueError('Missing required field "path" for local file')

        with open(fax_file['path'], 'rb') as file:
            file_content = file.read()
            base64_content = base64.b64encode(file_content).decode('utf-8')
        
        return {
            'fileName': filename,
            'fileData': base64_content
        }

    def _send_request(self, url: str, request_data: Dict) -> Dict:
        headers = {'accessToken': self.config.IFAX_API_KEY}
        response = requests.post(url, json=request_data, headers=headers).json()
        if response['status'] == 0:
            raise RuntimeError(response['message'])
        return response['data']
