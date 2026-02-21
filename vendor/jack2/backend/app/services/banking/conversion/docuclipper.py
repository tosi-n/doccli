import asyncio

from httpx import HTTPError, Response, codes

from app.core.config import settings
from app.core.exceptions import (
    ConvertDocumentAPIException,
    FailedToConvertBankStatementException,
)
from app.core.loggers import logger
from app.models.banking.statement import BankStatementDocument
from app.services.banking.conversion.base import BasePdfConversionService


class DocuClipperBankStatementConversionService(BasePdfConversionService):
    BASE_URL = str(settings.DOCUCLIPPER_BASE_URL)
    HEADERS = {
        "Authorization": f"Bearer {settings.DOCUCLIPPER_API_KEY}",
    }

    async def handle_csv_conversion(
        self,
        document: BankStatementDocument,
    ) -> str:
        """
        Queue the conversion task to run in the background
        """
        asyncio.create_task(self.convert_to_csv(document))

    async def _convert_to_csv(
        self,
        document: BankStatementDocument,
    ) -> str:
        """
        Convert PDF to CSV using DocuClipper API
        """
        # Step 1: Download the PDF file
        pdf_file = await self._s3_client.read_file(
            bucket=settings.AWS_S3_DOCUMENTS_BUCKET_NAME,
            s3_path=document.original_file_s3_path,
        )
        logger.info(
            "Downloaded PDF file from S3: %s", document.original_file_s3_path
        )

        # Step 2: Upload the document
        document_id = await self._upload_document(document, pdf_file)

        # Step 3: Create a job for bank statement extraction
        job_id = await self._create_extraction_job(document_id)

        # Step 4: Poll for job completion
        return await self._poll_for_completion(document, job_id)

    async def _make_request(
        self,
        method: str,
        url: str,
        params: dict | None = None,
        json: dict | None = None,
        files: dict | None = None,
    ) -> Response:
        """
        Make a request to the DocuClipper API with error handling
        """
        try:
            headers = None
            if json is not None:
                headers = {"Content-Type": "application/json"}

            response = await self._http_client.request(
                method=method,
                url=url,
                params=params,
                json=json,
                files=files,
                headers=headers,
            )
        except HTTPError as e:
            logger.error(
                "DocuClipper API call failed: %s",
                str(e),
                extra={
                    "error_type": str(type(e)),
                },
            )
            raise ConvertDocumentAPIException(
                f"DocuClipper API call failed: {str(e)}"
            )
        if not codes.is_success(response.status_code):
            logger.error(
                "DocuClipper API call failed - %s",
                response.text,
                extra={
                    "response": str(response.text),
                    "status_code": response.status_code,
                },
            )
            raise ConvertDocumentAPIException(
                f"DocuClipper API call failed: {str(response.text)}"
            )
        return response

    async def _upload_document(
        self, document: BankStatementDocument, pdf_file: bytes
    ) -> str:
        """
        Upload the document to DocuClipper and return the document ID
        """
        files = {
            "document": (
                document.filename,
                pdf_file,
                "application/pdf",
            )
        }
        logger.info("Uploading PDF file to DocuClipper %s", document.filename)

        upload_response = await self._make_request(
            method="POST",
            url="/document",
            params={"asyncProcessing": "false"},
            files=files,
        )
        document_id = upload_response.json().get("document", {}).get("id")
        if not document_id:
            raise FailedToConvertBankStatementException(
                "Failed to get document ID from DocuClipper"
            )

        logger.info("Uploaded PDF file to DocuClipper %s", document_id)
        return document_id

    async def _create_extraction_job(self, document_id: str) -> str:
        """
        Create a job for bank statement extraction and return the job ID
        """
        job_response = await self._make_request(
            method="POST",
            url="/job",
            json={
                "templateId": None,
                "documents": [document_id],
                "documentsToSplit": [],
                "enableMultiPage": False,
                "jobName": "",
                "selectedTemplateFields": None,
                "isGeneric": False,
                "enableBankMode": True,
            },
        )
        job_id = job_response.json().get("id")
        if not job_id:
            raise FailedToConvertBankStatementException(
                "Failed to get job ID from DocuClipper"
            )

        logger.info("Created job %s for bank statement extraction", job_id)
        return job_id

    async def _poll_for_completion(
        self, document: BankStatementDocument, job_id: str
    ) -> str:
        """
        Poll for job completion and upload the result to S3
        """
        while True:
            logger.info("Polling job %s status", job_id)
            status_response = await self._make_request(
                method="GET",
                url=f"/job/{job_id}",
            )
            status_data = status_response.json()
            logger.info(
                "DocuClipper Job %s status: %s", job_id, status_data["status"]
            )

            if status_data["status"] == "Succeeded":
                export_payload = {
                    "format": "custom-all",
                    "flattenTables": True,
                    "jobType": "FieldsToExcel1",
                    "documentIds": [],
                    "dateFormat": "YYYY-MM-DD",
                    "separateFilesForAccounts": True,
                    "fileType": "CSV",
                    "selectedFieldNames": ["bankMode"],
                }

                export_response = await self._make_request(
                    method="POST",
                    url=f"/job/{job_id}/export",
                    json=export_payload,
                )

                # Upload to S3
                s3_path = f"bank_statements/{document.company_id}/{document.id}/{document.filename}.csv"
                await self._s3_client.upload_content_to_s3(
                    content=export_response.content,
                    s3_path=s3_path,
                    content_type="text/csv",
                )
                logger.info("Uploaded Converted CSV file to S3 %s", s3_path)
                return s3_path

            if status_data["status"] != "InProgress":
                error_msg = status_data.get("error", "Unknown error")
                logger.error(
                    "DocuClipper job failed",
                    extra={
                        "job_id": job_id,
                        "error": status_data.get("error", "Unknown error"),
                        "status_data": status_data,
                    },
                )
                raise FailedToConvertBankStatementException(
                    f"Conversion failed: {error_msg}"
                )

            # Wait 5 seconds before polling again
            await asyncio.sleep(5)
