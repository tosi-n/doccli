import asyncio
import uuid
from datetime import datetime
from uuid import UUID

from fastapi import UploadFile
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.enums import (
    BankAccountStatementStatus,
    BankStatementConversionService,
    BankStatementDocumentStatus,
)
from app.core.exceptions import (
    FailedToUploadFileToS3Exception,
    MaximumUploadLimitReachedException,
    ObjectNotFoundException,
    UnsupportedBankStatementFormatException,
)
from app.core.services.s3_client import S3Client
from app.db.manager import db_manager
from app.messaging.messages import (
    AuditLogMessage,
    BankStatementConversionMessage,
)
from app.messaging.producers.bank_statement_conversion import (
    BankStatementConversionProducer,
)
from app.models import BankAccountStatement, User
from app.repositories.banking import (
    BankAccountStatementRepository,
    BankStatementDocumentRepository,
)
from app.schemas.banking import (
    BankAccountStatementDetailSchema,
    BankStatementDocumentSchema,
    CreateBankAccountStatementSchema,
    CreateBankStatementDocumentSchema,
    CreateBankTransactionSchema,
    ReadBankAccountStatementSchema,
    ReadBankStatementDocumentResponseSchema,
    UpdateBankAccountStatementSchema,
    UpdateBankStatementDocumentSchema,
    UploadBankStatementsResponseSchema,
)
from app.services.banking import BankAccountService
from app.services.banking.conversion.factory import pdf_conversion_factory
from app.services.banking.transaction import BankTransactionService
from app.services.base import BaseService
from app.services.company import CompanyService

__all__ = ["BankStatementService", "BankAccountStatementService"]


class BankStatementService(BaseService):
    ALLOWED_CONTENT_TYPES = ["application/pdf"]

    def __init__(self, db: AsyncSession):
        super().__init__(db)
        self._repository = BankStatementDocumentRepository(db)
        self._company_service = CompanyService(db)
        self._s3_client = S3Client()
        self._conversion_producer = BankStatementConversionProducer()

    async def __upload_file(
        self,
        company_id: UUID,
        user: User,
        file: UploadFile,
        audit_data: list[dict],
        accepted_files: list[str],
        rejected_files: list[str],
    ) -> None:
        document_id = str(uuid.uuid4())
        s3_path = f"bank_statements/{company_id}/{document_id}/{file.filename}"

        try:
            await self._s3_client.upload_file(
                file.file,
                s3_path,
                file.content_type,
                settings.AWS_S3_DOCUMENTS_BUCKET_NAME,
            )
        except FailedToUploadFileToS3Exception:
            rejected_files.append(file.filename)
            return

        async with db_manager.create_async_session() as session:
            await BankStatementDocumentRepository(session).create(
                obj_in=CreateBankStatementDocumentSchema(
                    id=document_id,
                    company_id=company_id,
                    user_id=user.id,
                    original_file_s3_path=s3_path,
                    filename=file.filename,
                    status=BankStatementDocumentStatus.UPLOADED,
                )
            )

            # Send message to conversion queue
            await self._conversion_producer.send(
                BankStatementConversionMessage(
                    company_id=company_id,
                    document_id=document_id,
                )
            )

            audit_data.append({"document_id": document_id})
            accepted_files.append(file.filename)

    async def upload_statements(
        self,
        company_id: UUID,
        user: User,
        files: list[UploadFile],
    ) -> tuple[list[str], list[str]]:
        """
        Upload multiple bank statement documents for a company.
        Returns a tuple of (accepted_files, rejected_files).
        """
        # Validate content types first
        for file in files:
            if file.content_type not in self.ALLOWED_CONTENT_TYPES:
                raise UnsupportedBankStatementFormatException(
                    f"Unsupported file format. Allowed formats are: {self.ALLOWED_CONTENT_TYPES}"
                )

        # Verify company exists
        await self._company_service.get_by_id(company_id)

        audit_data = []
        accepted_files, rejected_files = [], []

        # Process files in parallel using asyncio.gather
        await asyncio.gather(
            *[
                self.__upload_file(
                    company_id=company_id,
                    user=user,
                    file=file,
                    audit_data=audit_data,
                    accepted_files=accepted_files,
                    rejected_files=rejected_files,
                )
                for file in files
            ]
        )

        await self._send_audit_log_message(
            audit_log=AuditLogMessage(
                company_id=company_id,
                action_type="bank_statements_uploaded",
                message=f"Bank statements uploaded: {len(files)}",
                data=audit_data,
            )
        )

        return accepted_files, rejected_files

    async def get_company_statements(
        self,
        company_id: UUID,
        search: str | None = None,
    ) -> list[BankStatementDocumentSchema]:
        await self._company_service.get_by_id(company_id)
        return await self._repository.get_by_company_id(
            company_id, search=search
        )

    async def get_statement(
        self,
        company_id: UUID,
        document_id: UUID,
    ) -> BankStatementDocumentSchema:
        document = await self._repository.get_document(document_id, company_id)
        if not document:
            raise ObjectNotFoundException("Bank statement document not found")
        return document

    async def convert_obj_to_response_schema_with_download(
        self, document: BankStatementDocumentSchema
    ) -> ReadBankStatementDocumentResponseSchema:
        response_data = ReadBankStatementDocumentResponseSchema.model_validate(
            document
        )
        response_data.download_csv_url = (
            await self._s3_client.generate_presigned_url_download(
                document.converted_file_s3_path
            )
            if document.converted_file_s3_path
            else None
        )
        response_data.download_pdf_url = (
            await self._s3_client.generate_presigned_url_download(
                document.original_file_s3_path
            )
            if document.original_file_s3_path
            else None
        )
        return response_data

    async def convert_statement_to_csv(
        self,
        company_id: UUID,
        document_id: UUID,
    ) -> None:
        """
        Convert a bank statement from PDF to CSV format
        """
        document = await self.get_statement(company_id, document_id)

        # Update status to processing
        await self._repository.update(
            obj=document,
            updated_data=UpdateBankStatementDocumentSchema(
                status=BankStatementDocumentStatus.PROCESSING,
            ),
        )

        # Get conversion service and convert to CSV
        conversion_service = pdf_conversion_factory.get_conversion_service(
            provider=BankStatementConversionService.DOCUCLIPPER,
            db=self.db,
        )

        await conversion_service.handle_csv_conversion(
            document=document,
        )


class BankAccountStatementService(BaseService):
    ALLOWED_CONTENT_TYPES = ["application/pdf"]
    MAX_UPLOAD_LIMIT = 5
    MAX_FILE_SIZE = 10 * 1024 * 1024  # 10MB in bytes

    def __init__(self, db: AsyncSession):
        super().__init__(db)
        self._repository = BankAccountStatementRepository(db)
        self._bank_account_service = BankAccountService(db)
        self._conversion_producer = BankStatementConversionProducer()
        self._s3_client = S3Client()
        self._transaction_service = BankTransactionService(db)

    async def extract_details_from_statement(
        self,
        company_id: UUID,
        statement_id: UUID,
        bank_account_id: UUID,
    ) -> None:
        statement = await self.get_statement(
            company_id, bank_account_id, statement_id
        )
        if not statement:
            raise ObjectNotFoundException("Bank statement document not found")

        await self._repository.update(
            obj=statement,
            updated_data=UpdateBankAccountStatementSchema(
                status=BankAccountStatementStatus.EXTRACTING,
            ),
        )

        conversion_service = pdf_conversion_factory.get_conversion_service(
            provider=BankStatementConversionService.AZURE_DOCUMENT_INTELLIGENCE,  # pylint: disable=line-too-long
            db=self.db,
        )

        await conversion_service.handle_statement_extraction(
            statement=statement,
        )

    async def upload_bank_account_statement(
        self,
        company_id: UUID,
        bank_account_id: UUID,
        user_id: UUID,
        files: list[UploadFile],
    ) -> UploadBankStatementsResponseSchema:
        """
        Upload multiple bank statement documents for a bank account.
        Returns a tuple of (accepted_files, rejected_files).
        """
        await self._bank_account_service.get_by_id(bank_account_id)
        if len(files) > self.MAX_UPLOAD_LIMIT:
            raise MaximumUploadLimitReachedException(
                f"Maximum upload limit is {self.MAX_UPLOAD_LIMIT} files"
            )

        # Validate content types and file sizes first
        for file in files:
            if file.content_type not in self.ALLOWED_CONTENT_TYPES:
                raise UnsupportedBankStatementFormatException(
                    f"Unsupported file format. Allowed formats are: {self.ALLOWED_CONTENT_TYPES}"
                )

            if file.size > self.MAX_FILE_SIZE:
                raise MaximumUploadLimitReachedException(
                    f"File {file.filename} exceeds maximum size limit of {self.MAX_FILE_SIZE} bytes"
                )

        audit_data = []
        accepted_files, rejected_files = [], []

        # Process files in parallel using asyncio.gather
        await asyncio.gather(
            *[
                self.__upload_file(
                    company_id=company_id,
                    bank_account_id=bank_account_id,
                    user_id=user_id,
                    file=file,
                    audit_data=audit_data,
                    accepted_files=accepted_files,
                    rejected_files=rejected_files,
                )
                for file in files
            ]
        )

        return UploadBankStatementsResponseSchema(
            accepted_files=accepted_files,
            rejected_files=rejected_files,
        )

    async def __upload_file(
        self,
        company_id: UUID,
        bank_account_id: UUID,
        user_id: UUID,
        file: UploadFile,
        audit_data: list[dict],
        accepted_files: list[str],
        rejected_files: list[str],
    ) -> None:
        document_id = str(uuid.uuid4())
        s3_path = f"bank_account_statements/{company_id}/{bank_account_id}/{document_id}/{file.filename}"

        try:
            await self._s3_client.upload_file(
                file=file.file,
                file_path=s3_path,
                content_type=file.content_type,
                bucket_name=settings.AWS_S3_SENSITIVE_DOCUMENTS_BUCKET_NAME,
            )
        except FailedToUploadFileToS3Exception:
            rejected_files.append(file.filename)
            return

        async with db_manager.create_async_session() as session:
            await BankAccountStatementRepository(session).create(
                obj_in=CreateBankAccountStatementSchema(
                    id=document_id,
                    bank_account_id=bank_account_id,
                    company_id=company_id,
                    s3_path=s3_path,
                    filename=file.filename,
                    status=BankAccountStatementStatus.UPLOADED,
                    user_id=user_id,
                )
            )

            audit_data.append({"document_id": document_id})
            accepted_files.append(file.filename)

            await self._conversion_producer.send(
                BankStatementConversionMessage(
                    company_id=company_id,
                    bank_account_id=bank_account_id,
                    bank_account_statement_id=document_id,
                )
            )

    async def get_bank_account_statements(
        self,
        bank_account_id: UUID,
        company_id: UUID,
        offset: int = 0,
        limit: int = 10,
    ) -> tuple[int, list[BankAccountStatement]]:
        return await self._repository.get_by_bank_account_id(
            bank_account_id=bank_account_id,
            company_id=company_id,
            offset=offset,
            limit=limit,
        )

    async def get_statement(
        self,
        company_id: UUID,
        bank_account_id: UUID,
        document_id: UUID,
    ) -> BankAccountStatement:
        statement = await self.get_one_by_filters(
            filters=[
                BankAccountStatement.company_id == company_id,
                BankAccountStatement.bank_account_id == bank_account_id,
                BankAccountStatement.id == document_id,
            ]
        )
        if not statement:
            raise ObjectNotFoundException("Bank statement document not found")
        return statement

    async def convert_obj_to_response_schema(
        self, statement: BankAccountStatement
    ) -> ReadBankAccountStatementSchema:
        response_data = ReadBankAccountStatementSchema.model_validate(
            statement
        )
        response_data.pdf_url = (
            await self._s3_client.generate_presigned_url(
                statement.s3_path,
                bucket_name=settings.AWS_S3_SENSITIVE_DOCUMENTS_BUCKET_NAME,
            )
            if statement.s3_path
            else None
        )
        response_data.pdf_download_url = (
            await self._s3_client.generate_presigned_url_download(
                statement.s3_path,
                bucket_name=settings.AWS_S3_SENSITIVE_DOCUMENTS_BUCKET_NAME,
            )
            if statement.s3_path
            else None
        )
        return response_data

    async def update_statement(
        self,
        company_id: UUID,
        bank_account_id: UUID,
        document_id: UUID,
        update_data: UpdateBankAccountStatementSchema,
    ) -> BankAccountStatement:
        """
        Update bank statement details and recalculate totals
        """
        statement = await self.get_statement(
            company_id=company_id,
            bank_account_id=bank_account_id,
            document_id=document_id,
        )
        if not statement:
            raise ObjectNotFoundException("Bank statement document not found")

        # If extracted details are provided, recalculate total
        if update_data.extracted_details:
            update_data.total_amount = sum(
                detail.amount or 0 for detail in update_data.extracted_details
            )

        # Update the statement
        updated_statement = await self._repository.update(
            obj=statement,
            updated_data=update_data,
        )

        return updated_statement

    async def convert_statement_to_transactions(
        self,
        company_id: UUID,
        bank_account_id: UUID,
        document_id: UUID,
    ) -> None:
        """
        Convert extracted bank statement details into bank transactions.
        Returns the number of transactions created.
        """
        statement = await self.get_statement(
            company_id=company_id,
            bank_account_id=bank_account_id,
            document_id=document_id,
        )

        if not statement:
            raise ObjectNotFoundException("Bank statement not found")

        if (
            not statement.extracted_details
            or statement.status != BankAccountStatementStatus.EXTRACTED
        ):
            raise ObjectNotFoundException(
                "Bank statement details not found or not extracted"
            )

        # Convert extracted details to transactions
        transactions = []
        for detail_dict in statement.extracted_details:
            detail = BankAccountStatementDetailSchema.model_validate(
                detail_dict
            )
            if not detail.date_of_statement or not detail.amount:
                continue

            # Parse the date string into a datetime object
            transaction_date = datetime.combine(
                detail.date_of_statement, datetime.min.time()
            )

            transactions.append(
                CreateBankTransactionSchema(
                    account_id=bank_account_id,
                    company_id=company_id,
                    date=transaction_date,
                    booking_date=transaction_date,
                    value_date=transaction_date,
                    status="BOOKED",
                    amount=detail.amount,
                    description=detail.description,
                    supplier=detail.supplier,
                )
            )

        await self._transaction_service.store_transactions(
            company_id=company_id,
            transactions=transactions,
        )

        await self._repository.update(
            obj=statement,
            updated_data=UpdateBankAccountStatementSchema(
                status=BankAccountStatementStatus.CONFIRMED,
            ),
        )
