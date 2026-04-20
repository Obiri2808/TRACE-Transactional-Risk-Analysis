from __future__ import annotations

from pydantic import BaseModel, Field, field_validator
from typing import List, Optional, Tuple
from datetime import date, datetime
import json
import os
import pandas as pd

# =========================
# Pydantic Schemas
# =========================

class CustomerData(BaseModel):
    customer_id: str = Field(..., description="Unique customer identifier")
    name: str
    date_of_birth: date
    ssn_last_4: str = Field(..., description="Last 4 digits of SSN")
    address: str
    customer_since: date
    risk_rating: str = Field(..., description="Low / Medium / High")
    phone: Optional[str] = ""
    email: Optional[str] = ""

    @field_validator('ssn_last_4')
    @classmethod
    def ssn_last_4_must_be_4_digits(cls, v: str) -> str:
        if v is None:
            raise ValueError('ssn_last_4 is required')
        v = str(v)
        if len(v) != 4 or not v.isdigit():
            raise ValueError('ssn_last_4 must be exactly 4 digits')
        return v

    @field_validator('risk_rating')
    @classmethod
    def risk_rating_allowed(cls, v: str) -> str:
        allowed = {'Low','Medium','High'}
        if v not in allowed:
            raise ValueError(f"risk_rating must be one of {sorted(allowed)}")
        return v


class AccountData(BaseModel):
    account_id: str
    customer_id: str
    account_type: str
    opening_date: date
    current_balance: float
    average_monthly_balance: float
    status: str

    @field_validator('current_balance','average_monthly_balance')
    @classmethod
    def balances_non_negative(cls, v: float) -> float:
        if v < 0:
            raise ValueError('Balance fields must be non-negative')
        return float(v)


class TransactionData(BaseModel):
    transaction_id: str
    account_id: str
    transaction_date: date
    transaction_type: str
    amount: float
    description: Optional[str] = ""
    method: str
    counterparty: Optional[str] = ""
    location: Optional[str] = ""

    @field_validator('amount')
    @classmethod
    def amount_positive(cls, v: float) -> float:
        # transactions can be debits/credits; for this project we validate abs(amount) > 0
        if v == 0:
            raise ValueError('amount must be non-zero')
        return float(v)


class CaseData(BaseModel):
    case_id: str
    customer: CustomerData
    accounts: List[AccountData]
    transactions: List[TransactionData]


class RiskAnalystOutput(BaseModel):
    classification: str = Field(..., description="Structuring, Sanctions, Fraud, Money_Laundering, Other")
    confidence_score: float = Field(..., ge=0.0, le=1.0)
    risk_level: str = Field(..., description="Low, Medium, High, Critical")
    reasoning: str
    key_indicators: List[str]


class ComplianceOfficerOutput(BaseModel):
    narrative: str
    regulatory_citations: List[str] = Field(default_factory=list)


# =========================
# Utilities
# =========================

class ExplainabilityLogger:
    """Append-only JSONL logger for audit trails."""

    def __init__(self, log_path: str = 'outputs/audit_logs/audit_log.jsonl'):
        self.log_path = log_path
        os.makedirs(os.path.dirname(self.log_path), exist_ok=True)

    def log(self, event_type: str, payload: dict) -> None:
        entry = {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'event_type': event_type,
            'payload': payload,
        }
        with open(self.log_path, 'a', encoding='utf-8') as f:
            f.write(json.dumps(entry, ensure_ascii=False) + '
')


def load_csv_data(data_dir: str = 'data') -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load CSVs and apply minimal cleaning to optional fields."""
    customers_df = pd.read_csv(os.path.join(data_dir, 'customers.csv'), dtype={'ssn_last_4': str})
    accounts_df = pd.read_csv(os.path.join(data_dir, 'accounts.csv'))
    transactions_df = pd.read_csv(os.path.join(data_dir, 'transactions.csv'))

    # Fill optional fields if present
    for col in ['phone','email']:
        if col in customers_df.columns:
            customers_df[col] = customers_df[col].fillna('')

    for col in ['description','counterparty','location']:
        if col in transactions_df.columns:
            transactions_df[col] = transactions_df[col].fillna('')

    return customers_df, accounts_df, transactions_df


class DataLoader:
    """Build CaseData objects from CSV data."""

    def __init__(self, logger: ExplainabilityLogger, data_dir: str = 'data'):
        self.logger = logger
        self.data_dir = data_dir

    def create_case_from_data(self, customer: dict, accounts: List[dict], transactions: List[dict]) -> CaseData:
        # Ensure required identifiers
        case_id = customer.get('customer_id')
        case_id = f"CASE_{case_id}" if case_id else 'CASE_UNKNOWN'

        cust = CustomerData(**customer)
        accs = [AccountData(**a) for a in accounts]
        txns = [TransactionData(**t) for t in transactions]

        case = CaseData(case_id=case_id, customer=cust, accounts=accs, transactions=txns)
        self.logger.log('case_created', {'case_id': case.case_id, 'customer_id': cust.customer_id, 'accounts': len(accs), 'transactions': len(txns)})
        return case

    def load_case(self, customer_id: str) -> CaseData:
        customers_df, accounts_df, transactions_df = load_csv_data(self.data_dir)
        # select
        cust_rows = customers_df[customers_df['customer_id'] == customer_id]
        if cust_rows.empty:
            raise ValueError(f"customer_id not found: {customer_id}")
        customer = cust_rows.iloc[0].to_dict()

        acc_rows = accounts_df[accounts_df['customer_id'] == customer_id]
        accounts = [r.to_dict() for _, r in acc_rows.iterrows()]

        account_ids = set(acc_rows['account_id'].tolist())
        txn_rows = transactions_df[transactions_df['account_id'].isin(account_ids)]
        transactions = [r.to_dict() for _, r in txn_rows.iterrows()]

        self.logger.log('data_loaded', {'customer_id': customer_id, 'accounts': len(accounts), 'transactions': len(transactions)})
        return self.create_case_from_data(customer, accounts, transactions)
