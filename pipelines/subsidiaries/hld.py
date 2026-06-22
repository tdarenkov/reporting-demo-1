"""HLD — multi-format ingest into bronze + silver.

The HLD source is the messiest of the eight. The original loader covered
about six jobs in one file:

  - Sales / customers / SKUs as JSON         (bronze, simple)
  - Trial-balance CSV                         (bronze, dotted-date format
                                               + multi-row metadata header)
  - GL-detail CSV                             (bronze, header-row detection
                                               inside the file body)
  - Bank-statement CSV                        (bronze, dotted-date format
                                               + SEPA payment-purpose tags)
  - Counterparties                            (bronze, JSON in the demo)
  - Bank-transaction matching                 (SILVER — procedural matching
                                               that lives in the loader, not
                                               in dbt, because it's regex +
                                               fuzzy-string work)

The matching at the bottom is the procedural enrichment kept in Python:
SEPA tag extraction, name normalization (accent fold + legal-form strip),
and a fuzzy token-set match against counterparties to assign a
`counterparty_number`. The result writes to silver_hld.bank_transactions.
"""
from __future__ import annotations

import csv
import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterator, NamedTuple, TypedDict

import dlt
import psycopg

from ..core import log, config, io, loaders, transforms

SOURCE_DIR = config.SOURCES_ROOT / "hld"

logger = log.getLogger(__name__)


# ---------------------------------------------------------------------------
# Localized number / date helpers
#
# Source files use the European convention: dot-separated thousands, comma
# decimals, dd.mm.yyyy dates. Both helpers tolerate quoted values (some
# exporters wrap amounts in double quotes).
# ---------------------------------------------------------------------------

def parse_european_amount(s) -> Decimal | None:
    """'1.234,56' → Decimal('1234.56'); '"1.234,56"' (quoted) also works."""
    if s is None:
        return None
    s = str(s).strip().strip('"')
    if not s:
        return None
    try:
        return Decimal(s.replace(".", "").replace(",", "."))
    except (InvalidOperation, ValueError):
        return None


def parse_european_date(s) -> date | None:
    """'%d.%m.%Y' → date."""
    if not s:
        return None
    try:
        return datetime.strptime(str(s).strip(), "%d.%m.%Y").date()
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Bronze row schemas
# ---------------------------------------------------------------------------

class CustomersImportRow(TypedDict):
    customer_number: str
    customer_name: str
    iban: str
    bic: str
    vat_id: str
    updated_at: datetime


class SKUsImportRow(TypedDict, total=False):
    """SKU JSON passes through; only `updated_at` is added."""
    updated_at: datetime


class SalesImportRow(TypedDict):
    doc_id: int | None
    doc_num: int | None
    process_id: int | None
    doc_type: str
    period: int | None
    address_id: int | None
    customer_number: str
    date: date | None
    amount_header: Decimal | None
    detail_id: int | None
    process_pos_id: int | None
    salesperson_id: str | None
    sku: str
    amount: Decimal | None
    quantity: Decimal | None
    updated_at: datetime


class TBSImportRow(TypedDict):
    account: str
    account_name: str
    date: date | None
    start: Decimal
    period: Decimal
    end: Decimal
    updated_at: datetime


class GLImportRow(TypedDict):
    account: str
    account_name: str
    date: date | None
    posting_key: str | None
    contra_account: str | None
    contra_account_name: str | None
    contents: str | None
    doc_no: str | None
    amount: Decimal
    updated_at: datetime


class BankTransactionsImportRow(TypedDict):
    bank_account: str
    booking_date: date | None
    value_date: date | None
    booking_text: str
    purpose: str
    eref: str | None
    kref: str | None
    mref: str | None
    svwz: str | None
    invoice_no: str | None
    invoice_date: date | None
    customer_number_hint: str | None
    beneficiary: str
    iban: str
    bic: str
    amount: Decimal | None
    currency: str
    updated_at: datetime


# ---------------------------------------------------------------------------
# Resources — bronze
# ---------------------------------------------------------------------------

@dlt.resource(name="customers_import", write_disposition="replace")
def customers_import() -> Iterator[CustomersImportRow]:
    rows = io.read_json(SOURCE_DIR / "counterparties.json")
    now = transforms.utc_now()
    for r in rows:
        yield {
            "customer_number": r["account"],
            "customer_name": r["name"],
            "iban": r["iban"],
            "bic": r["bic"],
            "vat_id": r["vat_id"],
            "updated_at": now,
        }


@dlt.resource(name="skus_import", write_disposition="replace")
def skus_import() -> Iterator[dict]:
    rows = io.read_json(SOURCE_DIR / "skus.json")
    now = transforms.utc_now()
    for r in rows:
        yield {**r, "updated_at": now}


@dlt.resource(name="sales_import", write_disposition="replace")
def sales_import() -> Iterator[SalesImportRow]:
    rows = io.read_json(SOURCE_DIR / "sales.json")
    now = transforms.utc_now()
    c = transforms.coalesce
    for r in rows:
        yield {
            "doc_id": transforms.to_int(r["doc_id"]),
            "doc_num": transforms.to_int(r["doc_num"]),
            "process_id": transforms.to_int(r["process_id"]),
            "doc_type": r["doc_type"],
            "period": transforms.to_int(r["period"]),
            "address_id": transforms.to_int(r["address_id"]),
            "customer_number": r["customer_number"],
            "date": transforms.parse_date(r["date"], fmt="%Y-%m-%d"),
            "amount_header": transforms.to_decimal(r["amount_header"]),
            "detail_id": transforms.to_int(r["detail_id"]),
            "process_pos_id": transforms.to_int(r["process_pos_id"]),
            "salesperson_id": c(r["salesperson_id"]),
            "sku": r["sku"],
            "amount": transforms.to_decimal(r["amount"]),
            "quantity": transforms.to_decimal(r["quantity"]),
            "updated_at": now,
        }


# ---------------------------------------------------------------------------
# Trial-balance CSV — multi-row metadata header followed by tabular data.
#   Row 5 holds the period date and currency.
#   Row 7 has the column headers; data starts at row 8.
# ---------------------------------------------------------------------------

@dlt.resource(name="tbs_import", write_disposition="replace")
def tbs_import() -> Iterator[TBSImportRow]:
    path = io.latest_match(SOURCE_DIR, "hld_tb", suffix=".csv")
    lines = path.read_text(encoding="utf-8").splitlines()
    if len(lines) < 8:
        return

    # Row 5 (index 4): "...;01.03.2026;March 2026;EUR;..."
    meta = lines[4].split(";")
    period_date = parse_european_date(meta[1]) if len(meta) > 1 else None

    now = transforms.utc_now()
    for line in lines[7:]:
        if not line.strip():
            continue
        fields = line.split(";")
        if len(fields) < 10:
            continue
        # Field 0: account, padded to 5 digits (the source convention).
        account = fields[0].strip().zfill(5)
        account_name = fields[1].strip()
        eb_dr = parse_european_amount(fields[2]) or Decimal("0")
        eb_cr = parse_european_amount(fields[3]) or Decimal("0")
        per_dr = parse_european_amount(fields[4]) or Decimal("0")
        per_cr = parse_european_amount(fields[5]) or Decimal("0")
        bal_dr = parse_european_amount(fields[8]) or Decimal("0")
        bal_cr = parse_european_amount(fields[9]) or Decimal("0")

        yield {
            "account": account,
            "account_name": account_name,
            "date": period_date,
            "start": eb_dr - eb_cr,
            "period": per_dr - per_cr,
            "end": bal_dr - bal_cr,
            "updated_at": now,
        }


# ---------------------------------------------------------------------------
# GL-detail CSV — header-row detection: scan for the line that starts the
# data section, then read the rest as CSV.
# ---------------------------------------------------------------------------

_GL_HEADER_PREFIX = "account;account_name;date;"


@dlt.resource(name="gl_import", write_disposition="replace")
def gl_import() -> Iterator[GLImportRow]:
    path = io.latest_match(SOURCE_DIR, "hld_gl", suffix=".csv")
    lines = path.read_text(encoding="utf-8").splitlines()

    # Find the data-section header inside the file (rows above are metadata).
    header_idx = next(
        (i for i, ln in enumerate(lines) if ln.startswith(_GL_HEADER_PREFIX)),
        None,
    )
    if header_idx is None:
        return

    now = transforms.utc_now()
    for line in lines[header_idx + 1:]:
        if not line.strip():
            continue
        # csv.reader handles quoted fields properly (some exporters quote
        # text values that contain semicolons).
        fields = next(csv.reader([line], delimiter=";"))
        if len(fields) < 10:
            continue
        debit = parse_european_amount(fields[8]) or Decimal("0")
        credit = parse_european_amount(fields[9]) or Decimal("0")
        yield {
            "account": fields[0].strip().zfill(5),
            "account_name": fields[1].strip(),
            "date": parse_european_date(fields[2]),
            "posting_key": fields[3].strip().strip('"') or None,
            "contra_account": fields[4].strip().zfill(5) if fields[4].strip() else None,
            "contra_account_name": fields[5].strip().strip('"') or None,
            "contents": fields[6].strip().strip('"') or None,
            "doc_no": fields[7].strip().strip('"') or None,
            "amount": debit - credit,
            "updated_at": now,
        }


# ---------------------------------------------------------------------------
# Bank statement — payment-purpose (SEPA) tag parsing.
#
# SEPA defines a small set of structured tags concatenated into a single
# free-text "purpose" field. We split on `(TAG)+` and slice each section
# until the next known tag — same idiom as the original loader, fewer lines.
# ---------------------------------------------------------------------------

KNOWN_SEPA_TAGS = ("EREF", "KREF", "MREF", "CRED", "SVWZ", "ABWE", "ABWA", "PURP")
_SEPA_TAG_RE = re.compile(r"(" + "|".join(KNOWN_SEPA_TAGS) + r")\+")
_INVOICE_RE = re.compile(r"RNr\.?\s+([\w\-/]+)")
_INVOICE_DATE_RE = re.compile(r"RDat\.?\s+(\d{1,2}\.\d{1,2}\.\d{4})")
_CUSTOMER_RE = re.compile(r"KNr\.?\s+([\w\-]+)")


def extract_sepa_tags(purpose: str) -> dict[str, str]:
    """Split a SEPA payment-purpose string into {tag: value} pairs.

    SVWZ / EREF / KREF / MREF and friends are concatenated by '+' delimiters;
    each tag's value runs until the next known tag or end-of-string.
    """
    tags: dict[str, str] = {}
    matches = list(_SEPA_TAG_RE.finditer(purpose))
    for i, m in enumerate(matches):
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(purpose)
        tags[m.group(1)] = purpose[start:end].strip()
    return tags


def parse_payment_purpose(svwz: str) -> dict:
    """Pull invoice number, invoice date, and customer number out of the SVWZ text."""
    inv = _INVOICE_RE.search(svwz)
    invd = _INVOICE_DATE_RE.search(svwz)
    cust = _CUSTOMER_RE.search(svwz)
    return {
        "invoice_no": inv.group(1) if inv else None,
        "invoice_date": parse_european_date(invd.group(1)) if invd else None,
        "customer_number": cust.group(1) if cust else None,
    }


@dlt.resource(name="bank_transactions_import", write_disposition="replace")
def bank_transactions_import() -> Iterator[BankTransactionsImportRow]:
    path = io.latest_match(SOURCE_DIR, "hld_bank", suffix=".csv")
    now = transforms.utc_now()
    with path.open(encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=";")
        for r in reader:
            purpose = r["purpose"] or ""
            tags = extract_sepa_tags(purpose)
            parsed = parse_payment_purpose(tags.get("SVWZ", purpose))
            yield {
                "bank_account": r["bank_account"],
                "booking_date": parse_european_date(r["booking_date"]),
                "value_date": parse_european_date(r["value_date"]),
                "booking_text": r["booking_text"],
                "purpose": purpose,
                "eref": tags.get("EREF"),
                "kref": tags.get("KREF"),
                "mref": tags.get("MREF"),
                "svwz": tags.get("SVWZ"),
                "invoice_no": parsed["invoice_no"],
                "invoice_date": parsed["invoice_date"],
                "customer_number_hint": parsed["customer_number"],
                "beneficiary": r["beneficiary"],
                "iban": r["iban"],
                "bic": r["bic"],
                "amount": parse_european_amount(r["amount"]),
                "currency": r["currency"],
                "updated_at": now,
            }


# ---------------------------------------------------------------------------
# Source — bronze
# ---------------------------------------------------------------------------

@dlt.source(name="hld")
def hld_source():
    return [
        customers_import(), skus_import(), sales_import(),
        tbs_import(), gl_import(), bank_transactions_import(),
    ]


# ---------------------------------------------------------------------------
# SILVER — bank-transaction matching.
#
# Deliberate exception to the "pipelines own bronze, dbt owns silver" split:
# this enrichment is procedural and awkward in SQL — normalize names (accents
# to ASCII, strip legal-form suffixes), tokenize, and fuzzy-match a bank line's
# beneficiary against the counterparty list to assign a counterparty number —
# so it stays in Python and writes directly to silver_hld.bank_transactions.
# ---------------------------------------------------------------------------

ACCENT_TO_ASCII = {
    "ä": "ae", "ö": "oe", "ü": "ue", "ß": "ss",
    "Ä": "Ae", "Ö": "Oe", "Ü": "Ue",
    "é": "e", "è": "e", "ê": "e", "ë": "e",
    "á": "a", "à": "a", "â": "a",
    "í": "i", "ì": "i", "î": "i",
    "ó": "o", "ò": "o", "ô": "o",
    "ú": "u", "ù": "u", "û": "u",
    "ñ": "n", "ç": "c",
}
LEGAL_FORM_SUFFIXES = (
    "GmbH & Co. KG", "GmbH", "AG", "KG", "OHG", "S.A.", "S.A.S.", "S.r.l.",
    "Ltd.", "Limited", "Inc.", "Inc", "Corp.", "Corp", "LLC",
    "Holdings", "Holding",
)


def normalize_name(name: str) -> str:
    """Lowercase, ASCII-fold accents, strip legal-form suffixes, collapse whitespace."""
    s = name or ""
    for ch, repl in ACCENT_TO_ASCII.items():
        s = s.replace(ch, repl)
    for form in LEGAL_FORM_SUFFIXES:
        s = re.sub(rf"\b{re.escape(form)}\b", "", s, flags=re.IGNORECASE)
    s = re.sub(r"[^\w\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip().lower()


def jaccard_token_overlap(a: str, b: str) -> float:
    """Token-set overlap |A∩B| / |A∪B|. Cheap and effective for short names."""
    sa, sb = set(a.split()), set(b.split())
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)


class CounterpartyMatch(NamedTuple):
    """A resolved counterparty plus how it was matched (so the caller doesn't
    have to re-derive the method or re-run the fuzzy score)."""
    counterparty: dict
    method: str            # "iban" | "customer_hint" | "fuzzy_name"
    score: float | None    # Jaccard score for "fuzzy_name", else None


def match_counterparty(bank_row: dict, candidates: list[dict]) -> CounterpartyMatch | None:
    """Pick the best counterparty for a bank transaction.

    Match priority:
      1. Exact IBAN match.
      2. Customer-number hint embedded in the SVWZ tag (`KNr. XXXXX`).
      3. Fuzzy name match (Jaccard ≥ 0.5).
    Returns a CounterpartyMatch (counterparty + method + score) or None.
    """
    if bank_row.get("iban"):
        for c in candidates:
            if c["iban"] and bank_row["iban"] == c["iban"]:
                return CounterpartyMatch(c, "iban", None)

    hint = bank_row.get("customer_number_hint")
    if hint:
        for c in candidates:
            if c["customer_number"] == hint:
                return CounterpartyMatch(c, "customer_hint", None)

    target = normalize_name(bank_row.get("beneficiary") or "")
    if not target:
        return None
    best, score = None, 0.0
    for c in candidates:
        s = jaccard_token_overlap(target, normalize_name(c["customer_name"]))
        if s > score:
            best, score = c, s
    return CounterpartyMatch(best, "fuzzy_name", score) if score >= 0.5 else None


def enrich_bank_transactions() -> int:
    """Read bronze bank rows + counterparties, match, write silver. Returns row count."""
    dsn = config.database_url()
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT customer_number, customer_name, iban FROM bronze_hld.customers_import")
        candidates = [
            {"customer_number": r[0], "customer_name": r[1], "iban": r[2]}
            for r in cur.fetchall()
        ]
        # NB: dlt only creates columns for fields that actually had data in
        # the source. Resources that yield None for kref/mref every row mean
        # those columns may not exist on bronze. Pull only what we use.
        cur.execute("""
            SELECT booking_date, value_date, booking_text, purpose,
                   eref, svwz, invoice_no, invoice_date,
                   customer_number_hint, beneficiary, iban, bic, amount, currency
            FROM bronze_hld.bank_transactions_import
        """)
        cols = [d[0] for d in cur.description]
        bronze_rows = [dict(zip(cols, r)) for r in cur.fetchall()]

        now = transforms.utc_now()
        cur.execute("CREATE SCHEMA IF NOT EXISTS silver_hld")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS silver_hld.bank_transactions (
                booking_date            date,
                value_date              date,
                booking_text            text,
                beneficiary             text,
                iban                    text,
                bic                     text,
                amount                  numeric,
                currency                text,
                invoice_no              text,
                invoice_date            date,
                counterparty_number     text,
                counterparty_name       text,
                match_method            text,
                match_score             numeric,
                updated_at              timestamp(6) NOT NULL,
                PRIMARY KEY (booking_date, beneficiary, amount)
            )
        """)
        cur.execute("TRUNCATE silver_hld.bank_transactions")

        params = []
        for r in bronze_rows:
            match = match_counterparty(r, candidates)
            if match is None:
                cp_number = cp_name = None
                method, score = "unmatched", None
            else:
                cp_number = match.counterparty["customer_number"]
                cp_name = match.counterparty["customer_name"]
                method, score = match.method, match.score
            params.append((
                r["booking_date"], r["value_date"], r["booking_text"],
                r["beneficiary"], r["iban"], r["bic"],
                r["amount"], r["currency"], r["invoice_no"], r["invoice_date"],
                cp_number, cp_name, method, score, now,
            ))

        cur.executemany(
            """
            INSERT INTO silver_hld.bank_transactions
              (booking_date, value_date, booking_text, beneficiary, iban, bic,
               amount, currency, invoice_no, invoice_date,
               counterparty_number, counterparty_name, match_method, match_score,
               updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (booking_date, beneficiary, amount) DO UPDATE SET
              counterparty_number = EXCLUDED.counterparty_number,
              counterparty_name = EXCLUDED.counterparty_name,
              match_method = EXCLUDED.match_method,
              match_score = EXCLUDED.match_score,
              updated_at = EXCLUDED.updated_at
            """,
            params,
        )
        conn.commit()
    return len(params)


def run() -> None:
    """Bronze ingest, then silver enrichment."""
    pipeline = loaders.bronze_pipeline("hld")
    info = pipeline.run(hld_source())
    logger.info("%s", info)
    n = enrich_bank_transactions()
    logger.info("silver_hld.bank_transactions: %d rows enriched", n)


if __name__ == "__main__":
    run()
