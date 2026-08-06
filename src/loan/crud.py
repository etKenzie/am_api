from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Optional

try:
    from .date_filters import append_date_filters, month_bounds, start_of_day, end_of_day
except ImportError:
    from loan.date_filters import append_date_filters, month_bounds, start_of_day, end_of_day

# Loan type constants
# Aku Cicil is identified in loan_setting by loan_type (e.g. id 44); keep in sync with DB.
_AKU_CICIL_SETTING_IDS = "SELECT ls.id FROM loan_setting ls WHERE ls.loan_type = 'AkuCicil'"
LOAN_CONDITIONS = f"l.duration = 1 AND l.loan_id NOT IN ({_AKU_CICIL_SETTING_IDS})"
EXTRADANA_LOAN_CONDITIONS = (
    f"l.duration != 1 AND l.disbursement != 4 AND l.loan_id NOT IN ({_AKU_CICIL_SETTING_IDS})"
)
AKU_CICIL_CONDITION = f"l.loan_id IN ({_AKU_CICIL_SETTING_IDS})"
KASBON_CONDITION = LOAN_CONDITIONS  # Same as LOAN_CONDITIONS; used in td_loan_history context

# Union of kasbon + extradana + aku_cicil (single td_loan filter for combined queries)
ALL_LOAN_CONDITIONS = (
    f"(({LOAN_CONDITIONS}) OR ({EXTRADANA_LOAN_CONDITIONS}) OR ({AKU_CICIL_CONDITION}))"
)
INSTALLMENT_LOAN_CONDITIONS = (
    f"(({EXTRADANA_LOAN_CONDITIONS}) OR ({AKU_CICIL_CONDITION}))"
)

# Bad debt recovery: a record is only "bad debt" once it was actually paid (not merely
# overdue) and that payment landed three calendar months or more after the due date's
# month — a pure M+3 rule, no day-of-month cutoff. The due month plus the next two
# calendar months (M, M+1, M+2) still count as Repayment; only M+3 onward is Bad Debt
# Recovery. E.g. due any day in Jan: paid anytime in Jan/Feb/Mar is NOT bad debt (still
# Repayment), paid 1 Apr onward IS bad debt.
# Still-unpaid loans never match this, regardless of how overdue — they remain in
# repayment-risk's unrecovered/outstanding buckets instead.
_BAD_DEBT_LUMP_PREDICATE = (
    "l.payment_date IS NOT NULL AND l.payment_date != '0000-00-00' "
    "AND PERIOD_DIFF(DATE_FORMAT(l.payment_date, '%Y%m'), DATE_FORMAT(l.repayment_date, '%Y%m')) >= 3"
)
_BAD_DEBT_INSTALLMENT_PREDICATE = (
    "tlh.payment_date IS NOT NULL AND tlh.payment_date != '0000-00-00' "
    "AND PERIOD_DIFF(DATE_FORMAT(tlh.payment_date, '%Y%m'), DATE_FORMAT(tlh.due_date, '%Y%m')) >= 3"
)

# repayment-risk reporting-month attribution: each repayment is counted in exactly one
# month — the payment month if paid on/before its due date, or once it has crossed into
# Bad Debt Recovery (see the predicates above); otherwise the original due month. A row
# with no payment yet has no payment_date, so this always falls through to the due date,
# consistent with the live unrecovered/outstanding buckets (which are keyed off due date).
_REPORTING_DATE_LUMP = (
    "CASE WHEN l.payment_date IS NOT NULL AND l.payment_date != '0000-00-00' "
    "AND (l.payment_date <= l.repayment_date OR ({bad_debt})) "
    "THEN l.payment_date ELSE l.repayment_date END"
).format(bad_debt=_BAD_DEBT_LUMP_PREDICATE)
_REPORTING_DATE_INSTALLMENT = (
    "CASE WHEN tlh.payment_date IS NOT NULL AND tlh.payment_date != '0000-00-00' "
    "AND (tlh.payment_date <= tlh.due_date OR ({bad_debt})) "
    "THEN tlh.payment_date ELSE tlh.due_date END"
).format(bad_debt=_BAD_DEBT_INSTALLMENT_PREDICATE)

# Partial-payment credit: td_loan_payment / td_loan_payment_allocation is ak-mj's "Refund
# Management" side-channel for manual/extra repayments made outside normal payroll
# deduction (only a handful of rows out of ~127k fully-paid loans/installments go through
# it — the rest close via payroll deduction directly, which never touches these tables).
# A payment there only flips td_loan_history.status/td_loan.loan_status to Paid (2) once it
# fully covers the installment/loan (see ak-mj's M_refund::activate_loan_status); a payment
# that doesn't close the row out entirely leaves status/payment_date untouched, so every
# collected/recovered query above (gated on status = 2) never sees it even though real cash
# came in. These fragments credit that partial amount instead, using the payment's own
# created_at as its date (td_loan_payment.payment_date is unused/always NULL in practice)
# and the same M+3 rule as the row-level predicates above, applied per payment: a partial
# payment landing 3+ calendar months after the due month counts toward Bad Debt Recovery
# (dated to the payment's own month); otherwise it counts toward ordinary Repayment
# "collected" (dated to the due month, matching how a late-but-within-grace-period full
# payment is attributed). Scoped to status/loan_status = 4 (overdue, still open) rows only —
# a status = 2 row's payment(s) are already fully reflected via the row-level query above.
# ak-mj never splits a payment between principal and admin fee, so the split here is
# proportional to the loan's own principal:fee ratio (same ratio the row-level queries use
# for a full installment/loan).
_BAD_DEBT_PARTIAL_INSTALLMENT_PREDICATE = (
    "PERIOD_DIFF(DATE_FORMAT(pay.created_at, '%Y%m'), DATE_FORMAT(tlh.due_date, '%Y%m')) >= 3"
)
_REPORTING_DATE_PARTIAL_INSTALLMENT = (
    "CASE WHEN {bad_debt} THEN pay.created_at ELSE tlh.due_date END"
).format(bad_debt=_BAD_DEBT_PARTIAL_INSTALLMENT_PREDICATE)
_BAD_DEBT_PARTIAL_LUMP_PREDICATE = (
    "PERIOD_DIFF(DATE_FORMAT(pay.created_at, '%Y%m'), DATE_FORMAT(l.repayment_date, '%Y%m')) >= 3"
)
_REPORTING_DATE_PARTIAL_LUMP = (
    "CASE WHEN {bad_debt} THEN pay.created_at ELSE l.repayment_date END"
).format(bad_debt=_BAD_DEBT_PARTIAL_LUMP_PREDICATE)

_PARTIAL_PAYMENTS_INSTALLMENT_JOIN_SQL = """
    INNER JOIN (
        SELECT p.loan_history_id AS loan_history_id, p.amount, p.created_at
        FROM td_loan_payment p
        WHERE p.status = 1 AND p.loan_history_id IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM td_loan_payment_allocation a WHERE a.payment_id = p.id)
        UNION ALL
        SELECT a.loan_history_id, a.amount, a.created_at
        FROM td_loan_payment_allocation a
        INNER JOIN td_loan_payment p ON p.id = a.payment_id AND p.status = 1
    ) pay ON pay.loan_history_id = tlh.id"""


def _installment_partial_recovery_sql(loan_conditions_tl: str) -> str:
    """Per-payment-transaction principal/fee credit for still-open (status=4) installments
    that have received a partial payment. See the block comment above
    _BAD_DEBT_PARTIAL_INSTALLMENT_PREDICATE."""
    return f"""
    SELECT
        {_REPORTING_DATE_PARTIAL_INSTALLMENT} AS reporting_date,
        CASE WHEN {_BAD_DEBT_PARTIAL_INSTALLMENT_PREDICATE} THEN 1 ELSE 0 END AS is_bad_debt,
        tlh.id AS row_id,
        ROUND(pay.amount * ROUND(l.total_loan / l.duration, 0) / tlh.monthly, 0) AS principal_portion,
        pay.amount - ROUND(pay.amount * ROUND(l.total_loan / l.duration, 0) / tlh.monthly, 0) AS fee_portion
    FROM td_loan_history tlh
    INNER JOIN td_loan l ON tlh.loan_form_id = l.id
    {_LOAN_GMC_JOINS}
    {_PARTIAL_PAYMENTS_INSTALLMENT_JOIN_SQL}
    WHERE tlh.status = 4
      AND tlh.monthly > 0
      AND l.id_karyawan IS NOT NULL
      AND {loan_conditions_tl}
    """


def _lump_partial_recovery_sql(loan_conditions: str) -> str:
    """Per-payment-transaction principal/fee credit for still-open (loan_status=4) kasbon
    loans that have received a partial payment. See the block comment above
    _BAD_DEBT_PARTIAL_LUMP_PREDICATE."""
    return f"""
    SELECT
        {_REPORTING_DATE_PARTIAL_LUMP} AS reporting_date,
        CASE WHEN {_BAD_DEBT_PARTIAL_LUMP_PREDICATE} THEN 1 ELSE 0 END AS is_bad_debt,
        l.id AS row_id,
        ROUND(pay.amount * l.total_loan / l.total_payment, 0) AS principal_portion,
        pay.amount - ROUND(pay.amount * l.total_loan / l.total_payment, 0) AS fee_portion
    FROM td_loan l
    {_LOAN_GMC_JOINS}
    INNER JOIN (
        SELECT p.loan_id AS loan_id, p.amount, p.created_at
        FROM td_loan_payment p
        WHERE p.status = 1 AND p.loan_history_id IS NULL
    ) pay ON pay.loan_id = l.id
    WHERE l.loan_status = 4
      AND l.duration = 1
      AND l.total_payment > 0
      AND {loan_conditions}
    """


def _partial_recovery_totals(
    db: Session,
    base_sql: str,
    *,
    reporting_date_expr: str,
    bad_debt_predicate: str,
    bad_debt_filter: bool | None = None,
    employer_filter: str = None,
    sourced_to_filter: str = None,
    project_filter: str = None,
    client_segment_filter: str = None,
    product_type_filter: str = None,
    loan_status_filter: int = None,
    id_karyawan_filter: int = None,
    start_date: str = None,
    end_date: str = None,
    db_for_filters: Session = None,
) -> tuple[float, float, int]:
    """(principal_total, fee_total, distinct_row_count) for a partial-recovery base query
    (see _installment_partial_recovery_sql/_lump_partial_recovery_sql), after applying the
    standard repayment-risk org filters and optional reporting-date range. Bad Debt Recovery
    and repayment-risk's principal/admin-fee/performance metrics are mutually exclusive: pass
    bad_debt_filter=True to restrict to payments that themselves crossed the M+3 threshold
    (for the Bad Debt Recovery endpoints), bad_debt_filter=False to restrict to payments that
    have not (for repayment-risk's collected/unrecovered totals) — never leave it None for
    either of those two call sites, or the same payment gets credited to both."""
    params: dict = {}
    query = _apply_repayment_risk_filters(
        base_sql,
        params,
        employer_filter=employer_filter,
        sourced_to_filter=sourced_to_filter,
        project_filter=project_filter,
        client_segment_filter=client_segment_filter,
        product_type_filter=product_type_filter,
        loan_status_filter=loan_status_filter,
        id_karyawan_filter=id_karyawan_filter,
        db=db_for_filters or db,
    )
    if bad_debt_filter is True:
        query += f" AND {bad_debt_predicate}"
    elif bad_debt_filter is False:
        query += f" AND NOT ({bad_debt_predicate})"
    if start_date and end_date:
        query = append_date_filters(
            query, params, start_date=start_date, end_date=end_date, date_column=reporting_date_expr
        )
    wrapped = (
        "SELECT COALESCE(SUM(principal_portion), 0) AS principal_total, "
        "COALESCE(SUM(fee_portion), 0) AS fee_total, "
        f"COUNT(DISTINCT row_id) AS row_count FROM ({query}) t"
    )
    record = db.execute(text(wrapped), params).fetchone()
    if not record:
        return 0, 0, 0
    return (
        record[0] if record[0] is not None else 0,
        record[1] if record[1] is not None else 0,
        record[2] if record[2] is not None else 0,
    )


def _partial_recovery_monthly(
    db: Session,
    base_sql: str,
    *,
    reporting_date_expr: str,
    bad_debt_predicate: str,
    bad_debt_filter: bool | None = None,
    employer_filter: str = None,
    sourced_to_filter: str = None,
    project_filter: str = None,
    client_segment_filter: str = None,
    product_type_filter: str = None,
    loan_status_filter: int = None,
    id_karyawan_filter: int = None,
    start_date: str = None,
    end_date: str = None,
    db_for_filters: Session = None,
) -> dict:
    """Month-bucketed twin of _partial_recovery_totals: {month_year: (principal, fee, row_count)}.
    See _partial_recovery_totals for bad_debt_filter semantics."""
    params: dict = {}
    query = _apply_repayment_risk_filters(
        base_sql,
        params,
        employer_filter=employer_filter,
        sourced_to_filter=sourced_to_filter,
        project_filter=project_filter,
        client_segment_filter=client_segment_filter,
        product_type_filter=product_type_filter,
        loan_status_filter=loan_status_filter,
        id_karyawan_filter=id_karyawan_filter,
        db=db_for_filters or db,
    )
    if bad_debt_filter is True:
        query += f" AND {bad_debt_predicate}"
    elif bad_debt_filter is False:
        query += f" AND NOT ({bad_debt_predicate})"
    if start_date and end_date:
        query = append_date_filters(
            query, params, start_date=start_date, end_date=end_date, date_column=reporting_date_expr
        )
    wrapped = f"""
    SELECT DATE_FORMAT(reporting_date, '%M %Y') AS month_year,
        COALESCE(SUM(principal_portion), 0) AS principal_total,
        COALESCE(SUM(fee_portion), 0) AS fee_total,
        COUNT(DISTINCT row_id) AS row_count
    FROM ({query}) t
    GROUP BY DATE_FORMAT(reporting_date, '%M %Y')
    """
    monthly = {}
    for row in db.execute(text(wrapped), params).fetchall():
        if row[0] is None:
            continue
        monthly[row[0]] = (
            row[1] if row[1] is not None else 0,
            row[2] if row[2] is not None else 0,
            row[3] if row[3] is not None else 0,
        )
    return monthly


ALL_LOAN_TYPES = ("kasbon", "extradana", "aku_cicil")
ALLOWED_COMPANIES = (
    "PT Valdo Sumber Daya Mandiri",
    "PT Valdo International",
    "PT Toko Pandai",
    "PT Valdo Solusi Integra",
)
COMPANY_FILTER = (
    "('PT Valdo Sumber Daya Mandiri', 'PT Valdo International', "
    "'PT Toko Pandai', 'PT Valdo Solusi Integra')"
)

_cached_aku_cicil_id_list: Optional[str] = None

_LOAN_GMC_JOINS = """
        LEFT JOIN td_karyawan tk
            ON l.id_karyawan = tk.id_karyawan
        LEFT JOIN tbl_gmc emp
            ON tk.valdo_inc = emp.kode_gmc
            AND emp.group_gmc = 'sub_client'
            AND emp.aktif = 'Yes'
            AND emp.keterangan3 = 1
        LEFT JOIN tbl_gmc src
            ON tk.placement = src.kode_gmc
            AND src.group_gmc = 'placement_client'
            AND src.aktif = 'Yes'
            AND src.keterangan3 = 1
        LEFT JOIN tbl_gmc prj
            ON tk.project = prj.kode_gmc
            AND prj.group_gmc = 'client_project'
            AND prj.aktif = 'Yes'
            AND prj.keterangan3 = 1"""

_KARYAWAN_GMC_JOINS = """
        LEFT JOIN tbl_gmc emp
            ON tk.valdo_inc = emp.kode_gmc
            AND emp.group_gmc = 'sub_client'
            AND emp.aktif = 'Yes'
            AND emp.keterangan3 = 1
        LEFT JOIN tbl_gmc src
            ON tk.placement = src.kode_gmc
            AND src.group_gmc = 'placement_client'
            AND src.aktif = 'Yes'
            AND src.keterangan3 = 1
        LEFT JOIN tbl_gmc prj
            ON tk.project = prj.kode_gmc
            AND prj.group_gmc = 'client_project'
            AND prj.aktif = 'Yes'
            AND prj.keterangan3 = 1"""


def _get_aku_cicil_id_list(db: Session) -> str:
    global _cached_aku_cicil_id_list
    if _cached_aku_cicil_id_list is None:
        rows = db.execute(text(
            "SELECT ls.id FROM loan_setting ls WHERE ls.loan_type = 'AkuCicil'"
        )).fetchall()
        _cached_aku_cicil_id_list = ",".join(str(row[0]) for row in rows) if rows else "0"
    return _cached_aku_cicil_id_list


def _loan_conditions_from_ids(loan_type: str, aku_ids: str) -> str:
    loan = f"l.duration = 1 AND l.loan_id NOT IN ({aku_ids})"
    extradana = f"l.duration != 1 AND l.disbursement != 4 AND l.loan_id NOT IN ({aku_ids})"
    aku_cicil = f"l.loan_id IN ({aku_ids})"
    installment = f"(({extradana}) OR ({aku_cicil}))"
    all_loans = f"(({loan}) OR ({extradana}) OR ({aku_cicil}))"

    if is_all_loan_types(loan_type):
        return all_loans
    if loan_type == "extradana":
        return extradana
    if loan_type == "aku_cicil":
        return aku_cicil
    if loan_type == "installment":
        return installment
    if loan_type == "kasbon":
        return loan
    return loan


def _fetch_employee_counts_by_sourced_to(db: Session, company_filter: str) -> dict:
    """Eligible and active employee counts per placement (sourced_to)."""
    query = f"""
        SELECT
            src.keterangan AS sourced_to,
            SUM(CASE WHEN tk.loan_kasbon_eligible = '1' THEN 1 ELSE 0 END) AS eligible,
            COUNT(*) AS active
        FROM td_karyawan tk
        {_KARYAWAN_GMC_JOINS}
        WHERE tk.status = '1'
        AND src.keterangan IS NOT NULL
        AND emp.keterangan IN {company_filter}
        GROUP BY src.keterangan
    """
    rows = db.execute(text(query)).fetchall()
    return {
        row[0]: {"eligible": int(row[1] or 0), "active": int(row[2] or 0)}
        for row in rows
        if row[0] is not None
    }


def _project_management_join_sql(required: bool = True) -> str:
    join_type = "INNER" if required else "LEFT"
    return f"""
        {join_type} JOIN (
            SELECT DISTINCT gmc_id, client_segment, product_type
            FROM tbl_project_management
        ) tpm ON tpm.gmc_id = prj.id"""


def _project_management_label_joins_sql() -> str:
    return """
        LEFT JOIN tbl_gmc seg
            ON seg.kode_gmc = tpm.client_segment
            AND seg.group_gmc = 'segment'
            AND seg.keterangan3 = 1
            AND seg.aktif = 'Yes'
        LEFT JOIN tbl_gmc pt
            ON pt.kode_gmc = tpm.product_type
            AND pt.group_gmc = 'product_type'
            AND pt.keterangan3 = 1
            AND pt.aktif = 'Yes'"""


CLIENT_SEGMENT_ALL_BFSI = "all_bfsi"
CLIENT_SEGMENT_ALL_NON_BFSI = "all_non_bfsi"


def _normalize_client_segment_category(category_id: str, category_name: str) -> tuple[str | None, str | None]:
    combined = f"{category_id or ''} {category_name or ''}".lower().replace("-", " ").replace("_", " ")
    if "non" in combined and "bfsi" in combined:
        return "non_bfsi", "Non-BFSI"
    if combined.strip() in ("bfsi",):
        return "bfsi", "BFSI"
    if "bfsi" in combined:
        return "bfsi", "BFSI"
    return None, None


def _client_segment_category_for_filter_option(
    option_id: str,
    option_name: str,
    category_id: str,
    category_name: str,
) -> tuple[str | None, str | None]:
    """Resolve BFSI / Non-BFSI grouping for /loan/filters display."""
    normalized_id, normalized_name = _normalize_client_segment_category(
        category_id, category_name
    )
    if normalized_id:
        return normalized_id, normalized_name
    return _normalize_client_segment_category(option_name, option_id)


def _aggregate_client_segment_options() -> list[dict]:
    return [
        {
            "option_id": CLIENT_SEGMENT_ALL_BFSI,
            "option_name": "All BFSI",
            "category_id": "bfsi",
            "category_name": "BFSI",
            "is_aggregate": True,
        },
        {
            "option_id": CLIENT_SEGMENT_ALL_NON_BFSI,
            "option_name": "All Non-BFSI",
            "category_id": "non_bfsi",
            "category_name": "Non-BFSI",
            "is_aggregate": True,
        },
    ]


def _empty_client_segment_group(category_id: str, category_name: str) -> dict:
    return {
        "category_id": category_id,
        "category_name": category_name,
        "options": [],
    }


def _client_segment_codes_in_category_sql(category: str) -> str:
    """Segment codes for all_bfsi / all_non_bfsi — matches segment name/code like /loan/filters."""
    norm_name = "LOWER(REPLACE(REPLACE(seg.keterangan, '-', ' '), '_', ' '))"
    norm_code = "LOWER(REPLACE(REPLACE(seg.kode_gmc, '-', ' '), '_', ' '))"
    if category == "bfsi":
        name_match = f"""
            (
                ({norm_name} LIKE '%bfsi%' OR {norm_code} LIKE '%bfsi%')
                AND {norm_name} NOT LIKE '%non%bfsi%'
                AND {norm_code} NOT LIKE '%non%bfsi%'
            )
        """
    else:
        name_match = f"""
            (
                {norm_name} LIKE '%non%bfsi%'
                OR {norm_code} LIKE '%non%bfsi%'
            )
        """
    return f"""
        SELECT DISTINCT seg.kode_gmc
        FROM tbl_gmc seg
        INNER JOIN tbl_project_management tpm
            ON tpm.client_segment = seg.kode_gmc
        WHERE seg.group_gmc = 'segment'
          AND seg.keterangan3 = 1
          AND seg.aktif = 'Yes'
          AND tpm.client_segment IS NOT NULL
          AND tpm.client_segment <> ''
          AND {name_match}
    """


def _resolve_aggregate_segment_codes(db: Session, category: str) -> list[str]:
    """Load BFSI / Non-BFSI segment codes once per DB session (request)."""
    cache = db.info.setdefault("loan_segment_codes", {})
    if category not in cache:
        rows = db.execute(text(_client_segment_codes_in_category_sql(category))).fetchall()
        cache[category] = [row[0] for row in rows if row[0]]
    return cache[category]


def _segment_filter_predicate(
    client_segment_filter: str,
    params: dict,
    db: Session = None,
) -> str | None:
    if not client_segment_filter:
        return None

    client_segment_filter = client_segment_filter.strip()
    if client_segment_filter == CLIENT_SEGMENT_ALL_BFSI:
        category = "bfsi"
    elif client_segment_filter == CLIENT_SEGMENT_ALL_NON_BFSI:
        category = "non_bfsi"
    else:
        params["client_segment"] = client_segment_filter
        return "tpm.client_segment = :client_segment"

    if db is not None:
        codes = _resolve_aggregate_segment_codes(db, category)
        if not codes:
            return "1=0"
        placeholders = []
        for index, code in enumerate(codes):
            key = f"agg_seg_{category}_{index}"
            params[key] = code
            placeholders.append(f":{key}")
        return f"tpm.client_segment IN ({', '.join(placeholders)})"

    return f"tpm.client_segment IN ({_client_segment_codes_in_category_sql(category)})"


def _inject_project_management_join(
    query: str,
    client_segment_filter: str = None,
    product_type_filter: str = None,
    *,
    force_left_join: bool = False,
) -> str:
    if " tpm " in query or "\ntpm " in query:
        return query
    if not (client_segment_filter or product_type_filter or force_left_join):
        return query
    required = bool(client_segment_filter or product_type_filter)
    fragment = _project_management_join_sql(required=required)
    marker = "AND prj.keterangan3 = 1"
    if marker not in query:
        return query
    return query.replace(marker, marker + fragment, 1)


def _apply_project_management_filters(
    query: str,
    params: dict,
    client_segment_filter: str = None,
    product_type_filter: str = None,
    *,
    force_left_join: bool = False,
    db: Session = None,
) -> str:
    segment_predicate = _segment_filter_predicate(client_segment_filter, params, db)
    tpm_conditions = []
    if segment_predicate:
        tpm_conditions.append(segment_predicate)
    if product_type_filter:
        tpm_conditions.append("tpm.product_type = :product_type")
        params["product_type"] = product_type_filter

    if not tpm_conditions:
        return query

    if " tpm " in query or "\ntpm " in query:
        query += " AND " + " AND ".join(tpm_conditions)
        return query

    if force_left_join:
        query = _inject_project_management_join(
            query,
            client_segment_filter,
            product_type_filter,
            force_left_join=True,
        )
        query += " AND " + " AND ".join(tpm_conditions)
        return query

    query += f"""
        AND EXISTS (
            SELECT 1 FROM tbl_project_management tpm
            WHERE tpm.gmc_id = prj.id
            AND {" AND ".join(tpm_conditions)}
        )
    """
    return query


def _build_client_segment_filter_options(rows: list) -> dict:
    grouped: dict[str, dict] = {}
    flat_segments: list[dict] = []

    for row in rows:
        option_id, option_name, category_id, category_name = row[0], row[1], row[2], row[3]
        if option_id is None:
            continue

        normalized_id, normalized_name = _client_segment_category_for_filter_option(
            option_id, option_name, category_id, category_name
        )
        option = {
            "option_id": option_id,
            "option_name": option_name,
            "category_id": normalized_id,
            "category_name": normalized_name,
        }
        flat_segments.append(option)

        if normalized_id:
            group = grouped.setdefault(
                normalized_id,
                {
                    "category_id": normalized_id,
                    "category_name": normalized_name,
                    "options": [],
                },
            )
            group["options"].append(
                {"option_id": option_id, "option_name": option_name}
            )

    client_segment_groups = []
    for aggregate in _aggregate_client_segment_options():
        category_key = aggregate["category_id"]
        group = grouped.get(category_key) or _empty_client_segment_group(
            category_key, aggregate["category_name"]
        )
        child_options = [
            option
            for option in group["options"]
            if option.get("option_id") not in (CLIENT_SEGMENT_ALL_BFSI, CLIENT_SEGMENT_ALL_NON_BFSI)
        ]
        group["options"] = [{**aggregate, "is_aggregate": True}, *child_options]
        client_segment_groups.append(group)

    for group in grouped.values():
        if group["category_id"] not in ("bfsi", "non_bfsi"):
            client_segment_groups.append(group)

    client_segments = [dict(option, is_aggregate=True) for option in _aggregate_client_segment_options()]
    seen_option_ids = {option["option_id"] for option in client_segments}

    for group in client_segment_groups:
        for option in group["options"]:
            option_id = option.get("option_id")
            if not option_id or option_id in seen_option_ids:
                continue
            client_segments.append(option)
            seen_option_ids.add(option_id)

    for option in flat_segments:
        if option["category_id"] is not None or option["option_id"] in seen_option_ids:
            continue
        client_segments.append(
            {"option_id": option["option_id"], "option_name": option["option_name"]}
        )
        seen_option_ids.add(option["option_id"])

    return {
        "client_segments": client_segments,
        "client_segment_groups": client_segment_groups,
    }


def _fetch_project_management_filter_options(db: Session) -> dict:
    product_type_query = """
        SELECT
            tg.kode_gmc AS option_id,
            tg.keterangan AS option_name
        FROM tbl_project_management AS tpm
        INNER JOIN tbl_gmc AS tg
            ON tg.kode_gmc = tpm.product_type
            AND tg.group_gmc = 'product_type'
            AND tg.keterangan3 = 1
            AND tg.aktif = 'Yes'
        WHERE tpm.product_type IS NOT NULL
          AND tpm.product_type <> ''
        GROUP BY tg.kode_gmc, tg.keterangan
        ORDER BY option_id
    """
    client_segment_query = """
        SELECT
            tg.kode_gmc AS option_id,
            tg.keterangan AS option_name,
            COALESCE(parent.kode_gmc, tg.keterangan2) AS category_id,
            COALESCE(parent.keterangan, tg.keterangan2) AS category_name
        FROM tbl_project_management AS tpm
        INNER JOIN tbl_gmc AS tg
            ON tg.kode_gmc = tpm.client_segment
            AND tg.group_gmc = 'segment'
            AND tg.keterangan3 = 1
            AND tg.aktif = 'Yes'
        LEFT JOIN tbl_gmc AS parent
            ON parent.kode_gmc = tg.keterangan2
            AND parent.group_gmc = 'segment'
            AND parent.aktif = 'Yes'
        WHERE tpm.client_segment IS NOT NULL
          AND tpm.client_segment <> ''
        GROUP BY tg.kode_gmc, tg.keterangan, parent.kode_gmc, parent.keterangan, tg.keterangan2
        ORDER BY category_name, option_id
    """
    product_types = [
        {"option_id": row[0], "option_name": row[1]}
        for row in db.execute(text(product_type_query)).fetchall()
        if row[0] is not None
    ]
    segment_options = _build_client_segment_filter_options(
        db.execute(text(client_segment_query)).fetchall()
    )
    return {"product_types": product_types, **segment_options}


def _append_loan_org_filters(
    query: str,
    params: dict,
    *,
    id_karyawan_filter: int = None,
    employer_filter: str = None,
    sourced_to_filter: str = None,
    project_filter: str = None,
    client_segment_filter: str = None,
    product_type_filter: str = None,
    loan_status_filter: int = None,
    company_filter: str = COMPANY_FILTER,
    karyawan_prefix: str = "tk",
    loan_prefix: str = "l",
    force_project_management_join: bool = False,
    db: Session = None,
) -> str:
    query = _apply_project_management_filters(
        query,
        params,
        client_segment_filter,
        product_type_filter,
        force_left_join=force_project_management_join,
        db=db,
    )
    query += f" AND emp.keterangan IN {company_filter}"
    if id_karyawan_filter:
        query += f" AND {loan_prefix}.id_karyawan = :id_karyawan"
        params["id_karyawan"] = id_karyawan_filter
    if employer_filter and employer_filter in ALLOWED_COMPANIES:
        query += " AND emp.keterangan = :employer"
        params["employer"] = employer_filter
    if sourced_to_filter:
        query += " AND src.keterangan = :sourced_to"
        params["sourced_to"] = sourced_to_filter
    if project_filter:
        query += " AND prj.keterangan = :project"
        params["project"] = project_filter
    if loan_status_filter is not None:
        query += f" AND {loan_prefix}.loan_status = :loan_status"
        params["loan_status"] = loan_status_filter
    return query


def _append_karyawan_org_filters(
    query: str,
    params: dict,
    *,
    id_karyawan_filter: int = None,
    employer_filter: str = None,
    sourced_to_filter: str = None,
    project_filter: str = None,
    client_segment_filter: str = None,
    product_type_filter: str = None,
    company_filter: str = COMPANY_FILTER,
    db: Session = None,
) -> str:
    query = _apply_project_management_filters(
        query,
        params,
        client_segment_filter,
        product_type_filter,
        db=db,
    )
    query += f" AND emp.keterangan IN {company_filter}"
    if id_karyawan_filter:
        query += " AND tk.id_karyawan = :id_karyawan"
        params["id_karyawan"] = id_karyawan_filter
    if employer_filter and employer_filter in ALLOWED_COMPANIES:
        query += " AND emp.keterangan = :employer"
        params["employer"] = employer_filter
    if sourced_to_filter:
        query += " AND src.keterangan = :sourced_to"
        params["sourced_to"] = sourced_to_filter
    if project_filter:
        query += " AND prj.keterangan = :project"
        params["project"] = project_filter
    return query


def _eligible_date_range(
    *,
    start_date: str = None,
    end_date: str = None,
    as_of_date: str = None,
) -> tuple[str, str]:
    """Resolve YYYY-MM-DD range for eligible snapshot queries."""
    from datetime import date

    if start_date and end_date:
        return start_date.strip()[:10], end_date.strip()[:10]
    if as_of_date:
        as_of = as_of_date.strip()[:10]
        try:
            year, month, _ = as_of.split("-")
            range_start, range_end = month_bounds(int(month), int(year))
            return range_start, range_end
        except ValueError:
            return as_of, as_of
    today = date.today()
    return month_bounds(today.month, today.year)


def _month_year_date_range(month_year: str) -> tuple[str, str] | None:
    """Convert 'March 2026' to (first_day, last_day)."""
    from datetime import datetime
    try:
        dt = datetime.strptime(month_year, "%B %Y")
        return month_bounds(dt.month, dt.year)
    except ValueError:
        return None


_TOTAL_ELIGIBLE_SNAPSHOT_SQL = """
SELECT COALESCE(SUM(total_eligible), 0) AS total_eligible
FROM (
    SELECT COUNT(DISTINCT de.id_karyawan) AS total_eligible
    FROM data_record_eligible de
    WHERE de.snapshot_date BETWEEN :start_date AND :end_date
      AND de.is_loan_eligible = 1
      AND ({allowed_employer_predicate})
      AND (:f_employer IS NULL OR de.employer = :f_employer)
      AND (:f_sourced_to IS NULL OR de.sourced_to = :f_sourced_to)
      AND (:f_project IS NULL OR de.project = :f_project)
      AND (:f_branch IS NULL OR de.branch = :f_branch)
      AND ({segment_predicate})
      AND (:f_product IS NULL OR de.product_type = :f_product)

    UNION ALL

    -- Legacy fallback (pre data_record_eligible history): data_record.company is
    -- keyed by td_karyawan.klient, which is '1' for ALL FOUR Valdo entities (the
    -- klient/legal-group code, not the sub_client/employer code) — so this source
    -- can only ever give the combined 4-company total, never a single employer's
    -- number. Only used when no employer/dimension filter narrows below that.
    SELECT CAST(dr.value AS UNSIGNED) AS total_eligible
    FROM data_record dr
    INNER JOIN (
        SELECT company, MAX(created_at) AS max_created_at
        FROM data_record
        WHERE parameter = 'loan_eligible_company'
          AND company = '1'
          AND DATE(created_at) BETWEEN :start_date AND :end_date
        GROUP BY company
    ) latest_snapshot ON latest_snapshot.company = dr.company
        AND latest_snapshot.max_created_at = dr.created_at
    WHERE dr.parameter = 'loan_eligible_company'
      AND :allow_data_record_fallback = 1
      AND NOT EXISTS (
          SELECT 1
          FROM data_record_eligible
          WHERE snapshot_date BETWEEN :start_date AND :end_date
            AND is_loan_eligible = 1
      )
) x
"""


def _resolve_gmc_code(
    db: Session,
    *,
    value: str = None,
    group_gmc: str,
) -> str | None:
    """
    Map API filter text (keterangan) to tbl_gmc.kode_gmc for snapshot tables.
    If value already looks like a raw code (all digits), return it as-is.
    """
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return value

    cache = db.info.setdefault("loan_gmc_code_cache", {})
    cache_key = f"{group_gmc}::{value}"
    if cache_key in cache:
        return cache[cache_key]

    row = db.execute(
        text(
            """
            SELECT kode_gmc
            FROM tbl_gmc
            WHERE group_gmc = :group_gmc
              AND aktif = 'Yes'
              AND keterangan3 = 1
              AND keterangan = :keterangan
            LIMIT 1
            """
        ),
        {"group_gmc": group_gmc, "keterangan": value},
    ).fetchone()
    code = str(row[0]) if row and row[0] is not None else value
    cache[cache_key] = code
    return code


def _resolve_allowed_employer_codes(db: Session) -> list[str]:
    """kode_gmc (sub_client) for ALLOWED_COMPANIES, for scoping data_record_eligible.employer."""
    codes = []
    for name in ALLOWED_COMPANIES:
        code = _resolve_gmc_code(db, value=name, group_gmc="sub_client")
        if code:
            codes.append(code)
    return codes


def _allowed_employer_predicate(db: Session, params: dict, column: str = "de.employer") -> str:
    codes = _resolve_allowed_employer_codes(db)
    if not codes:
        return "1=0"
    placeholders = []
    for index, code in enumerate(codes):
        key = f"allowed_employer_{index}"
        params[key] = code
        placeholders.append(f":{key}")
    return f"{column} IN ({', '.join(placeholders)})"


def _eligible_segment_predicate(
    client_segment_filter: str,
    params: dict,
    db: Session = None,
) -> str:
    """Match other loan endpoints: exact code, or all_bfsi / all_non_bfsi aggregates."""
    if not client_segment_filter:
        return "1=1"

    client_segment_filter = client_segment_filter.strip()
    if client_segment_filter == CLIENT_SEGMENT_ALL_BFSI:
        category = "bfsi"
    elif client_segment_filter == CLIENT_SEGMENT_ALL_NON_BFSI:
        category = "non_bfsi"
    else:
        params["f_segment"] = client_segment_filter
        return "de.client_segment = :f_segment"

    codes = _resolve_aggregate_segment_codes(db, category) if db is not None else []
    if not codes:
        return "1=0"
    placeholders = []
    for index, code in enumerate(codes):
        key = f"elig_seg_{category}_{index}"
        params[key] = code
        placeholders.append(f":{key}")
    return f"de.client_segment IN ({', '.join(placeholders)})"


def get_total_eligible_employees(
    db: Session,
    *,
    start_date: str = None,
    end_date: str = None,
    as_of_date: str = None,
    employer_filter: str = None,
    sourced_to_filter: str = None,
    project_filter: str = None,
    branch_filter: str = None,
    client_segment_filter: str = None,
    product_type_filter: str = None,
) -> int:
    """
    Total eligible from snapshots, scoped to the 4 Valdo companies (ALLOWED_COMPANIES),
    using the same filter meanings as other loan endpoints:

    - employer      -> de.employer   (tbl_gmc sub_client kode_gmc; API sends keterangan)
    - sourced_to    -> de.sourced_to  (tbl_gmc placement_client kode_gmc)
    - project       -> de.project     (tbl_gmc client_project kode_gmc)
    - client_segment-> de.client_segment (incl. all_bfsi / all_non_bfsi)
    - product_type  -> de.product_type

    Note: de.company is td_karyawan.klient (the legal-group code, '1' for all four
    Valdo entities) and is NOT usable for employer-level filtering — use de.employer
    (td_karyawan.valdo_inc) instead.
    """
    try:
        range_start, range_end = _eligible_date_range(
            start_date=start_date,
            end_date=end_date,
            as_of_date=as_of_date,
        )

        # de.employer stores numeric kode_gmc, while API employer filter is keterangan text.
        employer_code = _resolve_gmc_code(
            db, value=employer_filter, group_gmc="sub_client"
        )
        sourced_to_code = _resolve_gmc_code(
            db, value=sourced_to_filter, group_gmc="placement_client"
        )
        project_code = _resolve_gmc_code(
            db, value=project_filter, group_gmc="client_project"
        )

        params = {
            "start_date": range_start,
            "end_date": range_end,
            "f_employer": employer_code,
            "f_sourced_to": sourced_to_code,
            "f_project": project_code,
            "f_branch": branch_filter,
            "f_product": product_type_filter,
        }
        segment_predicate = _eligible_segment_predicate(
            client_segment_filter, params, db
        )
        allowed_employer_predicate = _allowed_employer_predicate(db, params)

        # data_record fallback only supports the combined 4-company total (+ date);
        # skip when any filter narrows below that.
        detail_filters_used = any(
            [
                employer_code,
                sourced_to_code,
                project_code,
                branch_filter,
                client_segment_filter,
                product_type_filter,
            ]
        )
        params["allow_data_record_fallback"] = 0 if detail_filters_used else 1

        query = _TOTAL_ELIGIBLE_SNAPSHOT_SQL.format(
            segment_predicate=segment_predicate,
            allowed_employer_predicate=allowed_employer_predicate,
        )
        record = db.execute(text(query), params).fetchone()
        return int(record[0] or 0) if record else 0
    except Exception:
        import traceback
        traceback.print_exc()
        return 0


_TOTAL_ELIGIBLE_WITH_PROJECT_SNAPSHOT_SQL = """
SELECT COALESCE(SUM(total_coverage_project), 0) AS total_coverage_project
FROM (
    SELECT COUNT(DISTINCT de.project) AS total_coverage_project
    FROM data_record_eligible de
    WHERE de.snapshot_date BETWEEN :start_date AND :end_date
      AND de.is_loan_eligible = 1
      AND de.project IS NOT NULL AND de.project <> ''
      AND ({allowed_employer_predicate})
      AND (:f_employer IS NULL OR de.employer = :f_employer)
      AND (:f_sourced_to IS NULL OR de.sourced_to = :f_sourced_to)
      AND (:f_project IS NULL OR de.project = :f_project)
      AND (:f_branch IS NULL OR de.branch = :f_branch)
      AND ({segment_predicate})
      AND (:f_product IS NULL OR de.product_type = :f_product)

    UNION ALL

    -- Legacy fallback: data_record.company is keyed by td_karyawan.klient, which is
    -- '1' for all four Valdo entities combined — only usable for the unfiltered total.
    SELECT CAST(dr.value AS UNSIGNED) AS total_coverage_project
    FROM data_record dr
    INNER JOIN (
        SELECT company, MAX(created_at) AS max_created_at
        FROM data_record
        WHERE parameter = 'loan_project_covered'
          AND company = '1'
          AND DATE(created_at) BETWEEN :start_date AND :end_date
        GROUP BY company
    ) latest_snapshot ON latest_snapshot.company = dr.company
        AND latest_snapshot.max_created_at = dr.created_at
    WHERE dr.parameter = 'loan_project_covered'
      AND :allow_data_record_fallback = 1
      AND NOT EXISTS (
          SELECT 1
          FROM data_record_eligible
          WHERE snapshot_date BETWEEN :start_date AND :end_date
            AND is_loan_eligible = 1
            AND project IS NOT NULL AND project <> ''
      )
) x
"""


def get_total_coverage_project(
    db: Session,
    *,
    start_date: str = None,
    end_date: str = None,
    as_of_date: str = None,
    employer_filter: str = None,
    sourced_to_filter: str = None,
    project_filter: str = None,
    branch_filter: str = None,
    client_segment_filter: str = None,
    product_type_filter: str = None,
) -> int:
    """
    Count of distinct coverage projects (de.project) among eligible employees,
    scoped to the 4 Valdo companies. Same filter semantics as
    get_total_eligible_employees (de.employer, not de.company), plus a
    data_record('loan_project_covered') fallback for the unfiltered combined
    total on dates before data_record_eligible existed.
    """
    try:
        range_start, range_end = _eligible_date_range(
            start_date=start_date,
            end_date=end_date,
            as_of_date=as_of_date,
        )

        employer_code = _resolve_gmc_code(
            db, value=employer_filter, group_gmc="sub_client"
        )
        sourced_to_code = _resolve_gmc_code(
            db, value=sourced_to_filter, group_gmc="placement_client"
        )
        project_code = _resolve_gmc_code(
            db, value=project_filter, group_gmc="client_project"
        )

        params = {
            "start_date": range_start,
            "end_date": range_end,
            "f_employer": employer_code,
            "f_sourced_to": sourced_to_code,
            "f_project": project_code,
            "f_branch": branch_filter,
            "f_product": product_type_filter,
        }
        segment_predicate = _eligible_segment_predicate(
            client_segment_filter, params, db
        )
        allowed_employer_predicate = _allowed_employer_predicate(db, params)

        detail_filters_used = any(
            [
                employer_code,
                sourced_to_code,
                project_code,
                branch_filter,
                client_segment_filter,
                product_type_filter,
            ]
        )
        params["allow_data_record_fallback"] = 0 if detail_filters_used else 1

        query = _TOTAL_ELIGIBLE_WITH_PROJECT_SNAPSHOT_SQL.format(
            segment_predicate=segment_predicate,
            allowed_employer_predicate=allowed_employer_predicate,
        )
        record = db.execute(text(query), params).fetchone()
        return int(record[0] or 0) if record else 0
    except Exception:
        import traceback
        traceback.print_exc()
        return 0


_TOTAL_ACTIVE_SNAPSHOT_SQL = """
SELECT COALESCE(SUM(total_active), 0) AS total_active
FROM (
    SELECT COUNT(DISTINCT de.id_karyawan) AS total_active
    FROM data_record_eligible de
    WHERE de.snapshot_date BETWEEN :start_date AND :end_date
      AND ({allowed_employer_predicate})
      AND (:f_employer IS NULL OR de.employer = :f_employer)
      AND (:f_sourced_to IS NULL OR de.sourced_to = :f_sourced_to)
      AND (:f_project IS NULL OR de.project = :f_project)
      AND (:f_branch IS NULL OR de.branch = :f_branch)
      AND ({segment_predicate})
      AND (:f_product IS NULL OR de.product_type = :f_product)

    UNION ALL

    -- Legacy fallback: data_record.company is keyed by td_karyawan.klient, which is
    -- '1' for all four Valdo entities combined — only usable for the unfiltered total,
    -- and only for dates before data_record_eligible has any snapshot at all (that
    -- table only started capturing the full active population, not just eligible
    -- employees, from the is_loan_eligible migration date onward — see
    -- sql/data_record_eligible.sql in the ak-mj repo).
    SELECT CAST(dr.value AS UNSIGNED) AS total_active
    FROM data_record dr
    INNER JOIN (
        SELECT company, MAX(created_at) AS max_created_at
        FROM data_record
        WHERE parameter = 'active_employee_client'
          AND company = '1'
          AND DATE(created_at) BETWEEN :start_date AND :end_date
        GROUP BY company
    ) latest_snapshot ON latest_snapshot.company = dr.company
        AND latest_snapshot.max_created_at = dr.created_at
    WHERE dr.parameter = 'active_employee_client'
      AND :allow_data_record_fallback = 1
      AND NOT EXISTS (
          SELECT 1
          FROM data_record_eligible
          WHERE snapshot_date BETWEEN :start_date AND :end_date
      )
) x
"""


def get_total_active_employees(
    db: Session,
    *,
    start_date: str = None,
    end_date: str = None,
    as_of_date: str = None,
    employer_filter: str = None,
    sourced_to_filter: str = None,
    project_filter: str = None,
    branch_filter: str = None,
    client_segment_filter: str = None,
    product_type_filter: str = None,
) -> int:
    """
    Total active employees (td_karyawan.status = 1) from the data_record_eligible
    snapshot, scoped to the 4 Valdo companies. Same filter/date-range semantics as
    get_total_eligible_employees, but without the is_loan_eligible restriction.

    Caveat: rows written before the is_loan_eligible migration (see
    sql/data_record_eligible.sql in ak-mj) only ever captured eligible employees,
    so any date range overlapping that period will undercount active employees.
    """
    try:
        range_start, range_end = _eligible_date_range(
            start_date=start_date,
            end_date=end_date,
            as_of_date=as_of_date,
        )

        employer_code = _resolve_gmc_code(
            db, value=employer_filter, group_gmc="sub_client"
        )
        sourced_to_code = _resolve_gmc_code(
            db, value=sourced_to_filter, group_gmc="placement_client"
        )
        project_code = _resolve_gmc_code(
            db, value=project_filter, group_gmc="client_project"
        )

        params = {
            "start_date": range_start,
            "end_date": range_end,
            "f_employer": employer_code,
            "f_sourced_to": sourced_to_code,
            "f_project": project_code,
            "f_branch": branch_filter,
            "f_product": product_type_filter,
        }
        segment_predicate = _eligible_segment_predicate(
            client_segment_filter, params, db
        )
        allowed_employer_predicate = _allowed_employer_predicate(db, params)

        detail_filters_used = any(
            [
                employer_code,
                sourced_to_code,
                project_code,
                branch_filter,
                client_segment_filter,
                product_type_filter,
            ]
        )
        params["allow_data_record_fallback"] = 0 if detail_filters_used else 1

        query = _TOTAL_ACTIVE_SNAPSHOT_SQL.format(
            segment_predicate=segment_predicate,
            allowed_employer_predicate=allowed_employer_predicate,
        )
        record = db.execute(text(query), params).fetchone()
        return int(record[0] or 0) if record else 0
    except Exception:
        import traceback
        traceback.print_exc()
        return 0


def _append_proses_date_range(query: str, params: dict, start_date: str, end_date: str) -> str:
    return append_date_filters(
        query,
        params,
                start_date=start_date,
        end_date=end_date,
        date_column="l.proses_date",
    )


_UNRECOVERED_LUMP_PAYMENT_SQL = """
    SELECT GREATEST(l.total_payment - (
              SELECT COALESCE(SUM(amt), 0) FROM (
                SELECT p.amount amt FROM td_loan_payment p
                WHERE p.loan_id = l.id AND p.status = 1 AND p.loan_history_id IS NULL
                  AND NOT EXISTS (SELECT 1 FROM td_loan_payment_allocation a WHERE a.payment_id = p.id)
                UNION ALL
                SELECT a.amount FROM td_loan_payment_allocation a
                INNER JOIN td_loan_payment p ON p.id = a.payment_id
                WHERE p.loan_id = l.id AND p.status = 1 AND a.loan_history_id IS NULL
              ) t), 0) AS payment_due
    FROM td_loan l
    INNER JOIN td_karyawan tk ON l.id_karyawan = tk.id_karyawan
    INNER JOIN tbl_gmc emp
        ON tk.valdo_inc = emp.kode_gmc
        AND emp.group_gmc = 'sub_client'
        AND emp.aktif = 'Yes'
        AND emp.keterangan3 = 1
    LEFT JOIN tbl_gmc src
        ON tk.placement = src.kode_gmc
        AND src.group_gmc = 'placement_client'
        AND src.aktif = 'Yes'
        AND src.keterangan3 = 1
    LEFT JOIN tbl_gmc prj
        ON tk.project = prj.kode_gmc
        AND prj.group_gmc = 'client_project'
        AND prj.aktif = 'Yes'
        AND prj.keterangan3 = 1
    WHERE l.loan_status IN (1, 4)
      AND l.duration = 1
      AND (l.payment_date IS NULL OR l.payment_date = '0000-00-00')
      AND l.repayment_date < CURDATE()
"""

_UNRECOVERED_INSTALLMENT_PAYMENT_SQL = """
    SELECT GREATEST(th.monthly - (
              SELECT COALESCE(SUM(amt), 0) FROM (
                SELECT p.amount amt FROM td_loan_payment p
                WHERE p.loan_id = l.id AND p.status = 1 AND p.loan_history_id = th.id
                  AND NOT EXISTS (SELECT 1 FROM td_loan_payment_allocation a WHERE a.payment_id = p.id)
                UNION ALL
                SELECT a.amount FROM td_loan_payment_allocation a
                INNER JOIN td_loan_payment p ON p.id = a.payment_id
                WHERE p.loan_id = l.id AND p.status = 1 AND a.loan_history_id = th.id
              ) t), 0) AS payment_due
    FROM td_loan l
    INNER JOIN td_karyawan tk ON l.id_karyawan = tk.id_karyawan
    INNER JOIN tbl_gmc emp
        ON tk.valdo_inc = emp.kode_gmc
        AND emp.group_gmc = 'sub_client'
        AND emp.aktif = 'Yes'
        AND emp.keterangan3 = 1
    LEFT JOIN tbl_gmc src
        ON tk.placement = src.kode_gmc
        AND src.group_gmc = 'placement_client'
        AND src.aktif = 'Yes'
        AND src.keterangan3 = 1
    LEFT JOIN tbl_gmc prj
        ON tk.project = prj.kode_gmc
        AND prj.group_gmc = 'client_project'
        AND prj.aktif = 'Yes'
        AND prj.keterangan3 = 1
    INNER JOIN td_loan_history th ON th.loan_form_id = l.id
    WHERE l.loan_status IN (1, 4)
      AND l.duration > 1
      AND (th.payment_date IS NULL OR th.payment_date = '0000-00-00')
      AND th.due_date < CURDATE()
"""

# Outstanding = not yet paid, but due date hasn't arrived yet (still waiting), as opposed
# to "unrecovered" above which is not yet paid AND already past its due date.
_OUTSTANDING_LUMP_PAYMENT_SQL = _UNRECOVERED_LUMP_PAYMENT_SQL.replace(
    "AND l.repayment_date < CURDATE()", "AND l.repayment_date >= CURDATE()"
)
_OUTSTANDING_INSTALLMENT_PAYMENT_SQL = _UNRECOVERED_INSTALLMENT_PAYMENT_SQL.replace(
    "AND th.due_date < CURDATE()", "AND th.due_date >= CURDATE()"
)


def _unrecovered_repayment_scope(loan_type: str, db: Session) -> tuple[bool, bool, str | None]:
    """Return (include_lump_sum, include_installment, extra_loan_predicate)."""
    if is_all_loan_types(loan_type):
        # Full business UNION across every product: duration=1 lump + duration>1 installment.
        return True, True, None
    if loan_type in ("loan", "kasbon"):
        # Kasbon is lump-sum only (duration=1); exclude the handful of AkuCicil loans
        # that also happen to have duration=1, matching LOAN_CONDITIONS elsewhere.
        return True, False, LOAN_CONDITIONS
    if loan_type in ("extradana", "aku_cicil"):
        if loan_type == "extradana":
            extra = "l.loan_id IN (SELECT ls.id FROM loan_setting ls WHERE ls.loan_type LIKE 'Extradana%')"
        else:
            extra = AKU_CICIL_CONDITION
        return False, True, extra
    return True, True, None


def _apply_repayment_risk_filters(
    query: str,
    params: dict,
    *,
    employer_filter: str = None,
    sourced_to_filter: str = None,
    project_filter: str = None,
    client_segment_filter: str = None,
    product_type_filter: str = None,
    loan_status_filter: int = None,
    id_karyawan_filter: int = None,
    db: Session = None,
) -> str:
    """Append the employer/sourced_to/project/segment/product_type/loan_status/id_karyawan/
    company filters shared by every total_expected_repayment and principal/admin-fee
    repayment-risk query variant (kasbon vs. installment, summary vs. monthly)."""
    if id_karyawan_filter:
        query += " AND l.id_karyawan = :id_karyawan"
        params["id_karyawan"] = id_karyawan_filter

    query += f" AND emp.keterangan IN {COMPANY_FILTER}"

    if employer_filter and employer_filter in ALLOWED_COMPANIES:
        query += " AND emp.keterangan = :employer"
        params["employer"] = employer_filter

    if sourced_to_filter:
        query += " AND src.keterangan = :sourced_to"
        params["sourced_to"] = sourced_to_filter

    if project_filter:
        query += " AND prj.keterangan = :project"
        params["project"] = project_filter

    query = _apply_project_management_filters(
        query, params, client_segment_filter, product_type_filter, db=db
    )

    if loan_status_filter is not None:
        query += " AND l.loan_status = :loan_status"
        params["loan_status"] = loan_status_filter

    return query


def _recalculate_repayment_risk_derivatives(summary: dict) -> dict:
    """Derive the repayment-risk response's rate/residual fields from raw totals.

    Three independent attribution models feed this function and must not be mixed:
      - total_expected_repayment itself is the ak-mj-matched, disbursement-month figure
        (see get_total_expected_repayment) and is NOT used in any of the math below — it's
        a standalone total, passed through unchanged.
      - total_due_date_expected_repayment / total_unrecovered_repayment /
        total_outstanding_repayment (the "total repayment" section: total_collected_
        repayment, repayment_recovery_rate, unrecovered_rate, outstanding_rate) are
        due-date based and have no relationship to bad debt — a repayment here stays keyed
        to its original due date regardless of how late it was eventually paid.
      - total_loan_principal_collected/total_admin_fee_collected and their unrecovered/
        expected counterparts (the principal-repayment and admin-fee-repayment sections)
        are reporting-date based (see _REPORTING_DATE_LUMP/_INSTALLMENT) and loan_status=4
        ("Bad Debt Recovery") aware. The "performance" section (delinquency_by_expected_
        repayment, delinquency_by_admin_fee, admin_fee_profit) is derived from these
        bad-debt-aware totals and total_due_date_expected_repayment, not from
        total_unrecovered_repayment or the ak-mj-matched total_expected_repayment.
    """
    total_expected = summary.get("total_due_date_expected_repayment", 0) or 0
    principal_collected = summary.get("total_loan_principal_collected", 0) or 0
    admin_fee_collected = summary.get("total_admin_fee_collected", 0) or 0
    # total_unrecovered_repayment/total_outstanding_repayment (not-yet-due payments still
    # waiting) are fetched directly by the caller via get_total_unrecovered_repayment/
    # get_total_outstanding_repayment, not derived here.
    unrecovered = summary.get("total_unrecovered_repayment", 0) or 0
    outstanding = summary.get("total_outstanding_repayment", 0) or 0
    total_expected_principal = summary.get("total_expected_loan_principal", 0) or 0
    total_expected_admin_fee = summary.get("total_expected_admin_fee", 0) or 0
    unrecovered_principal = summary.get("total_unrecovered_loan_principal", 0) or 0
    unrecovered_admin_fee = summary.get("total_unrecovered_admin_fee", 0) or 0

    # total_collected_repayment must reconcile exactly with expected = collected +
    # unrecovered + outstanding, so it's derived as the residual rather than summed
    # from principal_collected + admin_fee_collected (which only recognizes loans
    # marked fully closed/lunas and misses partial payments already made on loans
    # that are still open — unrecovered/outstanding already account for that
    # remainder in full, so the residual is the true amount collected to date).
    collected = max(total_expected - unrecovered - outstanding, 0)
    summary["total_collected_repayment"] = collected
    summary["repayment_recovery_rate"] = (collected / total_expected) if total_expected > 0 else 0
    summary["unrecovered_rate"] = (unrecovered / total_expected) if total_expected > 0 else 0
    summary["outstanding_rate"] = (outstanding / total_expected) if total_expected > 0 else 0

    # Performance section: bad-debt based, independent of the due-date-based unrecovered/
    # outstanding above.
    bad_debt_unrecovered = unrecovered_principal + unrecovered_admin_fee
    summary["delinquency_by_expected_repayment"] = (
        (bad_debt_unrecovered / total_expected) if total_expected > 0 else 0
    )
    summary["delinquency_by_admin_fee"] = (
        (unrecovered_admin_fee / admin_fee_collected) if admin_fee_collected > 0 else 0
    )
    summary["admin_fee_profit"] = admin_fee_collected - unrecovered_admin_fee

    # Principal ("pokok") and admin fee ("bunga") collection rates, separate from
    # total_expected_repayment which is pokok + bunga combined.
    summary["principal_collection_rate"] = (
        (principal_collected / total_expected_principal) if total_expected_principal > 0 else 0
    )
    summary["admin_fee_collection_rate"] = (
        (admin_fee_collected / total_expected_admin_fee) if total_expected_admin_fee > 0 else 0
    )
    return summary


def _build_unrecovered_repayment_parts(
    *,
    include_lump: bool,
    include_installment: bool,
    extra_loan_predicate: str | None,
    group_by_month: bool,
    params: dict,
    employer_filter: str = None,
    sourced_to_filter: str = None,
    project_filter: str = None,
    client_segment_filter: str = None,
    product_type_filter: str = None,
    loan_status_filter: int = None,
    id_karyawan_filter: int = None,
    start_date: str = None,
    end_date: str = None,
    db: Session = None,
    lump_sql: str = _UNRECOVERED_LUMP_PAYMENT_SQL,
    installment_sql: str = _UNRECOVERED_INSTALLMENT_PAYMENT_SQL,
) -> list[str]:
    company_filter = COMPANY_FILTER
    parts: list[str] = []

    if include_lump:
        lump_select = (
            "DATE_FORMAT(l.repayment_date, '%M %Y') AS month_year, "
            if group_by_month
            else ""
        )
        lump_query = lump_sql.replace(
            "SELECT GREATEST(",
            f"SELECT {lump_select}GREATEST(",
            1,
        )
        if group_by_month:
            lump_query += " AND l.repayment_date IS NOT NULL"
        if extra_loan_predicate:
            lump_query += f" AND {extra_loan_predicate}"
        if start_date and end_date:
            lump_query = append_date_filters(
                lump_query,
                params,
                start_date=start_date,
                end_date=end_date,
                date_column="l.repayment_date",
            )
        lump_query = _append_loan_org_filters(
            lump_query,
            params,
            id_karyawan_filter=id_karyawan_filter,
            employer_filter=employer_filter,
            sourced_to_filter=sourced_to_filter,
            project_filter=project_filter,
            client_segment_filter=client_segment_filter,
            product_type_filter=product_type_filter,
            loan_status_filter=loan_status_filter,
            company_filter=company_filter,
            db=db,
        )
        parts.append(lump_query)

    if include_installment:
        installment_select = (
            "DATE_FORMAT(th.due_date, '%M %Y') AS month_year, "
            if group_by_month
            else ""
        )
        installment_query = installment_sql.replace(
            "SELECT GREATEST(",
            f"SELECT {installment_select}GREATEST(",
            1,
        )
        if extra_loan_predicate:
            installment_query += f" AND {extra_loan_predicate}"
        if start_date and end_date:
            installment_query = append_date_filters(
                installment_query,
                params,
                start_date=start_date,
                end_date=end_date,
                date_column="th.due_date",
            )
        installment_query = _append_loan_org_filters(
            installment_query,
            params,
            id_karyawan_filter=id_karyawan_filter,
            employer_filter=employer_filter,
            sourced_to_filter=sourced_to_filter,
            project_filter=project_filter,
            client_segment_filter=client_segment_filter,
            product_type_filter=product_type_filter,
            loan_status_filter=loan_status_filter,
            company_filter=company_filter,
            loan_prefix="l",
            db=db,
        )
        parts.append(installment_query)

    return parts


def get_total_unrecovered_repayment(
    db: Session,
    *,
    employer_filter: str = None,
    sourced_to_filter: str = None,
    project_filter: str = None,
    client_segment_filter: str = None,
    product_type_filter: str = None,
    loan_status_filter: int = None,
    id_karyawan_filter: int = None,
    start_date: str = None,
    end_date: str = None,
    loan_type: str = "loan",
) -> float:
    """Outstanding payment due: lump-sum (duration=1) + installment (duration>1) scopes."""
    try:
        include_lump, include_installment, extra_loan_predicate = _unrecovered_repayment_scope(
            loan_type, db
        )
        params: dict = {}
        parts = _build_unrecovered_repayment_parts(
            include_lump=include_lump,
            include_installment=include_installment,
            extra_loan_predicate=extra_loan_predicate,
            group_by_month=False,
            params=params,
            employer_filter=employer_filter,
            sourced_to_filter=sourced_to_filter,
            project_filter=project_filter,
            client_segment_filter=client_segment_filter,
            product_type_filter=product_type_filter,
            loan_status_filter=loan_status_filter,
            id_karyawan_filter=id_karyawan_filter,
            start_date=start_date,
            end_date=end_date,
            db=db,
        )

        if not parts:
            return 0

        if len(parts) == 1:
            query = f"SELECT COALESCE(SUM(payment_due), 0) FROM ({parts[0]}) x"
        else:
            query = f"SELECT COALESCE(SUM(payment_due), 0) FROM ({' UNION ALL '.join(parts)}) x"

        record = db.execute(text(query), params).fetchone()
        return record[0] if record and record[0] is not None else 0
    except Exception:
        import traceback
        traceback.print_exc()
        return 0


def get_total_unrecovered_repayment_monthly(
    db: Session,
    *,
    employer_filter: str = None,
    sourced_to_filter: str = None,
    project_filter: str = None,
    client_segment_filter: str = None,
    product_type_filter: str = None,
    loan_status_filter: int = None,
    id_karyawan_filter: int = None,
    start_date: str = None,
    end_date: str = None,
    loan_type: str = "loan",
) -> dict:
    """Outstanding payment due grouped by month_year."""
    try:
        include_lump, include_installment, extra_loan_predicate = _unrecovered_repayment_scope(
            loan_type, db
        )
        params: dict = {}
        parts = _build_unrecovered_repayment_parts(
            include_lump=include_lump,
            include_installment=include_installment,
            extra_loan_predicate=extra_loan_predicate,
            group_by_month=True,
            params=params,
            employer_filter=employer_filter,
            sourced_to_filter=sourced_to_filter,
            project_filter=project_filter,
            client_segment_filter=client_segment_filter,
            product_type_filter=product_type_filter,
            loan_status_filter=loan_status_filter,
            id_karyawan_filter=id_karyawan_filter,
            start_date=start_date,
            end_date=end_date,
            db=db,
        )

        if not parts:
            return {}

        union_sql = parts[0] if len(parts) == 1 else f"{' UNION ALL '.join(parts)}"
        query = f"""
        SELECT month_year, COALESCE(SUM(payment_due), 0) AS total_unrecovered_repayment
        FROM ({union_sql}) x
        WHERE month_year IS NOT NULL
        GROUP BY month_year
        """

        monthly_data = {}
        for row in db.execute(text(query), params).fetchall():
            if row[0] is None:
                continue
            monthly_data[row[0]] = row[1] if row[1] is not None else 0
        return monthly_data
    except Exception:
        import traceback
        traceback.print_exc()
        return {}


def get_total_outstanding_repayment(
    db: Session,
    *,
    employer_filter: str = None,
    sourced_to_filter: str = None,
    project_filter: str = None,
    client_segment_filter: str = None,
    product_type_filter: str = None,
    loan_status_filter: int = None,
    id_karyawan_filter: int = None,
    start_date: str = None,
    end_date: str = None,
    loan_type: str = "loan",
) -> float:
    """Payment due that is not yet paid and not yet past its due date (still waiting)."""
    try:
        include_lump, include_installment, extra_loan_predicate = _unrecovered_repayment_scope(
            loan_type, db
        )
        params: dict = {}
        parts = _build_unrecovered_repayment_parts(
            include_lump=include_lump,
            include_installment=include_installment,
            extra_loan_predicate=extra_loan_predicate,
            group_by_month=False,
            params=params,
            employer_filter=employer_filter,
            sourced_to_filter=sourced_to_filter,
            project_filter=project_filter,
            client_segment_filter=client_segment_filter,
            product_type_filter=product_type_filter,
            loan_status_filter=loan_status_filter,
            id_karyawan_filter=id_karyawan_filter,
            start_date=start_date,
            end_date=end_date,
            db=db,
            lump_sql=_OUTSTANDING_LUMP_PAYMENT_SQL,
            installment_sql=_OUTSTANDING_INSTALLMENT_PAYMENT_SQL,
        )

        if not parts:
            return 0

        if len(parts) == 1:
            query = f"SELECT COALESCE(SUM(payment_due), 0) FROM ({parts[0]}) x"
        else:
            query = f"SELECT COALESCE(SUM(payment_due), 0) FROM ({' UNION ALL '.join(parts)}) x"

        record = db.execute(text(query), params).fetchone()
        return record[0] if record and record[0] is not None else 0
    except Exception:
        import traceback
        traceback.print_exc()
        return 0


def get_total_outstanding_repayment_monthly(
    db: Session,
    *,
    employer_filter: str = None,
    sourced_to_filter: str = None,
    project_filter: str = None,
    client_segment_filter: str = None,
    product_type_filter: str = None,
    loan_status_filter: int = None,
    id_karyawan_filter: int = None,
    start_date: str = None,
    end_date: str = None,
    loan_type: str = "loan",
) -> dict:
    """Not-yet-due payment due (still waiting), grouped by month_year."""
    try:
        include_lump, include_installment, extra_loan_predicate = _unrecovered_repayment_scope(
            loan_type, db
        )
        params: dict = {}
        parts = _build_unrecovered_repayment_parts(
            include_lump=include_lump,
            include_installment=include_installment,
            extra_loan_predicate=extra_loan_predicate,
            group_by_month=True,
            params=params,
            employer_filter=employer_filter,
            sourced_to_filter=sourced_to_filter,
            project_filter=project_filter,
            client_segment_filter=client_segment_filter,
            product_type_filter=product_type_filter,
            loan_status_filter=loan_status_filter,
            id_karyawan_filter=id_karyawan_filter,
            start_date=start_date,
            end_date=end_date,
            db=db,
            lump_sql=_OUTSTANDING_LUMP_PAYMENT_SQL,
            installment_sql=_OUTSTANDING_INSTALLMENT_PAYMENT_SQL,
        )

        if not parts:
            return {}

        union_sql = parts[0] if len(parts) == 1 else f"{' UNION ALL '.join(parts)}"
        query = f"""
        SELECT month_year, COALESCE(SUM(payment_due), 0) AS total_outstanding_repayment
        FROM ({union_sql}) x
        WHERE month_year IS NOT NULL
        GROUP BY month_year
        """

        monthly_data = {}
        for row in db.execute(text(query), params).fetchall():
            if row[0] is None:
                continue
            monthly_data[row[0]] = row[1] if row[1] is not None else 0
        return monthly_data
    except Exception:
        import traceback
        traceback.print_exc()
        return {}


def get_total_expected_repayment(
    db: Session,
    *,
    employer_filter: str = None,
    sourced_to_filter: str = None,
    project_filter: str = None,
    client_segment_filter: str = None,
    product_type_filter: str = None,
    loan_status_filter: int = None,
    id_karyawan_filter: int = None,
    start_date: str = None,
    end_date: str = None,
    loan_type: str = "loan",
) -> float:
    """total_expected_repayment, matched to ak-mj's /loan/monthly_performance figure
    (M_loan::get_monthly_loan_drp's 'total_repayment'): SUM(td_loan.total_payment) for
    loans with loan_status IN (1,2,4), bucketed by disbursement month (l.proses_date) —
    not the due-date/reporting-date basis used by every other repayment-risk field
    (principal/admin-fee collected, performance, etc., which stay as-is).

    loan_type=all intentionally applies no duration/disbursement product predicate at
    all, mirroring ak-mj (which never splits this figure by product): the kasbon/
    extradana/aku_cicil conditions' union has edge-case gaps (e.g. installment loans with
    disbursement=4 match none of the three), so filtering by their union would undercount
    relative to ak-mj's flat total on months where such rows exist."""
    try:
        loan_conditions = None if is_all_loan_types(loan_type) else resolve_loan_conditions(loan_type, db)
        query = """
        SELECT SUM(l.total_payment) as total_expected_repayment
        FROM td_loan l
        {gmc_joins}
        WHERE l.loan_status IN (1, 2, 4)
        """.format(gmc_joins=_LOAN_GMC_JOINS)
        if loan_conditions:
            query += f" AND {loan_conditions}"

        params: dict = {}
        query = _apply_repayment_risk_filters(
            query,
            params,
            employer_filter=employer_filter,
            sourced_to_filter=sourced_to_filter,
            project_filter=project_filter,
            client_segment_filter=client_segment_filter,
            product_type_filter=product_type_filter,
            loan_status_filter=loan_status_filter,
            id_karyawan_filter=id_karyawan_filter,
            db=db,
        )
        if start_date and end_date:
            query = append_date_filters(
                query, params, start_date=start_date, end_date=end_date, date_column="l.proses_date"
            )

        record = db.execute(text(query), params).fetchone()
        return record[0] if record and record[0] is not None else 0
    except Exception:
        import traceback
        traceback.print_exc()
        return 0


def get_total_expected_repayment_monthly(
    db: Session,
    *,
    employer_filter: str = None,
    sourced_to_filter: str = None,
    project_filter: str = None,
    client_segment_filter: str = None,
    product_type_filter: str = None,
    loan_status_filter: int = None,
    id_karyawan_filter: int = None,
    start_date: str = None,
    end_date: str = None,
    loan_type: str = "loan",
) -> dict:
    """get_total_expected_repayment, grouped by disbursement month_year. See that
    function's docstring for the ak-mj parity rationale."""
    try:
        loan_conditions = None if is_all_loan_types(loan_type) else resolve_loan_conditions(loan_type, db)
        query = """
        SELECT DATE_FORMAT(l.proses_date, '%M %Y') as month_year, SUM(l.total_payment) as total_expected_repayment
        FROM td_loan l
        {gmc_joins}
        WHERE l.loan_status IN (1, 2, 4)
        """.format(gmc_joins=_LOAN_GMC_JOINS)
        if loan_conditions:
            query += f" AND {loan_conditions}"

        params: dict = {}
        query = _apply_repayment_risk_filters(
            query,
            params,
            employer_filter=employer_filter,
            sourced_to_filter=sourced_to_filter,
            project_filter=project_filter,
            client_segment_filter=client_segment_filter,
            product_type_filter=product_type_filter,
            loan_status_filter=loan_status_filter,
            id_karyawan_filter=id_karyawan_filter,
            db=db,
        )
        if start_date and end_date:
            query = append_date_filters(
                query, params, start_date=start_date, end_date=end_date, date_column="l.proses_date"
            )
        query += """
        GROUP BY DATE_FORMAT(l.proses_date, '%M %Y')
        ORDER BY MIN(l.proses_date)
        """

        monthly_data = {}
        for row in db.execute(text(query), params).fetchall():
            if row[0] is None:
                continue
            monthly_data[row[0]] = row[1] if row[1] is not None else 0
        return monthly_data
    except Exception:
        import traceback
        traceback.print_exc()
        return {}


def _apply_date_filters_to_named_queries(
    queries: dict[str, str],
    params: dict,
    *,
    start_date: str = None,
    end_date: str = None,
    date_columns: dict[str, str] | None = None,
) -> dict[str, str]:
    date_columns = date_columns or {}
    return {
        name: append_date_filters(
            query,
            params,
                start_date=start_date,
            end_date=end_date,
            date_column=date_columns.get(name, "l.proses_date"),
        )
        for name, query in queries.items()
    }


def is_all_loan_types(loan_type: str) -> bool:
    return bool(loan_type) and loan_type.lower() == "all"


def resolve_loan_conditions(loan_type: str, db: Session = None) -> str:
    if db is not None:
        return _loan_conditions_from_ids(loan_type, _get_aku_cicil_id_list(db))
    if is_all_loan_types(loan_type):
        return ALL_LOAN_CONDITIONS
    if loan_type == "extradana":
        return EXTRADANA_LOAN_CONDITIONS
    if loan_type == "aku_cicil":
        return AKU_CICIL_CONDITION
    if loan_type == "installment":
        return INSTALLMENT_LOAN_CONDITIONS
    if loan_type == "kasbon":
        return KASBON_CONDITION
    return LOAN_CONDITIONS


def _merge_repayment_risk_summaries(summaries: List[dict]) -> dict:
    """Sum the additive raw totals across loan types. Rate/residual fields (repayment_
    recovery_rate, delinquency_by_expected_repayment, admin_fee_profit, etc.) are not
    computed here — the caller always finalizes the combined dict via
    _recalculate_repayment_risk_derivatives after overwriting total_unrecovered_repayment/
    total_outstanding_repayment from the loan_type='all' queries."""
    sum_keys = (
        "total_expected_repayment",
        "total_due_date_expected_repayment",
        "total_loan_principal_collected",
        "total_admin_fee_collected",
        "total_unrecovered_repayment",
        "total_unrecovered_loan_principal",
        "total_unrecovered_admin_fee",
        "total_expected_loan_principal",
        "total_expected_admin_fee",
    )
    return {key: sum(summary.get(key, 0) or 0 for summary in summaries) for key in sum_keys}


_MONTHLY_REPAYMENT_RISK_SUM_KEYS = (
    "total_expected_repayment",
    "total_due_date_expected_repayment",
    "total_loan_principal_collected",
    "total_admin_fee_collected",
    "total_unrecovered_loan_principal",
    "total_unrecovered_admin_fee",
    "total_expected_loan_principal",
    "total_expected_admin_fee",
)


def _merge_monthly_repayment_risk(monthly_dicts: List[dict]) -> dict:
    """Sum the additive per-month fields across loan types. total_unrecovered_repayment
    and total_outstanding_repayment are intentionally excluded — the caller overwrites
    those from the loan_type='all' unrecovered/outstanding queries, then finalizes each
    month's derived rates via _recalculate_repayment_risk_derivatives."""
    merged: dict = {}
    for monthly_data in monthly_dicts:
        for month_year, metrics in monthly_data.items():
            bucket = merged.setdefault(month_year, {key: 0 for key in _MONTHLY_REPAYMENT_RISK_SUM_KEYS})
            for key in _MONTHLY_REPAYMENT_RISK_SUM_KEYS:
                bucket[key] += metrics.get(key, 0) or 0
    return merged


def _merge_karyawan_overdue_lists(lists: List[list]) -> list:
    merged = {}
    for overdue_list in lists:
        for row in overdue_list:
            karyawan_id = row.get("id_karyawan")
            if karyawan_id not in merged:
                merged[karyawan_id] = row.copy()
                continue

            existing = merged[karyawan_id]
            existing["total_amount_owed"] = (existing.get("total_amount_owed", 0) or 0) + (
                row.get("total_amount_owed", 0) or 0
            )
            existing["total_admin_fee"] = (existing.get("total_admin_fee", 0) or 0) + (
                row.get("total_admin_fee", 0) or 0
            )
            existing["total_payment"] = (existing.get("total_payment", 0) or 0) + (
                row.get("total_payment", 0) or 0
            )
            existing_repayment = existing.get("repayment_date")
            new_repayment = row.get("repayment_date")
            if new_repayment and (not existing_repayment or new_repayment > existing_repayment):
                existing["repayment_date"] = new_repayment
            if (row.get("days_overdue", 0) or 0) > (existing.get("days_overdue", 0) or 0):
                existing["days_overdue"] = row.get("days_overdue", 0)

    return sorted(
        merged.values(),
        key=lambda item: item.get("total_amount_owed", 0) or 0,
        reverse=True,
    )


def get_enhanced_karyawan(db: Session, limit: int = 1000000,
                          employer_filter: str = None, sourced_to_filter: str = None,
                          project_filter: str = None, client_segment_filter: str = None,
                          product_type_filter: str = None, id_karyawan_filter: int = None) -> List[dict]:
    """Get enhanced karyawan data with join to tbl_gmc table"""

    try:
        # Build the base query with table joins (same database)
        base_query = """
        SELECT
            tk.id_karyawan,
            tk.status,
            tk.loan_kasbon_eligible,
            tk.klient,
            emp.keterangan AS employer_name,
            src.keterangan AS sourced_to_name,
            prj.keterangan AS project_name
        FROM td_karyawan tk
        LEFT JOIN tbl_gmc emp
            ON tk.valdo_inc = emp.kode_gmc
            AND emp.group_gmc = 'sub_client'
            AND emp.aktif = 'Yes'
            AND emp.keterangan3 = 1
        LEFT JOIN tbl_gmc src
            ON tk.placement = src.kode_gmc
            AND src.group_gmc = 'placement_client'
            AND src.aktif = 'Yes'
            AND src.keterangan3 = 1
        LEFT JOIN tbl_gmc prj
            ON tk.project = prj.kode_gmc
            AND prj.group_gmc = 'client_project'
            AND prj.aktif = 'Yes'
            AND prj.keterangan3 = 1
        WHERE 1=1
        """

        # Build parameters dict for filters
        params = {}

        # Add filters
        if id_karyawan_filter:
            base_query += " AND tk.id_karyawan = :id_karyawan"
            params['id_karyawan'] = id_karyawan_filter

        if employer_filter:
            base_query += " AND emp.keterangan = :employer"
            params['employer'] = employer_filter

        if sourced_to_filter:
            base_query += " AND src.keterangan = :sourced_to"
            params['sourced_to'] = sourced_to_filter

        if project_filter:
            base_query += " AND prj.keterangan = :project"
            params['project'] = project_filter

        base_query = _apply_project_management_filters(
            base_query, params, client_segment_filter, product_type_filter, db=db
        )

        # Add limit
        base_query += f" LIMIT {limit}"



        # Execute the main query
        result = db.execute(text(base_query), params)
        records = result.fetchall()



        # Convert to list of dictionaries
        karyawan_list = []
        for record in records:
            karyawan_list.append({
                "id_karyawan": record[0],
                "status": record[1],
                "loan_kasbon_eligible": record[2],
                "klient": record[3],
                "employer_name": record[4],
                "sourced_to_name": record[5],
                "project_name": record[6]
            })

        return karyawan_list

    except Exception as e:
        import traceback
        traceback.print_exc()
        return []


def get_user_coverage_summary(db: Session,
                             employer_filter: str = None, sourced_to_filter: str = None,
                             project_filter: str = None, client_segment_filter: str = None, product_type_filter: str = None, id_karyawan_filter: int = None, start_date: str = None, end_date: str = None) -> dict:
    """Get user coverage summary with eligible count and loan request metrics"""

    try:
        # Set default loan conditions for kasbon/loan type
        loan_conditions = LOAN_CONDITIONS
        # Build the eligible count query
        eligible_count_query = """
        SELECT COUNT(*)
        FROM td_karyawan tk
        LEFT JOIN tbl_gmc emp
            ON tk.valdo_inc = emp.kode_gmc
            AND emp.group_gmc = 'sub_client'
            AND emp.aktif = 'Yes'
            AND emp.keterangan3 = 1
        LEFT JOIN tbl_gmc src
            ON tk.placement = src.kode_gmc
            AND src.group_gmc = 'placement_client'
            AND src.aktif = 'Yes'
            AND src.keterangan3 = 1
        LEFT JOIN tbl_gmc prj
            ON tk.project = prj.kode_gmc
            AND prj.group_gmc = 'client_project'
            AND prj.aktif = 'Yes'
            AND prj.keterangan3 = 1
        WHERE tk.status = '1'
        AND tk.loan_kasbon_eligible = '1'
        """

        # Build the processed loan requests query
        processed_requests_query = """
        SELECT COUNT(*)
        FROM td_loan l
        LEFT JOIN td_karyawan tk
            ON l.id_karyawan = tk.id_karyawan
        LEFT JOIN tbl_gmc emp
            ON tk.valdo_inc = emp.kode_gmc
            AND emp.group_gmc = 'sub_client'
            AND emp.aktif = 'Yes'
            AND emp.keterangan3 = 1
        LEFT JOIN tbl_gmc src
            ON tk.placement = src.kode_gmc
            AND src.group_gmc = 'placement_client'
            AND src.aktif = 'Yes'
            AND src.keterangan3 = 1
        LEFT JOIN tbl_gmc prj
            ON tk.project = prj.kode_gmc
            AND prj.group_gmc = 'client_project'
            AND prj.aktif = 'Yes'
            AND prj.keterangan3 = 1
        WHERE l.loan_status IN (1, 2, 3, 4)
        AND {loan_conditions}
        """.format(loan_conditions=loan_conditions)

        # Build the pending loan requests query
        pending_requests_query = """
        SELECT COUNT(*)
        FROM td_loan l
        LEFT JOIN td_karyawan tk
            ON l.id_karyawan = tk.id_karyawan
        LEFT JOIN tbl_gmc emp
            ON tk.valdo_inc = emp.kode_gmc
            AND emp.group_gmc = 'sub_client'
            AND emp.aktif = 'Yes'
            AND emp.keterangan3 = 1
        LEFT JOIN tbl_gmc src
            ON tk.placement = src.kode_gmc
            AND src.group_gmc = 'placement_client'
            AND src.aktif = 'Yes'
            AND src.keterangan3 = 1
        LEFT JOIN tbl_gmc prj
            ON tk.project = prj.kode_gmc
            AND prj.group_gmc = 'client_project'
            AND prj.aktif = 'Yes'
            AND prj.keterangan3 = 1
        WHERE l.loan_status = 0
        AND {loan_conditions}
        """.format(loan_conditions=loan_conditions)

        # Build the first-time borrowers query
        first_borrow_query = """
        SELECT COUNT(DISTINCT l.id_karyawan)
        FROM td_loan l
        LEFT JOIN td_karyawan tk
            ON l.id_karyawan = tk.id_karyawan
        LEFT JOIN tbl_gmc emp
            ON tk.valdo_inc = emp.kode_gmc
            AND emp.group_gmc = 'sub_client'
            AND emp.aktif = 'Yes'
            AND emp.keterangan3 = 1
        LEFT JOIN tbl_gmc src
            ON tk.placement = src.kode_gmc
            AND src.group_gmc = 'placement_client'
            AND src.aktif = 'Yes'
            AND src.keterangan3 = 1
        LEFT JOIN tbl_gmc prj
            ON tk.project = prj.kode_gmc
            AND prj.group_gmc = 'client_project'
            AND prj.aktif = 'Yes'
            AND prj.keterangan3 = 1
        WHERE l.loan_status IN (0, 1, 2, 3)
        AND NOT EXISTS (
            SELECT 1
            FROM td_loan l2
            WHERE l2.id_karyawan = l.id_karyawan
            AND l2.loan_status = 2
            AND l2.proses_date < l.proses_date
            AND {loan_conditions}
        )
        AND {loan_conditions}
        """.format(loan_conditions=loan_conditions)

        # Build the approved requests query
        approved_requests_query = """
        SELECT COUNT(*)
        FROM td_loan l
        LEFT JOIN td_karyawan tk
            ON l.id_karyawan = tk.id_karyawan
        LEFT JOIN tbl_gmc emp
            ON tk.valdo_inc = emp.kode_gmc
            AND emp.group_gmc = 'sub_client'
            AND emp.aktif = 'Yes'
            AND emp.keterangan3 = 1
        LEFT JOIN tbl_gmc src
            ON tk.placement = src.kode_gmc
            AND src.group_gmc = 'placement_client'
            AND src.aktif = 'Yes'
            AND src.keterangan3 = 1
        LEFT JOIN tbl_gmc prj
            ON tk.project = prj.kode_gmc
            AND prj.group_gmc = 'client_project'
            AND prj.aktif = 'Yes'
            AND prj.keterangan3 = 1
        WHERE l.loan_status IN (1, 2, 4)
        AND {loan_conditions}
        """.format(loan_conditions=loan_conditions)

        # Build the rejected requests query
        rejected_requests_query = """
        SELECT COUNT(*)
        FROM td_loan l
        LEFT JOIN td_karyawan tk
            ON l.id_karyawan = tk.id_karyawan
        LEFT JOIN tbl_gmc emp
            ON tk.valdo_inc = emp.kode_gmc
            AND emp.group_gmc = 'sub_client'
            AND emp.aktif = 'Yes'
            AND emp.keterangan3 = 1
        LEFT JOIN tbl_gmc src
            ON tk.placement = src.kode_gmc
            AND src.group_gmc = 'placement_client'
            AND src.aktif = 'Yes'
            AND src.keterangan3 = 1
        LEFT JOIN tbl_gmc prj
            ON tk.project = prj.kode_gmc
            AND prj.group_gmc = 'client_project'
            AND prj.aktif = 'Yes'
            AND prj.keterangan3 = 1
        WHERE l.loan_status = 3
        AND {loan_conditions}
        """.format(loan_conditions=loan_conditions)

        # Build the average approval time query
        avg_approval_time_query = """
        SELECT AVG(DATEDIFF(l.proses_date, l.received_date))
        FROM td_loan l
        LEFT JOIN td_karyawan tk
            ON l.id_karyawan = tk.id_karyawan
        LEFT JOIN tbl_gmc emp
            ON tk.valdo_inc = emp.kode_gmc
            AND emp.group_gmc = 'sub_client'
            AND emp.aktif = 'Yes'
            AND emp.keterangan3 = 1
        LEFT JOIN tbl_gmc src
            ON tk.placement = src.kode_gmc
            AND src.group_gmc = 'placement_client'
            AND src.aktif = 'Yes'
            AND src.keterangan3 = 1
        LEFT JOIN tbl_gmc prj
            ON tk.project = prj.kode_gmc
            AND prj.group_gmc = 'client_project'
            AND prj.aktif = 'Yes'
            AND prj.keterangan3 = 1
        WHERE l.loan_status = 1
        AND l.proses_date IS NOT NULL
        AND l.received_date IS NOT NULL
        AND {loan_conditions}
        """.format(loan_conditions=loan_conditions)

        # Build the total disbursed amount query
        total_disbursed_amount_query = """
        SELECT SUM(l.total_loan)
        FROM td_loan l
        LEFT JOIN td_karyawan tk
            ON l.id_karyawan = tk.id_karyawan
        LEFT JOIN tbl_gmc emp
            ON tk.valdo_inc = emp.kode_gmc
            AND emp.group_gmc = 'sub_client'
            AND emp.aktif = 'Yes'
            AND emp.keterangan3 = 1
        LEFT JOIN tbl_gmc src
            ON tk.placement = src.kode_gmc
            AND src.group_gmc = 'placement_client'
            AND src.aktif = 'Yes'
            AND src.keterangan3 = 1
        LEFT JOIN tbl_gmc prj
            ON tk.project = prj.kode_gmc
            AND prj.group_gmc = 'client_project'
            AND prj.aktif = 'Yes'
            AND prj.keterangan3 = 1
        WHERE l.loan_status IN (1, 2, 4)
        AND {loan_conditions}
        """.format(loan_conditions=loan_conditions)

        # Build the total loans query (for average calculation - count all loans, not unique borrowers)
        total_loans_query = """
        SELECT COUNT(*)
        FROM td_loan l
        LEFT JOIN td_karyawan tk
            ON l.id_karyawan = tk.id_karyawan
        LEFT JOIN tbl_gmc emp
            ON tk.valdo_inc = emp.kode_gmc
            AND emp.group_gmc = 'sub_client'
            AND emp.aktif = 'Yes'
            AND emp.keterangan3 = 1
        LEFT JOIN tbl_gmc src
            ON tk.placement = src.kode_gmc
            AND src.group_gmc = 'placement_client'
            AND src.aktif = 'Yes'
            AND src.keterangan3 = 1
        LEFT JOIN tbl_gmc prj
            ON tk.project = prj.kode_gmc
            AND prj.group_gmc = 'client_project'
            AND prj.aktif = 'Yes'
            AND prj.keterangan3 = 1
        WHERE l.loan_status IN (1, 2, 4)
        AND {loan_conditions}
        """.format(loan_conditions=loan_conditions)

        # Build parameters dict for filters
        params = {}

        # Add filters to all queries
        if id_karyawan_filter:
            eligible_count_query += " AND tk.id_karyawan = :id_karyawan"
            processed_requests_query += " AND l.id_karyawan = :id_karyawan"
            pending_requests_query += " AND l.id_karyawan = :id_karyawan"
            first_borrow_query += " AND l.id_karyawan = :id_karyawan"
            approved_requests_query += " AND l.id_karyawan = :id_karyawan"
            rejected_requests_query += " AND l.id_karyawan = :id_karyawan"
            avg_approval_time_query += " AND l.id_karyawan = :id_karyawan"
            total_disbursed_amount_query += " AND l.id_karyawan = :id_karyawan"
            total_loans_query += " AND l.id_karyawan = :id_karyawan"
            params['id_karyawan'] = id_karyawan_filter
        if employer_filter:
            eligible_count_query += " AND emp.keterangan = :employer"
            processed_requests_query += " AND emp.keterangan = :employer"
            pending_requests_query += " AND emp.keterangan = :employer"
            first_borrow_query += " AND emp.keterangan = :employer"
            approved_requests_query += " AND emp.keterangan = :employer"
            rejected_requests_query += " AND emp.keterangan = :employer"
            avg_approval_time_query += " AND emp.keterangan = :employer"
            total_disbursed_amount_query += " AND emp.keterangan = :employer"
            total_loans_query += " AND emp.keterangan = :employer"
            params['employer'] = employer_filter
        if sourced_to_filter:
            eligible_count_query += " AND src.keterangan = :sourced_to"
            processed_requests_query += " AND src.keterangan = :sourced_to"
            pending_requests_query += " AND src.keterangan = :sourced_to"
            first_borrow_query += " AND src.keterangan = :sourced_to"
            approved_requests_query += " AND src.keterangan = :sourced_to"
            rejected_requests_query += " AND src.keterangan = :sourced_to"
            avg_approval_time_query += " AND src.keterangan = :sourced_to"
            total_disbursed_amount_query += " AND src.keterangan = :sourced_to"
            total_loans_query += " AND src.keterangan = :sourced_to"
            params['sourced_to'] = sourced_to_filter
        if project_filter:
            eligible_count_query += " AND prj.keterangan = :project"
            processed_requests_query += " AND prj.keterangan = :project"
            pending_requests_query += " AND prj.keterangan = :project"
            first_borrow_query += " AND prj.keterangan = :project"
            approved_requests_query += " AND prj.keterangan = :project"
            rejected_requests_query += " AND prj.keterangan = :project"
            avg_approval_time_query += " AND prj.keterangan = :project"
            total_disbursed_amount_query += " AND prj.keterangan = :project"
            total_loans_query += " AND prj.keterangan = :project"
            params['project'] = project_filter
        if start_date and end_date:
            updated = _apply_date_filters_to_named_queries(
                {
                    "processed": processed_requests_query,
                    "pending": pending_requests_query,
                    "first": first_borrow_query,
                    "approved": approved_requests_query,
                    "rejected": rejected_requests_query,
                    "avg": avg_approval_time_query,
                    "disbursed": total_disbursed_amount_query,
                    "loans": total_loans_query,
                },
                params,
                start_date=start_date,
                end_date=end_date,
                date_columns={"pending": "l.received_date"},
            )
            processed_requests_query = updated["processed"]
            pending_requests_query = updated["pending"]
            first_borrow_query = updated["first"]
            approved_requests_query = updated["approved"]
            rejected_requests_query = updated["rejected"]
            avg_approval_time_query = updated["avg"]
            total_disbursed_amount_query = updated["disbursed"]
            total_loans_query = updated["loans"]

        _coverage_loan_queries = (
            processed_requests_query,
            pending_requests_query,
            first_borrow_query,
            approved_requests_query,
            rejected_requests_query,
            avg_approval_time_query,
            total_disbursed_amount_query,
            total_loans_query,
        )
        (
            processed_requests_query,
            pending_requests_query,
            first_borrow_query,
            approved_requests_query,
            rejected_requests_query,
            avg_approval_time_query,
            total_disbursed_amount_query,
            total_loans_query,
        ) = [
            _apply_project_management_filters(
                q, params, client_segment_filter, product_type_filter, db=db
            )
            for q in _coverage_loan_queries
        ]
        eligible_count_query = _apply_project_management_filters(
            eligible_count_query, params, client_segment_filter, product_type_filter, db=db
        )


        # Execute all queries
        eligible_result = db.execute(text(eligible_count_query), params)
        total_eligible = eligible_result.fetchone()[0]

        processed_result = db.execute(text(processed_requests_query), params)
        total_processed = processed_result.fetchone()[0]

        pending_result = db.execute(text(pending_requests_query), params)
        total_pending = pending_result.fetchone()[0]

        first_borrow_result = db.execute(text(first_borrow_query), params)
        total_first_borrow = first_borrow_result.fetchone()[0]

        approved_result = db.execute(text(approved_requests_query), params)
        total_approved = approved_result.fetchone()[0]

        rejected_result = db.execute(text(rejected_requests_query), params)
        total_rejected = rejected_result.fetchone()[0]

        avg_approval_time_result = db.execute(text(avg_approval_time_query), params)
        avg_approval_time = avg_approval_time_result.fetchone()[0] or 0

        # Execute the new queries
        total_disbursed_amount_result = db.execute(text(total_disbursed_amount_query), params)
        total_disbursed_amount = total_disbursed_amount_result.fetchone()[0] or 0

        total_loans_result = db.execute(text(total_loans_query), params)
        total_loans = total_loans_result.fetchone()[0] or 0

        # Calculate penetration rate
        penetration_rate = 0
        if total_eligible > 0:
            penetration_rate = total_processed / total_eligible

        # Calculate approval rate
        approval_rate = 0
        if total_processed > 0:
            approval_rate = total_approved / total_processed

        # Calculate rejected rate
        rejected_rate = 0
        if total_processed > 0:
            rejected_rate = total_rejected / total_processed

        # Calculate average disbursed amount (per loan, not per borrower)
        average_disbursed_amount = 0
        if total_loans > 0:
            average_disbursed_amount = total_disbursed_amount / total_loans



        return {
            "total_eligible_employees": total_eligible,
            "total_processed_loan_requests": total_processed,
            "total_pending_loan_requests": total_pending,
            "total_first_borrow": total_first_borrow,
            "total_approved_requests": total_approved,
            "total_rejected_requests": total_rejected,
            "total_disbursed_amount": total_disbursed_amount,
            "total_loans": total_loans,
            "average_disbursed_amount": average_disbursed_amount,
            "approval_rate": approval_rate,
            "rejected_rate": rejected_rate,
            "average_approval_time": avg_approval_time,
            "penetration_rate": penetration_rate
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "total_eligible_employees": 0,
            "total_processed_loan_requests": 0,
            "total_pending_loan_requests": 0,
            "total_first_borrow": 0,
            "total_approved_requests": 0,
            "total_rejected_requests": 0,
            "total_disbursed_amount": 0,
            "total_loans": 0,
            "average_disbursed_amount": 0,
            "approval_rate": 0,
            "rejected_rate": 0,
            "average_approval_time": 0,
            "penetration_rate": 0
        }


def get_user_coverage_monthly_summary(
    db: Session,
    start_date: str = None,
    end_date: str = None,
    employer_filter: str = None,
    sourced_to_filter: str = None,
    project_filter: str = None,
    client_segment_filter: str = None,
    product_type_filter: str = None,
    id_karyawan_filter: int = None,
) -> dict:
    """Monthly summary: eligible employees, processed requests, disbursed amount, penetration."""
    try:
        loan_conditions = LOAN_CONDITIONS
        company_filter = COMPANY_FILTER
        params = {}

        eligible_count_query = f"""
        SELECT COUNT(*)
        FROM td_karyawan tk
        {_KARYAWAN_GMC_JOINS}
        WHERE tk.status = '1'
        AND tk.loan_kasbon_eligible = '1'
        """
        eligible_count_query = _append_karyawan_org_filters(
            eligible_count_query,
            params,
            id_karyawan_filter=id_karyawan_filter,
            employer_filter=employer_filter,
            sourced_to_filter=sourced_to_filter,
            project_filter=project_filter,
            client_segment_filter=client_segment_filter,
            product_type_filter=product_type_filter,
            company_filter=company_filter,
            db=db,
        )

        monthly_query = f"""
        SELECT
            DATE_FORMAT(l.proses_date, '%M %Y') as month_year,
            COUNT(CASE WHEN l.loan_status IN (1, 2, 3, 4) THEN 1 END) as total_processed_loan_requests,
            COALESCE(SUM(CASE WHEN l.loan_status IN (1, 2, 4) THEN l.total_loan ELSE 0 END), 0) as total_disbursed_amount
        FROM td_loan l
        {_LOAN_GMC_JOINS}
        WHERE l.proses_date IS NOT NULL
        AND {loan_conditions}
        """
        monthly_query = _append_loan_org_filters(
            monthly_query,
            params,
            id_karyawan_filter=id_karyawan_filter,
            employer_filter=employer_filter,
            sourced_to_filter=sourced_to_filter,
            project_filter=project_filter,
            client_segment_filter=client_segment_filter,
            product_type_filter=product_type_filter,
            company_filter=company_filter,
            db=db,
        )
        if start_date and end_date:
            monthly_query = append_date_filters(
                monthly_query,
                params,
                start_date=start_date,
                end_date=end_date,
            )
        monthly_query += """
        GROUP BY DATE_FORMAT(l.proses_date, '%M %Y')
        ORDER BY MIN(l.proses_date)
        """

        total_eligible = db.execute(text(eligible_count_query), params).fetchone()[0] or 0
        monthly_data = {}
        for row in db.execute(text(monthly_query), params).fetchall():
            if row[0] is None:
                continue
            processed = row[1] or 0
            disbursed = row[2] or 0
            monthly_data[row[0]] = {
                "total_eligible_employees": total_eligible,
                "total_processed_loan_requests": processed,
                "total_disbursed_amount": disbursed,
                "penetration_rate": (processed / total_eligible) if total_eligible > 0 else 0,
            }
        return monthly_data
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {}


def get_requests_endpoint(db: Session,
                         employer_filter: str = None, sourced_to_filter: str = None,
                         project_filter: str = None, client_segment_filter: str = None, product_type_filter: str = None, id_karyawan_filter: int = None, start_date: str = None, end_date: str = None) -> dict:
    """Get requests metrics: total_approved_requests, total_rejected_requests, approval_rate, average_approval_time"""

    try:
        loan_conditions = LOAN_CONDITIONS
        # Build the approved requests query
        approved_requests_query = """
        SELECT COUNT(*)
        FROM td_loan l
        LEFT JOIN td_karyawan tk
            ON l.id_karyawan = tk.id_karyawan
        LEFT JOIN tbl_gmc emp
            ON tk.valdo_inc = emp.kode_gmc
            AND emp.group_gmc = 'sub_client'
            AND emp.aktif = 'Yes'
            AND emp.keterangan3 = 1
        LEFT JOIN tbl_gmc src
            ON tk.placement = src.kode_gmc
            AND src.group_gmc = 'placement_client'
            AND src.aktif = 'Yes'
            AND src.keterangan3 = 1
        LEFT JOIN tbl_gmc prj
            ON tk.project = prj.kode_gmc
            AND prj.group_gmc = 'client_project'
            AND prj.aktif = 'Yes'
            AND prj.keterangan3 = 1
        WHERE l.loan_status IN (1, 2, 4)
        AND {loan_conditions}
        """.format(loan_conditions=loan_conditions)

        # Build the rejected requests query
        rejected_requests_query = """
        SELECT COUNT(*)
        FROM td_loan l
        LEFT JOIN td_karyawan tk
            ON l.id_karyawan = tk.id_karyawan
        LEFT JOIN tbl_gmc emp
            ON tk.valdo_inc = emp.kode_gmc
            AND emp.group_gmc = 'sub_client'
            AND emp.aktif = 'Yes'
            AND emp.keterangan3 = 1
        LEFT JOIN tbl_gmc src
            ON tk.placement = src.kode_gmc
            AND src.group_gmc = 'placement_client'
            AND src.aktif = 'Yes'
            AND src.keterangan3 = 1
        LEFT JOIN tbl_gmc prj
            ON tk.project = prj.kode_gmc
            AND prj.group_gmc = 'client_project'
            AND prj.aktif = 'Yes'
            AND prj.keterangan3 = 1
        WHERE l.loan_status = 3
        AND {loan_conditions}
        """.format(loan_conditions=loan_conditions)

        # Build the total processed requests query (for approval rate calculation)
        total_processed_query = """
        SELECT COUNT(*)
        FROM td_loan l
        LEFT JOIN td_karyawan tk
            ON l.id_karyawan = tk.id_karyawan
        LEFT JOIN tbl_gmc emp
            ON tk.valdo_inc = emp.kode_gmc
            AND emp.group_gmc = 'sub_client'
            AND emp.aktif = 'Yes'
            AND emp.keterangan3 = 1
        LEFT JOIN tbl_gmc src
            ON tk.placement = src.kode_gmc
            AND src.group_gmc = 'placement_client'
            AND src.aktif = 'Yes'
            AND src.keterangan3 = 1
        LEFT JOIN tbl_gmc prj
            ON tk.project = prj.kode_gmc
            AND prj.group_gmc = 'client_project'
            AND prj.aktif = 'Yes'
            AND prj.keterangan3 = 1
        WHERE l.loan_status IN (1, 2, 3, 4)
        AND {loan_conditions}
        """.format(loan_conditions=loan_conditions)

        # Build the average approval time query
        avg_approval_time_query = """
        SELECT AVG(
            CASE
                WHEN l.proses_date IS NOT NULL
                AND l.received_date IS NOT NULL
                AND l.proses_date > l.received_date
                AND l.proses_date >= '1900-01-01'
                AND l.received_date >= '1900-01-01'
                THEN DATEDIFF(l.proses_date, l.received_date)
                ELSE NULL
            END
        )
        FROM td_loan l
        LEFT JOIN td_karyawan tk
            ON l.id_karyawan = tk.id_karyawan
        LEFT JOIN tbl_gmc emp
            ON tk.valdo_inc = emp.kode_gmc
            AND emp.group_gmc = 'sub_client'
            AND emp.aktif = 'Yes'
            AND emp.keterangan3 = 1
        LEFT JOIN tbl_gmc src
            ON tk.placement = src.kode_gmc
            AND src.group_gmc = 'placement_client'
            AND src.aktif = 'Yes'
            AND src.keterangan3 = 1
        LEFT JOIN tbl_gmc prj
            ON tk.project = prj.kode_gmc
            AND prj.group_gmc = 'client_project'
            AND prj.aktif = 'Yes'
            AND prj.keterangan3 = 1
        WHERE l.loan_status = 1
        AND {loan_conditions}
        """.format(loan_conditions=loan_conditions)

        # Build parameters dict for filters
        params = {}

        # Add filters to all queries
        if id_karyawan_filter:
            approved_requests_query += " AND l.id_karyawan = :id_karyawan"
            rejected_requests_query += " AND l.id_karyawan = :id_karyawan"
            total_processed_query += " AND l.id_karyawan = :id_karyawan"
            avg_approval_time_query += " AND l.id_karyawan = :id_karyawan"
            params['id_karyawan'] = id_karyawan_filter
        if employer_filter:
            approved_requests_query += " AND emp.keterangan = :employer"
            rejected_requests_query += " AND emp.keterangan = :employer"
            total_processed_query += " AND emp.keterangan = :employer"
            avg_approval_time_query += " AND emp.keterangan = :employer"
            params['employer'] = employer_filter
        if sourced_to_filter:
            approved_requests_query += " AND src.keterangan = :sourced_to"
            rejected_requests_query += " AND src.keterangan = :sourced_to"
            total_processed_query += " AND src.keterangan = :sourced_to"
            avg_approval_time_query += " AND src.keterangan = :sourced_to"
            params['sourced_to'] = sourced_to_filter
        if project_filter:
            approved_requests_query += " AND prj.keterangan = :project"
            rejected_requests_query += " AND prj.keterangan = :project"
            total_processed_query += " AND prj.keterangan = :project"
            avg_approval_time_query += " AND prj.keterangan = :project"
            params['project'] = project_filter
        if start_date and end_date:
            updated = _apply_date_filters_to_named_queries(
                {
                    "approved": approved_requests_query,
                    "rejected": rejected_requests_query,
                    "processed": total_processed_query,
                    "avg": avg_approval_time_query,
                },
                params,
                start_date=start_date,
                end_date=end_date,
            )
            approved_requests_query = updated["approved"]
            rejected_requests_query = updated["rejected"]
            total_processed_query = updated["processed"]
            avg_approval_time_query = updated["avg"]

        approved_requests_query = _apply_project_management_filters(
            approved_requests_query, params, client_segment_filter, product_type_filter, db=db
        )
        rejected_requests_query = _apply_project_management_filters(
            rejected_requests_query, params, client_segment_filter, product_type_filter, db=db
        )
        total_processed_query = _apply_project_management_filters(
            total_processed_query, params, client_segment_filter, product_type_filter, db=db
        )
        avg_approval_time_query = _apply_project_management_filters(
            avg_approval_time_query, params, client_segment_filter, product_type_filter, db=db
        )

        # Execute all queries
        approved_result = db.execute(text(approved_requests_query), params)
        total_approved = approved_result.fetchone()[0]

        rejected_result = db.execute(text(rejected_requests_query), params)
        total_rejected = rejected_result.fetchone()[0]

        total_processed_result = db.execute(text(total_processed_query), params)
        total_processed = total_processed_result.fetchone()[0]

        avg_approval_time_result = db.execute(text(avg_approval_time_query), params)
        avg_approval_time_record = avg_approval_time_result.fetchone()
        avg_approval_time = avg_approval_time_record[0] if avg_approval_time_record and avg_approval_time_record[0] is not None else 0

        # Calculate approval rate
        approval_rate = 0
        if total_processed > 0:
            approval_rate = total_approved / total_processed

        # Calculate rejected rate
        rejected_rate = 0
        if total_processed > 0:
            rejected_rate = total_rejected / total_processed



        return {
            "total_approved_requests": total_approved,
            "total_rejected_requests": total_rejected,
            "approval_rate": approval_rate,
            "rejected_rate": rejected_rate,
            "average_approval_time": avg_approval_time
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "total_approved_requests": 0,
            "total_rejected_requests": 0,
            "approval_rate": 0,
            "rejected_rate": 0,
            "average_approval_time": 0
        }


def get_disbursement_endpoint(db: Session,
                             employer_filter: str = None, sourced_to_filter: str = None,
                             project_filter: str = None, client_segment_filter: str = None, product_type_filter: str = None, id_karyawan_filter: int = None, start_date: str = None, end_date: str = None) -> dict:
    """Get disbursement metrics: total_disbursed_amount, average_disbursed_amount"""

    try:
        loan_conditions = LOAN_CONDITIONS
        # Build the total disbursed amount query
        total_disbursed_amount_query = """
        SELECT SUM(l.total_loan)
        FROM td_loan l
        LEFT JOIN td_karyawan tk
            ON l.id_karyawan = tk.id_karyawan
        LEFT JOIN tbl_gmc emp
            ON tk.valdo_inc = emp.kode_gmc
            AND emp.group_gmc = 'sub_client'
            AND emp.aktif = 'Yes'
            AND emp.keterangan3 = 1
        LEFT JOIN tbl_gmc src
            ON tk.placement = src.kode_gmc
            AND src.group_gmc = 'placement_client'
            AND src.aktif = 'Yes'
            AND src.keterangan3 = 1
        LEFT JOIN tbl_gmc prj
            ON tk.project = prj.kode_gmc
            AND prj.group_gmc = 'client_project'
            AND prj.aktif = 'Yes'
            AND prj.keterangan3 = 1
        WHERE l.loan_status IN (1, 2, 4)
        AND {loan_conditions}
        """.format(loan_conditions=loan_conditions)

        # Build the total loan count query (for average calculation - count all loans, not unique borrowers)
        total_loans_query = """
        SELECT COUNT(*)
        FROM td_loan l
        LEFT JOIN td_karyawan tk
            ON l.id_karyawan = tk.id_karyawan
        LEFT JOIN tbl_gmc emp
            ON tk.valdo_inc = emp.kode_gmc
            AND emp.group_gmc = 'sub_client'
            AND emp.aktif = 'Yes'
            AND emp.keterangan3 = 1
        LEFT JOIN tbl_gmc src
            ON tk.placement = src.kode_gmc
            AND src.group_gmc = 'placement_client'
            AND src.aktif = 'Yes'
            AND src.keterangan3 = 1
        LEFT JOIN tbl_gmc prj
            ON tk.project = prj.kode_gmc
            AND prj.group_gmc = 'client_project'
            AND prj.aktif = 'Yes'
            AND prj.keterangan3 = 1
        WHERE l.loan_status IN (1, 2, 4)
        AND {loan_conditions}
        """.format(loan_conditions=loan_conditions)

        # Build parameters dict for filters
        params = {}

        # Add filters to all queries
        if id_karyawan_filter:
            total_disbursed_amount_query += " AND l.id_karyawan = :id_karyawan"
            total_loans_query += " AND l.id_karyawan = :id_karyawan"
            params['id_karyawan'] = id_karyawan_filter
        if employer_filter:
            total_disbursed_amount_query += " AND emp.keterangan = :employer"
            total_loans_query += " AND emp.keterangan = :employer"
            params['employer'] = employer_filter
        if sourced_to_filter:
            total_disbursed_amount_query += " AND src.keterangan = :sourced_to"
            total_loans_query += " AND src.keterangan = :sourced_to"
            params['sourced_to'] = sourced_to_filter
        if project_filter:
            total_disbursed_amount_query += " AND prj.keterangan = :project"
            total_loans_query += " AND prj.keterangan = :project"
            params['project'] = project_filter
        if start_date and end_date:
            updated = _apply_date_filters_to_named_queries(
                {
                    "disbursed": total_disbursed_amount_query,
                    "loans": total_loans_query,
                },
                params,
                start_date=start_date,
                end_date=end_date,
            )
            total_disbursed_amount_query = updated["disbursed"]
            total_loans_query = updated["loans"]

        total_disbursed_amount_query = _apply_project_management_filters(
            total_disbursed_amount_query, params, client_segment_filter, product_type_filter, db=db
        )
        total_loans_query = _apply_project_management_filters(
            total_loans_query, params, client_segment_filter, product_type_filter, db=db
        )

        # Execute all queries
        total_disbursed_amount_result = db.execute(text(total_disbursed_amount_query), params)
        total_disbursed_amount = total_disbursed_amount_result.fetchone()[0] or 0

        total_loans_result = db.execute(text(total_loans_query), params)
        total_loans = total_loans_result.fetchone()[0] or 0

        # Calculate average disbursed amount (per loan, not per borrower)
        average_disbursed_amount = 0
        if total_loans > 0:
            average_disbursed_amount = total_disbursed_amount / total_loans



        return {
            "total_disbursed_amount": total_disbursed_amount,
            "average_disbursed_amount": average_disbursed_amount
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "total_disbursed_amount": 0,
            "average_disbursed_amount": 0
        }


def get_disbursement_monthly_endpoint(db: Session, start_date: str = None, end_date: str = None,
                                    employer_filter: str = None, sourced_to_filter: str = None,
                                    project_filter: str = None, client_segment_filter: str = None,
                                    product_type_filter: str = None, id_karyawan_filter: int = None) -> dict:
    """Get disbursement monthly data: total disbursed amount and average disbursed amount by month"""

    try:
        loan_conditions = LOAN_CONDITIONS
        monthly_query = """
        SELECT
            DATE_FORMAT(l.proses_date, '%M %Y') as month_year,
            SUM(CASE WHEN l.loan_status IN (1, 2, 4) THEN l.total_loan ELSE 0 END) as total_disbursed_amount,
            COUNT(CASE WHEN l.loan_status IN (1, 2, 4) THEN 1 END) as total_loans
        FROM td_loan l
        LEFT JOIN td_karyawan tk
            ON l.id_karyawan = tk.id_karyawan
        LEFT JOIN tbl_gmc emp
            ON tk.valdo_inc = emp.kode_gmc
            AND emp.group_gmc = 'sub_client'
            AND emp.aktif = 'Yes'
            AND emp.keterangan3 = 1
        LEFT JOIN tbl_gmc src
            ON tk.placement = src.kode_gmc
            AND src.group_gmc = 'placement_client'
            AND src.aktif = 'Yes'
            AND src.keterangan3 = 1
        LEFT JOIN tbl_gmc prj
            ON tk.project = prj.kode_gmc
            AND prj.group_gmc = 'client_project'
            AND prj.aktif = 'Yes'
            AND prj.keterangan3 = 1
        WHERE l.proses_date IS NOT NULL
        AND l.proses_date BETWEEN :start_date AND :end_date
        AND {loan_conditions}
        """.format(loan_conditions=loan_conditions)

        # Build parameters dict for monthly query
        monthly_params = {
            'start_date': start_date,
            'end_date': end_date
        }

        # Add filters to monthly query
        if id_karyawan_filter:
            monthly_query += " AND l.id_karyawan = :id_karyawan"
            monthly_params['id_karyawan'] = id_karyawan_filter
        if employer_filter:
            monthly_query += " AND emp.keterangan = :employer"
            monthly_params['employer'] = employer_filter
        if sourced_to_filter:
            monthly_query += " AND src.keterangan = :sourced_to"
            monthly_params['sourced_to'] = sourced_to_filter
        if project_filter:
            monthly_query += " AND prj.keterangan = :project"
            monthly_params['project'] = project_filter

        monthly_query = _apply_project_management_filters(
            monthly_query, monthly_params, client_segment_filter, product_type_filter, db=db
        )

        monthly_query += """
        GROUP BY DATE_FORMAT(l.proses_date, '%M %Y')
        ORDER BY l.proses_date
        """



        # Execute monthly query
        result = db.execute(text(monthly_query), monthly_params)
        rows = result.fetchall()

        # Process results
        monthly_data = {}
        for row in rows:
            month_year = row[0]
            if month_year is None:
                continue

            total_disbursed_amount = row[1] or 0
            total_loans = row[2] or 0

            # Calculate average disbursed amount
            average_disbursed_amount = 0
            if total_loans > 0:
                average_disbursed_amount = total_disbursed_amount / total_loans

            monthly_data[month_year] = {
                "total_disbursed_amount": total_disbursed_amount,
                "total_loans": total_loans,
                "average_disbursed_amount": average_disbursed_amount
            }



        return monthly_data

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {}


def get_loans_with_karyawan(db: Session, limit: int = 1000000,
                           employer_filter: str = None, sourced_to_filter: str = None,
                           project_filter: str = None, client_segment_filter: str = None,
                           product_type_filter: str = None, loan_status_filter: int = None,
                           id_karyawan_filter: int = None, loan_type: str = "loan") -> List[dict]:
    """Get loans data with enhanced karyawan information"""

    try:
        loan_conditions = resolve_loan_conditions(loan_type, db)

        base_query = f"""
        SELECT
            l.id,
            l.id_karyawan,
            l.loan_id,
            l.purpose,
            l.duration,
            l.total_loan,
            l.admin_fee,
            l.total_payment,
            l.repayment_date,
            l.received_date,
            l.send_date,
            l.loan_status,
            l.user_proses,
            l.proses_date,
            l.payment_date,
            l.disbursement,
            l.refNumberTransaction,
            l.is_non_approval,
            emp.keterangan AS employer_name,
            src.keterangan AS sourced_to_name,
            prj.kode_gmc AS project_code,
            prj.keterangan AS project_name,
            tpm.client_segment AS client_segment_id,
            seg.keterangan AS client_segment_name,
            tpm.product_type AS product_type_id,
            pt.keterangan AS product_type_name
        FROM td_loan l
        INNER JOIN td_karyawan tk
            ON l.id_karyawan = tk.id_karyawan
        LEFT JOIN tbl_gmc emp
            ON tk.valdo_inc = emp.kode_gmc
            AND emp.group_gmc = 'sub_client'
            AND emp.aktif = 'Yes'
            AND emp.keterangan3 = 1
        LEFT JOIN tbl_gmc src
            ON tk.placement = src.kode_gmc
            AND src.group_gmc = 'placement_client'
            AND src.aktif = 'Yes'
            AND src.keterangan3 = 1
        INNER JOIN tbl_gmc prj
            ON tk.project = prj.kode_gmc
            AND prj.group_gmc = 'client_project'
            AND prj.keterangan3 = 1
        {_project_management_join_sql(required=False)}
        {_project_management_label_joins_sql()}
        WHERE {loan_conditions}
        """

        params = {}

        if id_karyawan_filter:
            base_query += " AND l.id_karyawan = :id_karyawan"
            params['id_karyawan'] = id_karyawan_filter

        if employer_filter:
            base_query += " AND emp.keterangan = :employer"
            params['employer'] = employer_filter

        if sourced_to_filter:
            base_query += " AND src.keterangan = :sourced_to"
            params['sourced_to'] = sourced_to_filter

        if project_filter:
            base_query += " AND prj.keterangan = :project"
            params['project'] = project_filter

        base_query = _apply_project_management_filters(
            base_query, params, client_segment_filter, product_type_filter, db=db
        )

        if loan_status_filter is not None:
            base_query += " AND l.loan_status = :loan_status"
            params['loan_status'] = loan_status_filter
        else:
            base_query += " AND l.loan_status IN (1, 2, 4)"

        base_query += f" LIMIT {limit}"

        result = db.execute(text(base_query), params)
        records = result.fetchall()

        loans_list = []
        for record in records:
            loans_list.append({
                "id": record[0],
                "id_karyawan": record[1],
                "loan_id": record[2],
                "purpose": record[3],
                "duration": record[4],
                "total_loan": record[5],
                "admin_fee": record[6],
                "total_payment": record[7],
                "repayment_date": str(record[8]) if record[8] else None,
                "received_date": str(record[9]) if record[9] else None,
                "send_date": str(record[10]) if record[10] else None,
                "loan_status": record[11],
                "user_process": record[12],
                "process_date": str(record[13]) if record[13] else None,
                "payment_date": str(record[14]) if record[14] else None,
                "disbursement": record[15],
                "ref_number_transaction": record[16],
                "is_non_approved": record[17],
                "employer_name": record[18],
                "sourced_to_name": record[19],
                "project_code": record[20],
                "project_name": record[21],
                "client_segment_id": record[22],
                "client_segment_name": record[23],
                "product_type_id": record[24],
                "product_type_name": record[25],
            })

        return loans_list

    except Exception as e:
        import traceback
        traceback.print_exc()
        return []


def get_available_filter_values(db: Session, employer_filter: str = None, placement_filter: str = None, loan_type: str = "loan") -> dict:
    """Get available filter values from tbl_gmc table for different categories with cascading filters"""

    try:
        # Get employers (sub_client) - conditional based on loan type
        company_filter = COMPANY_FILTER

        employer_query = f"""
        SELECT DISTINCT keterangan
        FROM tbl_gmc
        WHERE group_gmc = 'sub_client'
        AND aktif = 'Yes'
        AND keterangan3 = 1
        AND keterangan IN {company_filter}
        ORDER BY keterangan
        """

        # Get placement clients - filtered by employer if provided, but only show those related to the two allowed companies
        placement_query = f"""
        SELECT DISTINCT src.keterangan
        FROM tbl_gmc src
        INNER JOIN td_karyawan tk ON src.kode_gmc = tk.placement
        INNER JOIN tbl_gmc emp ON tk.valdo_inc = emp.kode_gmc
        WHERE emp.keterangan IN {company_filter}
        AND emp.group_gmc = 'sub_client'
        AND emp.aktif = 'Yes'
        AND emp.keterangan3 = 1
        AND src.group_gmc = 'placement_client'
        AND src.aktif = 'Yes'
        AND src.keterangan3 = 1
        """

        if employer_filter:
            placement_query += " AND emp.keterangan = :employer"

        placement_query += " ORDER BY src.keterangan"

        # Get projects - filtered by employer and/or placement if provided, but only show those related to the allowed companies
        project_query = f"""
        SELECT DISTINCT prj.keterangan
        FROM tbl_gmc prj
        INNER JOIN td_karyawan tk ON prj.kode_gmc = tk.project
        INNER JOIN tbl_gmc emp ON tk.valdo_inc = emp.kode_gmc
        WHERE emp.keterangan IN {company_filter}
        AND emp.group_gmc = 'sub_client'
        AND emp.aktif = 'Yes'
        AND emp.keterangan3 = 1
        AND prj.group_gmc = 'client_project'
        AND prj.aktif = 'Yes'
        AND prj.keterangan3 = 1
        """

        if employer_filter:
            project_query += " AND emp.keterangan = :employer"

        if placement_filter:
            project_query += " AND EXISTS (SELECT 1 FROM tbl_gmc src INNER JOIN td_karyawan tk2 ON src.kode_gmc = tk2.placement WHERE tk2.id_karyawan = tk.id_karyawan AND src.keterangan = :placement AND src.group_gmc = 'placement_client' AND src.aktif = 'Yes' AND src.keterangan3 = 1)"

        project_query += " ORDER BY prj.keterangan"

        # Build parameters for filtered queries
        placement_params = {}
        project_params = {}

        if employer_filter:
            placement_params['employer'] = employer_filter
            project_params['employer'] = employer_filter

        if placement_filter:
            project_params['placement'] = placement_filter

        # Execute queries
        employers = [row[0] for row in db.execute(text(employer_query)).fetchall()]
        placements = [row[0] for row in db.execute(text(placement_query), placement_params).fetchall()]
        projects = [row[0] for row in db.execute(text(project_query), project_params).fetchall()]
        pm_options = _fetch_project_management_filter_options(db)

        return {
            "employers": employers,
            "placements": placements,
            "projects": projects,
            "client_segments": pm_options["client_segments"],
            "client_segment_groups": pm_options["client_segment_groups"],
            "product_types": pm_options["product_types"],
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        fallback_segments = _build_client_segment_filter_options([])
        return {
            "employers": [],
            "placements": [],
            "projects": [],
            "client_segments": fallback_segments["client_segments"],
            "client_segment_groups": fallback_segments["client_segment_groups"],
            "product_types": [],
        }


def get_loan_fees_summary(db: Session,
                          employer_filter: str = None, sourced_to_filter: str = None,
                          project_filter: str = None, client_segment_filter: str = None, product_type_filter: str = None, loan_status_filter: int = None,
                          id_karyawan_filter: int = None, start_date: str = None, end_date: str = None) -> dict:
    """Get loan fees summary (total expected and collected admin fees)"""

    try:
        loan_conditions = LOAN_CONDITIONS
        # Build the query to calculate admin fees
        fees_query = """
        SELECT
            SUM(CASE WHEN l.loan_status IN (1, 2, 4) THEN l.admin_fee ELSE 0 END) as total_expected_admin_fee,
            COUNT(CASE WHEN l.loan_status IN (1, 2, 4) THEN 1 END) as expected_loans_count,
            SUM(CASE WHEN l.loan_status = 2 THEN l.admin_fee ELSE 0 END) as total_collected_admin_fee,
            COUNT(CASE WHEN l.loan_status = 2 THEN 1 END) as collected_loans_count,
            SUM(CASE WHEN l.loan_status = 4 THEN l.total_loan ELSE 0 END) as total_failed_payment
        FROM td_loan l
        LEFT JOIN td_karyawan tk
            ON l.id_karyawan = tk.id_karyawan
        LEFT JOIN tbl_gmc emp
            ON tk.valdo_inc = emp.kode_gmc
            AND emp.group_gmc = 'sub_client'
            AND emp.aktif = 'Yes'
            AND emp.keterangan3 = 1
        LEFT JOIN tbl_gmc src
            ON tk.placement = src.kode_gmc
            AND src.group_gmc = 'placement_client'
            AND src.aktif = 'Yes'
            AND src.keterangan3 = 1
        LEFT JOIN tbl_gmc prj
            ON tk.project = prj.kode_gmc
            AND prj.group_gmc = 'client_project'
            AND prj.aktif = 'Yes'
            AND prj.keterangan3 = 1
        WHERE 1=1
        AND {loan_conditions}
        """.format(loan_conditions=loan_conditions)

        # Build parameters dict for filters
        params = {}

        # Add filters
        if id_karyawan_filter:
            fees_query += " AND l.id_karyawan = :id_karyawan"
            params['id_karyawan'] = id_karyawan_filter

        if employer_filter:
            fees_query += " AND emp.keterangan = :employer"
            params['employer'] = employer_filter

        if sourced_to_filter:
            fees_query += " AND src.keterangan = :sourced_to"
            params['sourced_to'] = sourced_to_filter

        if project_filter:
            fees_query += " AND prj.keterangan = :project"
            params['project'] = project_filter

        fees_query = _apply_project_management_filters(fees_query, params, client_segment_filter, product_type_filter, db=db)

        if loan_status_filter is not None:
            fees_query += " AND l.loan_status = :loan_status"
            params['loan_status'] = loan_status_filter

        # Add date filters based on proses_date
        if start_date and end_date:
            fees_query = append_date_filters(
                fees_query,
                params,
                start_date=start_date,
                end_date=end_date,
            )

        # Execute the query
        result = db.execute(text(fees_query), params)
        record = result.fetchone()

        # Extract the values (handle None values)
        total_expected = record[0] if record[0] is not None else 0
        expected_count = record[1] if record[1] is not None else 0
        total_collected = record[2] if record[2] is not None else 0
        collected_count = record[3] if record[3] is not None else 0
        total_failed_payment = record[4] if record[4] is not None else 0

        # Calculate admin_fee_profit: total_collected_admin_fee - total_failed_payment
        admin_fee_profit = total_collected - total_failed_payment


        return {
            "total_expected_admin_fee": total_expected,
            "expected_loans_count": expected_count,
            "total_collected_admin_fee": total_collected,
            "collected_loans_count": collected_count,
            "total_failed_payment": total_failed_payment,
            "admin_fee_profit": admin_fee_profit
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "total_expected_admin_fee": 0,
            "expected_loans_count": 0,
            "total_collected_admin_fee": 0,
            "collected_loans_count": 0,
            "total_failed_payment": 0,
            "admin_fee_profit": 0
        }


def get_loan_fees_monthly_summary(db: Session,
                                  employer_filter: str = None, sourced_to_filter: str = None,
                                  project_filter: str = None, client_segment_filter: str = None, product_type_filter: str = None, loan_status_filter: int = None,
                                  id_karyawan_filter: int = None, start_date: str = None,
                                  end_date: str = None) -> dict:
    """Get loan fees summary separated by months within a date range"""

    try:
        loan_conditions = LOAN_CONDITIONS
        # Build the query to calculate admin fees by month
        fees_query = """
        SELECT
            DATE_FORMAT(l.proses_date, '%M %Y') as month_year,
            SUM(CASE WHEN l.loan_status IN (1, 2, 4) THEN l.admin_fee ELSE 0 END) as total_expected_admin_fee,
            COUNT(CASE WHEN l.loan_status IN (1, 2, 4) THEN 1 END) as expected_loans_count,
            SUM(CASE WHEN l.loan_status = 2 THEN l.admin_fee ELSE 0 END) as total_collected_admin_fee,
            COUNT(CASE WHEN l.loan_status = 2 THEN 1 END) as collected_loans_count,
            SUM(CASE WHEN l.loan_status = 4 THEN l.total_loan ELSE 0 END) as total_failed_payment
        FROM td_loan l
        LEFT JOIN td_karyawan tk
            ON l.id_karyawan = tk.id_karyawan
        LEFT JOIN tbl_gmc emp
            ON tk.valdo_inc = emp.kode_gmc
            AND emp.group_gmc = 'sub_client'
            AND emp.aktif = 'Yes'
            AND emp.keterangan3 = 1
        LEFT JOIN tbl_gmc src
            ON tk.placement = src.kode_gmc
            AND src.group_gmc = 'placement_client'
            AND src.aktif = 'Yes'
            AND src.keterangan3 = 1
        LEFT JOIN tbl_gmc prj
            ON tk.project = prj.kode_gmc
            AND prj.group_gmc = 'client_project'
            AND prj.aktif = 'Yes'
            AND prj.keterangan3 = 1
        WHERE l.proses_date IS NOT NULL
        AND {loan_conditions}
        """.format(loan_conditions=loan_conditions)

        # Build parameters dict for filters
        params = {}

        # Add filters
        if id_karyawan_filter:
            fees_query += " AND l.id_karyawan = :id_karyawan"
            params['id_karyawan'] = id_karyawan_filter

        if employer_filter:
            fees_query += " AND emp.keterangan = :employer"
            params['employer'] = employer_filter

        if sourced_to_filter:
            fees_query += " AND src.keterangan = :sourced_to"
            params['sourced_to'] = sourced_to_filter

        if project_filter:
            fees_query += " AND prj.keterangan = :project"
            params['project'] = project_filter

        fees_query = _apply_project_management_filters(fees_query, params, client_segment_filter, product_type_filter, db=db)

        if loan_status_filter is not None:
            fees_query += " AND l.loan_status = :loan_status"
            params['loan_status'] = loan_status_filter

        # Add date range filters based on proses_date
        if start_date:
            fees_query += " AND l.proses_date >= :start_date"
            params['start_date'] = start_date

        if end_date:
            fees_query += " AND l.proses_date <= :end_date"
            params['end_date'] = end_date

        # Group by month and year, order by date
        fees_query += """
        GROUP BY DATE_FORMAT(l.proses_date, '%M %Y')
        ORDER BY MIN(l.proses_date)
        """

        # Execute the query
        result = db.execute(text(fees_query), params)
        records = result.fetchall()

        # Convert to dictionary with month_year as key
        monthly_data = {}
        for record in records:
            month_year = record[0]
            # Skip records with NULL month_year
            if month_year is None:
                continue

            total_expected = record[1] if record[1] is not None else 0
            expected_count = record[2] if record[2] is not None else 0
            total_collected = record[3] if record[3] is not None else 0
            collected_count = record[4] if record[4] is not None else 0
            total_failed_payment = record[5] if record[5] is not None else 0

            # Calculate admin_fee_profit: total_collected_admin_fee - total_failed_payment
            admin_fee_profit = total_collected - total_failed_payment

            monthly_data[month_year] = {
                "total_expected_admin_fee": total_expected,
                "expected_loans_count": expected_count,
                "total_collected_admin_fee": total_collected,
                "collected_loans_count": collected_count,
                "total_failed_payment": total_failed_payment,
                "admin_fee_profit": admin_fee_profit
            }


        return monthly_data

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {}


def get_loan_risk_summary(db: Session,
                          employer_filter: str = None, sourced_to_filter: str = None,
                          project_filter: str = None, client_segment_filter: str = None, product_type_filter: str = None, loan_status_filter: int = None,
                          id_karyawan_filter: int = None, start_date: str = None, end_date: str = None) -> dict:
    """Get loan risk summary with various risk metrics"""

    try:
        loan_conditions = LOAN_CONDITIONS
        # Build the query to calculate risk metrics
        risk_query = """
        SELECT
            SUM(CASE WHEN l.loan_status = 4 THEN l.total_loan ELSE 0 END) as total_unrecovered_loan,
            COUNT(CASE WHEN l.loan_status = 4 THEN 1 END) as unrecovered_loan_count,
            SUM(CASE WHEN l.loan_status IN (1, 4) THEN l.total_payment ELSE 0 END) as total_expected_repayment,
            SUM(CASE WHEN l.loan_status = 2 THEN l.total_loan ELSE 0 END) as total_paid,
            SUM(CASE WHEN l.loan_status IN (1, 2, 4) THEN l.total_loan ELSE 0 END) as total_disbursed
        FROM td_loan l
        LEFT JOIN td_karyawan tk
            ON l.id_karyawan = tk.id_karyawan
        LEFT JOIN tbl_gmc emp
            ON tk.valdo_inc = emp.kode_gmc
            AND emp.group_gmc = 'sub_client'
            AND emp.aktif = 'Yes'
            AND emp.keterangan3 = 1
        LEFT JOIN tbl_gmc src
            ON tk.placement = src.kode_gmc
            AND src.group_gmc = 'placement_client'
            AND src.aktif = 'Yes'
            AND src.keterangan3 = 1
        LEFT JOIN tbl_gmc prj
            ON tk.project = prj.kode_gmc
            AND prj.group_gmc = 'client_project'
            AND prj.aktif = 'Yes'
            AND prj.keterangan3 = 1
        WHERE 1=1
        AND {loan_conditions}
        """.format(loan_conditions=loan_conditions)

        # Build parameters dict for filters
        params = {}

        # Add filters
        if id_karyawan_filter:
            risk_query += " AND l.id_karyawan = :id_karyawan"
            params['id_karyawan'] = id_karyawan_filter

        if employer_filter:
            risk_query += " AND emp.keterangan = :employer"
            params['employer'] = employer_filter

        if sourced_to_filter:
            risk_query += " AND src.keterangan = :sourced_to"
            params['sourced_to'] = sourced_to_filter

        if project_filter:
            risk_query += " AND prj.keterangan = :project"
            params['project'] = project_filter

        risk_query = _apply_project_management_filters(risk_query, params, client_segment_filter, product_type_filter, db=db)

        if loan_status_filter is not None:
            risk_query += " AND l.loan_status = :loan_status"
            params['loan_status'] = loan_status_filter

        # Add month and year filters based on proses_date
        if start_date and end_date:
            risk_query = append_date_filters(
                risk_query,
                params,
                start_date=start_date,
                end_date=end_date,
            )

        # Execute the query
        result = db.execute(text(risk_query), params)
        record = result.fetchone()

        # Extract the values (handle None values)
        total_unrecovered_loan = record[0] if record[0] is not None else 0
        unrecovered_loan_count = record[1] if record[1] is not None else 0
        total_expected_repayment = record[2] if record[2] is not None else 0
        total_paid = record[3] if record[3] is not None else 0
        total_disbursed = record[4] if record[4] is not None else 0

        # Calculate loan principal recovery rate
        loan_principal_recovery_rate = 0
        if total_disbursed > 0:
            loan_principal_recovery_rate = total_paid / total_disbursed


        return {
            "total_unrecovered_loan": total_unrecovered_loan,
            "unrecovered_loan_count": unrecovered_loan_count,
            "total_expected_repayment": total_expected_repayment,
            "loan_principal_recovery_rate": loan_principal_recovery_rate
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "total_unrecovered_loan": 0,
            "unrecovered_loan_count": 0,
            "total_expected_repayment": 0,
            "loan_principal_recovery_rate": 0
        }


def get_loan_risk_monthly_summary(db: Session,
                                  employer_filter: str = None, sourced_to_filter: str = None,
                                  project_filter: str = None, client_segment_filter: str = None, product_type_filter: str = None, loan_status_filter: int = None,
                                  id_karyawan_filter: int = None, start_date: str = None,
                                  end_date: str = None) -> dict:
    """Get loan risk summary separated by months within a date range"""

    try:
        loan_conditions = LOAN_CONDITIONS
        # Build the query to calculate risk metrics by month
        risk_query = """
        SELECT
            DATE_FORMAT(l.proses_date, '%M %Y') as month_year,
            SUM(CASE WHEN l.loan_status = 4 THEN l.total_loan ELSE 0 END) as total_unrecovered_loan,
            COUNT(CASE WHEN l.loan_status = 4 THEN 1 END) as unrecovered_loan_count,
            SUM(CASE WHEN l.loan_status IN (1, 4) THEN l.total_payment ELSE 0 END) as total_expected_repayment,
            SUM(CASE WHEN l.loan_status = 2 THEN l.total_loan ELSE 0 END) as total_paid,
            SUM(CASE WHEN l.loan_status IN (1, 2, 4) THEN l.total_loan ELSE 0 END) as total_disbursed
        FROM td_loan l
        LEFT JOIN td_karyawan tk
            ON l.id_karyawan = tk.id_karyawan
        LEFT JOIN tbl_gmc emp
            ON tk.valdo_inc = emp.kode_gmc
            AND emp.group_gmc = 'sub_client'
            AND emp.aktif = 'Yes'
            AND emp.keterangan3 = 1
        LEFT JOIN tbl_gmc src
            ON tk.placement = src.kode_gmc
            AND src.group_gmc = 'placement_client'
            AND src.aktif = 'Yes'
            AND src.keterangan3 = 1
        LEFT JOIN tbl_gmc prj
            ON tk.project = prj.kode_gmc
            AND prj.group_gmc = 'client_project'
            AND prj.aktif = 'Yes'
            AND prj.keterangan3 = 1
        WHERE l.proses_date IS NOT NULL
        AND {loan_conditions}
        """.format(loan_conditions=loan_conditions)

        # Build parameters dict for filters
        params = {}

        # Add filters
        if id_karyawan_filter:
            risk_query += " AND l.id_karyawan = :id_karyawan"
            params['id_karyawan'] = id_karyawan_filter

        if employer_filter:
            risk_query += " AND emp.keterangan = :employer"
            params['employer'] = employer_filter

        if sourced_to_filter:
            risk_query += " AND src.keterangan = :sourced_to"
            params['sourced_to'] = sourced_to_filter

        if project_filter:
            risk_query += " AND prj.keterangan = :project"
            params['project'] = project_filter

        risk_query = _apply_project_management_filters(risk_query, params, client_segment_filter, product_type_filter, db=db)

        if loan_status_filter is not None:
            risk_query += " AND l.loan_status = :loan_status"
            params['loan_status'] = loan_status_filter

        # Add date range filters based on proses_date
        if start_date:
            risk_query += " AND l.proses_date >= :start_date"
            params['start_date'] = start_date

        if end_date:
            risk_query += " AND l.proses_date <= :end_date"
            params['end_date'] = end_date

        # Group by month and year, order by date
        risk_query += """
        GROUP BY DATE_FORMAT(l.proses_date, '%M %Y')
        ORDER BY MIN(l.proses_date)
        """



        # Execute the query
        result = db.execute(text(risk_query), params)
        records = result.fetchall()

        # Convert to dictionary with month_year as key
        monthly_data = {}
        for record in records:
            month_year = record[0]
            # Skip records with NULL month_year
            if month_year is None:
                continue

            total_unrecovered_kasbon = record[1] if record[1] is not None else 0
            unrecovered_kasbon_count = record[2] if record[2] is not None else 0
            total_expected_repayment = record[3] if record[3] is not None else 0
            total_paid = record[4] if record[4] is not None else 0
            total_disbursed = record[5] if record[5] is not None else 0

            # Calculate kasbon principal recovery rate
            kasbon_principal_recovery_rate = 0
            if total_disbursed > 0:
                loan_principal_recovery_rate = total_paid / total_disbursed

            monthly_data[month_year] = {
                "total_unrecovered_kasbon": total_unrecovered_kasbon,
                "unrecovered_kasbon_count": unrecovered_kasbon_count,
                "total_expected_repayment": total_expected_repayment,
                "loan_principal_recovery_rate": loan_principal_recovery_rate
            }



        return monthly_data

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {}


def get_karyawan_overdue_summary(db: Session,
                                 employer_filter: str = None, sourced_to_filter: str = None,
                                 project_filter: str = None, client_segment_filter: str = None, product_type_filter: str = None, loan_status_filter: int = None,
                                 id_karyawan_filter: int = None, start_date: str = None, end_date: str = None, loan_type: str = "loan") -> List[dict]:
    """Get karyawan data for those with overdue loans (status 4)"""

    try:
        if is_all_loan_types(loan_type):
            return _merge_karyawan_overdue_lists([
                get_karyawan_overdue_summary(
                    db,
                    employer_filter=employer_filter,
                    sourced_to_filter=sourced_to_filter,
                    project_filter=project_filter,
                    client_segment_filter=client_segment_filter,
                    product_type_filter=product_type_filter,
                    loan_status_filter=loan_status_filter,
                    id_karyawan_filter=id_karyawan_filter,
                    start_date=start_date,
                    end_date=end_date,
                    loan_type="kasbon",
                ),
                get_karyawan_overdue_summary(
                    db,
                    employer_filter=employer_filter,
                    sourced_to_filter=sourced_to_filter,
                    project_filter=project_filter,
                    client_segment_filter=client_segment_filter,
                    product_type_filter=product_type_filter,
                    loan_status_filter=loan_status_filter,
                    id_karyawan_filter=id_karyawan_filter,
                    start_date=start_date,
                    end_date=end_date,
                    loan_type="installment",
                ),
            ])

        loan_conditions = resolve_loan_conditions(loan_type, db)

        # For kasbon and default, use td_loan table directly (like the old "loan" type)
        # For extradana, aku_cicil, and combined installment types, use td_loan_history table
        if loan_type not in ("extradana", "aku_cicil", "installment"):
            # Use td_loan table directly for kasbon
            # Netting out partial payments already recorded against a lump-sum loan
            # (duration = 1): a loan can be status = 4 (overdue) while still having
            # a partial amount paid via td_loan_payment / td_loan_payment_allocation.
            # total_payment is the remaining pokok+bunga still owed (monthly - paid);
            # total_amount_owed (pokok) and total_admin_fee (bunga) are that same
            # remainder split proportionally, so owed + admin_fee == total_payment.
            # Mirrors the netting done in _UNRECOVERED_LUMP_PAYMENT_SQL.
            _lump_paid_subquery = """(
                SELECT COALESCE(SUM(amt), 0) FROM (
                    SELECT p.amount amt FROM td_loan_payment p
                    WHERE p.loan_id = l.id AND p.status = 1 AND p.loan_history_id IS NULL
                      AND NOT EXISTS (SELECT 1 FROM td_loan_payment_allocation a WHERE a.payment_id = p.id)
                    UNION ALL
                    SELECT a.amount FROM td_loan_payment_allocation a
                    INNER JOIN td_loan_payment p ON p.id = a.payment_id
                    WHERE p.loan_id = l.id AND p.status = 1 AND a.loan_history_id IS NULL
                ) t
            )"""
            _lump_remaining_payment = f"GREATEST(l.total_payment - {_lump_paid_subquery}, 0)"

            overdue_query = """
            SELECT DISTINCT
                tk.id_karyawan,
                tk.ktp AS ktp,
                tk.nama AS name,
                emp.keterangan AS company,
                src.keterangan AS sourced_to,
                prj.keterangan AS project,
                ROUND(SUM(CASE WHEN l.total_payment > 0
                    THEN l.total_loan * {remaining_payment} / l.total_payment
                    ELSE 0 END), 0) as total_amount_owed,
                MAX(l.repayment_date) as repayment_date,
                ROUND(SUM(CASE WHEN l.total_payment > 0
                    THEN l.admin_fee * {remaining_payment} / l.total_payment
                    ELSE 0 END), 0) as total_admin_fee,
                SUM({remaining_payment}) as total_payment
            FROM td_loan l""".format(remaining_payment=_lump_remaining_payment) + """
            LEFT JOIN td_karyawan tk
                ON l.id_karyawan = tk.id_karyawan
            LEFT JOIN tbl_gmc emp
                ON tk.valdo_inc = emp.kode_gmc
                AND emp.group_gmc = 'sub_client'
                AND emp.aktif = 'Yes'
                AND emp.keterangan3 = 1
            LEFT JOIN tbl_gmc src
                ON tk.placement = src.kode_gmc
                AND src.group_gmc = 'placement_client'
                AND src.aktif = 'Yes'
                AND src.keterangan3 = 1
            LEFT JOIN tbl_gmc prj
                ON tk.project = prj.kode_gmc
                AND prj.group_gmc = 'client_project'
                AND prj.aktif = 'Yes'
                AND prj.keterangan3 = 1
            WHERE l.loan_status = 4
            AND l.id_karyawan IS NOT NULL
            AND {loan_conditions}
            """.format(loan_conditions=loan_conditions)
        else:
            # For extradana and aku_cicil, use td_loan_history table
            # Adapt loan_conditions for td_loan_history context by replacing l. with tl.
            loan_conditions_tl = loan_conditions.replace('l.', 'tl.')

            # Netting out partial payments already recorded against an installment
            # (duration > 1): a given month's td_loan_history row can be status = 4
            # (overdue) while still having a partial amount paid via td_loan_payment /
            # td_loan_payment_allocation for that specific installment.
            # total_payment is the remaining pokok+bunga still owed (monthly - paid);
            # total_amount_owed (pokok) and total_admin_fee (bunga) are that same
            # remainder split proportionally, so owed + admin_fee == total_payment.
            # Mirrors the netting done in _UNRECOVERED_INSTALLMENT_PAYMENT_SQL.
            _installment_paid_subquery = """(
                SELECT COALESCE(SUM(amt), 0) FROM (
                    SELECT p.amount amt FROM td_loan_payment p
                    WHERE p.loan_id = tl.id AND p.status = 1 AND p.loan_history_id = tlh.id
                      AND NOT EXISTS (SELECT 1 FROM td_loan_payment_allocation a WHERE a.payment_id = p.id)
                    UNION ALL
                    SELECT a.amount FROM td_loan_payment_allocation a
                    INNER JOIN td_loan_payment p ON p.id = a.payment_id
                    WHERE p.loan_id = tl.id AND p.status = 1 AND a.loan_history_id = tlh.id
                ) t
            )"""
            _installment_remaining_payment = f"GREATEST(tlh.monthly - {_installment_paid_subquery}, 0)"

            overdue_query = """
            SELECT DISTINCT
                tk.id_karyawan,
                tk.ktp AS ktp,
                tk.nama AS name,
                emp.keterangan AS company,
                src.keterangan AS sourced_to,
                prj.keterangan AS project,
                ROUND(SUM(CASE WHEN tlh.monthly > 0
                    THEN ROUND(tl.total_loan / tl.duration, 0) * {remaining_payment} / tlh.monthly
                    ELSE 0 END), 0) as total_amount_owed,
                MAX(tlh.due_date) as repayment_date,
                ROUND(SUM(CASE WHEN tlh.monthly > 0
                    THEN ROUND(tl.admin_fee / tl.duration, 0) * {remaining_payment} / tlh.monthly
                    ELSE 0 END), 0) as total_admin_fee,
                SUM({remaining_payment}) as total_payment
            FROM td_loan_history tlh""".format(remaining_payment=_installment_remaining_payment) + """
            LEFT JOIN td_loan tl ON tlh.loan_form_id = tl.id
            LEFT JOIN td_karyawan tk ON tl.id_karyawan = tk.id_karyawan
            LEFT JOIN tbl_gmc emp
                ON tk.valdo_inc = emp.kode_gmc
                AND emp.group_gmc = 'sub_client'
                AND emp.aktif = 'Yes'
                AND emp.keterangan3 = 1
            LEFT JOIN tbl_gmc src
                ON tk.placement = src.kode_gmc
                AND src.group_gmc = 'placement_client'
                AND src.aktif = 'Yes'
                AND src.keterangan3 = 1
            LEFT JOIN tbl_gmc prj
                ON tk.project = prj.kode_gmc
                AND prj.group_gmc = 'client_project'
                AND prj.aktif = 'Yes'
                AND prj.keterangan3 = 1
            WHERE tlh.due_date IS NOT NULL
            AND tlh.status = 4
            AND tl.id_karyawan IS NOT NULL
            AND {loan_conditions_tl}
            """.format(loan_conditions_tl=loan_conditions_tl)

        # Build parameters dict for filters
        params = {}

        # Determine if using td_loan (kasbon/default) or td_loan_history (extradana/aku_cicil/installment)
        use_td_loan = loan_type not in ("extradana", "aku_cicil", "installment")

        # Add filters
        if id_karyawan_filter:
            if use_td_loan:
                overdue_query += " AND l.id_karyawan = :id_karyawan"
            else:
                overdue_query += " AND tl.id_karyawan = :id_karyawan"
            params['id_karyawan'] = id_karyawan_filter

        # Restrict to only PT Valdo companies
        company_filter = COMPANY_FILTER
        overdue_query += f" AND emp.keterangan IN {company_filter}"

        # If employer_filter is provided and it's one of the allowed companies, filter further
        if employer_filter and employer_filter in ALLOWED_COMPANIES:
            overdue_query += " AND emp.keterangan = :employer"
            params['employer'] = employer_filter

        if sourced_to_filter:
            overdue_query += " AND src.keterangan = :sourced_to"
            params['sourced_to'] = sourced_to_filter

        if project_filter:
            overdue_query += " AND prj.keterangan = :project"
            params['project'] = project_filter

        overdue_query = _apply_project_management_filters(overdue_query, params, client_segment_filter, product_type_filter, db=db)

        if loan_status_filter is not None:
            if use_td_loan:
                overdue_query += " AND l.loan_status = :loan_status"
            else:
                overdue_query += " AND tlh.status = :loan_status"
            params['loan_status'] = loan_status_filter

        if start_date and end_date:
            if use_td_loan:
                overdue_query += " AND l.repayment_date >= :start_date"
                overdue_query += " AND l.repayment_date <= :end_date"
            else:
                overdue_query += " AND tlh.due_date >= :start_date"
                overdue_query += " AND tlh.due_date <= :end_date"
            params["start_date"] = start_date
            params["end_date"] = end_date

        overdue_query += """
        GROUP BY tk.id_karyawan, tk.nama, tk.ktp, emp.keterangan, src.keterangan, prj.keterangan
        ORDER BY total_amount_owed DESC
        """

        result = db.execute(text(overdue_query), params)
        records = result.fetchall()

        overdue_list = []
        for record in records:
            if record[0] is None:
                continue

            days_overdue = 0
            if record[7] is not None:
                from datetime import datetime, date
                try:
                    repayment_date = record[7]
                    if isinstance(repayment_date, str):
                        repayment_date = datetime.strptime(repayment_date, '%Y-%m-%d').date()
                    elif hasattr(repayment_date, 'date'):
                        repayment_date = repayment_date.date()
                    today = date.today()
                    days_overdue = (today - repayment_date).days
                except Exception:
                    days_overdue = 0

            overdue_list.append({
                "id_karyawan": record[0],
                "ktp": record[1],
                "name": record[2],
                "company": record[3],
                "sourced_to": record[4],
                "project": record[5],
                "total_amount_owed": record[6] if record[6] is not None else 0,
                "repayment_date": str(record[7]) if record[7] else None,
                "days_overdue": days_overdue,
                "admin_fee": record[8] if record[8] is not None else 0,
                "total_payment": record[9] if record[9] is not None else 0
            })

        return overdue_list

    except Exception as e:
        import traceback
        traceback.print_exc()
        return []


def get_loan_purpose_summary(db: Session,
                            employer_filter: str = None, sourced_to_filter: str = None,
                            project_filter: str = None, client_segment_filter: str = None, product_type_filter: str = None, loan_status_filter: int = None,
                            id_karyawan_filter: int = None, start_date: str = None,
                            end_date: str = None, loan_type: str = "loan") -> List[dict]:
    """Get loan summary grouped by purpose with filters"""

    try:
        if loan_type == "loan":
            loan_conditions = LOAN_CONDITIONS
        elif loan_type == "extradana":
            loan_conditions = EXTRADANA_LOAN_CONDITIONS
        else:
            loan_conditions = LOAN_CONDITIONS

        purpose_query = """
        SELECT
            lp.id as purpose_id,
            lp.purpose as purpose_name,
            COUNT(l.id) as total_count,
            SUM(l.total_loan) as total_amount
        FROM td_loan l
        LEFT JOIN loan_purpose lp
            ON l.purpose = lp.id
        LEFT JOIN td_karyawan tk
            ON l.id_karyawan = tk.id_karyawan
        LEFT JOIN tbl_gmc emp
            ON tk.valdo_inc = emp.kode_gmc
            AND emp.group_gmc = 'sub_client'
            AND emp.aktif = 'Yes'
            AND emp.keterangan3 = 1
        LEFT JOIN tbl_gmc src
            ON tk.placement = src.kode_gmc
            AND src.group_gmc = 'placement_client'
            AND src.aktif = 'Yes'
            AND src.keterangan3 = 1
        LEFT JOIN tbl_gmc prj
            ON tk.project = prj.kode_gmc
            AND prj.group_gmc = 'client_project'
            AND prj.aktif = 'Yes'
            AND prj.keterangan3 = 1
        WHERE 1=1
        AND {loan_conditions}
        """.format(loan_conditions=loan_conditions)

        params = {}

        if id_karyawan_filter:
            purpose_query += " AND l.id_karyawan = :id_karyawan"
            params['id_karyawan'] = id_karyawan_filter

        if employer_filter:
            purpose_query += " AND emp.keterangan = :employer"
            params['employer'] = employer_filter

        if sourced_to_filter:
            purpose_query += " AND src.keterangan = :sourced_to"
            params['sourced_to'] = sourced_to_filter

        if project_filter:
            purpose_query += " AND prj.keterangan = :project"
            params['project'] = project_filter

        purpose_query = _apply_project_management_filters(purpose_query, params, client_segment_filter, product_type_filter, db=db)

        if loan_status_filter is not None:
            purpose_query += " AND l.loan_status = :loan_status"
            params['loan_status'] = loan_status_filter

        purpose_query = append_date_filters(
            purpose_query,
            params,
            start_date=start_date,
            end_date=end_date,
        )

        purpose_query += """
        GROUP BY lp.id, lp.purpose
        ORDER BY total_amount DESC
        """

        result = db.execute(text(purpose_query), params)
        records = result.fetchall()

        purpose_list = []
        for record in records:
            purpose_list.append({
                "purpose_id": record[0],
                "purpose_name": record[1] if record[1] else "Unknown Purpose",
                "total_count": record[2] if record[2] is not None else 0,
                "total_amount": record[3] if record[3] is not None else 0
            })

        return purpose_list

    except Exception as e:
        import traceback
        traceback.print_exc()
        return []


# reject_reason column comment on td_loan: 1:End of Contract;2:High Risk;3:Bad Attitude;4:CRO Instruction;5:Fake Request
REJECT_REASON_LABELS = {
    1: "End of Contract",
    2: "High Risk",
    3: "Bad Attitude",
    4: "CRO Instruction",
    5: "Fake Request",
}

# td_karyawan.gender has no lookup table; values observed in data are '1' (Male) / '2' (Female)
GENDER_LABELS = {
    "1": "Male",
    "2": "Female",
}

_AGE_RANGE_CASE_SQL = """
        CASE
            WHEN tk.tgl_lahir IS NULL THEN 'Unknown'
            WHEN TIMESTAMPDIFF(YEAR, tk.tgl_lahir, CURDATE()) < 18 THEN 'Unknown'
            WHEN TIMESTAMPDIFF(YEAR, tk.tgl_lahir, CURDATE()) BETWEEN 18 AND 25 THEN '18-25'
            WHEN TIMESTAMPDIFF(YEAR, tk.tgl_lahir, CURDATE()) BETWEEN 26 AND 35 THEN '26-35'
            WHEN TIMESTAMPDIFF(YEAR, tk.tgl_lahir, CURDATE()) BETWEEN 36 AND 45 THEN '36-45'
            WHEN TIMESTAMPDIFF(YEAR, tk.tgl_lahir, CURDATE()) BETWEEN 46 AND 55 THEN '46-55'
            WHEN TIMESTAMPDIFF(YEAR, tk.tgl_lahir, CURDATE()) BETWEEN 56 AND 65 THEN '56-65'
            ELSE 'Unknown'
        END"""

_AGE_RANGE_SORT_CASE_SQL = """
        CASE age_range
            WHEN '18-25' THEN 1
            WHEN '26-35' THEN 2
            WHEN '36-45' THEN 3
            WHEN '46-55' THEN 4
            WHEN '56-65' THEN 5
            ELSE 6
        END"""


def get_loan_applicant_insights(db: Session,
                               employer_filter: str = None, sourced_to_filter: str = None,
                               project_filter: str = None, client_segment_filter: str = None,
                               product_type_filter: str = None, loan_status_filter: int = None,
                               id_karyawan_filter: int = None, start_date: str = None,
                               end_date: str = None, loan_type: str = "loan") -> dict:
    """Get combined loan applicant insights: top reject reasons, applicants by gender, applicants by age range"""

    try:
        loan_conditions = resolve_loan_conditions(loan_type, db)
        params = {}

        def apply_common_filters(query: str) -> str:
            if id_karyawan_filter:
                query += " AND l.id_karyawan = :id_karyawan"
                params['id_karyawan'] = id_karyawan_filter
            if employer_filter:
                query += " AND emp.keterangan = :employer"
                params['employer'] = employer_filter
            if sourced_to_filter:
                query += " AND src.keterangan = :sourced_to"
                params['sourced_to'] = sourced_to_filter
            if project_filter:
                query += " AND prj.keterangan = :project"
                params['project'] = project_filter
            return query

        # Top reject reasons is always scoped to rejected loans (loan_status = 3),
        # regardless of the loan_status_filter param, since it's not a meaningful metric otherwise.
        reject_query = f"""
        SELECT
            l.reject_reason AS reject_reason_id,
            COUNT(l.id) AS total_count
        FROM td_loan l
        {_LOAN_GMC_JOINS}
        WHERE {loan_conditions}
        AND l.loan_status = 3
        """
        reject_query = apply_common_filters(reject_query)
        reject_query = _apply_project_management_filters(
            reject_query, params, client_segment_filter, product_type_filter, db=db
        )
        reject_query = append_date_filters(reject_query, params, start_date=start_date, end_date=end_date)
        reject_query += " GROUP BY l.reject_reason ORDER BY total_count DESC"

        gender_query = f"""
        SELECT
            tk.gender AS gender_code,
            COUNT(l.id) AS total_count
        FROM td_loan l
        {_LOAN_GMC_JOINS}
        WHERE {loan_conditions}
        AND l.loan_status IN (1, 2, 4)
        """
        gender_query = apply_common_filters(gender_query)
        if loan_status_filter is not None:
            gender_query += " AND l.loan_status = :loan_status"
            params['loan_status'] = loan_status_filter
        gender_query = _apply_project_management_filters(
            gender_query, params, client_segment_filter, product_type_filter, db=db
        )
        gender_query = append_date_filters(gender_query, params, start_date=start_date, end_date=end_date)
        gender_query += " GROUP BY tk.gender ORDER BY total_count DESC"

        age_query = f"""
        SELECT
            {_AGE_RANGE_CASE_SQL} AS age_range,
            COUNT(l.id) AS total_count
        FROM td_loan l
        {_LOAN_GMC_JOINS}
        WHERE {loan_conditions}
        AND l.loan_status IN (1, 2, 4)
        """
        age_query = apply_common_filters(age_query)
        if loan_status_filter is not None:
            age_query += " AND l.loan_status = :loan_status"
            params['loan_status'] = loan_status_filter
        age_query = _apply_project_management_filters(
            age_query, params, client_segment_filter, product_type_filter, db=db
        )
        age_query = append_date_filters(age_query, params, start_date=start_date, end_date=end_date)
        age_query += f" GROUP BY age_range ORDER BY {_AGE_RANGE_SORT_CASE_SQL}"

        reject_rows = db.execute(text(reject_query), params).fetchall()
        gender_rows = db.execute(text(gender_query), params).fetchall()
        age_rows = db.execute(text(age_query), params).fetchall()

        top_reject_reasons = [
            {
                "reject_reason_id": row[0],
                "reject_reason_name": REJECT_REASON_LABELS.get(row[0], "Not Specified"),
                "total_count": row[1] if row[1] is not None else 0,
            }
            for row in reject_rows
        ]

        applicants_by_gender = [
            {
                "gender_code": row[0],
                "gender_name": GENDER_LABELS.get(row[0], "Unknown"),
                "total_count": row[1] if row[1] is not None else 0,
            }
            for row in gender_rows
        ]

        applicants_by_age_range = [
            {
                "age_range": row[0],
                "total_count": row[1] if row[1] is not None else 0,
            }
            for row in age_rows
        ]

        return {
            "top_reject_reasons": top_reject_reasons,
            "applicants_by_gender": applicants_by_gender,
            "applicants_by_age_range": applicants_by_age_range,
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "top_reject_reasons": [],
            "applicants_by_gender": [],
            "applicants_by_age_range": [],
        }


def get_total_admin_fee_collected(db: Session,
                                 employer_filter: str = None, sourced_to_filter: str = None,
                                 project_filter: str = None, client_segment_filter: str = None, product_type_filter: str = None, loan_status_filter: int = None,
                                 id_karyawan_filter: int = None, start_date: str = None, end_date: str = None, loan_type: str = "loan") -> float:
    """Get total admin fee collected amount based on loan type"""

    try:
        if loan_type == "loan":
            loan_conditions = LOAN_CONDITIONS
        elif loan_type == "extradana":
            loan_conditions = EXTRADANA_LOAN_CONDITIONS
        elif loan_type == "aku_cicil":
            loan_conditions = AKU_CICIL_CONDITION
        else:
            loan_conditions = LOAN_CONDITIONS

        params = {}

        if loan_type == "loan":
            admin_fee_collected_query = """
            SELECT SUM(CASE WHEN l.loan_status = 2 THEN l.admin_fee ELSE 0 END) as total_admin_fee_collected
            FROM td_loan l
            LEFT JOIN td_karyawan tk
                ON l.id_karyawan = tk.id_karyawan
            LEFT JOIN tbl_gmc emp
                ON tk.valdo_inc = emp.kode_gmc
                AND emp.group_gmc = 'sub_client'
                AND emp.aktif = 'Yes'
                AND emp.keterangan3 = 1
            LEFT JOIN tbl_gmc src
                ON tk.placement = src.kode_gmc
                AND src.group_gmc = 'placement_client'
                AND src.aktif = 'Yes'
                AND src.keterangan3 = 1
            LEFT JOIN tbl_gmc prj
                ON tk.project = prj.kode_gmc
                AND prj.group_gmc = 'client_project'
                AND prj.aktif = 'Yes'
                AND prj.keterangan3 = 1
            WHERE {loan_conditions}
            """.format(loan_conditions=loan_conditions)

        else:
            if loan_type == "extradana":
                loan_conditions_tl = "l.loan_id IN (SELECT ls.id FROM loan_setting ls WHERE ls.loan_type LIKE 'Extradana%')"
            else:
                loan_conditions_tl = loan_conditions

            admin_fee_collected_query = """
            SELECT SUM(ROUND(l.admin_fee / l.duration, 0)) as total_admin_fee_collected
            FROM td_loan_history tlh
            LEFT JOIN td_loan l ON tlh.loan_form_id = l.id
            LEFT JOIN td_karyawan tk ON l.id_karyawan = tk.id_karyawan
            LEFT JOIN tbl_gmc emp
                ON tk.valdo_inc = emp.kode_gmc
                AND emp.group_gmc = 'sub_client'
                AND emp.aktif = 'Yes'
                AND emp.keterangan3 = 1
            LEFT JOIN tbl_gmc src
                ON tk.placement = src.kode_gmc
                AND src.group_gmc = 'placement_client'
                AND src.aktif = 'Yes'
                AND src.keterangan3 = 1
            LEFT JOIN tbl_gmc prj
                ON tk.project = prj.kode_gmc
                AND prj.group_gmc = 'client_project'
                AND prj.aktif = 'Yes'
                AND prj.keterangan3 = 1
            WHERE tlh.due_date IS NOT NULL
            AND l.loan_status IN (1, 2, 4)
            AND {loan_conditions_tl}
            """.format(loan_conditions_tl=loan_conditions_tl)

        if id_karyawan_filter:
            admin_fee_collected_query += " AND l.id_karyawan = :id_karyawan"
            params['id_karyawan'] = id_karyawan_filter

        company_filter = COMPANY_FILTER
        admin_fee_collected_query += f" AND emp.keterangan IN {company_filter}"

        if employer_filter and employer_filter in ALLOWED_COMPANIES:
            admin_fee_collected_query += " AND emp.keterangan = :employer"
            params['employer'] = employer_filter

        if sourced_to_filter:
            admin_fee_collected_query += " AND src.keterangan = :sourced_to"
            params['sourced_to'] = sourced_to_filter

        if project_filter:
            admin_fee_collected_query += " AND prj.keterangan = :project"
            params['project'] = project_filter

        admin_fee_collected_query = _apply_project_management_filters(admin_fee_collected_query, params, client_segment_filter, product_type_filter, db=db)

        if loan_status_filter is not None:
            admin_fee_collected_query += " AND l.loan_status = :loan_status"
            params['loan_status'] = loan_status_filter

        if start_date and end_date:
            if loan_type in ("extradana", "aku_cicil"):
                admin_fee_collected_query += " AND tlh.due_date >= :start_date AND tlh.due_date <= :end_date"
            else:
                admin_fee_collected_query += " AND l.proses_date >= :start_date AND l.proses_date <= :end_date"
            params["start_date"] = start_date
            params["end_date"] = end_date

        result = db.execute(text(admin_fee_collected_query), params)
        record = result.fetchone()
        return record[0] if record[0] is not None else 0

    except Exception as e:
        import traceback
        traceback.print_exc()
        return 0


def get_total_loan_principal_collected(db: Session,
                                        employer_filter: str = None, sourced_to_filter: str = None,
                                        project_filter: str = None, client_segment_filter: str = None, product_type_filter: str = None, loan_status_filter: int = None,
                                        id_karyawan_filter: int = None, start_date: str = None, end_date: str = None, loan_type: str = "loan") -> float:
    """Get total loan principal collected amount based on loan type"""

    try:
        # Determine loan conditions based on loan type
        if loan_type == "loan":
            loan_conditions = LOAN_CONDITIONS
        elif loan_type == "extradana":
            loan_conditions = EXTRADANA_LOAN_CONDITIONS
        elif loan_type == "aku_cicil":
            loan_conditions = AKU_CICIL_CONDITION
        else:
            loan_conditions = LOAN_CONDITIONS  # default to loan

        # Build parameters dict for filters
        params = {}

        if loan_type == "loan":
            # For loan, use the existing logic from td_loan table
            principal_collected_query = """
            SELECT SUM(CASE WHEN l.loan_status = 2 THEN l.total_loan ELSE 0 END) as total_loan_principal_collected
            FROM td_loan l
            LEFT JOIN td_karyawan tk
                ON l.id_karyawan = tk.id_karyawan
            LEFT JOIN tbl_gmc emp
                ON tk.valdo_inc = emp.kode_gmc
                AND emp.group_gmc = 'sub_client'
                AND emp.aktif = 'Yes'
                AND emp.keterangan3 = 1
            LEFT JOIN tbl_gmc src
                ON tk.placement = src.kode_gmc
                AND src.group_gmc = 'placement_client'
                AND src.aktif = 'Yes'
                AND src.keterangan3 = 1
            LEFT JOIN tbl_gmc prj
                ON tk.project = prj.kode_gmc
                AND prj.group_gmc = 'client_project'
                AND prj.aktif = 'Yes'
                AND prj.keterangan3 = 1
            WHERE {loan_conditions}
            """.format(loan_conditions=loan_conditions)

        else:  # extradana or aku_cicil
            # For extradana and aku_cicil, use td_loan_history table with monthly principal calculation
            # Note: td_loan is aliased as 'l' in this query, so loan_conditions with 'l.' prefix work correctly
            # Special handling for extradana - use loan_setting table
            if loan_type == "extradana":
                loan_conditions_tl = "l.loan_id IN (SELECT ls.id FROM loan_setting ls WHERE ls.loan_type LIKE 'Extradana%')"
            else:
                # For aku_cicil, loan_conditions already use 'l.' prefix which matches the alias
                loan_conditions_tl = loan_conditions

            principal_collected_query = """
            SELECT SUM(ROUND(l.total_loan / l.duration, 0)) as total_loan_principal_collected
            FROM td_loan_history tlh
            LEFT JOIN td_loan l ON tlh.loan_form_id = l.id
            LEFT JOIN td_karyawan tk ON l.id_karyawan = tk.id_karyawan
            LEFT JOIN tbl_gmc emp
                ON tk.valdo_inc = emp.kode_gmc
                AND emp.group_gmc = 'sub_client'
                AND emp.aktif = 'Yes'
                AND emp.keterangan3 = 1
            LEFT JOIN tbl_gmc src
                ON tk.placement = src.kode_gmc
                AND src.group_gmc = 'placement_client'
                AND src.aktif = 'Yes'
                AND src.keterangan3 = 1
            LEFT JOIN tbl_gmc prj
                ON tk.project = prj.kode_gmc
                AND prj.group_gmc = 'client_project'
                AND prj.aktif = 'Yes'
                AND prj.keterangan3 = 1
            WHERE tlh.due_date IS NOT NULL
            AND l.loan_status IN (1, 2, 4)
            AND {loan_conditions_tl}
            """.format(loan_conditions_tl=loan_conditions_tl)

        # Add filters
        if id_karyawan_filter:
            principal_collected_query += " AND l.id_karyawan = :id_karyawan"
            params['id_karyawan'] = id_karyawan_filter

        # Restrict to only PT Valdo companies (conditional based on loan type)
        company_filter = COMPANY_FILTER

        principal_collected_query += f" AND emp.keterangan IN {company_filter}"

        # If employer_filter is provided and it's one of the allowed companies, filter further
        if employer_filter and employer_filter in ALLOWED_COMPANIES:
            principal_collected_query += " AND emp.keterangan = :employer"
            params['employer'] = employer_filter

        if sourced_to_filter:
            principal_collected_query += " AND src.keterangan = :sourced_to"
            params['sourced_to'] = sourced_to_filter

        if project_filter:
            principal_collected_query += " AND prj.keterangan = :project"
            params['project'] = project_filter

        principal_collected_query = _apply_project_management_filters(principal_collected_query, params, client_segment_filter, product_type_filter, db=db)

        if loan_status_filter is not None:
            principal_collected_query += " AND l.loan_status = :loan_status"
            params['loan_status'] = loan_status_filter

        # Add date filters based on due_date for extradana, proses_date for loan
        if start_date and end_date:
            if loan_type in ("extradana", "aku_cicil"):
                principal_collected_query += " AND tlh.due_date >= :start_date AND tlh.due_date <= :end_date"
            else:
                principal_collected_query += " AND l.proses_date >= :start_date AND l.proses_date <= :end_date"
            params["start_date"] = start_date
            params["end_date"] = end_date

        result = db.execute(text(principal_collected_query), params)
        record = result.fetchone()

        # Extract the value (handle None values)
        total_loan_principal_collected = record[0] if record[0] is not None else 0

        return total_loan_principal_collected

    except Exception as e:
        import traceback
        traceback.print_exc()
        return 0


def get_expected_repayment(db: Session,
                          employer_filter: str = None, sourced_to_filter: str = None,
                          project_filter: str = None, client_segment_filter: str = None, product_type_filter: str = None, loan_status_filter: int = None,
                          id_karyawan_filter: int = None, start_date: str = None, end_date: str = None, loan_type: str = "loan") -> float:
    """Get expected repayment amount based on loan type"""

    try:
        params = {}

        if loan_type in ("extradana", "aku_cicil"):
            # For extradana and aku_cicil, use td_loan_history table with due_date and monthly sum
            if loan_type == "extradana":
                loan_conditions_tl = "l.loan_id IN (SELECT ls.id FROM loan_setting ls WHERE ls.loan_type LIKE 'Extradana%')"
            else:
                loan_conditions_tl = AKU_CICIL_CONDITION

            expected_repayment_query = """
            SELECT SUM(tlh.monthly) as total_expected_repayment
            FROM td_loan_history tlh
            LEFT JOIN td_loan l ON tlh.loan_form_id = l.id
            LEFT JOIN td_karyawan tk ON l.id_karyawan = tk.id_karyawan
            LEFT JOIN tbl_gmc emp
                ON tk.valdo_inc = emp.kode_gmc
                AND emp.group_gmc = 'sub_client'
                AND emp.aktif = 'Yes'
                AND emp.keterangan3 = 1
            LEFT JOIN tbl_gmc src
                ON tk.placement = src.kode_gmc
                AND src.group_gmc = 'placement_client'
                AND src.aktif = 'Yes'
                AND src.keterangan3 = 1
            LEFT JOIN tbl_gmc prj
                ON tk.project = prj.kode_gmc
                AND prj.group_gmc = 'client_project'
                AND prj.aktif = 'Yes'
                AND prj.keterangan3 = 1
            WHERE tlh.due_date IS NOT NULL
            AND l.loan_status IN (1, 2, 4)
            AND {loan_conditions_tl}
            """.format(loan_conditions_tl=loan_conditions_tl)
        else:
            expected_repayment_query = """
            SELECT SUM(l.total_payment) as total_expected_repayment
            FROM td_loan l
            LEFT JOIN td_karyawan tk
                ON l.id_karyawan = tk.id_karyawan
            LEFT JOIN tbl_gmc emp
                ON tk.valdo_inc = emp.kode_gmc
                AND emp.group_gmc = 'sub_client'
                AND emp.aktif = 'Yes'
                AND emp.keterangan3 = 1
            LEFT JOIN tbl_gmc src
                ON tk.placement = src.kode_gmc
                AND src.group_gmc = 'placement_client'
                AND src.aktif = 'Yes'
                AND src.keterangan3 = 1
            LEFT JOIN tbl_gmc prj
                ON tk.project = prj.kode_gmc
                AND prj.group_gmc = 'client_project'
                AND prj.aktif = 'Yes'
                AND prj.keterangan3 = 1
            WHERE l.loan_status IN (1, 2, 4)
            """

        # Add filters
        if id_karyawan_filter:
            expected_repayment_query += " AND l.id_karyawan = :id_karyawan"
            params['id_karyawan'] = id_karyawan_filter

        company_filter = COMPANY_FILTER

        expected_repayment_query += f" AND emp.keterangan IN {company_filter}"

        if employer_filter and employer_filter in ALLOWED_COMPANIES:
            expected_repayment_query += " AND emp.keterangan = :employer"
            params['employer'] = employer_filter

        if sourced_to_filter:
            expected_repayment_query += " AND src.keterangan = :sourced_to"
            params['sourced_to'] = sourced_to_filter

        if project_filter:
            expected_repayment_query += " AND prj.keterangan = :project"
            params['project'] = project_filter

        expected_repayment_query = _apply_project_management_filters(expected_repayment_query, params, client_segment_filter, product_type_filter, db=db)

        if loan_status_filter is not None:
            expected_repayment_query += " AND l.loan_status = :loan_status"
            params['loan_status'] = loan_status_filter

        if start_date and end_date:
            if loan_type in ("extradana", "aku_cicil"):
                expected_repayment_query = append_date_filters(
                    expected_repayment_query,
                    params,
                    start_date=start_date,
                    end_date=end_date,
                    date_column="tlh.due_date",
                )
            else:
                expected_repayment_query = append_date_filters(
                    expected_repayment_query,
                    params,
                    start_date=start_date,
                    end_date=end_date,
                    date_column="l.proses_date",
                )

        # Execute the query
        result = db.execute(text(expected_repayment_query), params)
        record = result.fetchone()

        # Extract the value (handle None values)
        total_expected_repayment = record[0] if record[0] is not None else 0

        return total_expected_repayment

    except Exception as e:
        import traceback
        traceback.print_exc()
        return 0


def get_repayment_risk_summary(db: Session,
                               employer_filter: str = None, sourced_to_filter: str = None,
                               project_filter: str = None, client_segment_filter: str = None, product_type_filter: str = None, loan_status_filter: int = None,
                               id_karyawan_filter: int = None, start_date: str = None, end_date: str = None, loan_type: str = "loan") -> dict:
    """Get repayment risk summary with various repayment and risk metrics.

    Three independent attribution models are in play here, and they must not be conflated:

    - total_expected_repayment is matched to ak-mj's /loan/monthly_performance figure:
      SUM(td_loan.total_payment), bucketed by disbursement month (l.proses_date), for
      loan_status IN (1,2,4). See get_total_expected_repayment's docstring — this is
      computed independently of the other fields below, including for loan_type=all,
      which applies no product predicate at all (matching ak-mj, which never splits this
      figure by product).
    - total_collected_repayment / total_unrecovered_repayment / total_outstanding_repayment
      (and their rates) are due-date based and have nothing to do with bad debt: a
      repayment is filtered/bucketed by its raw due date (l.repayment_date for kasbon,
      tlh.due_date for extradana/aku_cicil) regardless of how late it was eventually paid.
    - total_loan_principal_collected/total_admin_fee_collected and their unrecovered/
      expected counterparts (the principal-repayment and admin-fee-repayment metrics), plus
      the delinquency/admin_fee_profit "performance" metrics derived from them, count toward
      exactly one reporting period: their payment date if paid on/before the due date or
      once they've crossed into Bad Debt Recovery (see _REPORTING_DATE_LUMP/_INSTALLMENT),
      otherwise their original due date. When start_date/end_date are given, these fields
      filter on that reporting date, not the raw due date, so a Bad-Debt-recovered
      repayment is captured by the date range covering its payment month, not its due month.
    """

    try:
        total_expected_repayment = get_total_expected_repayment(
            db,
            employer_filter=employer_filter,
            sourced_to_filter=sourced_to_filter,
            project_filter=project_filter,
            client_segment_filter=client_segment_filter,
            product_type_filter=product_type_filter,
            loan_status_filter=loan_status_filter,
            id_karyawan_filter=id_karyawan_filter,
            start_date=start_date,
            end_date=end_date,
            loan_type=loan_type,
        )

        if is_all_loan_types(loan_type):
            summaries = [
                get_repayment_risk_summary(
                    db,
                    employer_filter=employer_filter,
                    sourced_to_filter=sourced_to_filter,
                    project_filter=project_filter,
                    client_segment_filter=client_segment_filter,
                    product_type_filter=product_type_filter,
                    loan_status_filter=loan_status_filter,
                    id_karyawan_filter=id_karyawan_filter,
                    start_date=start_date,
                    end_date=end_date,
                    loan_type=product_type,
                )
                for product_type in ALL_LOAN_TYPES
            ]
            combined = _merge_repayment_risk_summaries(summaries)
            combined["total_expected_repayment"] = total_expected_repayment
            combined["total_unrecovered_repayment"] = get_total_unrecovered_repayment(
                db,
                employer_filter=employer_filter,
                sourced_to_filter=sourced_to_filter,
                project_filter=project_filter,
                client_segment_filter=client_segment_filter,
                product_type_filter=product_type_filter,
                loan_status_filter=loan_status_filter,
                id_karyawan_filter=id_karyawan_filter,
                start_date=start_date,
                end_date=end_date,
                loan_type="all",
            )
            combined["total_outstanding_repayment"] = get_total_outstanding_repayment(
                db,
                employer_filter=employer_filter,
                sourced_to_filter=sourced_to_filter,
                project_filter=project_filter,
                client_segment_filter=client_segment_filter,
                product_type_filter=product_type_filter,
                loan_status_filter=loan_status_filter,
                id_karyawan_filter=id_karyawan_filter,
                start_date=start_date,
                end_date=end_date,
                loan_type="all",
            )
            return _recalculate_repayment_risk_derivatives(combined)

        loan_conditions = resolve_loan_conditions(loan_type, db)

        # extradana / aku_cicil: query td_loan_history; kasbon / loan: query td_loan
        # directly, for principal/admin-fee collected. total_expected_repayment was
        # already computed above via get_total_expected_repayment, independent of this
        # branching.
        if loan_type in ("extradana", "aku_cicil"):
            if loan_type == "extradana":
                loan_conditions_tl = (
                    "l.loan_id IN (SELECT ls.id FROM loan_setting ls WHERE ls.loan_type LIKE 'Extradana%')"
                )
            else:
                loan_conditions_tl = loan_conditions

            # total_due_date_expected_repayment: internal-only, due-date-based twin of
            # total_expected_repayment, used solely so _recalculate_repayment_risk_
            # derivatives' collected/rate/delinquency math keeps its original (pre-ak-mj-
            # parity) basis instead of picking up the new disbursement-month figure.
            due_date_expected_query = """
            SELECT SUM(tlh.monthly) as total_due_date_expected_repayment
            FROM td_loan_history tlh
            INNER JOIN td_loan l ON tlh.loan_form_id = l.id
            {gmc_joins}
            WHERE tlh.due_date IS NOT NULL
            AND l.id_karyawan IS NOT NULL
            AND l.loan_status IN (1, 2, 4)
            AND {loan_conditions_tl}
            """.format(gmc_joins=_LOAN_GMC_JOINS, loan_conditions_tl=loan_conditions_tl)

            due_date_expected_params: dict = {}
            due_date_expected_query = _apply_repayment_risk_filters(
                due_date_expected_query,
                due_date_expected_params,
                employer_filter=employer_filter,
                sourced_to_filter=sourced_to_filter,
                project_filter=project_filter,
                client_segment_filter=client_segment_filter,
                product_type_filter=product_type_filter,
                loan_status_filter=loan_status_filter,
                id_karyawan_filter=id_karyawan_filter,
                db=db,
            )
            if start_date and end_date:
                due_date_expected_query = append_date_filters(
                    due_date_expected_query,
                    due_date_expected_params,
                    start_date=start_date,
                    end_date=end_date,
                    date_column="tlh.due_date",
                )
            due_date_expected_record = db.execute(text(due_date_expected_query), due_date_expected_params).fetchone()
            total_due_date_expected_repayment = (
                due_date_expected_record[0] if due_date_expected_record and due_date_expected_record[0] is not None else 0
            )

            risk_query = """
            SELECT
                SUM(CASE WHEN tlh.status = 2 THEN ROUND(l.total_loan / l.duration, 0) ELSE 0 END) as total_loan_principal_collected,
                SUM(CASE WHEN tlh.status = 2 THEN ROUND(l.admin_fee / l.duration, 0) ELSE 0 END) as total_admin_fee_collected,
                SUM(CASE WHEN tlh.status = 4 THEN ROUND(l.total_loan / l.duration, 0) ELSE 0 END) as total_unrecovered_loan_principal,
                SUM(CASE WHEN tlh.status = 4 THEN ROUND(l.admin_fee / l.duration, 0) ELSE 0 END) as total_unrecovered_admin_fee,
                SUM(ROUND(l.total_loan / l.duration, 0)) as total_expected_loan_principal,
                SUM(ROUND(l.admin_fee / l.duration, 0)) as total_expected_admin_fee
            FROM td_loan_history tlh
            INNER JOIN td_loan l ON tlh.loan_form_id = l.id
            {gmc_joins}
            WHERE tlh.due_date IS NOT NULL
            AND l.id_karyawan IS NOT NULL
            AND l.loan_status IN (1, 2, 4)
            AND {loan_conditions_tl}
            """.format(gmc_joins=_LOAN_GMC_JOINS, loan_conditions_tl=loan_conditions_tl)

            params: dict = {}
            risk_query = _apply_repayment_risk_filters(
                risk_query,
                params,
                employer_filter=employer_filter,
                sourced_to_filter=sourced_to_filter,
                project_filter=project_filter,
                client_segment_filter=client_segment_filter,
                product_type_filter=product_type_filter,
                loan_status_filter=loan_status_filter,
                id_karyawan_filter=id_karyawan_filter,
                db=db,
            )
            if start_date and end_date:
                risk_query = append_date_filters(
                    risk_query,
                    params,
                    start_date=start_date,
                    end_date=end_date,
                    date_column=_REPORTING_DATE_INSTALLMENT,
                )

            record = db.execute(text(risk_query), params).fetchone()
            total_loan_principal_collected = record[0] if record and record[0] is not None else 0
            total_admin_fee_collected = record[1] if record and record[1] is not None else 0
            total_unrecovered_loan_principal = record[2] if record and record[2] is not None else 0
            total_unrecovered_admin_fee = record[3] if record and record[3] is not None else 0
            total_expected_loan_principal = record[4] if record and record[4] is not None else 0
            total_expected_admin_fee = record[5] if record and record[5] is not None else 0
        else:
            # kasbon / loan: single td_loan aggregate for principal/admin-fee collected.
            # total_due_date_expected_repayment: see comment in the extradana/aku_cicil
            # branch above.
            due_date_expected_query = """
            SELECT SUM(l.total_payment) as total_due_date_expected_repayment
            FROM td_loan l
            {gmc_joins}
            WHERE l.loan_status IN (1, 2, 4)
            AND {loan_conditions}
            """.format(gmc_joins=_LOAN_GMC_JOINS, loan_conditions=loan_conditions)

            due_date_expected_params: dict = {}
            due_date_expected_query = _apply_repayment_risk_filters(
                due_date_expected_query,
                due_date_expected_params,
                employer_filter=employer_filter,
                sourced_to_filter=sourced_to_filter,
                project_filter=project_filter,
                client_segment_filter=client_segment_filter,
                product_type_filter=product_type_filter,
                loan_status_filter=loan_status_filter,
                id_karyawan_filter=id_karyawan_filter,
                db=db,
            )
            if start_date and end_date:
                due_date_expected_query = append_date_filters(
                    due_date_expected_query,
                    due_date_expected_params,
                    start_date=start_date,
                    end_date=end_date,
                    date_column="l.repayment_date",
                )
            due_date_expected_record = db.execute(text(due_date_expected_query), due_date_expected_params).fetchone()
            total_due_date_expected_repayment = (
                due_date_expected_record[0] if due_date_expected_record and due_date_expected_record[0] is not None else 0
            )

            risk_query = """
            SELECT
                SUM(CASE WHEN l.loan_status = 2 THEN l.total_loan ELSE 0 END) as total_loan_principal_collected,
                SUM(CASE WHEN l.loan_status = 2 THEN l.admin_fee ELSE 0 END) as total_admin_fee_collected,
                SUM(CASE WHEN l.loan_status IN (4) THEN l.total_loan ELSE 0 END) as total_unrecovered_loan_principal,
                SUM(CASE WHEN l.loan_status IN (4) THEN l.admin_fee ELSE 0 END) as total_unrecovered_admin_fee,
                SUM(l.total_loan) as total_expected_loan_principal,
                SUM(l.admin_fee) as total_expected_admin_fee
            FROM td_loan l
            {gmc_joins}
            WHERE l.loan_status IN (1, 2, 4)
            AND {loan_conditions}
            """.format(gmc_joins=_LOAN_GMC_JOINS, loan_conditions=loan_conditions)

            params: dict = {}
            risk_query = _apply_repayment_risk_filters(
                risk_query,
                params,
                employer_filter=employer_filter,
                sourced_to_filter=sourced_to_filter,
                project_filter=project_filter,
                client_segment_filter=client_segment_filter,
                product_type_filter=product_type_filter,
                loan_status_filter=loan_status_filter,
                id_karyawan_filter=id_karyawan_filter,
                db=db,
            )
            if start_date and end_date:
                risk_query = append_date_filters(
                    risk_query,
                    params,
                    start_date=start_date,
                    end_date=end_date,
                    date_column=_REPORTING_DATE_LUMP,
                )

            record = db.execute(text(risk_query), params).fetchone()
            total_loan_principal_collected = record[0] if record and record[0] is not None else 0
            total_admin_fee_collected = record[1] if record and record[1] is not None else 0
            total_unrecovered_loan_principal = record[2] if record and record[2] is not None else 0
            total_unrecovered_admin_fee = record[3] if record and record[3] is not None else 0
            total_expected_loan_principal = record[4] if record and record[4] is not None else 0
            total_expected_admin_fee = record[5] if record and record[5] is not None else 0

        total_unrecovered_repayment = get_total_unrecovered_repayment(
            db,
            employer_filter=employer_filter,
            sourced_to_filter=sourced_to_filter,
            project_filter=project_filter,
            client_segment_filter=client_segment_filter,
            product_type_filter=product_type_filter,
            loan_status_filter=loan_status_filter,
            id_karyawan_filter=id_karyawan_filter,
            start_date=start_date,
            end_date=end_date,
            loan_type=loan_type,
        )

        total_outstanding_repayment = get_total_outstanding_repayment(
            db,
            employer_filter=employer_filter,
            sourced_to_filter=sourced_to_filter,
            project_filter=project_filter,
            client_segment_filter=client_segment_filter,
            product_type_filter=product_type_filter,
            loan_status_filter=loan_status_filter,
            id_karyawan_filter=id_karyawan_filter,
            start_date=start_date,
            end_date=end_date,
            loan_type=loan_type,
        )

        return _recalculate_repayment_risk_derivatives({
            "total_expected_repayment": total_expected_repayment,
            "total_due_date_expected_repayment": total_due_date_expected_repayment,
            "total_loan_principal_collected": total_loan_principal_collected,
            "total_admin_fee_collected": total_admin_fee_collected,
            "total_unrecovered_repayment": total_unrecovered_repayment,
            "total_unrecovered_loan_principal": total_unrecovered_loan_principal,
            "total_unrecovered_admin_fee": total_unrecovered_admin_fee,
            "total_outstanding_repayment": total_outstanding_repayment,
            "total_expected_loan_principal": total_expected_loan_principal,
            "total_expected_admin_fee": total_expected_admin_fee,
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "total_expected_repayment": 0,
            "total_due_date_expected_repayment": 0,
            "total_loan_principal_collected": 0,
            "total_admin_fee_collected": 0,
            "total_unrecovered_repayment": 0,
            "total_unrecovered_loan_principal": 0,
            "total_unrecovered_admin_fee": 0,
            "total_outstanding_repayment": 0,
            "total_expected_loan_principal": 0,
            "total_expected_admin_fee": 0,
            "repayment_recovery_rate": 0,
            "delinquency_by_expected_repayment": 0,
            "delinquency_by_admin_fee": 0,
            "outstanding_rate": 0,
            "principal_collection_rate": 0,
            "admin_fee_collection_rate": 0,
            "admin_fee_profit": 0
        }


def get_repayment_risk_monthly_summary(db: Session,
                                       employer_filter: str = None, sourced_to_filter: str = None,
                                       project_filter: str = None, client_segment_filter: str = None, product_type_filter: str = None, loan_status_filter: int = None,
                                       id_karyawan_filter: int = None, start_date: str = None,
                                       end_date: str = None, loan_type: str = "loan") -> dict:
    """Get repayment risk summary separated by months within a date range.

    Three independent attribution models are in play here, matching get_repayment_risk_
    summary (see that function's docstring): total_expected_repayment is matched to
    ak-mj's /loan/monthly_performance figure — SUM(td_loan.total_payment) bucketed by
    disbursement month (l.proses_date), computed via get_total_expected_repayment_monthly
    independently of everything else, including for loan_type=all (no product predicate,
    matching ak-mj). total_collected_repayment/unrecovered/outstanding rates are bucketed
    by raw due month and have nothing to do with bad debt. Principal/admin-fee collected/
    unrecovered (and the performance metrics derived from them) are bucketed by reporting
    month — the payment month if paid on/before its due date or once it has crossed into
    Bad Debt Recovery, otherwise its original due month (see _REPORTING_DATE_LUMP/
    _INSTALLMENT).
    """

    try:
        expected_monthly = get_total_expected_repayment_monthly(
            db,
            employer_filter=employer_filter,
            sourced_to_filter=sourced_to_filter,
            project_filter=project_filter,
            client_segment_filter=client_segment_filter,
            product_type_filter=product_type_filter,
            loan_status_filter=loan_status_filter,
            id_karyawan_filter=id_karyawan_filter,
            start_date=start_date,
            end_date=end_date,
            loan_type=loan_type,
        )

        if is_all_loan_types(loan_type):
            monthly_dicts = [
                get_repayment_risk_monthly_summary(
                    db,
                    employer_filter=employer_filter,
                    sourced_to_filter=sourced_to_filter,
                    project_filter=project_filter,
                    client_segment_filter=client_segment_filter,
                    product_type_filter=product_type_filter,
                    loan_status_filter=loan_status_filter,
                    id_karyawan_filter=id_karyawan_filter,
                    start_date=start_date,
                    end_date=end_date,
                    loan_type=product_type,
                )
                for product_type in ALL_LOAN_TYPES
            ]
            merged = _merge_monthly_repayment_risk(monthly_dicts)
            unrecovered_monthly = get_total_unrecovered_repayment_monthly(
                db,
                employer_filter=employer_filter,
                sourced_to_filter=sourced_to_filter,
                project_filter=project_filter,
                client_segment_filter=client_segment_filter,
                product_type_filter=product_type_filter,
                loan_status_filter=loan_status_filter,
                id_karyawan_filter=id_karyawan_filter,
                start_date=start_date,
                end_date=end_date,
                loan_type="all",
            )
            outstanding_monthly = get_total_outstanding_repayment_monthly(
                db,
                employer_filter=employer_filter,
                sourced_to_filter=sourced_to_filter,
                project_filter=project_filter,
                client_segment_filter=client_segment_filter,
                product_type_filter=product_type_filter,
                loan_status_filter=loan_status_filter,
                id_karyawan_filter=id_karyawan_filter,
                start_date=start_date,
                end_date=end_date,
                loan_type="all",
            )
            all_months = (
                set(merged.keys())
                | set(expected_monthly.keys())
                | set(unrecovered_monthly.keys())
                | set(outstanding_monthly.keys())
            )
            for month_year in all_months:
                bucket = merged.setdefault(month_year, {key: 0 for key in _MONTHLY_REPAYMENT_RISK_SUM_KEYS})
                bucket["total_expected_repayment"] = expected_monthly.get(month_year, 0) or 0
                bucket["total_unrecovered_repayment"] = unrecovered_monthly.get(month_year, 0) or 0
                bucket["total_outstanding_repayment"] = outstanding_monthly.get(month_year, 0) or 0
                merged[month_year] = _recalculate_repayment_risk_derivatives(bucket)
            return merged

        loan_conditions = resolve_loan_conditions(loan_type, db)

        # For extradana and aku_cicil, query from td_loan_history; for loan (kasbon), query
        # from td_loan. total_expected_repayment was already computed above via
        # get_total_expected_repayment_monthly, independent of this branching.
        # Principal/admin-fee collected/unrecovered are bucketed by the reporting-date CASE
        # expression, so a Bad-Debt-recovered repayment moves into its payment month
        # instead of staying in its due month for those fields only.
        if loan_type == "extradana" or loan_type == "aku_cicil":
            if loan_type == "extradana":
                loan_conditions_tl = (
                    "l.loan_id IN (SELECT ls.id FROM loan_setting ls WHERE ls.loan_type LIKE 'Extradana%')"
                )
            else:
                loan_conditions_tl = loan_conditions

            due_date_column = "tlh.due_date"
            reporting_date = _REPORTING_DATE_INSTALLMENT

            # total_due_date_expected_repayment: internal-only, due-date-based twin of
            # total_expected_repayment — see get_repayment_risk_summary's comment.
            due_date_expected_query = """
            SELECT
                DATE_FORMAT(tlh.due_date, '%M %Y') as month_year,
                SUM(tlh.monthly) as total_due_date_expected_repayment
            FROM td_loan_history tlh
            INNER JOIN td_loan l ON tlh.loan_form_id = l.id
            {gmc_joins}
            WHERE tlh.due_date IS NOT NULL
            AND l.loan_status IN (1, 2, 4)
            AND {loan_conditions_tl}
            """.format(gmc_joins=_LOAN_GMC_JOINS, loan_conditions_tl=loan_conditions_tl)

            risk_query = """
            SELECT
                DATE_FORMAT({reporting_date}, '%M %Y') as month_year,
                SUM(CASE WHEN tlh.status = 2 THEN ROUND(l.total_loan / l.duration, 0) ELSE 0 END) as total_loan_principal_collected,
                SUM(CASE WHEN tlh.status = 2 THEN ROUND(l.admin_fee / l.duration, 0) ELSE 0 END) as total_admin_fee_collected,
                SUM(CASE WHEN tlh.status = 4 THEN ROUND(l.total_loan / l.duration, 0) ELSE 0 END) as total_unrecovered_loan_principal,
                SUM(CASE WHEN tlh.status = 4 THEN ROUND(l.admin_fee / l.duration, 0) ELSE 0 END) as total_unrecovered_admin_fee,
                SUM(ROUND(l.total_loan / l.duration, 0)) as total_expected_loan_principal,
                SUM(ROUND(l.admin_fee / l.duration, 0)) as total_expected_admin_fee
            FROM td_loan_history tlh
            INNER JOIN td_loan l ON tlh.loan_form_id = l.id
            {gmc_joins}
            WHERE tlh.due_date IS NOT NULL
            AND l.loan_status IN (1, 2, 4)
            AND {loan_conditions_tl}
            """.format(reporting_date=reporting_date, gmc_joins=_LOAN_GMC_JOINS, loan_conditions_tl=loan_conditions_tl)
        else:
            due_date_column = "l.repayment_date"
            reporting_date = _REPORTING_DATE_LUMP

            due_date_expected_query = """
            SELECT
                DATE_FORMAT(l.repayment_date, '%M %Y') as month_year,
                SUM(l.total_payment) as total_due_date_expected_repayment
            FROM td_loan l
            {gmc_joins}
            WHERE l.loan_status IN (1, 2, 4)
            AND {loan_conditions}
            """.format(gmc_joins=_LOAN_GMC_JOINS, loan_conditions=loan_conditions)

            risk_query = """
            SELECT
                DATE_FORMAT({reporting_date}, '%M %Y') as month_year,
                SUM(CASE WHEN l.loan_status = 2 THEN l.total_loan ELSE 0 END) as total_loan_principal_collected,
                SUM(CASE WHEN l.loan_status = 2 THEN l.admin_fee ELSE 0 END) as total_admin_fee_collected,
                SUM(CASE WHEN l.loan_status = 4 THEN l.total_loan ELSE 0 END) as total_unrecovered_loan_principal,
                SUM(CASE WHEN l.loan_status = 4 THEN l.admin_fee ELSE 0 END) as total_unrecovered_admin_fee,
                SUM(l.total_loan) as total_expected_loan_principal,
                SUM(l.admin_fee) as total_expected_admin_fee
            FROM td_loan l
            {gmc_joins}
            WHERE l.loan_status IN (1, 2, 4)
            AND {loan_conditions}
            """.format(reporting_date=reporting_date, gmc_joins=_LOAN_GMC_JOINS, loan_conditions=loan_conditions)

        due_date_expected_params: dict = {}
        due_date_expected_query = _apply_repayment_risk_filters(
            due_date_expected_query,
            due_date_expected_params,
            employer_filter=employer_filter,
            sourced_to_filter=sourced_to_filter,
            project_filter=project_filter,
            client_segment_filter=client_segment_filter,
            product_type_filter=product_type_filter,
            loan_status_filter=loan_status_filter,
            id_karyawan_filter=id_karyawan_filter,
            db=db,
        )
        if start_date and end_date:
            due_date_expected_query = append_date_filters(
                due_date_expected_query,
                due_date_expected_params,
                start_date=start_date,
                end_date=end_date,
                date_column=due_date_column,
            )
        due_date_expected_query += f"""
        GROUP BY DATE_FORMAT({due_date_column}, '%M %Y')
        ORDER BY MIN({due_date_column})
        """
        due_date_expected_monthly = {
            row[0]: (row[1] if row[1] is not None else 0)
            for row in db.execute(text(due_date_expected_query), due_date_expected_params).fetchall()
            if row[0] is not None
        }

        params: dict = {}
        risk_query = _apply_repayment_risk_filters(
            risk_query,
            params,
            employer_filter=employer_filter,
            sourced_to_filter=sourced_to_filter,
            project_filter=project_filter,
            client_segment_filter=client_segment_filter,
            product_type_filter=product_type_filter,
            loan_status_filter=loan_status_filter,
            id_karyawan_filter=id_karyawan_filter,
            db=db,
        )
        if start_date and end_date:
            risk_query = append_date_filters(
                risk_query,
                params,
                start_date=start_date,
                end_date=end_date,
                date_column=reporting_date,
            )

        risk_query += f"""
        GROUP BY DATE_FORMAT({reporting_date}, '%M %Y')
        ORDER BY MIN({reporting_date})
        """

        # Execute the query
        result = db.execute(text(risk_query), params)
        records = result.fetchall()

        monthly_unrecovered = get_total_unrecovered_repayment_monthly(
            db,
            employer_filter=employer_filter,
            sourced_to_filter=sourced_to_filter,
            project_filter=project_filter,
            client_segment_filter=client_segment_filter,
            product_type_filter=product_type_filter,
            loan_status_filter=loan_status_filter,
            id_karyawan_filter=id_karyawan_filter,
            start_date=start_date,
            end_date=end_date,
            loan_type=loan_type,
        )

        monthly_outstanding = get_total_outstanding_repayment_monthly(
            db,
            employer_filter=employer_filter,
            sourced_to_filter=sourced_to_filter,
            project_filter=project_filter,
            client_segment_filter=client_segment_filter,
            product_type_filter=product_type_filter,
            loan_status_filter=loan_status_filter,
            id_karyawan_filter=id_karyawan_filter,
            start_date=start_date,
            end_date=end_date,
            loan_type=loan_type,
        )

        # Principal/admin-fee collected/unrecovered, keyed by reporting month.
        principal_by_month = {}
        for record in records:
            month_year = record[0]
            if month_year is None:
                continue
            principal_by_month[month_year] = {
                "total_loan_principal_collected": record[1] if record[1] is not None else 0,
                "total_admin_fee_collected": record[2] if record[2] is not None else 0,
                "total_unrecovered_loan_principal": record[3] if record[3] is not None else 0,
                "total_unrecovered_admin_fee": record[4] if record[4] is not None else 0,
                "total_expected_loan_principal": record[5] if record[5] is not None else 0,
                "total_expected_admin_fee": record[6] if record[6] is not None else 0,
            }

        # total_expected_repayment (expected_monthly, keyed by disbursement month),
        # total_due_date_expected_repayment (due_date_expected_monthly, keyed by due
        # month), and the principal/admin-fee fields (principal_by_month, keyed by
        # reporting month) can each produce month_year keys the others don't have, so
        # union across every source before building each month's bucket.
        all_months = (
            set(expected_monthly.keys())
            | set(due_date_expected_monthly.keys())
            | set(principal_by_month.keys())
            | set(monthly_unrecovered.keys())
            | set(monthly_outstanding.keys())
        )

        monthly_data = {}
        for month_year in all_months:
            bucket = {key: 0 for key in _MONTHLY_REPAYMENT_RISK_SUM_KEYS}
            bucket["total_expected_repayment"] = expected_monthly.get(month_year, 0) or 0
            bucket["total_due_date_expected_repayment"] = due_date_expected_monthly.get(month_year, 0) or 0
            bucket.update(principal_by_month.get(month_year, {}))
            bucket["total_unrecovered_repayment"] = monthly_unrecovered.get(month_year, 0) or 0
            bucket["total_outstanding_repayment"] = monthly_outstanding.get(month_year, 0) or 0
            monthly_data[month_year] = _recalculate_repayment_risk_derivatives(bucket)

        return monthly_data

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {}


def _finalize_bad_debt_recovery(total_principal_recovered, total_admin_fee_recovered, loan_request_count) -> dict:
    total_recovery = total_principal_recovered + total_admin_fee_recovered
    return {
        "total_recovery": total_recovery,
        "total_principal_recovered": total_principal_recovered,
        "total_admin_fee_recovered": total_admin_fee_recovered,
        "principal_rate": (total_principal_recovered / total_recovery) if total_recovery > 0 else 0,
        "admin_fee_rate": (total_admin_fee_recovered / total_recovery) if total_recovery > 0 else 0,
        "loan_request_count": loan_request_count,
    }


def get_bad_debt_recovery_summary(db: Session,
                                  employer_filter: str = None, sourced_to_filter: str = None,
                                  project_filter: str = None, client_segment_filter: str = None, product_type_filter: str = None, loan_status_filter: int = None,
                                  id_karyawan_filter: int = None, start_date: str = None, end_date: str = None, loan_type: str = "loan") -> dict:
    """Get bad debt recovery summary: loans/installments paid three calendar months or more
    after their due month (the M+3 rule; see _BAD_DEBT_*_PREDICATE). These same repayments
    are also reported in repayment-risk, attributed to their payment month rather than
    their due month — see _REPORTING_DATE_LUMP/_INSTALLMENT.

    Also includes partial payments (td_loan_payment/td_loan_payment_allocation) against
    still-open (status=4) rows whose partial payment itself landed 3+ calendar months after
    the due month — see _BAD_DEBT_PARTIAL_INSTALLMENT_PREDICATE/_BAD_DEBT_PARTIAL_LUMP_
    PREDICATE. Fully-closed (status=2) rows are unaffected; that path is unchanged."""

    try:
        if is_all_loan_types(loan_type):
            summaries = [
                get_bad_debt_recovery_summary(
                    db,
                    employer_filter=employer_filter,
                    sourced_to_filter=sourced_to_filter,
                    project_filter=project_filter,
                    client_segment_filter=client_segment_filter,
                    product_type_filter=product_type_filter,
                    loan_status_filter=loan_status_filter,
                    id_karyawan_filter=id_karyawan_filter,
                    start_date=start_date,
                    end_date=end_date,
                    loan_type=product_type,
                )
                for product_type in ALL_LOAN_TYPES
            ]
            return _finalize_bad_debt_recovery(
                sum(s["total_principal_recovered"] for s in summaries),
                sum(s["total_admin_fee_recovered"] for s in summaries),
                sum(s["loan_request_count"] for s in summaries),
            )

        loan_conditions = resolve_loan_conditions(loan_type, db)
        params = {}

        if loan_type in ("extradana", "aku_cicil"):
            if loan_type == "extradana":
                loan_conditions_tl = (
                    "l.loan_id IN (SELECT ls.id FROM loan_setting ls WHERE ls.loan_type LIKE 'Extradana%')"
                )
            else:
                loan_conditions_tl = loan_conditions

            query = """
            SELECT
                SUM(ROUND(l.total_loan / l.duration, 0)) as total_principal_recovered,
                SUM(ROUND(l.admin_fee / l.duration, 0)) as total_admin_fee_recovered,
                COUNT(DISTINCT l.id) as loan_request_count
            FROM td_loan_history tlh
            INNER JOIN td_loan l ON tlh.loan_form_id = l.id
            {karyawan_joins}
            WHERE tlh.due_date IS NOT NULL
            AND l.id_karyawan IS NOT NULL
            AND {loan_conditions_tl}
            AND tlh.status = 2
            AND {bad_debt_predicate}
            """.format(
                karyawan_joins=_LOAN_GMC_JOINS,
                loan_conditions_tl=loan_conditions_tl,
                bad_debt_predicate=_BAD_DEBT_INSTALLMENT_PREDICATE,
            )
            date_column = "tlh.payment_date"
            partial_base_sql = _installment_partial_recovery_sql(loan_conditions_tl)
            partial_reporting_date = _REPORTING_DATE_PARTIAL_INSTALLMENT
            partial_bad_debt_predicate = _BAD_DEBT_PARTIAL_INSTALLMENT_PREDICATE
        else:
            query = """
            SELECT
                SUM(l.total_loan) as total_principal_recovered,
                SUM(l.admin_fee) as total_admin_fee_recovered,
                COUNT(DISTINCT l.id) as loan_request_count
            FROM td_loan l
            {karyawan_joins}
            WHERE {loan_conditions}
            AND l.loan_status = 2
            AND {bad_debt_predicate}
            """.format(
                karyawan_joins=_LOAN_GMC_JOINS,
                loan_conditions=loan_conditions,
                bad_debt_predicate=_BAD_DEBT_LUMP_PREDICATE,
            )
            date_column = "l.payment_date"
            partial_base_sql = _lump_partial_recovery_sql(loan_conditions)
            partial_reporting_date = _REPORTING_DATE_PARTIAL_LUMP
            partial_bad_debt_predicate = _BAD_DEBT_PARTIAL_LUMP_PREDICATE

        query = _append_loan_org_filters(
            query,
            params,
            id_karyawan_filter=id_karyawan_filter,
            employer_filter=employer_filter,
            sourced_to_filter=sourced_to_filter,
            project_filter=project_filter,
            client_segment_filter=client_segment_filter,
            product_type_filter=product_type_filter,
            loan_status_filter=loan_status_filter,
            db=db,
        )

        if start_date and end_date:
            query = append_date_filters(
                query,
                params,
                start_date=start_date,
                end_date=end_date,
                date_column=date_column,
            )

        record = db.execute(text(query), params).fetchone()
        total_principal_recovered = record[0] if record and record[0] is not None else 0
        total_admin_fee_recovered = record[1] if record and record[1] is not None else 0
        loan_request_count = record[2] if record and record[2] is not None else 0

        partial_principal, partial_fee, partial_row_count = _partial_recovery_totals(
            db,
            partial_base_sql,
            reporting_date_expr=partial_reporting_date,
            bad_debt_predicate=partial_bad_debt_predicate,
            bad_debt_filter=True,
            employer_filter=employer_filter,
            sourced_to_filter=sourced_to_filter,
            project_filter=project_filter,
            client_segment_filter=client_segment_filter,
            product_type_filter=product_type_filter,
            loan_status_filter=loan_status_filter,
            id_karyawan_filter=id_karyawan_filter,
            start_date=start_date,
            end_date=end_date,
        )
        total_principal_recovered += partial_principal
        total_admin_fee_recovered += partial_fee
        loan_request_count += partial_row_count

        return _finalize_bad_debt_recovery(total_principal_recovered, total_admin_fee_recovered, loan_request_count)

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "total_recovery": 0,
            "total_principal_recovered": 0,
            "total_admin_fee_recovered": 0,
            "principal_rate": 0,
            "admin_fee_rate": 0,
            "loan_request_count": 0,
        }


def get_bad_debt_recovery_monthly_summary(db: Session,
                                          employer_filter: str = None, sourced_to_filter: str = None,
                                          project_filter: str = None, client_segment_filter: str = None, product_type_filter: str = None, loan_status_filter: int = None,
                                          id_karyawan_filter: int = None, start_date: str = None,
                                          end_date: str = None, loan_type: str = "loan") -> dict:
    """Get bad debt recovery separated by month, bucketed by the month the late payment posted.
    Matches the reporting month repayment-risk-monthly attributes the same repayment to."""

    try:
        if is_all_loan_types(loan_type):
            monthly_dicts = [
                get_bad_debt_recovery_monthly_summary(
                    db,
                    employer_filter=employer_filter,
                    sourced_to_filter=sourced_to_filter,
                    project_filter=project_filter,
                    client_segment_filter=client_segment_filter,
                    product_type_filter=product_type_filter,
                    loan_status_filter=loan_status_filter,
                    id_karyawan_filter=id_karyawan_filter,
                    start_date=start_date,
                    end_date=end_date,
                    loan_type=product_type,
                )
                for product_type in ALL_LOAN_TYPES
            ]
            merged: dict = {}
            for monthly in monthly_dicts:
                for month_year, metrics in monthly.items():
                    bucket = merged.setdefault(month_year, {
                        "total_principal_recovered": 0,
                        "total_admin_fee_recovered": 0,
                        "loan_request_count": 0,
                    })
                    bucket["total_principal_recovered"] += metrics["total_principal_recovered"]
                    bucket["total_admin_fee_recovered"] += metrics["total_admin_fee_recovered"]
                    bucket["loan_request_count"] += metrics["loan_request_count"]
            return {
                month_year: _finalize_bad_debt_recovery(
                    metrics["total_principal_recovered"],
                    metrics["total_admin_fee_recovered"],
                    metrics["loan_request_count"],
                )
                for month_year, metrics in merged.items()
            }

        loan_conditions = resolve_loan_conditions(loan_type, db)
        params = {}

        if loan_type in ("extradana", "aku_cicil"):
            if loan_type == "extradana":
                loan_conditions_tl = (
                    "l.loan_id IN (SELECT ls.id FROM loan_setting ls WHERE ls.loan_type LIKE 'Extradana%')"
                )
            else:
                loan_conditions_tl = loan_conditions

            query = """
            SELECT
                DATE_FORMAT(tlh.payment_date, '%M %Y') as month_year,
                SUM(ROUND(l.total_loan / l.duration, 0)) as total_principal_recovered,
                SUM(ROUND(l.admin_fee / l.duration, 0)) as total_admin_fee_recovered,
                COUNT(DISTINCT l.id) as loan_request_count
            FROM td_loan_history tlh
            INNER JOIN td_loan l ON tlh.loan_form_id = l.id
            {karyawan_joins}
            WHERE tlh.due_date IS NOT NULL
            AND l.id_karyawan IS NOT NULL
            AND {loan_conditions_tl}
            AND tlh.status = 2
            AND {bad_debt_predicate}
            """.format(
                karyawan_joins=_LOAN_GMC_JOINS,
                loan_conditions_tl=loan_conditions_tl,
                bad_debt_predicate=_BAD_DEBT_INSTALLMENT_PREDICATE,
            )
            date_column = "tlh.payment_date"
            partial_base_sql = _installment_partial_recovery_sql(loan_conditions_tl)
            partial_reporting_date = _REPORTING_DATE_PARTIAL_INSTALLMENT
            partial_bad_debt_predicate = _BAD_DEBT_PARTIAL_INSTALLMENT_PREDICATE
        else:
            query = """
            SELECT
                DATE_FORMAT(l.payment_date, '%M %Y') as month_year,
                SUM(l.total_loan) as total_principal_recovered,
                SUM(l.admin_fee) as total_admin_fee_recovered,
                COUNT(DISTINCT l.id) as loan_request_count
            FROM td_loan l
            {karyawan_joins}
            WHERE {loan_conditions}
            AND l.loan_status = 2
            AND {bad_debt_predicate}
            """.format(
                karyawan_joins=_LOAN_GMC_JOINS,
                loan_conditions=loan_conditions,
                bad_debt_predicate=_BAD_DEBT_LUMP_PREDICATE,
            )
            date_column = "l.payment_date"
            partial_base_sql = _lump_partial_recovery_sql(loan_conditions)
            partial_reporting_date = _REPORTING_DATE_PARTIAL_LUMP
            partial_bad_debt_predicate = _BAD_DEBT_PARTIAL_LUMP_PREDICATE

        query = _append_loan_org_filters(
            query,
            params,
            id_karyawan_filter=id_karyawan_filter,
            employer_filter=employer_filter,
            sourced_to_filter=sourced_to_filter,
            project_filter=project_filter,
            client_segment_filter=client_segment_filter,
            product_type_filter=product_type_filter,
            loan_status_filter=loan_status_filter,
            db=db,
        )

        if start_date and end_date:
            query = append_date_filters(
                query,
                params,
                start_date=start_date,
                end_date=end_date,
                date_column=date_column,
            )

        query += f" GROUP BY DATE_FORMAT({date_column}, '%M %Y') ORDER BY MIN({date_column})"

        records = db.execute(text(query), params).fetchall()

        monthly_data = {}
        for record in records:
            month_year = record[0]
            if month_year is None:
                continue
            total_principal_recovered = record[1] if record[1] is not None else 0
            total_admin_fee_recovered = record[2] if record[2] is not None else 0
            loan_request_count = record[3] if record[3] is not None else 0
            monthly_data[month_year] = {
                "total_principal_recovered": total_principal_recovered,
                "total_admin_fee_recovered": total_admin_fee_recovered,
                "loan_request_count": loan_request_count,
            }

        partial_monthly = _partial_recovery_monthly(
            db,
            partial_base_sql,
            reporting_date_expr=partial_reporting_date,
            bad_debt_predicate=partial_bad_debt_predicate,
            bad_debt_filter=True,
            employer_filter=employer_filter,
            sourced_to_filter=sourced_to_filter,
            project_filter=project_filter,
            client_segment_filter=client_segment_filter,
            product_type_filter=product_type_filter,
            loan_status_filter=loan_status_filter,
            id_karyawan_filter=id_karyawan_filter,
            start_date=start_date,
            end_date=end_date,
        )
        for month_year, (partial_principal, partial_fee, partial_row_count) in partial_monthly.items():
            bucket = monthly_data.setdefault(month_year, {
                "total_principal_recovered": 0,
                "total_admin_fee_recovered": 0,
                "loan_request_count": 0,
            })
            bucket["total_principal_recovered"] += partial_principal
            bucket["total_admin_fee_recovered"] += partial_fee
            bucket["loan_request_count"] += partial_row_count

        return {
            month_year: _finalize_bad_debt_recovery(
                metrics["total_principal_recovered"],
                metrics["total_admin_fee_recovered"],
                metrics["loan_request_count"],
            )
            for month_year, metrics in monthly_data.items()
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {}


def get_disbursed_amount(db: Session,
                        employer_filter: str = None, sourced_to_filter: str = None,
                        project_filter: str = None, client_segment_filter: str = None, product_type_filter: str = None, loan_status_filter: int = None,
                        id_karyawan_filter: int = None, start_date: str = None,
                        end_date: str = None, loan_type: str = "kasbon") -> dict:
    """Centralized function to get disbursed amount with consistent logic"""

    try:
        # Determine loan conditions based on loan type
        if loan_type == "loan":
            loan_conditions = LOAN_CONDITIONS
        elif loan_type == "extradana":
            loan_conditions = EXTRADANA_LOAN_CONDITIONS
        elif loan_type == "aku_cicil":
            loan_conditions = AKU_CICIL_CONDITION
        else:
            loan_conditions = LOAN_CONDITIONS  # default to loan

        # Build the disbursed amount query
        disbursed_query = """
        SELECT SUM(l.total_loan)
        FROM td_loan l
        LEFT JOIN td_karyawan tk
            ON l.id_karyawan = tk.id_karyawan
        LEFT JOIN tbl_gmc emp
            ON tk.valdo_inc = emp.kode_gmc
            AND emp.group_gmc = 'sub_client'
            AND emp.aktif = 'Yes'
            AND emp.keterangan3 = 1
        LEFT JOIN tbl_gmc src
            ON tk.placement = src.kode_gmc
            AND src.group_gmc = 'placement_client'
            AND src.aktif = 'Yes'
            AND src.keterangan3 = 1
        LEFT JOIN tbl_gmc prj
            ON tk.project = prj.kode_gmc
            AND prj.group_gmc = 'client_project'
            AND prj.aktif = 'Yes'
            AND prj.keterangan3 = 1
        WHERE l.loan_status IN (1, 2, 4)
        AND {loan_conditions}
        """.format(loan_conditions=loan_conditions)

        # Build parameters dict for filters
        params = {}

        # Apply all filters consistently
        if id_karyawan_filter:
            disbursed_query += " AND l.id_karyawan = :id_karyawan"
            params['id_karyawan'] = id_karyawan_filter

        # Restrict to only PT Valdo companies (conditional based on loan type)
        company_filter = COMPANY_FILTER

        disbursed_query += f" AND emp.keterangan IN {company_filter}"

        # If employer_filter is provided and it's one of the allowed companies, filter further
        if employer_filter and employer_filter in ALLOWED_COMPANIES:
            disbursed_query += " AND emp.keterangan = :employer"
            params['employer'] = employer_filter

        if sourced_to_filter:
            disbursed_query += " AND src.keterangan = :sourced_to"
            params['sourced_to'] = sourced_to_filter

        if project_filter:
            disbursed_query += " AND prj.keterangan = :project"
            params['project'] = project_filter

        disbursed_query = _apply_project_management_filters(disbursed_query, params, client_segment_filter, product_type_filter, db=db)

        if loan_status_filter is not None:
            disbursed_query += " AND l.loan_status = :loan_status"
            params['loan_status'] = loan_status_filter

        # Apply date range filters using proper month boundaries
        if start_date and end_date:
            disbursed_query += " AND l.proses_date >= :start_date AND l.proses_date < :end_date"
            params['start_date'] = start_date
            params['end_date'] = end_date


        # Execute query
        result = db.execute(text(disbursed_query), params)
        total_disbursed_amount = result.fetchone()[0] or 0


        return {
            "total_disbursed_amount": total_disbursed_amount
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "total_disbursed_amount": 0
        }


def get_coverage_utilization_summary(db: Session,
                                    employer_filter: str = None, sourced_to_filter: str = None,
                                    project_filter: str = None, client_segment_filter: str = None, product_type_filter: str = None, loan_status_filter: int = None,
                                    id_karyawan_filter: int = None, start_date: str = None, end_date: str = None, loan_type: str = "loan") -> dict:
    """Get comprehensive coverage and utilization summary combining multiple metrics"""

    try:
        company_filter = COMPANY_FILTER
        params = {}
        loan_conditions = resolve_loan_conditions(loan_type, db)

        total_active_employees = get_total_active_employees(
            db,
            start_date=start_date,
            end_date=end_date,
            employer_filter=employer_filter,
            sourced_to_filter=sourced_to_filter,
            project_filter=project_filter,
            client_segment_filter=client_segment_filter,
            product_type_filter=product_type_filter,
        )

        total_eligible_employees = get_total_eligible_employees(
            db,
            start_date=start_date,
            end_date=end_date,
            employer_filter=employer_filter,
            sourced_to_filter=sourced_to_filter,
            project_filter=project_filter,
            client_segment_filter=client_segment_filter,
            product_type_filter=product_type_filter,
        )

        total_coverage_project = get_total_coverage_project(
            db,
            start_date=start_date,
            end_date=end_date,
            employer_filter=employer_filter,
            sourced_to_filter=sourced_to_filter,
            project_filter=project_filter,
            client_segment_filter=client_segment_filter,
            product_type_filter=product_type_filter,
        )

        # Loan requests by received_date; approved/rejected/disbursed by proses_date.
        if start_date and end_date:
            params["start_date"] = start_of_day(start_date)
            params["end_date"] = end_of_day(end_date)
            loan_metrics_query = f"""
            SELECT
                COUNT(CASE
                    WHEN l.received_date >= :start_date
                     AND l.received_date <= :end_date
                    THEN 1 END) AS total_loan_requests,
                COUNT(CASE
                    WHEN l.loan_status IN (1, 2, 4)
                     AND l.proses_date >= :start_date
                     AND l.proses_date <= :end_date
                    THEN 1 END) AS total_approved_requests,
                COUNT(CASE
                    WHEN l.loan_status = 3
                     AND l.proses_date >= :start_date
                     AND l.proses_date <= :end_date
                    THEN 1 END) AS total_rejected_requests,
                COALESCE(SUM(CASE
                    WHEN l.loan_status IN (1, 2, 4)
                     AND l.proses_date >= :start_date
                     AND l.proses_date <= :end_date
                    THEN l.total_loan ELSE 0 END), 0) AS total_disbursed_amount,
                AVG(CASE
                    WHEN l.loan_status IN (1, 2, 4)
                     AND l.proses_date >= :start_date
                     AND l.proses_date <= :end_date
                     AND l.received_date IS NOT NULL
                     AND l.proses_date > l.received_date
                     AND l.received_date >= '1900-01-01'
                    THEN DATEDIFF(l.proses_date, l.received_date)
                    ELSE NULL END) AS average_approval_time
            FROM td_loan l
            {_LOAN_GMC_JOINS}
            WHERE (
                (l.received_date >= :start_date AND l.received_date <= :end_date)
                OR (l.proses_date >= :start_date AND l.proses_date <= :end_date)
            )
            AND {loan_conditions}
            """
        else:
            loan_metrics_query = f"""
            SELECT
                COUNT(CASE WHEN l.received_date IS NOT NULL THEN 1 END) AS total_loan_requests,
                COUNT(CASE WHEN l.loan_status IN (1, 2, 4) THEN 1 END) AS total_approved_requests,
                COUNT(CASE WHEN l.loan_status = 3 THEN 1 END) AS total_rejected_requests,
                COALESCE(SUM(CASE WHEN l.loan_status IN (1, 2, 4) THEN l.total_loan ELSE 0 END), 0) AS total_disbursed_amount,
                AVG(CASE
                    WHEN l.loan_status IN (1, 2, 4)
                     AND l.proses_date IS NOT NULL
                     AND l.received_date IS NOT NULL
                     AND l.proses_date > l.received_date
                     AND l.received_date >= '1900-01-01'
                    THEN DATEDIFF(l.proses_date, l.received_date)
                    ELSE NULL END) AS average_approval_time
            FROM td_loan l
            {_LOAN_GMC_JOINS}
            WHERE {loan_conditions}
            """

        loan_metrics_query = _append_loan_org_filters(
            loan_metrics_query,
            params,
            id_karyawan_filter=id_karyawan_filter,
            employer_filter=employer_filter,
            sourced_to_filter=sourced_to_filter,
            project_filter=project_filter,
            client_segment_filter=client_segment_filter,
            product_type_filter=product_type_filter,
            loan_status_filter=loan_status_filter,
            company_filter=company_filter,
            db=db,
        )

        first_borrow_query = f"""
        SELECT COUNT(DISTINCT l.id_karyawan)
        FROM td_loan l
        {_LOAN_GMC_JOINS}
        WHERE l.loan_status IN (0, 1, 2, 3)
        AND NOT EXISTS (
            SELECT 1
            FROM td_loan l2
            WHERE l2.id_karyawan = l.id_karyawan
            AND l2.loan_status = 2
            AND l2.proses_date < l.proses_date
            AND {loan_conditions}
        )
        AND {loan_conditions}
        """
        first_borrow_query = _append_loan_org_filters(
            first_borrow_query,
            params,
            id_karyawan_filter=id_karyawan_filter,
            employer_filter=employer_filter,
            sourced_to_filter=sourced_to_filter,
            project_filter=project_filter,
            client_segment_filter=client_segment_filter,
            product_type_filter=product_type_filter,
            loan_status_filter=loan_status_filter,
            company_filter=company_filter,
            db=db,
        )

        if start_date and end_date:
            first_borrow_query = _append_proses_date_range(
                first_borrow_query, params, start_date, end_date
            )

        eligible_rate = (total_eligible_employees / total_active_employees) if total_active_employees > 0 else 0.0

        metrics_row = db.execute(text(loan_metrics_query), params).fetchone()
        total_loan_requests = metrics_row[0] or 0
        total_approved_requests = metrics_row[1] or 0
        total_rejected_requests = metrics_row[2] or 0
        total_disbursed_amount = metrics_row[3] or 0
        average_approval_time = metrics_row[4] if metrics_row[4] is not None else 0
        disbursed_loans_count = total_approved_requests

        first_borrow_record = db.execute(text(first_borrow_query), params).fetchone()
        total_new_borrowers = first_borrow_record[0] if first_borrow_record and first_borrow_record[0] is not None else 0

        average_disbursed_amount = 0
        if disbursed_loans_count > 0:
            average_disbursed_amount = total_disbursed_amount / disbursed_loans_count

        penetration_rate = 0
        if total_eligible_employees > 0:
            penetration_rate = total_approved_requests / total_eligible_employees

        approval_rate = 0
        rejected_rate = 0
        total_processed_requests = total_approved_requests + total_rejected_requests
        if total_processed_requests > 0:
            approval_rate = total_approved_requests / total_processed_requests
            rejected_rate = total_rejected_requests / total_processed_requests

        return {
            "total_eligible_employees": total_eligible_employees,
            "total_coverage_project": total_coverage_project,
            "total_active_employees": total_active_employees,
            "total_loan_requests": total_loan_requests,
            "penetration_rate": penetration_rate,
            "eligible_rate": eligible_rate,
            "total_approved_requests": total_approved_requests,
            "total_rejected_requests": total_rejected_requests,
            "approval_rate": approval_rate,
            "rejected_rate": rejected_rate,
            "total_new_borrowers": total_new_borrowers,
            "average_approval_time": average_approval_time,
            "total_disbursed_amount": total_disbursed_amount,
            "average_disbursed_amount": average_disbursed_amount
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "total_eligible_employees": 0,
            "total_coverage_project": 0,
            "total_active_employees": 0,
            "total_loan_requests": 0,
            "penetration_rate": 0,
            "eligible_rate": 0.0,
            "total_approved_requests": 0,
            "total_rejected_requests": 0,
            "approval_rate": 0,
            "rejected_rate": 0,
            "total_new_borrowers": 0,
            "average_approval_time": 0,
            "total_disbursed_amount": 0,
            "average_disbursed_amount": 0
        }


def get_coverage_utilization_monthly_summary(db: Session,
                                            employer_filter: str = None, sourced_to_filter: str = None,
                                            project_filter: str = None, client_segment_filter: str = None, product_type_filter: str = None, loan_status_filter: int = None,
                                            id_karyawan_filter: int = None, start_date: str = None,
                                            end_date: str = None, loan_type: str = "loan") -> dict:
    """Get coverage utilization summary separated by months within a date range"""

    try:
        loan_conditions = resolve_loan_conditions(loan_type, db)

        # Build the eligible count query (exactly as in get_user_coverage_summary)
        eligible_count_query = """
        SELECT COUNT(*)
        FROM td_karyawan tk
        LEFT JOIN tbl_gmc emp
            ON tk.valdo_inc = emp.kode_gmc
            AND emp.group_gmc = 'sub_client'
            AND emp.aktif = 'Yes'
            AND emp.keterangan3 = 1
        LEFT JOIN tbl_gmc src
            ON tk.placement = src.kode_gmc
            AND src.group_gmc = 'placement_client'
            AND src.aktif = 'Yes'
            AND src.keterangan3 = 1
        LEFT JOIN tbl_gmc prj
            ON tk.project = prj.kode_gmc
            AND prj.group_gmc = 'client_project'
            AND prj.aktif = 'Yes'
            AND prj.keterangan3 = 1
        WHERE tk.status = '1'
        AND tk.loan_kasbon_eligible = '1'
        """

        # Build the processed loan requests query (exactly as in get_user_coverage_summary)
        processed_requests_query = """
        SELECT COUNT(*)
        FROM td_loan l
        LEFT JOIN td_karyawan tk
            ON l.id_karyawan = tk.id_karyawan
        LEFT JOIN tbl_gmc emp
            ON tk.valdo_inc = emp.kode_gmc
            AND emp.group_gmc = 'sub_client'
            AND emp.aktif = 'Yes'
            AND emp.keterangan3 = 1
        LEFT JOIN tbl_gmc src
            ON tk.placement = src.kode_gmc
            AND src.group_gmc = 'placement_client'
            AND src.aktif = 'Yes'
            AND src.keterangan3 = 1
        LEFT JOIN tbl_gmc prj
            ON tk.project = prj.kode_gmc
            AND prj.group_gmc = 'client_project'
            AND prj.aktif = 'Yes'
            AND prj.keterangan3 = 1
        WHERE l.loan_status IN (1, 2, 3, 4)
        AND {loan_conditions}
        """.format(loan_conditions=loan_conditions)

        # Build the approved requests query (exactly as in get_user_coverage_summary)
        approved_requests_query = """
        SELECT COUNT(*)
        FROM td_loan l
        LEFT JOIN td_karyawan tk
            ON l.id_karyawan = tk.id_karyawan
        LEFT JOIN tbl_gmc emp
            ON tk.valdo_inc = emp.kode_gmc
            AND emp.group_gmc = 'sub_client'
            AND emp.aktif = 'Yes'
            AND emp.keterangan3 = 1
        LEFT JOIN tbl_gmc src
            ON tk.placement = src.kode_gmc
            AND src.group_gmc = 'placement_client'
            AND src.aktif = 'Yes'
            AND src.keterangan3 = 1
        LEFT JOIN tbl_gmc prj
            ON tk.project = prj.kode_gmc
            AND prj.group_gmc = 'client_project'
            AND prj.aktif = 'Yes'
            AND prj.keterangan3 = 1
        WHERE l.loan_status IN (1, 2, 4)
        AND {loan_conditions}
        """.format(loan_conditions=loan_conditions)

        # Build the rejected requests query (exactly as in get_user_coverage_summary)
        rejected_requests_query = """
        SELECT COUNT(*)
        FROM td_loan l
        LEFT JOIN td_karyawan tk
            ON l.id_karyawan = tk.id_karyawan
        LEFT JOIN tbl_gmc emp
            ON tk.valdo_inc = emp.kode_gmc
            AND emp.group_gmc = 'sub_client'
            AND emp.aktif = 'Yes'
            AND emp.keterangan3 = 1
        LEFT JOIN tbl_gmc src
            ON tk.placement = src.kode_gmc
            AND src.group_gmc = 'placement_client'
            AND src.aktif = 'Yes'
            AND src.keterangan3 = 1
        LEFT JOIN tbl_gmc prj
            ON tk.project = prj.kode_gmc
            AND prj.group_gmc = 'client_project'
            AND prj.aktif = 'Yes'
            AND prj.keterangan3 = 1
        WHERE l.loan_status = 3
        AND {loan_conditions}
        """.format(loan_conditions=loan_conditions)

        # Build the total disbursed amount query (exactly as in get_user_coverage_summary)
        total_disbursed_amount_query = """
        SELECT SUM(l.total_loan)
        FROM td_loan l
        LEFT JOIN td_karyawan tk
            ON l.id_karyawan = tk.id_karyawan
        LEFT JOIN tbl_gmc emp
            ON tk.valdo_inc = emp.kode_gmc
            AND emp.group_gmc = 'sub_client'
            AND emp.aktif = 'Yes'
            AND emp.keterangan3 = 1
        LEFT JOIN tbl_gmc src
            ON tk.placement = src.kode_gmc
            AND src.group_gmc = 'placement_client'
            AND src.aktif = 'Yes'
            AND src.keterangan3 = 1
        LEFT JOIN tbl_gmc prj
            ON tk.project = prj.kode_gmc
            AND prj.group_gmc = 'client_project'
            AND prj.aktif = 'Yes'
            AND prj.keterangan3 = 1
        WHERE l.loan_status IN (1, 2, 4)
        AND {loan_conditions}
        """.format(loan_conditions=loan_conditions)

        # Build the first-time borrowers query (exactly as in get_user_coverage_summary)
        first_borrow_query = """
        SELECT COUNT(DISTINCT l.id_karyawan)
        FROM td_loan l
        LEFT JOIN td_karyawan tk
            ON l.id_karyawan = tk.id_karyawan
        LEFT JOIN tbl_gmc emp
            ON tk.valdo_inc = emp.kode_gmc
            AND emp.group_gmc = 'sub_client'
            AND emp.aktif = 'Yes'
            AND emp.keterangan3 = 1
        LEFT JOIN tbl_gmc src
            ON tk.placement = src.kode_gmc
            AND src.group_gmc = 'placement_client'
            AND src.aktif = 'Yes'
            AND src.keterangan3 = 1
        LEFT JOIN tbl_gmc prj
            ON tk.project = prj.kode_gmc
            AND prj.group_gmc = 'client_project'
            AND prj.aktif = 'Yes'
            AND prj.keterangan3 = 1
        WHERE l.loan_status IN (0, 1, 2, 3)
        AND NOT EXISTS (
            SELECT 1
            FROM td_loan l2
            WHERE l2.id_karyawan = l.id_karyawan
            AND l2.loan_status = 2
            AND l2.proses_date < l.proses_date
            AND {loan_conditions}
        )
        AND {loan_conditions}
        """.format(loan_conditions=loan_conditions)

        # Build parameters dict for filters
        params = {}

        # Add filters to all queries (exactly as in get_user_coverage_summary)
        if id_karyawan_filter:
            eligible_count_query += " AND tk.id_karyawan = :id_karyawan"
            processed_requests_query += " AND l.id_karyawan = :id_karyawan"
            approved_requests_query += " AND l.id_karyawan = :id_karyawan"
            rejected_requests_query += " AND l.id_karyawan = :id_karyawan"
            total_disbursed_amount_query += " AND l.id_karyawan = :id_karyawan"
            first_borrow_query += " AND l.id_karyawan = :id_karyawan"
            params['id_karyawan'] = id_karyawan_filter

        # Restrict to allowed Valdo companies
        company_filter = COMPANY_FILTER

        eligible_count_query += f" AND emp.keterangan IN {company_filter}"
        processed_requests_query += f" AND emp.keterangan IN {company_filter}"
        approved_requests_query += f" AND emp.keterangan IN {company_filter}"
        rejected_requests_query += f" AND emp.keterangan IN {company_filter}"
        total_disbursed_amount_query += f" AND emp.keterangan IN {company_filter}"
        first_borrow_query += f" AND emp.keterangan IN {company_filter}"

        # If employer_filter is provided and it's one of the allowed companies, filter further
        if employer_filter and employer_filter in ALLOWED_COMPANIES:
            eligible_count_query += " AND emp.keterangan = :employer"
            processed_requests_query += " AND emp.keterangan = :employer"
            approved_requests_query += " AND emp.keterangan = :employer"
            rejected_requests_query += " AND emp.keterangan = :employer"
            total_disbursed_amount_query += " AND emp.keterangan = :employer"
            first_borrow_query += " AND emp.keterangan = :employer"
            params['employer'] = employer_filter

        if sourced_to_filter:
            eligible_count_query += " AND src.keterangan = :sourced_to"
            processed_requests_query += " AND src.keterangan = :sourced_to"
            approved_requests_query += " AND src.keterangan = :sourced_to"
            rejected_requests_query += " AND src.keterangan = :sourced_to"
            total_disbursed_amount_query += " AND src.keterangan = :sourced_to"
            first_borrow_query += " AND src.keterangan = :sourced_to"
            params['sourced_to'] = sourced_to_filter

        if project_filter:
            eligible_count_query += " AND prj.keterangan = :project"
            processed_requests_query += " AND prj.keterangan = :project"
            approved_requests_query += " AND prj.keterangan = :project"
            rejected_requests_query += " AND prj.keterangan = :project"
            total_disbursed_amount_query += " AND prj.keterangan = :project"
            first_borrow_query += " AND prj.keterangan = :project"
            params['project'] = project_filter

        if loan_status_filter is not None:
            processed_requests_query += " AND l.loan_status = :loan_status"
            approved_requests_query += " AND l.loan_status = :loan_status"
            rejected_requests_query += " AND l.loan_status = :loan_status"
            total_disbursed_amount_query += " AND l.loan_status = :loan_status"
            first_borrow_query += " AND l.loan_status = :loan_status"
            params['loan_status'] = loan_status_filter

        # Add date range filters based on proses_date
        if start_date:
            processed_requests_query += " AND l.proses_date >= :start_date"
            approved_requests_query += " AND l.proses_date >= :start_date"
            rejected_requests_query += " AND l.proses_date >= :start_date"
            total_disbursed_amount_query += " AND l.proses_date >= :start_date"
            first_borrow_query += " AND l.proses_date >= :start_date"
            params['start_date'] = start_date

        if end_date:
            processed_requests_query += " AND l.proses_date <= :end_date"
            approved_requests_query += " AND l.proses_date <= :end_date"
            rejected_requests_query += " AND l.proses_date <= :end_date"
            total_disbursed_amount_query += " AND l.proses_date <= :end_date"
            first_borrow_query += " AND l.proses_date <= :end_date"
            params['end_date'] = end_date

        # Monthly loan metrics: requests by received_date; approved/rejected/disbursed by proses_date.
        monthly_requests_query = f"""
        SELECT
            DATE_FORMAT(l.received_date, '%M %Y') as month_year,
            COUNT(*) as total_loan_requests
        FROM td_loan l
        {_LOAN_GMC_JOINS}
        WHERE l.received_date IS NOT NULL
        """

        monthly_proses_query = f"""
        SELECT
            DATE_FORMAT(l.proses_date, '%M %Y') as month_year,
            COUNT(CASE WHEN l.loan_status IN (1, 2, 4) THEN 1 END) as total_approved_requests,
            COUNT(CASE WHEN l.loan_status = 3 THEN 1 END) as total_rejected_requests,
            COALESCE(SUM(CASE WHEN l.loan_status IN (1, 2, 4) THEN l.total_loan ELSE 0 END), 0) as total_disbursed_amount
        FROM td_loan l
        {_LOAN_GMC_JOINS}
        WHERE l.proses_date IS NOT NULL
        """

        monthly_first_borrow_query = f"""
        SELECT
            DATE_FORMAT(l.proses_date, '%M %Y') as month_year,
            COUNT(DISTINCT l.id_karyawan) as total_first_borrow
        FROM td_loan l
        {_LOAN_GMC_JOINS}
        WHERE l.loan_status IN (0, 1, 2, 3)
        AND NOT EXISTS (
            SELECT 1
            FROM td_loan l2
            WHERE l2.id_karyawan = l.id_karyawan
            AND l2.loan_status = 2
            AND l2.proses_date < l.proses_date
        )
        """

        monthly_requests_query = _append_loan_org_filters(
            monthly_requests_query,
            params,
            id_karyawan_filter=id_karyawan_filter,
            employer_filter=employer_filter,
            sourced_to_filter=sourced_to_filter,
            project_filter=project_filter,
            client_segment_filter=client_segment_filter,
            product_type_filter=product_type_filter,
            loan_status_filter=loan_status_filter,
            company_filter=company_filter,
            db=db,
        )
        monthly_proses_query = _append_loan_org_filters(
            monthly_proses_query,
            params,
            id_karyawan_filter=id_karyawan_filter,
            employer_filter=employer_filter,
            sourced_to_filter=sourced_to_filter,
            project_filter=project_filter,
            client_segment_filter=client_segment_filter,
            product_type_filter=product_type_filter,
            loan_status_filter=loan_status_filter,
            company_filter=company_filter,
            db=db,
        )
        monthly_first_borrow_query = _append_loan_org_filters(
            monthly_first_borrow_query,
            params,
            id_karyawan_filter=id_karyawan_filter,
            employer_filter=employer_filter,
            sourced_to_filter=sourced_to_filter,
            project_filter=project_filter,
            client_segment_filter=client_segment_filter,
            product_type_filter=product_type_filter,
            loan_status_filter=loan_status_filter,
            company_filter=company_filter,
            db=db,
        )

        if start_date and end_date:
            monthly_requests_query = append_date_filters(
                monthly_requests_query,
                params,
                start_date=start_date,
                end_date=end_date,
                date_column="l.received_date",
            )
            monthly_proses_query = append_date_filters(
                monthly_proses_query,
                params,
                start_date=start_date,
                end_date=end_date,
                date_column="l.proses_date",
            )
            monthly_first_borrow_query = append_date_filters(
                monthly_first_borrow_query,
                params,
                start_date=start_date,
                end_date=end_date,
                date_column="l.proses_date",
            )

        monthly_requests_query += " GROUP BY DATE_FORMAT(l.received_date, '%M %Y') ORDER BY MIN(l.received_date)"
        monthly_proses_query += " GROUP BY DATE_FORMAT(l.proses_date, '%M %Y') ORDER BY MIN(l.proses_date)"
        monthly_first_borrow_query += " GROUP BY DATE_FORMAT(l.proses_date, '%M %Y') ORDER BY MIN(l.proses_date)"

        monthly_requests_result = db.execute(text(monthly_requests_query), params)
        monthly_proses_result = db.execute(text(monthly_proses_query), params)
        monthly_first_borrow_result = db.execute(text(monthly_first_borrow_query), params)

        monthly_processed_data = {
            row[0]: row[1] for row in monthly_requests_result.fetchall() if row[0] is not None
        }
        monthly_approved_data = {}
        monthly_rejected_data = {}
        monthly_disbursed_data = {}
        for row in monthly_proses_result.fetchall():
            if row[0] is None:
                continue
            monthly_approved_data[row[0]] = row[1]
            monthly_rejected_data[row[0]] = row[2]
            monthly_disbursed_data[row[0]] = row[3]
        monthly_first_borrow_data = {
            row[0]: row[1] for row in monthly_first_borrow_result.fetchall() if row[0] is not None
        }

        # Combine all monthly data
        monthly_data = {}
        all_months = set(monthly_processed_data.keys()) | set(monthly_approved_data.keys()) | set(monthly_rejected_data.keys()) | set(monthly_disbursed_data.keys()) | set(monthly_first_borrow_data.keys())

        for month_year in all_months:
            total_loan_requests = monthly_processed_data.get(month_year, 0) or 0
            total_approved_requests = monthly_approved_data.get(month_year, 0) or 0
            total_rejected_requests = monthly_rejected_data.get(month_year, 0) or 0
            total_disbursed_amount = monthly_disbursed_data.get(month_year, 0) or 0
            total_first_borrow = monthly_first_borrow_data.get(month_year, 0) or 0
            month_range = _month_year_date_range(month_year)
            if month_range:
                range_start, range_end = month_range
            else:
                range_start, range_end = start_date, end_date
            total_eligible_employees = get_total_eligible_employees(
                db,
                start_date=range_start,
                end_date=range_end,
                employer_filter=employer_filter,
                sourced_to_filter=sourced_to_filter,
                project_filter=project_filter,
                client_segment_filter=client_segment_filter,
                product_type_filter=product_type_filter,
            )
            total_coverage_project = get_total_coverage_project(
                db,
                start_date=range_start,
                end_date=range_end,
                employer_filter=employer_filter,
                sourced_to_filter=sourced_to_filter,
                project_filter=project_filter,
                client_segment_filter=client_segment_filter,
                product_type_filter=product_type_filter,
            )
            penetration_rate = 0
            if total_eligible_employees > 0:
                penetration_rate = total_loan_requests / total_eligible_employees

            approval_rate = 0
            rejected_rate = 0
            total_processed_requests = total_approved_requests + total_rejected_requests
            if total_processed_requests > 0:
                approval_rate = total_approved_requests / total_processed_requests
                rejected_rate = total_rejected_requests / total_processed_requests

            monthly_data[month_year] = {
                "total_first_borrow": total_first_borrow,
                "total_loan_requests": total_loan_requests,
                "total_approved_requests": total_approved_requests,
                "total_rejected_requests": total_rejected_requests,
                "total_eligible_employees": total_eligible_employees,
                "total_coverage_project": total_coverage_project,
                "penetration_rate": penetration_rate,
                "approval_rate": approval_rate,
                "rejected_rate": rejected_rate,
                "total_disbursed_amount": total_disbursed_amount
            }

        return monthly_data

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "total_eligible_employees": 0,
            "total_coverage_project": 0,
            "total_loan_requests": 0,
            "penetration_rate": 0,
            "total_approved_requests": 0,
            "total_rejected_requests": 0,
            "approval_rate": 0,
            "total_new_borrowers": 0,
            "total_disbursed_amount": 0
        }


def _apply_installment_delinquency_override(
    db: Session,
    client_disbursements: list,
    counts_by_sourced_to: dict,
    *,
    loan_conditions: str,
    company_filter: str,
    client_segment_filter: str = None,
    product_type_filter: str = None,
    start_date: str = None,
    end_date: str = None,
) -> None:
    """Recompute delinquent_requests/total_unrecovered_payment/delinquency_rate for
    extradana/aku_cicil/installment loan_type using td_loan_history (per-installment
    due_date/status) instead of td_loan (proses_date/loan_status), and add any client
    (sourced_to/project) that only shows up via an overdue installment in this period.
    See get_client_summary for why this is scoped separately from disbursement metrics."""

    loan_conditions_tl = loan_conditions.replace('l.', 'tl.')
    params: dict = {}

    # Net out partial payments already recorded against this specific installment (via
    # td_loan_payment / td_loan_payment_allocation) — an installment can be status = 4
    # (overdue) while part of its `monthly` amount has already been paid. Mirrors the
    # netting in get_karyawan_overdue_summary / _UNRECOVERED_INSTALLMENT_PAYMENT_SQL.
    _installment_paid_subquery = """(
        SELECT COALESCE(SUM(amt), 0) FROM (
            SELECT p.amount amt FROM td_loan_payment p
            WHERE p.loan_id = tl.id AND p.status = 1 AND p.loan_history_id = tlh.id
              AND NOT EXISTS (SELECT 1 FROM td_loan_payment_allocation a WHERE a.payment_id = p.id)
            UNION ALL
            SELECT a.amount FROM td_loan_payment_allocation a
            INNER JOIN td_loan_payment p ON p.id = a.payment_id
            WHERE p.loan_id = tl.id AND p.status = 1 AND a.loan_history_id = tlh.id
        ) t
    )"""
    _installment_remaining_payment = f"GREATEST(tlh.monthly - {_installment_paid_subquery}, 0)"

    query = f"""
    SELECT
        src.keterangan as sourced_to,
        prj.keterangan as project,
        COUNT(DISTINCT CASE WHEN tlh.status = 4 THEN tlh.loan_form_id END) as delinquent_requests,
        SUM(CASE WHEN tlh.status IN (1, 4) THEN {_installment_remaining_payment} ELSE 0 END) as total_unrecovered_payment,
        SUM(CASE WHEN tlh.status IN (1, 2, 4) THEN {_installment_remaining_payment} ELSE 0 END) as denom
    FROM td_loan_history tlh
    INNER JOIN td_loan tl ON tlh.loan_form_id = tl.id
    LEFT JOIN td_karyawan tk
        ON tl.id_karyawan = tk.id_karyawan
    LEFT JOIN tbl_gmc emp
        ON tk.valdo_inc = emp.kode_gmc
        AND emp.group_gmc = 'sub_client'
        AND emp.aktif = 'Yes'
        AND emp.keterangan3 = 1
    LEFT JOIN tbl_gmc src
        ON tk.placement = src.kode_gmc
        AND src.group_gmc = 'placement_client'
        AND src.aktif = 'Yes'
        AND src.keterangan3 = 1
    LEFT JOIN tbl_gmc prj
        ON tk.project = prj.kode_gmc
        AND prj.group_gmc = 'client_project'
        AND prj.aktif = 'Yes'
        AND prj.keterangan3 = 1
    WHERE tlh.due_date IS NOT NULL
    AND {loan_conditions_tl}
    AND src.keterangan IS NOT NULL
    AND emp.keterangan IN {company_filter}
    """

    query = _apply_project_management_filters(
        query, params, client_segment_filter, product_type_filter, db=db
    )

    if start_date and end_date:
        query = append_date_filters(
            query,
            params,
            start_date=start_date,
            end_date=end_date,
            date_column="tlh.due_date",
        )

    query += " GROUP BY src.keterangan, prj.keterangan"

    rows = db.execute(text(query), params).fetchall()

    index_by_key = {
        f"{row['sourced_to']}_{row['project']}": row for row in client_disbursements
    }

    for sourced_to, project, delinquent_requests, unrecovered, denom in rows:
        sourced_to = sourced_to if sourced_to else "Unknown"
        project = project if project else "Unknown"
        key = f"{sourced_to}_{project}"

        delinquent_requests = int(delinquent_requests) if delinquent_requests else 0
        unrecovered = float(unrecovered) if unrecovered else 0.0
        denom = float(denom) if denom else 0.0
        delinquency_rate = (unrecovered / denom) if denom > 0 else 0.0

        existing = index_by_key.get(key)
        if existing is not None:
            existing["delinquent_requests"] = delinquent_requests
            existing["total_unrecovered_payment"] = unrecovered
            existing["delinquency_rate"] = delinquency_rate
            existing["admin_fee_profit"] = existing["total_admin_fee_collected"] - unrecovered
        else:
            employee_data = counts_by_sourced_to.get(sourced_to, {"eligible": 0, "active": 0})
            new_row = {
                "sourced_to": sourced_to,
                "project": project,
                "total_disbursement": 0,
                "total_requests": 0,
                "approved_requests": 0,
                "delinquent_requests": delinquent_requests,
                "eligible_employees": employee_data["eligible"],
                "active_employees": employee_data["active"],
                "eligible_rate": (employee_data["eligible"] / employee_data["active"]) if employee_data["active"] > 0 else 0,
                "penetration_rate": 0,
                "total_admin_fee_collected": 0,
                "total_unrecovered_payment": unrecovered,
                "admin_fee_profit": -unrecovered,
                "delinquency_rate": delinquency_rate,
            }
            client_disbursements.append(new_row)
            index_by_key[key] = new_row


def get_client_summary(db: Session, start_date: str = None, end_date: str = None, loan_type: str = "kasbon",
                       client_segment_filter: str = None, product_type_filter: str = None) -> list:
    """Get comprehensive client summary with disbursement and other metrics"""

    try:
        loan_conditions = resolve_loan_conditions(loan_type, db)
        company_filter = COMPANY_FILTER

        # Installment products (extradana/aku_cicil/installment) are billed month-by-month via
        # td_loan_history rather than the single td_loan row. A loan can be disbursed (proses_date)
        # in one month while one of its later installments (due_date) becomes overdue in a
        # completely different month. The disbursement metrics below intentionally stay scoped to
        # td_loan/proses_date (when was the loan requested), but delinquency must instead be scoped
        # to which installment is actually due/overdue within [start_date, end_date] — otherwise a
        # loan disbursed in-period but overdue on a later month is wrongly counted here, while a
        # client whose installment is overdue *this* period (disbursed earlier) is wrongly dropped.
        # This mirrors get_karyawan_overdue_summary's td_loan_history/due_date handling.
        needs_installment_delinquency = loan_type in ("extradana", "aku_cicil", "installment")

        # Build parameters dict for filters (needed for both queries)
        params = {}

        # Get employee counts using the exact same approach as coverage utilization
        # For each sourced_to and project combination, we'll run the same query as coverage utilization
        employee_counts = {}

        # Keyed only by sourced_to (not project) — reused for any client added by the
        # installment delinquency override below, which may not appear in combinations_query.
        counts_by_sourced_to = {}

        # Get unique sourced_to and project combinations from the loan data first
        combinations_query = f"""
        SELECT DISTINCT
            src.keterangan as sourced_to,
            prj.keterangan as project
        FROM td_loan l
        LEFT JOIN td_karyawan tk
            ON l.id_karyawan = tk.id_karyawan
        LEFT JOIN tbl_gmc emp
            ON tk.valdo_inc = emp.kode_gmc
            AND emp.group_gmc = 'sub_client'
            AND emp.aktif = 'Yes'
            AND emp.keterangan3 = 1
        LEFT JOIN tbl_gmc src
            ON tk.placement = src.kode_gmc
            AND src.group_gmc = 'placement_client'
            AND src.aktif = 'Yes'
            AND src.keterangan3 = 1
        LEFT JOIN tbl_gmc prj
            ON tk.project = prj.kode_gmc
            AND prj.group_gmc = 'client_project'
            AND prj.aktif = 'Yes'
            AND prj.keterangan3 = 1
        WHERE {loan_conditions}
        AND src.keterangan IS NOT NULL
        AND emp.keterangan IN {company_filter}
        """

        if start_date and end_date:
            combinations_query = append_date_filters(
                combinations_query,
                params,
                start_date=start_date,
                end_date=end_date,
            )

        combinations_query = _apply_project_management_filters(
            combinations_query, params, client_segment_filter, product_type_filter, db=db
        )

        try:
            combinations_result = db.execute(text(combinations_query), params)
            combinations = combinations_result.fetchall()

            counts_by_sourced_to = _fetch_employee_counts_by_sourced_to(db, company_filter)
            for combo in combinations:
                sourced_to = combo[0] if combo[0] else "Unknown"
                project = combo[1] if combo[1] else "Unknown"
                key = f"{sourced_to}_{project}"
                employee_counts[key] = counts_by_sourced_to.get(
                    sourced_to,
                    {"eligible": 0, "active": 0},
                )

        except Exception:
            # Fallback: return empty employee counts
            employee_counts = {}

        # Build the main loan summary query (without correlated subqueries)
        client_summary_query = """
        SELECT
            src.keterangan as sourced_to,
            prj.keterangan as project,
            SUM(CASE WHEN l.loan_status IN (1, 2, 4) THEN l.total_loan ELSE 0 END) as total_disbursement,
            COUNT(CASE WHEN l.loan_status IN (1, 2, 3, 4) THEN 1 END) as total_requests,
            COUNT(CASE WHEN l.loan_status IN (1, 2, 4) THEN 1 END) as approved_requests,
            COUNT(CASE WHEN l.loan_status IN (1, 4) THEN 1 END) as delinquent_requests,
            SUM(CASE WHEN l.loan_status = 2 THEN l.admin_fee ELSE 0 END) as total_admin_fee_collected,
            SUM(CASE WHEN l.loan_status IN (1, 4) THEN l.total_payment ELSE 0 END) as total_unrecovered_payment,
            CASE
                WHEN SUM(CASE WHEN l.loan_status IN (1, 2, 4) THEN l.total_payment ELSE 0 END) > 0
                THEN SUM(CASE WHEN l.loan_status IN (1, 4) THEN l.total_payment ELSE 0 END) / SUM(CASE WHEN l.loan_status IN (1, 2, 4) THEN l.total_payment ELSE 0 END)
                ELSE 0
            END as delinquency_rate,
            COUNT(DISTINCT CASE WHEN l.loan_status IN (1, 2, 3, 4) THEN l.id_karyawan END) as unique_requesting_employees
        FROM td_loan l
        LEFT JOIN td_karyawan tk
            ON l.id_karyawan = tk.id_karyawan
        LEFT JOIN tbl_gmc emp
            ON tk.valdo_inc = emp.kode_gmc
            AND emp.group_gmc = 'sub_client'
            AND emp.aktif = 'Yes'
            AND emp.keterangan3 = 1
        LEFT JOIN tbl_gmc src
            ON tk.placement = src.kode_gmc
            AND src.group_gmc = 'placement_client'
            AND src.aktif = 'Yes'
            AND src.keterangan3 = 1
        LEFT JOIN tbl_gmc prj
            ON tk.project = prj.kode_gmc
            AND prj.group_gmc = 'client_project'
            AND prj.aktif = 'Yes'
            AND prj.keterangan3 = 1
        WHERE {loan_conditions}
        AND src.keterangan IS NOT NULL
        AND emp.keterangan IN {company_filter}
        """.format(loan_conditions=loan_conditions, company_filter=company_filter)

        client_summary_query = _apply_project_management_filters(
            client_summary_query, params, client_segment_filter, product_type_filter, db=db
        )

        if start_date and end_date:
            client_summary_query = append_date_filters(
                client_summary_query,
                params,
                start_date=start_date,
                end_date=end_date,
            )

        # Group by sourced_to and project
        client_summary_query += """
        GROUP BY src.keterangan, prj.keterangan
        ORDER BY src.keterangan, prj.keterangan
        """

        # Execute main query
        result = db.execute(text(client_summary_query), params)
        records = result.fetchall()

        # Format results
        client_disbursements = []
        for record in records:
            sourced_to = record[0] if record[0] else "Unknown"
            project = record[1] if record[1] else "Unknown"
            key = f"{sourced_to}_{project}"

            # Get employee counts from the pre-calculated dictionary
            employee_data = employee_counts.get(key, {"eligible": 0, "active": 0})

            client_disbursements.append({
                "sourced_to": sourced_to,
                "project": project,
                "total_disbursement": float(record[2]) if record[2] else 0,
                "total_requests": int(record[3]) if record[3] else 0,
                "approved_requests": int(record[4]) if record[4] else 0,
                "delinquent_requests": int(record[5]) if record[5] else 0,
                "eligible_employees": employee_data["eligible"],
                "active_employees": employee_data["active"],
                "eligible_rate": (employee_data["eligible"] / employee_data["active"]) if employee_data["active"] > 0 else 0,
                "penetration_rate": (int(record[9]) / employee_data["eligible"]) if employee_data["eligible"] > 0 else 0,  # unique_requesting_employees / eligible_employees
                "total_admin_fee_collected": float(record[6]) if record[6] else 0,
                "total_unrecovered_payment": float(record[7]) if record[7] else 0,
                "admin_fee_profit": (float(record[6]) if record[6] else 0) - (float(record[7]) if record[7] else 0),
                "delinquency_rate": float(record[8]) if record[8] else 0,
            })

        if needs_installment_delinquency:
            _apply_installment_delinquency_override(
                db,
                client_disbursements,
                counts_by_sourced_to,
                loan_conditions=loan_conditions,
                company_filter=company_filter,
                client_segment_filter=client_segment_filter,
                product_type_filter=product_type_filter,
                start_date=start_date,
                end_date=end_date,
            )

        return client_disbursements

    except Exception as e:
        import traceback
        traceback.print_exc()
        return []
