from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List

# Loan type constants
LOAN_CONDITIONS = "l.duration = 1 AND l.loan_id != 35"
EXTRADANA_LOAN_CONDITIONS = "l.duration != 1 AND l.disbursement != 4 AND l.loan_id != 35"
AKU_CICIL_CONDITION = "l.loan_id = 35"
KASBON_CONDITION = "l.duration = 1 AND l.loan_id != 35"  # Same as LOAN_CONDITIONS but will be used in td_loan_history context


def get_enhanced_karyawan(db: Session, limit: int = 1000000, 
                          employer_filter: str = None, sourced_to_filter: str = None, 
                          project_filter: str = None, id_karyawan_filter: int = None) -> List[dict]:
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
                             project_filter: str = None, id_karyawan_filter: int = None,
                             month_filter: int = None, year_filter: int = None) -> dict:
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
        if month_filter:
            processed_requests_query += " AND MONTH(l.proses_date) = :month"
            pending_requests_query += " AND MONTH(l.received_date) = :month"
            first_borrow_query += " AND MONTH(l.proses_date) = :month"
            approved_requests_query += " AND MONTH(l.proses_date) = :month"
            rejected_requests_query += " AND MONTH(l.proses_date) = :month"
            avg_approval_time_query += " AND MONTH(l.proses_date) = :month"
            total_disbursed_amount_query += " AND MONTH(l.proses_date) = :month"
            total_loans_query += " AND MONTH(l.proses_date) = :month"
            params['month'] = month_filter
        if year_filter:
            processed_requests_query += " AND YEAR(l.proses_date) = :year"
            pending_requests_query += " AND YEAR(l.received_date) = :year"
            first_borrow_query += " AND YEAR(l.proses_date) = :year"
            approved_requests_query += " AND YEAR(l.proses_date) = :year"
            rejected_requests_query += " AND YEAR(l.proses_date) = :year"
            avg_approval_time_query += " AND YEAR(l.proses_date) = :year"
            total_disbursed_amount_query += " AND YEAR(l.proses_date) = :year"
            total_loans_query += " AND YEAR(l.proses_date) = :year"
            params['year'] = year_filter

        
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
            "average_approval_time": 0,
            "penetration_rate": 0
        }


def get_requests_endpoint(db: Session, 
                         employer_filter: str = None, sourced_to_filter: str = None, 
                         project_filter: str = None, id_karyawan_filter: int = None,
                         month_filter: int = None, year_filter: int = None) -> dict:
    """Get requests metrics: total_approved_requests, total_rejected_requests, approval_rate, average_approval_time"""
    
    try:
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
        if month_filter:
            approved_requests_query += " AND MONTH(l.proses_date) = :month"
            rejected_requests_query += " AND MONTH(l.proses_date) = :month"
            total_processed_query += " AND MONTH(l.proses_date) = :month"
            avg_approval_time_query += " AND MONTH(l.proses_date) = :month"
            params['month'] = month_filter
        if year_filter:
            approved_requests_query += " AND YEAR(l.proses_date) = :year"
            rejected_requests_query += " AND YEAR(l.proses_date) = :year"
            total_processed_query += " AND YEAR(l.proses_date) = :year"
            avg_approval_time_query += " AND YEAR(l.proses_date) = :year"
            params['year'] = year_filter

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
        

        
        return {
            "total_approved_requests": total_approved,
            "total_rejected_requests": total_rejected,
            "approval_rate": approval_rate,
            "average_approval_time": avg_approval_time
        }
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "total_approved_requests": 0,
            "total_rejected_requests": 0,
            "approval_rate": 0,
            "average_approval_time": 0
        }


def get_disbursement_endpoint(db: Session, 
                             employer_filter: str = None, sourced_to_filter: str = None, 
                             project_filter: str = None, id_karyawan_filter: int = None,
                             month_filter: int = None, year_filter: int = None) -> dict:
    """Get disbursement metrics: total_disbursed_amount, average_disbursed_amount"""
    
    try:
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
        if month_filter:
            total_disbursed_amount_query += " AND MONTH(l.proses_date) = :month"
            total_loans_query += " AND MONTH(l.proses_date) = :month"
            params['month'] = month_filter
        if year_filter:
            total_disbursed_amount_query += " AND YEAR(l.proses_date) = :year"
            total_loans_query += " AND YEAR(l.proses_date) = :year"
            params['year'] = year_filter

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


def get_disbursement_monthly_endpoint(db: Session, start_date: str, end_date: str,
                                    employer_filter: str = None, sourced_to_filter: str = None, 
                                    project_filter: str = None, id_karyawan_filter: int = None) -> dict:
    """Get disbursement monthly data: total disbursed amount and average disbursed amount by month"""
    
    try:
        # Build the monthly disbursement query
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
                           project_filter: str = None, loan_status_filter: int = None,
                           id_karyawan_filter: int = None) -> List[dict]:
    """Get loans data with enhanced karyawan information"""
    
    try:
        # First, let's check if the loans table exists and see its structure

        try:
            table_check = db.execute(text("SHOW TABLES LIKE 'td_loan'"))
            if not table_check.fetchone():
        
                return []
    
        except Exception as e:
    
            return []
        
        # Check loans table structure
        try:
            structure_result = db.execute(text("DESCRIBE td_loan"))
            columns = structure_result.fetchall()
            
        except Exception as e:
            pass
        
        # Check if td_karyawan has the required columns for joining
        try:
            karyawan_structure = db.execute(text("DESCRIBE td_karyawan"))
            karyawan_columns = karyawan_structure.fetchall()

        except Exception as e:
            pass
        
        # Build the base query with joins to get karyawan and codes data
        base_query = """
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
            prj.keterangan AS project_name
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
            
        if loan_status_filter is not None:
            base_query += " AND l.loan_status = :loan_status"
            params['loan_status'] = loan_status_filter
        
        # Add limit
        base_query += f" LIMIT {limit}"
        

        
        # Execute the query
        result = db.execute(text(base_query), params)
        records = result.fetchall()
        

        
        # Convert to list of dictionaries
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
                "project_name": record[20]
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
        if loan_type == "extradana":
            # For extradana, include all three companies (based on the example query)
            company_filter = "('PT Valdo Sumber Daya Mandiri', 'PT Valdo International', 'PT Toko Pandai')"
        elif loan_type == "aku_cicil":
            # For aku_cicil, include all three companies
            company_filter = "('PT Valdo Sumber Daya Mandiri', 'PT Valdo International', 'PT Toko Pandai')"
        else:
            # For loan, include all three companies
            company_filter = "('PT Valdo Sumber Daya Mandiri', 'PT Valdo International', 'PT Toko Pandai')"
            
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
        

        
        return {
            "employers": employers,
            "placements": placements,
            "projects": projects
        }
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "employers": [],
            "placements": [],
            "projects": []
        }


def get_loan_fees_summary(db: Session, 
                          employer_filter: str = None, sourced_to_filter: str = None, 
                          project_filter: str = None, loan_status_filter: int = None,
                          id_karyawan_filter: int = None, month_filter: int = None,
                          year_filter: int = None) -> dict:
    """Get loan fees summary (total expected and collected admin fees)"""
    
    try:
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
            
        if loan_status_filter is not None:
            fees_query += " AND l.loan_status = :loan_status"
            params['loan_status'] = loan_status_filter
            
        # Add month and year filters based on proses_date
        if month_filter is not None:
            fees_query += " AND MONTH(l.proses_date) = :month"
            params['month'] = month_filter
            
        if year_filter is not None:
            fees_query += " AND YEAR(l.proses_date) = :year"
            params['year'] = year_filter
        

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
                                  project_filter: str = None, loan_status_filter: int = None,
                                  id_karyawan_filter: int = None, start_date: str = None,
                                  end_date: str = None) -> dict:
    """Get loan fees summary separated by months within a date range"""
    
    try:
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
                          project_filter: str = None, loan_status_filter: int = None,
                          id_karyawan_filter: int = None, month_filter: int = None,
                          year_filter: int = None) -> dict:
    """Get loan risk summary with various risk metrics"""
    
    try:
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
            
        if loan_status_filter is not None:
            risk_query += " AND l.loan_status = :loan_status"
            params['loan_status'] = loan_status_filter
            
        # Add month and year filters based on proses_date
        if month_filter is not None:
            risk_query += " AND MONTH(l.proses_date) = :month"
            params['month'] = month_filter
            
        if year_filter is not None:
            risk_query += " AND YEAR(l.proses_date) = :year"
            params['year'] = year_filter
        
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
                                  project_filter: str = None, loan_status_filter: int = None,
                                  id_karyawan_filter: int = None, start_date: str = None,
                                  end_date: str = None) -> dict:
    """Get loan risk summary separated by months within a date range"""
    
    try:
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
                                 project_filter: str = None, loan_status_filter: int = None,
                                 id_karyawan_filter: int = None, month_filter: int = None,
                                 year_filter: int = None, loan_type: str = "loan") -> List[dict]:
    """Get karyawan data for those with overdue loans (status 4)"""
    
    try:
        # Determine loan conditions based on loan type
        if loan_type == "extradana":
            loan_conditions = EXTRADANA_LOAN_CONDITIONS
        elif loan_type == "aku_cicil":
            loan_conditions = AKU_CICIL_CONDITION
        elif loan_type == "kasbon":
            loan_conditions = KASBON_CONDITION
        else:
            loan_conditions = LOAN_CONDITIONS  # default
        
        # For kasbon and default, use td_loan table directly (like the old "loan" type)
        # For extradana and aku_cicil, use td_loan_history table
        if loan_type == "kasbon" or loan_type not in ["extradana", "aku_cicil", "kasbon"]:
            # Use td_loan table directly for kasbon
            overdue_query = """
            SELECT DISTINCT
                tk.id_karyawan,
                tk.ktp AS ktp,
                tk.nama AS name,
                emp.keterangan AS company,
                src.keterangan AS sourced_to,
                prj.keterangan AS project,
                SUM(l.total_loan) as total_amount_owed,
                MAX(l.repayment_date) as repayment_date,
                SUM(l.admin_fee) as total_admin_fee,
                SUM(l.total_payment) as total_payment
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
            WHERE l.loan_status = 4
            AND l.id_karyawan IS NOT NULL
            AND {loan_conditions}
            """.format(loan_conditions=loan_conditions)
        else:
            # For extradana and aku_cicil, use td_loan_history table
            # Adapt loan_conditions for td_loan_history context by replacing l. with tl.
            loan_conditions_tl = loan_conditions.replace('l.', 'tl.')
            
            overdue_query = """
            SELECT DISTINCT
                tk.id_karyawan,
                tk.ktp AS ktp,
                tk.nama AS name,
                emp.keterangan AS company,
                src.keterangan AS sourced_to,
                prj.keterangan AS project,
                SUM(ROUND(tl.total_loan / tl.duration, 0)) as total_amount_owed,
                MAX(tlh.due_date) as repayment_date,
                SUM(ROUND(tl.admin_fee / tl.duration, 0)) as total_admin_fee,
                SUM(tlh.monthly) as total_payment
            FROM td_loan_history tlh
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
        
        # Determine if using td_loan (kasbon/default) or td_loan_history (extradana/aku_cicil)
        use_td_loan = loan_type == "kasbon" or loan_type not in ["extradana", "aku_cicil", "kasbon"]
        
        # Add filters
        if id_karyawan_filter:
            if use_td_loan:
                overdue_query += " AND l.id_karyawan = :id_karyawan"
            else:
                overdue_query += " AND tl.id_karyawan = :id_karyawan"
            params['id_karyawan'] = id_karyawan_filter
            
        # Restrict to only PT Valdo companies
        company_filter = "('PT Valdo Sumber Daya Mandiri', 'PT Valdo International', 'PT Toko Pandai')"
        overdue_query += f" AND emp.keterangan IN {company_filter}"
        
        # If employer_filter is provided and it's one of the allowed companies, filter further
        if employer_filter and employer_filter in ['PT Valdo Sumber Daya Mandiri', 'PT Valdo International', 'PT Toko Pandai']:
            overdue_query += " AND emp.keterangan = :employer"
            params['employer'] = employer_filter
            
        if sourced_to_filter:
            overdue_query += " AND src.keterangan = :sourced_to"
            params['sourced_to'] = sourced_to_filter
            
        if project_filter:
            overdue_query += " AND prj.keterangan = :project"
            params['project'] = project_filter
            
        if loan_status_filter is not None:
            if use_td_loan:
                overdue_query += " AND l.loan_status = :loan_status"
            else:
                overdue_query += " AND tlh.status = :loan_status"
            params['loan_status'] = loan_status_filter
            
        # Add month and year filters
        if month_filter is not None and year_filter is not None:
            import calendar
            start_date = f"{year_filter}-{month_filter:02d}-01"
            
            if use_td_loan:
                # For kasbon/default, filter by proses_date in td_loan using range format
                last_day = calendar.monthrange(year_filter, month_filter)[1]
                end_date = f"{year_filter}-{month_filter:02d}-{last_day:02d}"
                overdue_query += " AND l.proses_date >= :start_date"
                overdue_query += " AND l.proses_date <= :end_date"
                params['start_date'] = start_date
                params['end_date'] = end_date
            else:
                # For extradana and aku_cicil, filter by due_date in td_loan_history
                if month_filter == 12:
                    next_month_date = f"{year_filter + 1}-01-01"
                else:
                    next_month_date = f"{year_filter}-{month_filter + 1:02d}-01"
                overdue_query += " AND tlh.due_date >= :start_date"
                overdue_query += " AND tlh.due_date < :next_month_date"
                params['start_date'] = start_date
                params['next_month_date'] = next_month_date
        
        # Group by karyawan and order by total amount owed (descending)
        overdue_query += """
        GROUP BY tk.id_karyawan, tk.nama, tk.ktp, emp.keterangan, src.keterangan, prj.keterangan
        ORDER BY total_amount_owed DESC
        """
        

        
        # Execute the query
        result = db.execute(text(overdue_query), params)
        records = result.fetchall()
        
        # Convert to list of dictionaries
        overdue_list = []
        for record in records:
            # Skip records with NULL id_karyawan (shouldn't happen due to WHERE clause, but just in case)
            if record[0] is None:
                continue
                
            # Calculate days overdue
            days_overdue = 0
            if record[7] is not None:  # repayment_date
                from datetime import datetime, date
                try:
                    repayment_date = record[7]
                    if isinstance(repayment_date, str):
                        repayment_date = datetime.strptime(repayment_date, '%Y-%m-%d').date()
                    elif hasattr(repayment_date, 'date'):
                        repayment_date = repayment_date.date()
                    else:
                        repayment_date = repayment_date
                    
                    today = date.today()
                    days_overdue = (today - repayment_date).days
                except:
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
                            project_filter: str = None, loan_status_filter: int = None,
                            id_karyawan_filter: int = None, month_filter: int = None, 
                                 year_filter: int = None, loan_type: str = "loan") -> List[dict]:
    """Get loan summary grouped by purpose with filters"""
    

    try:
        # Determine loan conditions based on loan type
        if loan_type == "loan":
            loan_conditions = LOAN_CONDITIONS
        elif loan_type == "extradana":
            loan_conditions = EXTRADANA_LOAN_CONDITIONS
        else:
            loan_conditions = LOAN_CONDITIONS  

        # Build the query to analyze loans by purpose
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
        
        # Build parameters dict for filters
        params = {}
        
        # Add filters
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
            
        if loan_status_filter is not None:
            purpose_query += " AND l.loan_status = :loan_status"
            params['loan_status'] = loan_status_filter
            
        # Add month and year filters based on proses_date
        if month_filter is not None:
            purpose_query += " AND MONTH(l.proses_date) = :month"
            params['month'] = month_filter
            
        if year_filter is not None:
            purpose_query += " AND YEAR(l.proses_date) = :year"
            params['year'] = year_filter
        
        # Group by purpose and order by total amount (descending)
        purpose_query += """
        GROUP BY lp.id, lp.purpose
        ORDER BY total_amount DESC
        """
        
        
        # Execute the query
        result = db.execute(text(purpose_query), params)
        records = result.fetchall()
        
        # Convert to list of dictionaries
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


def get_total_admin_fee_collected(db: Session, 
                                 employer_filter: str = None, sourced_to_filter: str = None, 
                                 project_filter: str = None, loan_status_filter: int = None,
                                 id_karyawan_filter: int = None, month_filter: int = None,
                                 year_filter: int = None, loan_type: str = "loan") -> float:
    """Get total admin fee collected amount based on loan type"""
    
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
            
        else:  # extradana or aku_cicil
            # For extradana and aku_cicil, use td_loan_history table with monthly admin fee calculation
            # Note: td_loan is aliased as 'l' in this query, so loan_conditions with 'l.' prefix work correctly
            # Special handling for extradana - use loan_setting table
            if loan_type == "extradana":
                loan_conditions_tl = "l.loan_id IN (SELECT ls.id FROM loan_setting ls WHERE ls.loan_type LIKE 'Extradana%')"
            else:
                # For aku_cicil, loan_conditions already use 'l.' prefix which matches the alias
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
        
        # Add filters
        if id_karyawan_filter:
            admin_fee_collected_query += " AND l.id_karyawan = :id_karyawan"
            params['id_karyawan'] = id_karyawan_filter
            
        # Restrict to only PT Valdo companies (conditional based on loan type)
        if loan_type == "extradana":
            # For extradana, include all three companies (based on the example query)
            company_filter = "('PT Valdo Sumber Daya Mandiri', 'PT Valdo International', 'PT Toko Pandai')"
        elif loan_type == "aku_cicil":
            # For aku_cicil, include all three companies
            company_filter = "('PT Valdo Sumber Daya Mandiri', 'PT Valdo International', 'PT Toko Pandai')"
        else:
            # For loan, include all three companies
            company_filter = "('PT Valdo Sumber Daya Mandiri', 'PT Valdo International', 'PT Toko Pandai')"
            
        admin_fee_collected_query += f" AND emp.keterangan IN {company_filter}"
        
        # If employer_filter is provided and it's one of the allowed companies, filter further
        if employer_filter and employer_filter in ['PT Valdo Sumber Daya Mandiri', 'PT Valdo International', 'PT Toko Pandai']:
            admin_fee_collected_query += " AND emp.keterangan = :employer"
            params['employer'] = employer_filter
            
        if sourced_to_filter:
            admin_fee_collected_query += " AND src.keterangan = :sourced_to"
            params['sourced_to'] = sourced_to_filter
            
        if project_filter:
            admin_fee_collected_query += " AND prj.keterangan = :project"
            params['project'] = project_filter
            
        if loan_status_filter is not None:
            admin_fee_collected_query += " AND l.loan_status = :loan_status"
            params['loan_status'] = loan_status_filter
            
        # Add month and year filters based on due_date for extradana, proses_date for loan
        if month_filter is not None and year_filter is not None:
            import calendar
            start_date = f"{year_filter}-{month_filter:02d}-01"
            # For extradana, use < next month format like the example
            if month_filter == 12:
                next_month_date = f"{year_filter + 1}-01-01"
            else:
                next_month_date = f"{year_filter}-{month_filter + 1:02d}-01"

            if loan_type == "extradana":
                # For extradana, filter by due_date in td_loan_history using the example format
                admin_fee_collected_query += " AND tlh.due_date >= :start_date"
                admin_fee_collected_query += " AND tlh.due_date < :next_month_date"
                params['start_date'] = start_date
                params['next_month_date'] = next_month_date
            elif loan_type == "aku_cicil":
                # For aku_cicil, filter by due_date in td_loan_history using the example format
                admin_fee_collected_query += " AND tlh.due_date >= :start_date"
                admin_fee_collected_query += " AND tlh.due_date < :next_month_date"
                params['start_date'] = start_date
                params['next_month_date'] = next_month_date
            else:
                # For loan, filter by proses_date in td_loan using range format
                last_day = calendar.monthrange(year_filter, month_filter)[1]
                end_date = f"{year_filter}-{month_filter:02d}-{last_day:02d}"
                admin_fee_collected_query += " AND l.proses_date >= :start_date"
                admin_fee_collected_query += " AND l.proses_date <= :end_date"
                params['start_date'] = start_date
                params['end_date'] = end_date
        
        # Execute the query
        result = db.execute(text(admin_fee_collected_query), params)
        record = result.fetchone()
        
        # Extract the value (handle None values)
        total_admin_fee_collected = record[0] if record[0] is not None else 0
        
        return total_admin_fee_collected
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return 0


def get_total_loan_principal_collected(db: Session, 
                                        employer_filter: str = None, sourced_to_filter: str = None, 
                                        project_filter: str = None, loan_status_filter: int = None,
                                        id_karyawan_filter: int = None, month_filter: int = None,
                                        year_filter: int = None, loan_type: str = "loan") -> float:
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
        if loan_type == "extradana":
            # For extradana, include all three companies (based on the example query)
            company_filter = "('PT Valdo Sumber Daya Mandiri', 'PT Valdo International', 'PT Toko Pandai')"
        elif loan_type == "aku_cicil":
            # For aku_cicil, include all three companies
            company_filter = "('PT Valdo Sumber Daya Mandiri', 'PT Valdo International', 'PT Toko Pandai')"
        else:
            # For loan, include all three companies
            company_filter = "('PT Valdo Sumber Daya Mandiri', 'PT Valdo International', 'PT Toko Pandai')"
            
        principal_collected_query += f" AND emp.keterangan IN {company_filter}"
        
        # If employer_filter is provided and it's one of the allowed companies, filter further
        if employer_filter and employer_filter in ['PT Valdo Sumber Daya Mandiri', 'PT Valdo International', 'PT Toko Pandai']:
            principal_collected_query += " AND emp.keterangan = :employer"
            params['employer'] = employer_filter
            
        if sourced_to_filter:
            principal_collected_query += " AND src.keterangan = :sourced_to"
            params['sourced_to'] = sourced_to_filter
            
        if project_filter:
            principal_collected_query += " AND prj.keterangan = :project"
            params['project'] = project_filter
            
        if loan_status_filter is not None:
            principal_collected_query += " AND l.loan_status = :loan_status"
            params['loan_status'] = loan_status_filter
            
        # Add month and year filters based on due_date for extradana, proses_date for loan
        if month_filter is not None and year_filter is not None:
            import calendar
            start_date = f"{year_filter}-{month_filter:02d}-01"
            # For extradana, use < next month format like the example
            if month_filter == 12:
                next_month_date = f"{year_filter + 1}-01-01"
            else:
                next_month_date = f"{year_filter}-{month_filter + 1:02d}-01"

            if loan_type == "extradana":
                # For extradana, filter by due_date in td_loan_history using the example format
                principal_collected_query += " AND tlh.due_date >= :start_date"
                principal_collected_query += " AND tlh.due_date < :next_month_date"
                params['start_date'] = start_date
                params['next_month_date'] = next_month_date
            elif loan_type == "aku_cicil":
                # For aku_cicil, filter by due_date in td_loan_history using the example format
                principal_collected_query += " AND tlh.due_date >= :start_date"
                principal_collected_query += " AND tlh.due_date < :next_month_date"
                params['start_date'] = start_date
                params['next_month_date'] = next_month_date
            else:
                # For loan, filter by proses_date in td_loan using range format
                last_day = calendar.monthrange(year_filter, month_filter)[1]
                end_date = f"{year_filter}-{month_filter:02d}-{last_day:02d}"
                principal_collected_query += " AND l.proses_date >= :start_date"
                principal_collected_query += " AND l.proses_date <= :end_date"
                params['start_date'] = start_date
                params['end_date'] = end_date
        
        # Execute the query
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
                          project_filter: str = None, loan_status_filter: int = None,
                          id_karyawan_filter: int = None, month_filter: int = None,
                                        year_filter: int = None, loan_type: str = "loan") -> float:
    """Get expected repayment amount based on loan type"""
    
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
            expected_repayment_query = """
            SELECT SUM(CASE WHEN l.loan_status IN (1, 2, 4) THEN l.total_payment ELSE 0 END) as total_expected_repayment
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
            # For extradana and aku_cicil, use td_loan_history table with due_date and monthly sum
            # Note: td_loan is aliased as 'l' in this query, so loan_conditions with 'l.' prefix work correctly
            # Special handling for extradana - use loan_setting table
            if loan_type == "extradana":
                loan_conditions_tl = "l.loan_id IN (SELECT ls.id FROM loan_setting ls WHERE ls.loan_type LIKE 'Extradana%')"
            else:
                # For aku_cicil, loan_conditions already use 'l.' prefix which matches the alias
                loan_conditions_tl = loan_conditions
            
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
        
        # Add filters
        if id_karyawan_filter:
            expected_repayment_query += " AND l.id_karyawan = :id_karyawan"
            params['id_karyawan'] = id_karyawan_filter
            
        # Restrict to only PT Valdo companies (conditional based on loan type)
        if loan_type == "extradana":
            # For extradana, include all three companies (based on the example query)
            company_filter = "('PT Valdo Sumber Daya Mandiri', 'PT Valdo International', 'PT Toko Pandai')"
        elif loan_type == "aku_cicil":
            # For aku_cicil, include all three companies
            company_filter = "('PT Valdo Sumber Daya Mandiri', 'PT Valdo International', 'PT Toko Pandai')"
        else:
            # For loan, include all three companies
            company_filter = "('PT Valdo Sumber Daya Mandiri', 'PT Valdo International', 'PT Toko Pandai')"
            
        expected_repayment_query += f" AND emp.keterangan IN {company_filter}"
        
        # If employer_filter is provided and it's one of the allowed companies, filter further
        if employer_filter and employer_filter in ['PT Valdo Sumber Daya Mandiri', 'PT Valdo International', 'PT Toko Pandai']:
            expected_repayment_query += " AND emp.keterangan = :employer"
            params['employer'] = employer_filter
            
        if sourced_to_filter:
            expected_repayment_query += " AND src.keterangan = :sourced_to"
            params['sourced_to'] = sourced_to_filter
            
        if project_filter:
            expected_repayment_query += " AND prj.keterangan = :project"
            params['project'] = project_filter
            
        if loan_status_filter is not None:
            expected_repayment_query += " AND l.loan_status = :loan_status"
            params['loan_status'] = loan_status_filter
            
        # Add month and year filters based on due_date for extradana, proses_date for loan
        if month_filter is not None and year_filter is not None:
            import calendar
            start_date = f"{year_filter}-{month_filter:02d}-01"
            # For extradana, use < next month format like the example
            if month_filter == 12:
                next_month_date = f"{year_filter + 1}-01-01"
            else:
                next_month_date = f"{year_filter}-{month_filter + 1:02d}-01"

            if loan_type == "extradana":
                # For extradana, filter by due_date in td_loan_history using the example format
                expected_repayment_query += " AND tlh.due_date >= :start_date"
                expected_repayment_query += " AND tlh.due_date < :next_month_date"
                params['start_date'] = start_date
                params['next_month_date'] = next_month_date
            elif loan_type == "aku_cicil":
                # For aku_cicil, filter by due_date in td_loan_history using the example format
                expected_repayment_query += " AND tlh.due_date >= :start_date"
                expected_repayment_query += " AND tlh.due_date < :next_month_date"
                params['start_date'] = start_date
                params['next_month_date'] = next_month_date
            else:
                # For loan, filter by proses_date in td_loan using range format
                last_day = calendar.monthrange(year_filter, month_filter)[1]
                end_date = f"{year_filter}-{month_filter:02d}-{last_day:02d}"
                expected_repayment_query += " AND l.proses_date >= :start_date"
                expected_repayment_query += " AND l.proses_date <= :end_date"
                params['start_date'] = start_date
                params['end_date'] = end_date
        
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
                               project_filter: str = None, loan_status_filter: int = None,
                               id_karyawan_filter: int = None, month_filter: int = None,
                               year_filter: int = None, loan_type: str = "loan") -> dict:
    """Get repayment risk summary with various repayment and risk metrics"""
    
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
        
        # Build the query to calculate repayment risk metrics
        risk_query = """
        SELECT
            SUM(CASE WHEN l.loan_status IN (1, 2, 4) THEN l.total_payment ELSE 0 END) as total_expected_repayment,
            SUM(CASE WHEN l.loan_status = 2 THEN l.total_loan ELSE 0 END) as total_loan_principal_collected,
            SUM(CASE WHEN l.loan_status = 2 THEN l.admin_fee ELSE 0 END) as total_admin_fee_collected,
            SUM(CASE WHEN l.loan_status IN (4) THEN l.total_payment ELSE 0 END) as total_unrecovered_repayment,
            SUM(CASE WHEN l.loan_status IN (4) THEN l.total_loan ELSE 0 END) as total_unrecovered_loan_principal,
            SUM(CASE WHEN l.loan_status IN (4) THEN l.admin_fee ELSE 0 END) as total_unrecovered_admin_fee
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
        
        # Build parameters dict for filters
        params = {}
        
        # Add filters
        if id_karyawan_filter:
            risk_query += " AND l.id_karyawan = :id_karyawan"
            params['id_karyawan'] = id_karyawan_filter
            
        # Restrict to only PT Valdo companies (conditional based on loan type)
        if loan_type == "extradana":
            # For extradana, include all three companies (based on the example query)
            company_filter = "('PT Valdo Sumber Daya Mandiri', 'PT Valdo International', 'PT Toko Pandai')"
        elif loan_type == "aku_cicil":
            # For aku_cicil, include all three companies
            company_filter = "('PT Valdo Sumber Daya Mandiri', 'PT Valdo International', 'PT Toko Pandai')"
        else:
            # For loan, include all three companies
            company_filter = "('PT Valdo Sumber Daya Mandiri', 'PT Valdo International', 'PT Toko Pandai')"
            
        risk_query += f" AND emp.keterangan IN {company_filter}"
        
        # If employer_filter is provided and it's one of the allowed companies, filter further
        if employer_filter and employer_filter in ['PT Valdo Sumber Daya Mandiri', 'PT Valdo International', 'PT Toko Pandai']:
            risk_query += " AND emp.keterangan = :employer"
            params['employer'] = employer_filter
            
        if sourced_to_filter:
            risk_query += " AND src.keterangan = :sourced_to"
            params['sourced_to'] = sourced_to_filter
            
        if project_filter:
            risk_query += " AND prj.keterangan = :project"
            params['project'] = project_filter
            
        if loan_status_filter is not None:
            risk_query += " AND l.loan_status = :loan_status"
            params['loan_status'] = loan_status_filter
            
        # Add month and year filters based on proses_date
        if month_filter is not None:
            risk_query += " AND MONTH(l.proses_date) = :month"
            params['month'] = month_filter
            
        if year_filter is not None:
            risk_query += " AND YEAR(l.proses_date) = :year"
            params['year'] = year_filter
        
        # Execute the query
        result = db.execute(text(risk_query), params)
        record = result.fetchone()
        
        # Extract the values (handle None values)
        total_expected_repayment = record[0] if record[0] is not None else 0
        total_loan_principal_collected = record[1] if record[1] is not None else 0
        total_admin_fee_collected = record[2] if record[2] is not None else 0
        total_unrecovered_repayment = record[3] if record[3] is not None else 0
        total_unrecovered_loan_principal = record[4] if record[4] is not None else 0
        total_unrecovered_admin_fee = record[5] if record[5] is not None else 0
        
        # For extradana, override total_expected_repayment, total_loan_principal_collected, and total_admin_fee_collected with dedicated functions
        if loan_type == "extradana":
            total_expected_repayment = get_expected_repayment(
                db=db,
                employer_filter=employer_filter,
                sourced_to_filter=sourced_to_filter,
                project_filter=project_filter,
                loan_status_filter=loan_status_filter,
                id_karyawan_filter=id_karyawan_filter,
                month_filter=month_filter,
                year_filter=year_filter,
                loan_type=loan_type
            )
            total_loan_principal_collected = get_total_loan_principal_collected(
                db=db,
                employer_filter=employer_filter,
                sourced_to_filter=sourced_to_filter,
                project_filter=project_filter,
                loan_status_filter=loan_status_filter,
                id_karyawan_filter=id_karyawan_filter,
                month_filter=month_filter,
                year_filter=year_filter,
                loan_type=loan_type
            )
            total_admin_fee_collected = get_total_admin_fee_collected(
                db=db,
                employer_filter=employer_filter,
                sourced_to_filter=sourced_to_filter,
                project_filter=project_filter,
                loan_status_filter=loan_status_filter,
                id_karyawan_filter=id_karyawan_filter,
                month_filter=month_filter,
                year_filter=year_filter,
                loan_type=loan_type
            )
        elif loan_type == "aku_cicil":
            total_expected_repayment = get_expected_repayment(
                db=db,
                employer_filter=employer_filter,
                sourced_to_filter=sourced_to_filter,
                project_filter=project_filter,
                loan_status_filter=loan_status_filter,
                id_karyawan_filter=id_karyawan_filter,
                month_filter=month_filter,
                year_filter=year_filter,
                loan_type=loan_type
            )
            total_loan_principal_collected = get_total_loan_principal_collected(
                db=db,
                employer_filter=employer_filter,
                sourced_to_filter=sourced_to_filter,
                project_filter=project_filter,
                loan_status_filter=loan_status_filter,
                id_karyawan_filter=id_karyawan_filter,
                month_filter=month_filter,
                year_filter=year_filter,
                loan_type=loan_type
            )
            total_admin_fee_collected = get_total_admin_fee_collected(
                db=db,
                employer_filter=employer_filter,
                sourced_to_filter=sourced_to_filter,
                project_filter=project_filter,
                loan_status_filter=loan_status_filter,
                id_karyawan_filter=id_karyawan_filter,
                month_filter=month_filter,
                year_filter=year_filter,
                loan_type=loan_type
            )
        
        # For extradana and aku_cicil, calculate unrecovered amounts from td_loan_history
        if loan_type in ["extradana", "aku_cicil"]:
            # Adapt loan_conditions for td_loan_history context
            loan_conditions_tl = loan_conditions.replace('l.', 'tl.')
            if loan_type == "extradana":
                loan_conditions_tl = "tl.loan_id IN (SELECT ls.id FROM loan_setting ls WHERE ls.loan_type LIKE 'Extradana%')"
            
            # Build query for unrecovered amounts from td_loan_history
            unrecovered_query = """
            SELECT
                SUM(tlh.monthly) as total_unrecovered_repayment,
                SUM(ROUND(tl.total_loan / tl.duration, 0)) as total_unrecovered_loan_principal,
                SUM(ROUND(tl.admin_fee / tl.duration, 0)) as total_unrecovered_admin_fee
            FROM td_loan_history tlh
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
            
            # Add filters
            unrecovered_params = {}
            if id_karyawan_filter:
                unrecovered_query += " AND tl.id_karyawan = :id_karyawan"
                unrecovered_params['id_karyawan'] = id_karyawan_filter
            
            company_filter = "('PT Valdo Sumber Daya Mandiri', 'PT Valdo International', 'PT Toko Pandai')"
            unrecovered_query += f" AND emp.keterangan IN {company_filter}"
            
            if employer_filter and employer_filter in ['PT Valdo Sumber Daya Mandiri', 'PT Valdo International', 'PT Toko Pandai']:
                unrecovered_query += " AND emp.keterangan = :employer"
                unrecovered_params['employer'] = employer_filter
            
            if sourced_to_filter:
                unrecovered_query += " AND src.keterangan = :sourced_to"
                unrecovered_params['sourced_to'] = sourced_to_filter
            
            if project_filter:
                unrecovered_query += " AND prj.keterangan = :project"
                unrecovered_params['project'] = project_filter
            
            if loan_status_filter is not None:
                unrecovered_query += " AND tlh.status = :loan_status"
                unrecovered_params['loan_status'] = loan_status_filter
            
            # Add month and year filters based on due_date
            if month_filter is not None and year_filter is not None:
                start_date = f"{year_filter}-{month_filter:02d}-01"
                if month_filter == 12:
                    next_month_date = f"{year_filter + 1}-01-01"
                else:
                    next_month_date = f"{year_filter}-{month_filter + 1:02d}-01"
                unrecovered_query += " AND tlh.due_date >= :start_date"
                unrecovered_query += " AND tlh.due_date < :next_month_date"
                unrecovered_params['start_date'] = start_date
                unrecovered_params['next_month_date'] = next_month_date
            
            # Execute unrecovered query
            unrecovered_result = db.execute(text(unrecovered_query), unrecovered_params)
            unrecovered_record = unrecovered_result.fetchone()
            
            if unrecovered_record:
                total_unrecovered_repayment = unrecovered_record[0] if unrecovered_record[0] is not None else 0
                total_unrecovered_loan_principal = unrecovered_record[1] if unrecovered_record[1] is not None else 0
                total_unrecovered_admin_fee = unrecovered_record[2] if unrecovered_record[2] is not None else 0
        
        # Calculate derived metrics
        repayment_recovery_rate = 0
        if total_expected_repayment > 0:
            repayment_recovery_rate = (total_loan_principal_collected + total_admin_fee_collected) / total_expected_repayment
        
        delinquencies_rate = 0
        if total_expected_repayment > 0:
            delinquencies_rate = total_unrecovered_repayment / total_expected_repayment
        
        admin_fee_profit = total_admin_fee_collected - total_unrecovered_repayment
        
        return {
            "total_expected_repayment": total_expected_repayment,
            "total_loan_principal_collected": total_loan_principal_collected,
            "total_admin_fee_collected": total_admin_fee_collected,
            "total_unrecovered_repayment": total_unrecovered_repayment,
            "total_unrecovered_loan_principal": total_unrecovered_loan_principal,
            "total_unrecovered_admin_fee": total_unrecovered_admin_fee,
            "repayment_recovery_rate": repayment_recovery_rate,
            "delinquencies_rate": delinquencies_rate,
            "admin_fee_profit": admin_fee_profit
        }
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "total_expected_repayment": 0,
            "total_loan_principal_collected": 0,
            "total_admin_fee_collected": 0,
            "total_unrecovered_repayment": 0,
            "total_unrecovered_loan_principal": 0,
            "total_unrecovered_admin_fee": 0,
            "repayment_recovery_rate": 0,
            "delinquencies_rate": 0,
            "admin_fee_profit": 0
        }


def get_repayment_risk_monthly_summary(db: Session, 
                                       employer_filter: str = None, sourced_to_filter: str = None, 
                                       project_filter: str = None, loan_status_filter: int = None,
                                       id_karyawan_filter: int = None, start_date: str = None,
                                       end_date: str = None, loan_type: str = "loan") -> dict:
    """Get repayment risk summary separated by months within a date range"""
    
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
        
        # Build the query to calculate repayment risk metrics by month (excluding total_expected_repayment)
        risk_query = """
        SELECT
            DATE_FORMAT(l.proses_date, '%M %Y') as month_year,
            SUM(CASE WHEN l.loan_status = 2 THEN l.total_loan ELSE 0 END) as total_loan_principal_collected,
            SUM(CASE WHEN l.loan_status IN (1, 4) THEN l.total_payment ELSE 0 END) as total_unrecovered_repayment,
            SUM(CASE WHEN l.loan_status = 2 THEN l.admin_fee ELSE 0 END) as total_admin_fee_collected
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
            
        # Restrict to only PT Valdo companies (conditional based on loan type)
        if loan_type == "extradana":
            # For extradana, include all three companies (based on the example query)
            company_filter = "('PT Valdo Sumber Daya Mandiri', 'PT Valdo International', 'PT Toko Pandai')"
        elif loan_type == "aku_cicil":
            # For aku_cicil, include all three companies
            company_filter = "('PT Valdo Sumber Daya Mandiri', 'PT Valdo International', 'PT Toko Pandai')"
        else:
            # For loan, include all three companies
            company_filter = "('PT Valdo Sumber Daya Mandiri', 'PT Valdo International', 'PT Toko Pandai')"
            
        risk_query += f" AND emp.keterangan IN {company_filter}"
        
        # If employer_filter is provided and it's one of the allowed companies, filter further
        if employer_filter and employer_filter in ['PT Valdo Sumber Daya Mandiri', 'PT Valdo International', 'PT Toko Pandai']:
            risk_query += " AND emp.keterangan = :employer"
            params['employer'] = employer_filter
            
        if sourced_to_filter:
            risk_query += " AND src.keterangan = :sourced_to"
            params['sourced_to'] = sourced_to_filter
            
        if project_filter:
            risk_query += " AND prj.keterangan = :project"
            params['project'] = project_filter
            
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
                
            # Extract month and year from month_year string (e.g., "January 2025")
            month_name, year_str = month_year.split(' ')
            year = int(year_str)
            month_num = {
                'January': 1, 'February': 2, 'March': 3, 'April': 4, 'May': 5, 'June': 6,
                'July': 7, 'August': 8, 'September': 9, 'October': 10, 'November': 11, 'December': 12
            }[month_name]
            
            # Get total_expected_repayment and total_kasbon_principal_collected using the dedicated functions for this specific month
            total_expected_repayment = get_expected_repayment(
                db=db,
                employer_filter=employer_filter,
                sourced_to_filter=sourced_to_filter,
                project_filter=project_filter,
                loan_status_filter=loan_status_filter,
                id_karyawan_filter=id_karyawan_filter,
                month_filter=month_num,
                year_filter=year,
                loan_type=loan_type
            )
            
            # For extradana, use the dedicated functions; for kasbon, use the query results
            if loan_type == "extradana":
                total_loan_principal_collected = get_total_loan_principal_collected(
                    db=db,
                    employer_filter=employer_filter,
                    sourced_to_filter=sourced_to_filter,
                    project_filter=project_filter,
                    loan_status_filter=loan_status_filter,
                    id_karyawan_filter=id_karyawan_filter,
                    month_filter=month_num,
                    year_filter=year,
                    loan_type=loan_type
                )
                total_admin_fee_collected = get_total_admin_fee_collected(
                    db=db,
                    employer_filter=employer_filter,
                    sourced_to_filter=sourced_to_filter,
                    project_filter=project_filter,
                    loan_status_filter=loan_status_filter,
                    id_karyawan_filter=id_karyawan_filter,
                    month_filter=month_num,
                    year_filter=year,
                    loan_type=loan_type
                )
            elif loan_type == "aku_cicil":
                total_loan_principal_collected = get_total_loan_principal_collected(
                    db=db,
                    employer_filter=employer_filter,
                    sourced_to_filter=sourced_to_filter,
                    project_filter=project_filter,
                    loan_status_filter=loan_status_filter,
                    id_karyawan_filter=id_karyawan_filter,
                    month_filter=month_num,
                    year_filter=year,
                    loan_type=loan_type
                )
                total_admin_fee_collected = get_total_admin_fee_collected(
                    db=db,
                    employer_filter=employer_filter,
                    sourced_to_filter=sourced_to_filter,
                    project_filter=project_filter,
                    loan_status_filter=loan_status_filter,
                    id_karyawan_filter=id_karyawan_filter,
                    month_filter=month_num,
                    year_filter=year,
                    loan_type=loan_type
                )
            else:
                total_loan_principal_collected = record[1] if record[1] is not None else 0
                total_admin_fee_collected = record[3] if record[3] is not None else 0
            total_unrecovered_repayment = record[2] if record[2] is not None else 0
            
            # Calculate repayment recovery rate
            repayment_recovery_rate = 0
            if total_expected_repayment > 0:
                repayment_recovery_rate = (total_loan_principal_collected + total_admin_fee_collected) / total_expected_repayment
            
            # Calculate admin fee profit
            admin_fee_profit = total_admin_fee_collected - total_unrecovered_repayment
            
            monthly_data[month_year] = {
                "repayment_recovery_rate": repayment_recovery_rate,
                "total_expected_repayment": total_expected_repayment,
                "total_loan_principal_collected": total_loan_principal_collected,
                "total_unrecovered_repayment": total_unrecovered_repayment,
                "admin_fee_profit": admin_fee_profit
            }
        
        return monthly_data
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {}


def get_disbursed_amount(db: Session, 
                        employer_filter: str = None, sourced_to_filter: str = None, 
                        project_filter: str = None, loan_status_filter: int = None,
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
        if loan_type == "extradana":
            # For extradana, include all three companies (based on the example query)
            company_filter = "('PT Valdo Sumber Daya Mandiri', 'PT Valdo International', 'PT Toko Pandai')"
        elif loan_type == "aku_cicil":
            # For aku_cicil, include all three companies
            company_filter = "('PT Valdo Sumber Daya Mandiri', 'PT Valdo International', 'PT Toko Pandai')"
        else:
            # For loan, include all three companies
            company_filter = "('PT Valdo Sumber Daya Mandiri', 'PT Valdo International', 'PT Toko Pandai')"
            
        disbursed_query += f" AND emp.keterangan IN {company_filter}"
        
        # If employer_filter is provided and it's one of the allowed companies, filter further
        if employer_filter and employer_filter in ['PT Valdo Sumber Daya Mandiri', 'PT Valdo International', 'PT Toko Pandai']:
            disbursed_query += " AND emp.keterangan = :employer"
            params['employer'] = employer_filter
            
        if sourced_to_filter:
            disbursed_query += " AND src.keterangan = :sourced_to"
            params['sourced_to'] = sourced_to_filter
            
        if project_filter:
            disbursed_query += " AND prj.keterangan = :project"
            params['project'] = project_filter
            
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
                                    project_filter: str = None, loan_status_filter: int = None,
                                    id_karyawan_filter: int = None, month_filter: int = None,
                                    year_filter: int = None, loan_type: str = "loan") -> dict:
    """Get comprehensive coverage and utilization summary combining multiple metrics"""
    
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
        
        # Build the active employees count query (same as eligible but without loan_kasbon_eligible filter)
        active_count_query = """
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
        
        # Build the average approval time query (exactly as in get_user_coverage_summary)
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
        WHERE l.loan_status IN (1, 2, 4)
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
        
        # Build the total loans query (exactly as in get_user_coverage_summary)
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
        
        # Build the first-time borrowers query (using the same logic as get_user_coverage_summary)
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
            active_count_query += " AND tk.id_karyawan = :id_karyawan"
            processed_requests_query += " AND l.id_karyawan = :id_karyawan"
            approved_requests_query += " AND l.id_karyawan = :id_karyawan"
            rejected_requests_query += " AND l.id_karyawan = :id_karyawan"
            avg_approval_time_query += " AND l.id_karyawan = :id_karyawan"
            total_disbursed_amount_query += " AND l.id_karyawan = :id_karyawan"
            total_loans_query += " AND l.id_karyawan = :id_karyawan"
            first_borrow_query += " AND l.id_karyawan = :id_karyawan"
            params['id_karyawan'] = id_karyawan_filter
            
        # Restrict to only PT Valdo companies (conditional based on loan type)
        if loan_type == "extradana":
            # For extradana, include all three companies (based on the example query)
            company_filter = "('PT Valdo Sumber Daya Mandiri', 'PT Valdo International', 'PT Toko Pandai')"
        elif loan_type == "aku_cicil":
            # For aku_cicil, include all three companies
            company_filter = "('PT Valdo Sumber Daya Mandiri', 'PT Valdo International', 'PT Toko Pandai')"
        else:
            # For loan, include all three companies
            company_filter = "('PT Valdo Sumber Daya Mandiri', 'PT Valdo International', 'PT Toko Pandai')"
            
        eligible_count_query += f" AND emp.keterangan IN {company_filter}"
        active_count_query += f" AND emp.keterangan IN {company_filter}"
        processed_requests_query += f" AND emp.keterangan IN {company_filter}"
        approved_requests_query += f" AND emp.keterangan IN {company_filter}"
        rejected_requests_query += f" AND emp.keterangan IN {company_filter}"
        avg_approval_time_query += f" AND emp.keterangan IN {company_filter}"
        total_disbursed_amount_query += f" AND emp.keterangan IN {company_filter}"
        total_loans_query += f" AND emp.keterangan IN {company_filter}"
        first_borrow_query += f" AND emp.keterangan IN {company_filter}"
        
        # If employer_filter is provided and it's one of the allowed companies, filter further
        if employer_filter and employer_filter in ['PT Valdo Sumber Daya Mandiri', 'PT Valdo International', 'PT Toko Pandai']:
            eligible_count_query += " AND emp.keterangan = :employer"
            active_count_query += " AND emp.keterangan = :employer"
            processed_requests_query += " AND emp.keterangan = :employer"
            approved_requests_query += " AND emp.keterangan = :employer"
            rejected_requests_query += " AND emp.keterangan = :employer"
            avg_approval_time_query += " AND emp.keterangan = :employer"
            total_disbursed_amount_query += " AND emp.keterangan = :employer"
            total_loans_query += " AND emp.keterangan = :employer"
            first_borrow_query += " AND emp.keterangan = :employer"
            params['employer'] = employer_filter
            
        if sourced_to_filter:
            eligible_count_query += " AND src.keterangan = :sourced_to"
            active_count_query += " AND src.keterangan = :sourced_to"
            processed_requests_query += " AND src.keterangan = :sourced_to"
            approved_requests_query += " AND src.keterangan = :sourced_to"
            rejected_requests_query += " AND src.keterangan = :sourced_to"
            avg_approval_time_query += " AND src.keterangan = :sourced_to"
            total_disbursed_amount_query += " AND src.keterangan = :sourced_to"
            total_loans_query += " AND src.keterangan = :sourced_to"
            first_borrow_query += " AND src.keterangan = :sourced_to"
            params['sourced_to'] = sourced_to_filter
            
        if project_filter:
            eligible_count_query += " AND prj.keterangan = :project"
            active_count_query += " AND prj.keterangan = :project"
            processed_requests_query += " AND prj.keterangan = :project"
            approved_requests_query += " AND prj.keterangan = :project"
            rejected_requests_query += " AND prj.keterangan = :project"
            avg_approval_time_query += " AND prj.keterangan = :project"
            total_disbursed_amount_query += " AND prj.keterangan = :project"
            total_loans_query += " AND prj.keterangan = :project"
            first_borrow_query += " AND prj.keterangan = :project"
            params['project'] = project_filter
            
        if loan_status_filter is not None:
            processed_requests_query += " AND l.loan_status = :loan_status"
            approved_requests_query += " AND l.loan_status = :loan_status"
            rejected_requests_query += " AND l.loan_status = :loan_status"
            avg_approval_time_query += " AND l.loan_status = :loan_status"
            total_disbursed_amount_query += " AND l.loan_status = :loan_status"
            total_loans_query += " AND l.loan_status = :loan_status"
            first_borrow_query += " AND l.loan_status = :loan_status"
            params['loan_status'] = loan_status_filter
            
        # Add month and year filters based on proses_date (matching working monthly summary logic)
        if month_filter is not None and year_filter is not None:
            # Convert month/year to date range (matching monthly summary approach)
            import calendar
            start_date = f"{year_filter}-{month_filter:02d}-01"
            # Get last day of the month (matching monthly summary)
            last_day = calendar.monthrange(year_filter, month_filter)[1]
            end_date = f"{year_filter}-{month_filter:02d}-{last_day:02d}"

            
            # Apply date range filters to all loan-related queries (matching monthly summary)
            processed_requests_query += " AND l.proses_date >= :start_date"
            approved_requests_query += " AND l.proses_date >= :start_date"
            rejected_requests_query += " AND l.proses_date >= :start_date"
            avg_approval_time_query += " AND l.proses_date >= :start_date"
            total_disbursed_amount_query += " AND l.proses_date >= :start_date"
            total_loans_query += " AND l.proses_date >= :start_date"
            first_borrow_query += " AND l.proses_date >= :start_date"
            params['start_date'] = start_date
            
            processed_requests_query += " AND l.proses_date <= :end_date"
            approved_requests_query += " AND l.proses_date <= :end_date"
            rejected_requests_query += " AND l.proses_date <= :end_date"
            avg_approval_time_query += " AND l.proses_date <= :end_date"
            total_disbursed_amount_query += " AND l.proses_date <= :end_date"
            total_loans_query += " AND l.proses_date <= :end_date"
            first_borrow_query += " AND l.proses_date <= :end_date"
            params['end_date'] = end_date
        

        eligible_result = db.execute(text(eligible_count_query), params)
        total_eligible_employees = eligible_result.fetchone()[0] or 0
        
        active_result = db.execute(text(active_count_query), params)
        total_active_employees = active_result.fetchone()[0] or 0
        
        # Calculate eligible rate
        eligible_rate = (total_eligible_employees / total_active_employees) if total_active_employees > 0 else 0.0
        
        processed_result = db.execute(text(processed_requests_query), params)
        total_loan_requests = processed_result.fetchone()[0] or 0
    
        
        approved_result = db.execute(text(approved_requests_query), params)
        total_approved_requests = approved_result.fetchone()[0] or 0
        
        rejected_result = db.execute(text(rejected_requests_query), params)
        total_rejected_requests = rejected_result.fetchone()[0] or 0
        
        avg_approval_time_result = db.execute(text(avg_approval_time_query), params)
        avg_approval_time_record = avg_approval_time_result.fetchone()
        average_approval_time = avg_approval_time_record[0] if avg_approval_time_record and avg_approval_time_record[0] is not None else 0
        
        # Use the same query logic as the working monthly summary
        total_disbursed_amount_result = db.execute(text(total_disbursed_amount_query), params)
        total_disbursed_amount = total_disbursed_amount_result.fetchone()[0] or 0

        
        total_loans_result = db.execute(text(total_loans_query), params)
        disbursed_loans_count = total_loans_result.fetchone()[0] or 0
        
        # Execute the first borrow query
        first_borrow_result = db.execute(text(first_borrow_query), params)
        first_borrow_record = first_borrow_result.fetchone()
        total_new_borrowers = first_borrow_record[0] if first_borrow_record and first_borrow_record[0] is not None else 0
        
        # Calculate average disbursed amount
        average_disbursed_amount = 0
        if disbursed_loans_count > 0:
            average_disbursed_amount = total_disbursed_amount / disbursed_loans_count
        
        # Calculate derived metrics
        penetration_rate = 0
        if total_eligible_employees > 0:
            penetration_rate = total_loan_requests / total_eligible_employees
        
        approval_rate = 0
        total_processed_requests = total_approved_requests + total_rejected_requests
        if total_processed_requests > 0:
            approval_rate = total_approved_requests / total_processed_requests
        
        # Calculate average disbursed amount
        average_disbursed_amount = 0
        if disbursed_loans_count > 0:
            average_disbursed_amount = total_disbursed_amount / disbursed_loans_count
        
        return {
            "total_eligible_employees": total_eligible_employees,
            "total_active_employees": total_active_employees,
            "total_loan_requests": total_loan_requests,
            "penetration_rate": penetration_rate,
            "eligible_rate": eligible_rate,
            "total_approved_requests": total_approved_requests,
            "total_rejected_requests": total_rejected_requests,
            "approval_rate": approval_rate,
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
            "total_active_employees": 0,
            "total_loan_requests": 0,
            "penetration_rate": 0,
            "eligible_rate": 0.0,
            "total_approved_requests": 0,
            "total_rejected_requests": 0,
            "approval_rate": 0,
            "total_new_borrowers": 0,
            "average_approval_time": 0,
            "total_disbursed_amount": 0,
            "average_disbursed_amount": 0
        }


def get_coverage_utilization_monthly_summary(db: Session, 
                                            employer_filter: str = None, sourced_to_filter: str = None, 
                                            project_filter: str = None, loan_status_filter: int = None,
                                            id_karyawan_filter: int = None, start_date: str = None,
                                            end_date: str = None, loan_type: str = "loan") -> dict:
    """Get coverage utilization summary separated by months within a date range"""
    
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
            
        # Restrict to only PT Valdo companies (conditional based on loan type)
        if loan_type == "extradana":
            # For extradana, include all three companies (based on the example query)
            company_filter = "('PT Valdo Sumber Daya Mandiri', 'PT Valdo International', 'PT Toko Pandai')"
        elif loan_type == "aku_cicil":
            # For aku_cicil, include all three companies
            company_filter = "('PT Valdo Sumber Daya Mandiri', 'PT Valdo International', 'PT Toko Pandai')"
        else:
            # For loan, include all three companies
            company_filter = "('PT Valdo Sumber Daya Mandiri', 'PT Valdo International', 'PT Toko Pandai')"
            
        eligible_count_query += f" AND emp.keterangan IN {company_filter}"
        processed_requests_query += f" AND emp.keterangan IN {company_filter}"
        approved_requests_query += f" AND emp.keterangan IN {company_filter}"
        rejected_requests_query += f" AND emp.keterangan IN {company_filter}"
        total_disbursed_amount_query += f" AND emp.keterangan IN {company_filter}"
        first_borrow_query += f" AND emp.keterangan IN {company_filter}"
        
        # If employer_filter is provided and it's one of the allowed companies, filter further
        if employer_filter and employer_filter in ['PT Valdo Sumber Daya Mandiri', 'PT Valdo International', 'PT Toko Pandai']:
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
        
        # Build monthly queries with GROUP BY (using exact same logic as above but grouped by month)
        monthly_processed_query = """
        SELECT 
            DATE_FORMAT(l.proses_date, '%M %Y') as month_year,
            COUNT(*) as total_loan_requests
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
        
        monthly_approved_query = """
        SELECT 
            DATE_FORMAT(l.proses_date, '%M %Y') as month_year,
            COUNT(*) as total_approved_requests
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
        
        monthly_rejected_query = """
        SELECT 
            DATE_FORMAT(l.proses_date, '%M %Y') as month_year,
            COUNT(*) as total_rejected_requests
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
        
        monthly_disbursed_query = """
        SELECT 
            DATE_FORMAT(l.proses_date, '%M %Y') as month_year,
            SUM(l.total_loan) as total_disbursed_amount
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
        
        monthly_first_borrow_query = """
        SELECT 
            DATE_FORMAT(l.proses_date, '%M %Y') as month_year,
            COUNT(DISTINCT l.id_karyawan) as total_first_borrow
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
        
        # Add filters to monthly queries
        if id_karyawan_filter:
            monthly_processed_query += " AND l.id_karyawan = :id_karyawan"
            monthly_approved_query += " AND l.id_karyawan = :id_karyawan"
            monthly_rejected_query += " AND l.id_karyawan = :id_karyawan"
            monthly_disbursed_query += " AND l.id_karyawan = :id_karyawan"
            monthly_first_borrow_query += " AND l.id_karyawan = :id_karyawan"
            
        if employer_filter:
            monthly_processed_query += " AND emp.keterangan = :employer"
            monthly_approved_query += " AND emp.keterangan = :employer"
            monthly_rejected_query += " AND emp.keterangan = :employer"
            monthly_disbursed_query += " AND emp.keterangan = :employer"
            monthly_first_borrow_query += " AND emp.keterangan = :employer"
            
        if sourced_to_filter:
            monthly_processed_query += " AND src.keterangan = :sourced_to"
            monthly_approved_query += " AND src.keterangan = :sourced_to"
            monthly_rejected_query += " AND src.keterangan = :sourced_to"
            monthly_disbursed_query += " AND src.keterangan = :sourced_to"
            monthly_first_borrow_query += " AND src.keterangan = :sourced_to"
            
        if project_filter:
            monthly_processed_query += " AND prj.keterangan = :project"
            monthly_approved_query += " AND prj.keterangan = :project"
            monthly_rejected_query += " AND prj.keterangan = :project"
            monthly_disbursed_query += " AND prj.keterangan = :project"
            monthly_first_borrow_query += " AND prj.keterangan = :project"
            
        if loan_status_filter is not None:
            monthly_processed_query += " AND l.loan_status = :loan_status"
            monthly_approved_query += " AND l.loan_status = :loan_status"
            monthly_rejected_query += " AND l.loan_status = :loan_status"
            monthly_disbursed_query += " AND l.loan_status = :loan_status"
            monthly_first_borrow_query += " AND l.loan_status = :loan_status"
            
        # Apply company filtering to monthly queries
        monthly_processed_query += f" AND emp.keterangan IN {company_filter}"
        monthly_approved_query += f" AND emp.keterangan IN {company_filter}"
        monthly_rejected_query += f" AND emp.keterangan IN {company_filter}"
        monthly_disbursed_query += f" AND emp.keterangan IN {company_filter}"
        monthly_first_borrow_query += f" AND emp.keterangan IN {company_filter}"
            
        if start_date:
            monthly_processed_query += " AND l.proses_date >= :start_date"
            monthly_approved_query += " AND l.proses_date >= :start_date"
            monthly_rejected_query += " AND l.proses_date >= :start_date"
            monthly_disbursed_query += " AND l.proses_date >= :start_date"
            monthly_first_borrow_query += " AND l.proses_date >= :start_date"
            
        if end_date:
            monthly_processed_query += " AND l.proses_date <= :end_date"
            monthly_approved_query += " AND l.proses_date <= :end_date"
            monthly_rejected_query += " AND l.proses_date <= :end_date"
            monthly_disbursed_query += " AND l.proses_date <= :end_date"
            monthly_first_borrow_query += " AND l.proses_date <= :end_date"
        
        # Add GROUP BY and ORDER BY to all monthly queries
        monthly_processed_query += " GROUP BY DATE_FORMAT(l.proses_date, '%M %Y') ORDER BY MIN(l.proses_date)"
        monthly_approved_query += " GROUP BY DATE_FORMAT(l.proses_date, '%M %Y') ORDER BY MIN(l.proses_date)"
        monthly_rejected_query += " GROUP BY DATE_FORMAT(l.proses_date, '%M %Y') ORDER BY MIN(l.proses_date)"
        monthly_disbursed_query += " GROUP BY DATE_FORMAT(l.proses_date, '%M %Y') ORDER BY MIN(l.proses_date)"
        monthly_first_borrow_query += " GROUP BY DATE_FORMAT(l.proses_date, '%M %Y') ORDER BY MIN(l.proses_date)"
        
        monthly_processed_result = db.execute(text(monthly_processed_query), params)
        monthly_approved_result = db.execute(text(monthly_approved_query), params)
        monthly_rejected_result = db.execute(text(monthly_rejected_query), params)
        monthly_disbursed_result = db.execute(text(monthly_disbursed_query), params)
        monthly_first_borrow_result = db.execute(text(monthly_first_borrow_query), params)
        
        # Get all monthly data
        monthly_processed_data = {row[0]: row[1] for row in monthly_processed_result.fetchall() if row[0] is not None}
        monthly_approved_data = {row[0]: row[1] for row in monthly_approved_result.fetchall() if row[0] is not None}
        monthly_rejected_data = {row[0]: row[1] for row in monthly_rejected_result.fetchall() if row[0] is not None}
        monthly_disbursed_data = {row[0]: row[1] for row in monthly_disbursed_result.fetchall() if row[0] is not None}
        monthly_first_borrow_data = {row[0]: row[1] for row in monthly_first_borrow_result.fetchall() if row[0] is not None}
        
        
        # Get total eligible employees (same for all months)
        eligible_result = db.execute(text(eligible_count_query), params)
        total_eligible_employees = eligible_result.fetchone()[0] or 0
        
        # Combine all monthly data
        monthly_data = {}
        all_months = set(monthly_processed_data.keys()) | set(monthly_approved_data.keys()) | set(monthly_rejected_data.keys()) | set(monthly_disbursed_data.keys()) | set(monthly_first_borrow_data.keys())
        
        for month_year in all_months:
            total_loan_requests = monthly_processed_data.get(month_year, 0) or 0
            total_approved_requests = monthly_approved_data.get(month_year, 0) or 0
            total_rejected_requests = monthly_rejected_data.get(month_year, 0) or 0
            total_disbursed_amount = monthly_disbursed_data.get(month_year, 0) or 0
            total_first_borrow = monthly_first_borrow_data.get(month_year, 0) or 0
            # Calculate penetration rate
            penetration_rate = 0
            if total_eligible_employees > 0:
                penetration_rate = total_loan_requests / total_eligible_employees
            
            monthly_data[month_year] = {
                "total_first_borrow": total_first_borrow,
                "total_loan_requests": total_loan_requests,
                "total_approved_requests": total_approved_requests,
                "total_rejected_requests": total_rejected_requests,
                "penetration_rate": penetration_rate,
                "total_disbursed_amount": total_disbursed_amount
            }
        
        return monthly_data
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "total_eligible_employees": 0,
            "total_loan_requests": 0,
            "penetration_rate": 0,
            "total_approved_requests": 0,
            "total_rejected_requests": 0,
            "approval_rate": 0,
            "total_new_borrowers": 0,
            "total_disbursed_amount": 0
        }


def get_client_summary(db: Session, month_filter: int = None, year_filter: int = None, loan_type: str = "kasbon") -> list:
    """Get comprehensive client summary with disbursement and other metrics"""
    
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
        
        # Determine company filter based on loan type
        if loan_type == "extradana":
            # For extradana, include all three companies (based on the example query)
            company_filter = "('PT Valdo Sumber Daya Mandiri', 'PT Valdo International', 'PT Toko Pandai')"
        else:
            # For loan, include all three companies
            company_filter = "('PT Valdo Sumber Daya Mandiri', 'PT Valdo International', 'PT Toko Pandai')"
        
        # Build parameters dict for filters (needed for both queries)
        params = {}
        
        # Add month and year filters to params
        if month_filter is not None:
            params['month'] = month_filter
        if year_filter is not None:
            params['year'] = year_filter
        
        # Get employee counts using the exact same approach as coverage utilization
        # For each sourced_to and project combination, we'll run the same query as coverage utilization
        employee_counts = {}
        
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
        
        if month_filter is not None:
            combinations_query += " AND MONTH(l.proses_date) = :month"
        if year_filter is not None:
            combinations_query += " AND YEAR(l.proses_date) = :year"
        
        try:
            combinations_result = db.execute(text(combinations_query), params)
            combinations = combinations_result.fetchall()
            
            # For each combination, get employee counts using the exact same query as coverage utilization
            for combo in combinations:
                sourced_to = combo[0] if combo[0] else "Unknown"
                project = combo[1] if combo[1] else "Unknown"
                key = f"{sourced_to}_{project}"
                
                # Use the exact same eligible count query as coverage utilization (only filter by sourced_to)
                eligible_count_query = f"""
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
                AND src.keterangan = :sourced_to
                AND emp.keterangan IN {company_filter}
                """
                
                # Execute the eligible count query for this combination
                eligible_params = {"sourced_to": sourced_to}
                eligible_result = db.execute(text(eligible_count_query), eligible_params)
                eligible_count = eligible_result.fetchone()[0] or 0
                
                # For active employees, we'll use the same approach but without loan_kasbon_eligible filter
                active_count_query = f"""
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
                AND src.keterangan = :sourced_to
                AND emp.keterangan IN {company_filter}
                """
                
                active_result = db.execute(text(active_count_query), eligible_params)
                active_count = active_result.fetchone()[0] or 0
                
                employee_counts[key] = {
                    "eligible": eligible_count,
                    "active": active_count
                }
                
        except Exception as e:
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
        
        # Add month and year filters to the main query (params already defined above)
        if month_filter is not None:
            client_summary_query += " AND MONTH(l.proses_date) = :month"
            
        if year_filter is not None:
            client_summary_query += " AND YEAR(l.proses_date) = :year"
        
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
         
        return client_disbursements
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return []


