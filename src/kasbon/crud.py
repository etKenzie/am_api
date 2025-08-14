from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List


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
        
        print(f"🔍 Executing enhanced query with filters:")
        print(f"   id_karyawan: {id_karyawan_filter}")
        print(f"   employer: {employer_filter}")
        print(f"   sourced_to: {sourced_to_filter}")
        print(f"   project: {project_filter}")
        
        # Execute the main query
        result = db.execute(text(base_query), params)
        records = result.fetchall()
        
        print(f"✅ Enhanced query returned {len(records)} records")
        
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
        print(f"❌ Error in enhanced karyawan query: {e}")
        print(f"   Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        return []


def get_user_coverage_summary(db: Session, 
                             employer_filter: str = None, sourced_to_filter: str = None, 
                             project_filter: str = None, id_karyawan_filter: int = None,
                             month_filter: int = None, year_filter: int = None) -> dict:
    """Get user coverage summary with eligible count and kasbon request metrics"""
    
    try:
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
        
        # Build the processed kasbon requests query
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
        """
        
        # Build the pending kasbon requests query
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
        """
        
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
        )
        """
        
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
        """
        
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
        """
        
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
        """
        
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
        """
        
        # Build the total unique requests query
        total_unique_requests_query = """
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
        WHERE 1=1
        """
        
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
            total_unique_requests_query += " AND l.id_karyawan = :id_karyawan"
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
            total_unique_requests_query += " AND emp.keterangan = :employer"
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
            total_unique_requests_query += " AND src.keterangan = :sourced_to"
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
            total_unique_requests_query += " AND prj.keterangan = :project"
            params['project'] = project_filter
        if month_filter:
            processed_requests_query += " AND MONTH(l.proses_date) = :month"
            pending_requests_query += " AND MONTH(l.received_date) = :month"
            first_borrow_query += " AND MONTH(l.proses_date) = :month"
            approved_requests_query += " AND MONTH(l.proses_date) = :month"
            rejected_requests_query += " AND MONTH(l.proses_date) = :month"
            avg_approval_time_query += " AND MONTH(l.proses_date) = :month"
            total_disbursed_amount_query += " AND MONTH(l.proses_date) = :month"
            total_unique_requests_query += " AND MONTH(l.proses_date) = :month"
            params['month'] = month_filter
        if year_filter:
            processed_requests_query += " AND YEAR(l.proses_date) = :year"
            pending_requests_query += " AND YEAR(l.received_date) = :year"
            first_borrow_query += " AND YEAR(l.proses_date) = :year"
            approved_requests_query += " AND YEAR(l.proses_date) = :year"
            rejected_requests_query += " AND YEAR(l.proses_date) = :year"
            avg_approval_time_query += " AND YEAR(l.proses_date) = :year"
            total_disbursed_amount_query += " AND YEAR(l.proses_date) = :year"
            total_unique_requests_query += " AND YEAR(l.proses_date) = :year"
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
        avg_approval_time = avg_approval_time_result.fetchone()[0]
        
        # Execute the new queries
        total_disbursed_amount_result = db.execute(text(total_disbursed_amount_query), params)
        total_disbursed_amount = total_disbursed_amount_result.fetchone()[0] or 0
        
        total_unique_requests_result = db.execute(text(total_unique_requests_query), params)
        total_unique_requests = total_unique_requests_result.fetchone()[0] or 0
        
        # Calculate penetration rate
        penetration_rate = 0
        if total_eligible > 0:
            penetration_rate = total_processed / total_eligible
            
        # Calculate approval rate
        approval_rate = 0
        if total_processed > 0:
            approval_rate = total_approved / total_processed
            
        # Calculate average disbursed amount
        average_disbursed_amount = 0
        if total_unique_requests > 0:
            average_disbursed_amount = total_disbursed_amount / total_unique_requests
        
        print(f"📊 User coverage summary:")
        print(f"   Total eligible employees: {total_eligible}")
        print(f"   Total processed kasbon requests: {total_processed}")
        print(f"   Total pending kasbon requests: {total_pending}")
        print(f"   Total first-time borrowers: {total_first_borrow}")
        print(f"   Total approved requests: {total_approved}")
        print(f"   Total rejected requests: {total_rejected}")
        print(f"   Total disbursed amount: {total_disbursed_amount}")
        print(f"   Total unique requests: {total_unique_requests}")
        print(f"   Average disbursed amount: {average_disbursed_amount:.2f}")
        print(f"   Penetration rate: {penetration_rate:.2%}")
        print(f"   Approval rate: {approval_rate:.2%}")
        print(f"   Average approval time: {avg_approval_time:.1f} days")
        
        return {
            "total_eligible_employees": total_eligible,
            "total_processed_kasbon_requests": total_processed,
            "total_pending_kasbon_requests": total_pending,
            "total_first_borrow": total_first_borrow,
            "total_approved_requests": total_approved,
            "total_rejected_requests": total_rejected,
            "total_disbursed_amount": total_disbursed_amount,
            "total_unique_requests": total_unique_requests,
            "average_disbursed_amount": average_disbursed_amount,
            "approval_rate": approval_rate,
            "average_approval_time": avg_approval_time,
            "penetration_rate": penetration_rate
        }
        
    except Exception as e:
        print(f"❌ Error in user coverage query: {e}")
        print(f"   Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        return {
            "total_eligible_employees": 0,
            "total_processed_kasbon_requests": 0,
            "total_pending_kasbon_requests": 0,
            "total_first_borrow": 0,
            "total_approved_requests": 0,
            "total_rejected_requests": 0,
            "total_disbursed_amount": 0,
            "total_unique_requests": 0,
            "average_disbursed_amount": 0,
            "approval_rate": 0,
            "average_approval_time": 0,
            "penetration_rate": 0
        }


def get_user_coverage_monthly_summary(db: Session, start_date: str, end_date: str,
                                     employer_filter: str = None, sourced_to_filter: str = None, 
                                     project_filter: str = None, id_karyawan_filter: int = None) -> dict:
    """Get user coverage summary monthly data with eligible count and kasbon request metrics"""
    
    try:
        # First, get the total eligible employees (same logic as get_user_coverage_summary)
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
        
        # Build parameters dict for eligible count query
        eligible_params = {}
        
        # Add filters to eligible count query
        if id_karyawan_filter:
            eligible_count_query += " AND tk.id_karyawan = :id_karyawan"
            eligible_params['id_karyawan'] = id_karyawan_filter
        if employer_filter:
            eligible_count_query += " AND emp.keterangan = :employer"
            eligible_params['employer'] = employer_filter
        if sourced_to_filter:
            eligible_count_query += " AND src.keterangan = :sourced_to"
            eligible_params['sourced_to'] = sourced_to_filter
        if project_filter:
            eligible_count_query += " AND prj.keterangan = :project"
            eligible_params['project'] = project_filter
        
        # Execute eligible count query
        eligible_result = db.execute(text(eligible_count_query), eligible_params)
        total_eligible_employees = eligible_result.fetchone()[0]
        
        # Build the monthly loan requests query
        monthly_query = """
        SELECT 
            DATE_FORMAT(l.proses_date, '%M %Y') as month_year,
            COUNT(CASE WHEN l.loan_status IN (0, 1, 2, 3) THEN l.id END) as total_processed_kasbon_requests,
            SUM(CASE WHEN l.loan_status IN (1, 2, 4) THEN l.total_loan ELSE 0 END) as total_disbursed_amount
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
        """
        
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
        
        print(f"🔍 Executing monthly user coverage query with filters:")
        print(f"   start_date: {start_date}")
        print(f"   end_date: {end_date}")
        print(f"   id_karyawan: {id_karyawan_filter}")
        print(f"   employer: {employer_filter}")
        print(f"   sourced_to: {sourced_to_filter}")
        print(f"   project: {project_filter}")
        print(f"   Total eligible employees: {total_eligible_employees}")
        
        # Execute monthly query
        result = db.execute(text(monthly_query), monthly_params)
        rows = result.fetchall()
        
        # Process results
        monthly_data = {}
        for row in rows:
            month_year = row[0]
            if month_year is None:
                continue
                
            total_processed = row[1] or 0
            total_disbursed_amount = row[2] or 0
            
            # Calculate penetration rate using the total eligible employees
            penetration_rate = 0
            if total_eligible_employees > 0:
                penetration_rate = total_processed / total_eligible_employees
            
            monthly_data[month_year] = {
                "total_eligible_employees": total_eligible_employees,
                "total_processed_kasbon_requests": total_processed,
                "total_disbursed_amount": total_disbursed_amount,
                "penetration_rate": penetration_rate
            }
        
        print(f"📊 Monthly user coverage summary completed with {len(monthly_data)} months")
        
        return monthly_data
        
    except Exception as e:
        print(f"❌ Error in monthly user coverage query: {e}")
        print(f"   Error type: {type(e).__name__}")
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
        print("🔍 Checking if loans table exists...")
        try:
            table_check = db.execute(text("SHOW TABLES LIKE 'td_loan'"))
            if not table_check.fetchone():
                print("❌ Table 'loans' not found!")
                return []
            print("✅ Table 'td_loan' exists")
        except Exception as e:
            print(f"❌ Error checking td_loan table: {e}")
            return []
        
        # Check loans table structure
        try:
            structure_result = db.execute(text("DESCRIBE td_loan"))
            columns = structure_result.fetchall()
            print("📋 td_loan table structure:")
            for col in columns:
                print(f"   {col[0]} - {col[1]}")
        except Exception as e:
            print(f"❌ Error getting td_loan table structure: {e}")
        
        # Check if td_karyawan has the required columns for joining
        try:
            karyawan_structure = db.execute(text("DESCRIBE td_karyawan"))
            karyawan_columns = karyawan_structure.fetchall()
            print("📋 td_karyawan table structure (key columns):")
            for col in karyawan_columns:
                if col[0] in ['id_karyawan', 'valdo_inc', 'placement', 'project']:
                    print(f"   {col[0]} - {col[1]}")
        except Exception as e:
            print(f"❌ Error getting td_karyawan table structure: {e}")
        
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
        """
        
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
        
        print(f"🔍 Executing loans query with filters:")
        print(f"   id_karyawan: {id_karyawan_filter}")
        print(f"   employer: {employer_filter}")
        print(f"   sourced_to: {sourced_to_filter}")
        print(f"   project: {project_filter}")
        print(f"   loan_status: {loan_status_filter}")
        
        # Execute the query
        result = db.execute(text(base_query), params)
        records = result.fetchall()
        
        print(f"✅ Loans query returned {len(records)} records")
        
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
        print(f"❌ Error in loans query: {e}")
        print(f"   Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        return []


def get_available_filter_values(db: Session) -> dict:
    """Get available filter values from tbl_gmc table for different categories"""
    
    try:
        # Get employers (sub_client)
        employer_query = """
        SELECT DISTINCT keterangan 
        FROM tbl_gmc 
        WHERE group_gmc = 'sub_client' 
        AND aktif = 'Yes' 
        AND keterangan3 = 1
        ORDER BY keterangan
        """
        
        # Get placement clients
        placement_query = """
        SELECT DISTINCT keterangan 
        FROM tbl_gmc 
        WHERE group_gmc = 'placement_client' 
        AND aktif = 'Yes' 
        AND keterangan3 = 1
        ORDER BY keterangan
        """
        
        # Get projects
        project_query = """
        SELECT DISTINCT keterangan 
        FROM tbl_gmc 
        WHERE group_gmc = 'client_project' 
        AND aktif = 'Yes' 
        AND keterangan3 = 1
        ORDER BY keterangan
        """
        
        # Execute queries
        employers = [row[0] for row in db.execute(text(employer_query)).fetchall()]
        placements = [row[0] for row in db.execute(text(placement_query)).fetchall()]
        projects = [row[0] for row in db.execute(text(project_query)).fetchall()]
        
        return {
            "employers": employers,
            "placements": placements,
            "projects": projects
        }
        
    except Exception as e:
        print(f"❌ Error getting filter values: {e}")
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
        """
        
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
        
        print(f"🔍 Executing loan fees query with filters:")
        print(f"   id_karyawan: {id_karyawan_filter}")
        print(f"   employer: {employer_filter}")
        print(f"   sourced_to: {sourced_to_filter}")
        print(f"   project: {project_filter}")
        print(f"   loan_status: {loan_status_filter}")
        print(f"   month: {month_filter}")
        print(f"   year: {year_filter}")
        
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
        
        print(f"💰 Loan fees summary:")
        print(f"   Expected admin fee: {total_expected} (from {expected_count} loans with status 1,4)")
        print(f"   Collected admin fee: {total_collected} (from {collected_count} loans with status 2)")
        print(f"   Total failed payment: {total_failed_payment} (from loans with status 4)")
        print(f"   Admin fee profit: {admin_fee_profit}")
        
        return {
            "total_expected_admin_fee": total_expected,
            "expected_loans_count": expected_count,
            "total_collected_admin_fee": total_collected,
            "collected_loans_count": collected_count,
            "total_failed_payment": total_failed_payment,
            "admin_fee_profit": admin_fee_profit
        }
        
    except Exception as e:
        print(f"❌ Error in loan fees query: {e}")
        print(f"   Error type: {type(e).__name__}")
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
        """
        
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
        
        print(f"💰 Monthly loan fees summary:")
        print(f"   Found data for {len(monthly_data)} months")
        for month, data in monthly_data.items():
            print(f"   {month}: Expected={data['total_expected_admin_fee']}, Collected={data['total_collected_admin_fee']}")
        
        return monthly_data
        
    except Exception as e:
        print(f"❌ Error in monthly loan fees query: {e}")
        print(f"   Error type: {type(e).__name__}")
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
            SUM(CASE WHEN l.loan_status = 4 THEN l.total_loan ELSE 0 END) as total_unrecovered_kasbon,
            COUNT(CASE WHEN l.loan_status = 4 THEN 1 END) as unrecovered_kasbon_count,
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
        """
        
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
        total_unrecovered_kasbon = record[0] if record[0] is not None else 0
        unrecovered_kasbon_count = record[1] if record[1] is not None else 0
        total_expected_repayment = record[2] if record[2] is not None else 0
        total_paid = record[3] if record[3] is not None else 0
        total_disbursed = record[4] if record[4] is not None else 0
        
        # Calculate kasbon principal recovery rate
        kasbon_principal_recovery_rate = 0
        if total_disbursed > 0:
            kasbon_principal_recovery_rate = total_paid / total_disbursed
        
        print(f"📊 Loan risk summary:")
        print(f"   Total unrecovered kasbon: {total_unrecovered_kasbon} (from {unrecovered_kasbon_count} loans)")
        print(f"   Total expected repayment: {total_expected_repayment}")
        print(f"   Kasbon principal recovery rate: {kasbon_principal_recovery_rate:.2%}")
        
        return {
            "total_unrecovered_kasbon": total_unrecovered_kasbon,
            "unrecovered_kasbon_count": unrecovered_kasbon_count,
            "total_expected_repayment": total_expected_repayment,
            "kasbon_principal_recovery_rate": kasbon_principal_recovery_rate
        }
        
    except Exception as e:
        print(f"❌ Error in loan risk query: {e}")
        print(f"   Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        return {
            "total_unrecovered_kasbon": 0,
            "unrecovered_kasbon_count": 0,
            "total_expected_repayment": 0,
            "kasbon_principal_recovery_rate": 0
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
            SUM(CASE WHEN l.loan_status = 4 THEN l.total_loan ELSE 0 END) as total_unrecovered_kasbon,
            COUNT(CASE WHEN l.loan_status = 4 THEN 1 END) as unrecovered_kasbon_count,
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
        """
        
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
        
        print(f"🔍 Executing monthly loan risk query with filters:")
        print(f"   id_karyawan: {id_karyawan_filter}")
        print(f"   employer: {employer_filter}")
        print(f"   sourced_to: {sourced_to_filter}")
        print(f"   project: {project_filter}")
        print(f"   loan_status: {loan_status_filter}")
        print(f"   start_date: {start_date}")
        print(f"   end_date: {end_date}")
        
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
                kasbon_principal_recovery_rate = total_paid / total_disbursed
            
            monthly_data[month_year] = {
                "total_unrecovered_kasbon": total_unrecovered_kasbon,
                "unrecovered_kasbon_count": unrecovered_kasbon_count,
                "total_expected_repayment": total_expected_repayment,
                "kasbon_principal_recovery_rate": kasbon_principal_recovery_rate
            }
        
        print(f"📊 Monthly loan risk summary:")
        print(f"   Found data for {len(monthly_data)} months")
        for month, data in monthly_data.items():
            print(f"   {month}: Unrecovered={data['total_unrecovered_kasbon']}, Recovery Rate={data['kasbon_principal_recovery_rate']:.2%}")
        
        return monthly_data
        
    except Exception as e:
        print(f"❌ Error in monthly loan risk query: {e}")
        print(f"   Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        return {}


def get_karyawan_overdue_summary(db: Session, 
                                 employer_filter: str = None, sourced_to_filter: str = None, 
                                 project_filter: str = None, loan_status_filter: int = None,
                                 id_karyawan_filter: int = None, month_filter: int = None,
                                 year_filter: int = None) -> List[dict]:
    """Get karyawan data for those with overdue loans (status 4)"""
    
    try:
        # Build the query to get karyawan with overdue loans
        overdue_query = """
        SELECT DISTINCT
            tk.id_karyawan,
            tk.nama AS name,
            emp.keterangan AS company,
            src.keterangan AS sourced_to,
            prj.keterangan AS project,
            tk.rec_status,
            SUM(l.total_loan) as total_amount_owed
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
        """
        
        # Build parameters dict for filters
        params = {}
        
        # Add filters
        if id_karyawan_filter:
            overdue_query += " AND l.id_karyawan = :id_karyawan"
            params['id_karyawan'] = id_karyawan_filter
            
        if employer_filter:
            overdue_query += " AND emp.keterangan = :employer"
            params['employer'] = employer_filter
            
        if sourced_to_filter:
            overdue_query += " AND src.keterangan = :sourced_to"
            params['sourced_to'] = sourced_to_filter
            
        if project_filter:
            overdue_query += " AND prj.keterangan = :project"
            params['project'] = project_filter
            
        if loan_status_filter is not None:
            overdue_query += " AND l.loan_status = :loan_status"
            params['loan_status'] = loan_status_filter
            
        # Add month and year filters based on proses_date
        if month_filter is not None:
            overdue_query += " AND MONTH(l.proses_date) = :month"
            params['month'] = month_filter
            
        if year_filter is not None:
            overdue_query += " AND YEAR(l.proses_date) = :year"
            params['year'] = year_filter
        
        # Group by karyawan and order by total amount owed (descending)
        overdue_query += """
        GROUP BY tk.id_karyawan, tk.nama, emp.keterangan, src.keterangan, prj.keterangan, tk.rec_status
        ORDER BY total_amount_owed DESC
        """
        
        print(f"🔍 Executing karyawan overdue query with filters:")
        print(f"   id_karyawan: {id_karyawan_filter}")
        print(f"   employer: {employer_filter}")
        print(f"   sourced_to: {sourced_to_filter}")
        print(f"   project: {project_filter}")
        print(f"   loan_status: {loan_status_filter}")
        print(f"   month: {month_filter}")
        print(f"   year: {year_filter}")
        
        # Execute the query
        result = db.execute(text(overdue_query), params)
        records = result.fetchall()
        
        # Convert to list of dictionaries
        overdue_list = []
        for record in records:
            # Skip records with NULL id_karyawan (shouldn't happen due to WHERE clause, but just in case)
            if record[0] is None:
                continue
                
            overdue_list.append({
                "id_karyawan": record[0],
                "name": record[1],
                "company": record[2],
                "sourced_to": record[3],
                "project": record[4],
                "rec_status": record[5],
                "total_amount_owed": record[6] if record[6] is not None else 0
            })
        
        print(f"📊 Karyawan overdue summary:")
        print(f"   Found {len(overdue_list)} karyawan with overdue loans")
        for item in overdue_list[:5]:  # Show first 5 for logging
            print(f"   {item['name']}: {item['total_amount_owed']} owed")
        
        return overdue_list
        
    except Exception as e:
        print(f"❌ Error in karyawan overdue query: {e}")
        print(f"   Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        return []


def get_loan_purpose_summary(db: Session, 
                            employer_filter: str = None, sourced_to_filter: str = None, 
                            project_filter: str = None, loan_status_filter: int = None,
                            id_karyawan_filter: int = None, month_filter: int = None, 
                            year_filter: int = None) -> List[dict]:
    """Get loan summary grouped by purpose with filters"""
    
    try:
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
        """
        
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
        
        print(f"🔍 Executing loan purpose summary query with filters:")
        print(f"   id_karyawan: {id_karyawan_filter}")
        print(f"   employer: {employer_filter}")
        print(f"   sourced_to: {sourced_to_filter}")
        print(f"   project: {project_filter}")
        print(f"   loan_status: {loan_status_filter}")
        print(f"   month: {month_filter}")
        print(f"   year: {year_filter}")
        
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
        
        print(f"📊 Loan purpose summary:")
        print(f"   Found {len(purpose_list)} unique purposes")
        for item in purpose_list[:5]:  # Show first 5 for logging
            print(f"   {item['purpose_name']}: {item['total_count']} loans, {item['total_amount']} total")
        
        return purpose_list
        
    except Exception as e:
        print(f"❌ Error in loan purpose summary query: {e}")
        print(f"   Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        return []