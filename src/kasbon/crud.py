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


def get_eligible_count(db: Session, 
                      employer_filter: str = None, sourced_to_filter: str = None, 
                      project_filter: str = None, id_karyawan_filter: int = None) -> int:
    """Get count of eligible employees (status = '1' and loan_kasbon_eligible = '1')"""
    
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
        
        # Build parameters dict for filters
        params = {}
        
        # Add the same filters to the eligible count query
        if id_karyawan_filter:
            eligible_count_query += " AND tk.id_karyawan = :id_karyawan"
            params['id_karyawan'] = id_karyawan_filter
        if employer_filter:
            eligible_count_query += " AND emp.keterangan = :employer"
            params['employer'] = employer_filter
        if sourced_to_filter:
            eligible_count_query += " AND src.keterangan = :sourced_to"
            params['sourced_to'] = sourced_to_filter
        if project_filter:
            eligible_count_query += " AND prj.keterangan = :project"
            params['project'] = project_filter
        
        print(f"🔍 Executing eligible count query with filters:")
        print(f"   id_karyawan: {id_karyawan_filter}")
        print(f"   employer: {employer_filter}")
        print(f"   sourced_to: {sourced_to_filter}")
        print(f"   project: {project_filter}")
        
        # Execute the eligible count query
        eligible_result = db.execute(text(eligible_count_query), params)
        total_eligible = eligible_result.fetchone()[0]
        
        print(f"📊 Total eligible employees (status='1' AND loan_kasbon_eligible='1'): {total_eligible}")
        
        return total_eligible
        
    except Exception as e:
        print(f"❌ Error in eligible count query: {e}")
        print(f"   Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        return 0


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
                          id_karyawan_filter: int = None) -> dict:
    """Get loan fees summary (total expected and collected admin fees)"""
    
    try:
        # Build the query to calculate admin fees
        fees_query = """
        SELECT
            SUM(l.admin_fee) as total_expected_admin_fee,
            SUM(CASE WHEN l.received_date IS NOT NULL THEN l.admin_fee ELSE 0 END) as total_collected_admin_fee
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
        
        print(f"🔍 Executing loan fees query with filters:")
        print(f"   id_karyawan: {id_karyawan_filter}")
        print(f"   employer: {employer_filter}")
        print(f"   sourced_to: {sourced_to_filter}")
        print(f"   project: {project_filter}")
        print(f"   loan_status: {loan_status_filter}")
        
        # Execute the query
        result = db.execute(text(fees_query), params)
        record = result.fetchone()
        
        # Extract the values (handle None values)
        total_expected = record[0] if record[0] is not None else 0
        total_collected = record[1] if record[1] is not None else 0
        
        print(f"💰 Loan fees summary:")
        print(f"   Total expected admin fee: {total_expected}")
        print(f"   Total collected admin fee: {total_collected}")
        
        return {
            "total_expected_admin_fee": total_expected,
            "total_collected_admin_fee": total_collected
        }
        
    except Exception as e:
        print(f"❌ Error in loan fees query: {e}")
        print(f"   Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        return {
            "total_expected_admin_fee": 0,
            "total_collected_admin_fee": 0
        } 