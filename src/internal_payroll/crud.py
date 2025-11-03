from sqlalchemy.orm import Session
from sqlalchemy import text


def format_department_name(name: str) -> str:
    """Format department name to title case (first letter of each word capitalized)"""
    if not name:
        return name
    # Split by space and capitalize first letter of each word, rest lowercase
    return ' '.join(word.capitalize() if word else '' for word in name.split())


def get_total_payroll_disbursed(db: Session, month: int = None, year: int = None, dept_id: int = None) -> float:
    """Get total payroll disbursed (sum of take_home_pay) for a given month and year, optionally filtered by dept_id. Only counts departments that exist in payroll_cost_owner."""
    
    try:
        # Build the query to sum take_home_pay
        # Always join with payroll_header and payroll_cost_owner to filter only valid departments
        query = """
        SELECT SUM(pd.take_home_pay) as total_payroll_disbursed
        FROM payroll_detail pd
        INNER JOIN payroll_header ph ON pd.payroll_id = ph.payroll_id
        INNER JOIN payroll_cost_owner pco ON ph.dept_id = pco.id_department
        WHERE 1=1
        """
        
        params = {}
        
        # Add month filter if provided
        if month is not None:
            query += " AND pd.month = :month"
            params['month'] = month
        
        # Add year filter if provided
        if year is not None:
            query += " AND pd.year = :year"
            params['year'] = year
        
        # Add dept_id filter if provided
        if dept_id is not None:
            query += " AND ph.dept_id = :dept_id"
            params['dept_id'] = dept_id
        
        # Execute the query
        result = db.execute(text(query), params)
        record = result.fetchone()
        
        # Extract the value (handle None values)
        total_payroll_disbursed = record[0] if record[0] is not None else 0
        
        return total_payroll_disbursed
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return 0


def get_total_bpsjtk(db: Session, month: int = None, year: int = None, dept_id: int = None) -> float:
    """Get total BPJS TK (sum of all_bpjs_tk_comp) for a given month and year, optionally filtered by dept_id. Only counts departments that exist in payroll_cost_owner."""
    
    try:
        # Build the query to sum all_bpjs_tk_comp
        # Always join with payroll_header and payroll_cost_owner to filter only valid departments
        query = """
        SELECT SUM(pd.all_bpjs_tk_comp) as total_bpsjtk
        FROM payroll_detail pd
        INNER JOIN payroll_header ph ON pd.payroll_id = ph.payroll_id
        INNER JOIN payroll_cost_owner pco ON ph.dept_id = pco.id_department
        WHERE 1=1
        """
        
        params = {}
        
        # Add month filter if provided
        if month is not None:
            query += " AND pd.month = :month"
            params['month'] = month
        
        # Add year filter if provided
        if year is not None:
            query += " AND pd.year = :year"
            params['year'] = year
        
        # Add dept_id filter if provided
        if dept_id is not None:
            query += " AND ph.dept_id = :dept_id"
            params['dept_id'] = dept_id
        
        # Execute the query
        result = db.execute(text(query), params)
        record = result.fetchone()
        
        # Extract the value (handle None values)
        total_bpsjtk = record[0] if record[0] is not None else 0
        
        return total_bpsjtk
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return 0


def get_total_kesehatan(db: Session, month: int = None, year: int = None, dept_id: int = None) -> float:
    """Get total BPJS Kesehatan (sum of all_bpjs_kesehatan_comp) for a given month and year, optionally filtered by dept_id. Only counts departments that exist in payroll_cost_owner."""
    
    try:
        # Build the query to sum all_bpjs_kesehatan_comp
        # Always join with payroll_header and payroll_cost_owner to filter only valid departments
        query = """
        SELECT SUM(pd.all_bpjs_kesehatan_comp) as total_kesehatan
        FROM payroll_detail pd
        INNER JOIN payroll_header ph ON pd.payroll_id = ph.payroll_id
        INNER JOIN payroll_cost_owner pco ON ph.dept_id = pco.id_department
        WHERE 1=1
        """
        
        params = {}
        
        # Add month filter if provided
        if month is not None:
            query += " AND pd.month = :month"
            params['month'] = month
        
        # Add year filter if provided
        if year is not None:
            query += " AND pd.year = :year"
            params['year'] = year
        
        # Add dept_id filter if provided
        if dept_id is not None:
            query += " AND ph.dept_id = :dept_id"
            params['dept_id'] = dept_id
        
        # Execute the query
        result = db.execute(text(query), params)
        record = result.fetchone()
        
        # Extract the value (handle None values)
        total_kesehatan = record[0] if record[0] is not None else 0
        
        return total_kesehatan
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return 0


def get_total_pensiun(db: Session, month: int = None, year: int = None, dept_id: int = None) -> float:
    """Get total BPJS Pensiun (sum of all_bpjs_pensiun_comp) for a given month and year, optionally filtered by dept_id. Only counts departments that exist in payroll_cost_owner."""
    
    try:
        # Build the query to sum all_bpjs_pensiun_comp
        # Always join with payroll_header and payroll_cost_owner to filter only valid departments
        query = """
        SELECT SUM(pd.all_bpjs_pensiun_comp) as total_pensiun
        FROM payroll_detail pd
        INNER JOIN payroll_header ph ON pd.payroll_id = ph.payroll_id
        INNER JOIN payroll_cost_owner pco ON ph.dept_id = pco.id_department
        WHERE 1=1
        """
        
        params = {}
        
        # Add month filter if provided
        if month is not None:
            query += " AND pd.month = :month"
            params['month'] = month
        
        # Add year filter if provided
        if year is not None:
            query += " AND pd.year = :year"
            params['year'] = year
        
        # Add dept_id filter if provided
        if dept_id is not None:
            query += " AND ph.dept_id = :dept_id"
            params['dept_id'] = dept_id
        
        # Execute the query
        result = db.execute(text(query), params)
        record = result.fetchone()
        
        # Extract the value (handle None values)
        total_pensiun = record[0] if record[0] is not None else 0
        
        return total_pensiun
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return 0


def get_total_payroll_headcount(db: Session, month: int = None, year: int = None, dept_id: int = None) -> dict:
    """Get total payroll headcount with breakdown by status_kontrak (count of unique id_karyawan) for a given month and year, optionally filtered by dept_id. Only counts departments that exist in payroll_cost_owner."""
    
    try:
        # Build the query to count unique id_karyawan with breakdowns by status_kontrak
        # status_kontrak is in payroll_detail table
        # Always join with payroll_header and payroll_cost_owner to filter only valid departments
        query = """
        SELECT 
            COUNT(DISTINCT pd.id_karyawan) as total_headcount,
            COUNT(DISTINCT CASE WHEN pd.status_kontrak = 1 THEN pd.id_karyawan END) as pkwtt_headcount,
            COUNT(DISTINCT CASE WHEN pd.status_kontrak = 2 THEN pd.id_karyawan END) as pkwt_headcount,
            COUNT(DISTINCT CASE WHEN pd.status_kontrak = 3 THEN pd.id_karyawan END) as mitra_headcount
        FROM payroll_detail pd
        INNER JOIN payroll_header ph ON pd.payroll_id = ph.payroll_id
        INNER JOIN payroll_cost_owner pco ON ph.dept_id = pco.id_department
        WHERE 1=1
        """
        
        params = {}
        
        # Add month filter if provided
        if month is not None:
            query += " AND pd.month = :month"
            params['month'] = month
        
        # Add year filter if provided
        if year is not None:
            query += " AND pd.year = :year"
            params['year'] = year
        
        # Add dept_id filter if provided
        if dept_id is not None:
            query += " AND ph.dept_id = :dept_id"
            params['dept_id'] = dept_id
        
        # Execute the query
        result = db.execute(text(query), params)
        record = result.fetchone()
        
        # Extract the values (handle None values)
        total_headcount = record[0] if record[0] is not None else 0
        pkwtt_headcount = record[1] if record[1] is not None else 0
        pkwt_headcount = record[2] if record[2] is not None else 0
        mitra_headcount = record[3] if record[3] is not None else 0
        
        return {
            "total_headcount": total_headcount,
            "pkwtt_headcount": pkwtt_headcount,
            "pkwt_headcount": pkwt_headcount,
            "mitra_headcount": mitra_headcount
        }
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "total_headcount": 0,
            "pkwtt_headcount": 0,
            "pkwt_headcount": 0,
            "mitra_headcount": 0
        }


def get_total_department_count(db: Session, month: int = None, year: int = None) -> int:
    """Get total number of unique departments (dept_id) from payroll_header for a given month and year. Only counts departments that exist in payroll_cost_owner."""
    
    try:
        # Build the query to count unique dept_id
        # Join with payroll_cost_owner to filter only valid departments
        query = """
        SELECT COUNT(DISTINCT ph.dept_id) as total_department_count
        FROM payroll_header ph
        INNER JOIN payroll_cost_owner pco ON ph.dept_id = pco.id_department
        WHERE 1=1
        """
        
        params = {}
        
        # Add month filter if provided
        if month is not None:
            query += " AND ph.month = :month"
            params['month'] = month
        
        # Add year filter if provided
        if year is not None:
            query += " AND ph.year = :year"
            params['year'] = year
        
        # Execute the query
        result = db.execute(text(query), params)
        record = result.fetchone()
        
        # Extract the value (handle None values)
        total_department_count = record[0] if record[0] is not None else 0
        
        return total_department_count
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return 0


def get_department_filters(db: Session, month: int = None, year: int = None) -> list:
    """Get list of departments (dept_id and department_name) from payroll_header joined with payroll_cost_owner for a given month and year"""
    
    try:
        # Build the query to get distinct dept_id and department_name
        # Join payroll_header.dept_id with payroll_cost_owner.id_department
        # Use INNER JOIN to only show departments that exist in payroll_cost_owner
        query = """
        SELECT DISTINCT 
            ph.dept_id,
            pco.department_name
        FROM payroll_header ph
        INNER JOIN payroll_cost_owner pco ON ph.dept_id = pco.id_department
        WHERE 1=1
        """
        
        params = {}
        
        # Add month filter if provided
        if month is not None:
            query += " AND ph.month = :month"
            params['month'] = month
        
        # Add year filter if provided
        if year is not None:
            query += " AND ph.year = :year"
            params['year'] = year
        
        # Order by dept_id
        query += " ORDER BY ph.dept_id"
        
        # Execute the query
        result = db.execute(text(query), params)
        records = result.fetchall()
        
        # Convert to list of dictionaries and format department names
        departments = []
        for record in records:
            raw_department_name = record[1] if record[1] is not None else None
            formatted_name = format_department_name(raw_department_name) if raw_department_name else None
            
            departments.append({
                "dept_id": record[0] if record[0] is not None else None,
                "department_name": formatted_name
            })
        
        return departments
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return []


def get_monthly_payroll_summary(db: Session, start_month_str: str, end_month_str: str, dept_id: int = None) -> dict:
    """Get monthly payroll summaries combining total_disbursed and headcount for each month in the range. 
    Only counts departments that exist in payroll_cost_owner.
    
    Args:
        start_month_str: Start month in MM-YYYY format (e.g., "01-2025")
        end_month_str: End month in MM-YYYY format (e.g., "08-2025")
    """
    
    try:
        monthly_summaries = {}
        
        # Month names for formatting
        month_names = [
            "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December"
        ]
        
        # Parse start_month_str (MM-YYYY)
        start_parts = start_month_str.split("-")
        if len(start_parts) != 2:
            raise ValueError("start_month must be in MM-YYYY format")
        start_month = int(start_parts[0])
        start_year = int(start_parts[1])
        
        # Parse end_month_str (MM-YYYY)
        end_parts = end_month_str.split("-")
        if len(end_parts) != 2:
            raise ValueError("end_month must be in MM-YYYY format")
        end_month = int(end_parts[0])
        end_year = int(end_parts[1])
        
        # Validate months
        if start_month < 1 or start_month > 12 or end_month < 1 or end_month > 12:
            raise ValueError("Month must be between 1 and 12")
        
        # Generate list of month-year pairs
        month_year_pairs = []
        current_month = start_month
        current_year = start_year
        
        while True:
            month_year_pairs.append((current_month, current_year))
            
            # Check if we've reached the end
            if current_year == end_year and current_month == end_month:
                break
            
            # Move to next month
            current_month += 1
            if current_month > 12:
                current_month = 1
                current_year += 1
            
            # Safety check to prevent infinite loops
            if current_year > end_year + 1:
                break
        
        # Process each month-year pair
        for month, year in month_year_pairs:
            # Get total disbursed for this month
            total_disbursed = get_total_payroll_disbursed(
                db,
                month=month,
                year=year,
                dept_id=dept_id
            )
            
            # Get headcount for this month
            headcount_data = get_total_payroll_headcount(
                db,
                month=month,
                year=year,
                dept_id=dept_id
            )
            
            # Format month name (e.g., "January 2025")
            month_key = f"{month_names[month - 1]} {year}"
            
            # Combine the data
            monthly_summaries[month_key] = {
                "total_disbursed": total_disbursed,
                "total_headcount": headcount_data.get("total_headcount", 0),
                "pkwtt_headcount": headcount_data.get("pkwtt_headcount", 0),
                "pkwt_headcount": headcount_data.get("pkwt_headcount", 0),
                "mitra_headcount": headcount_data.get("mitra_headcount", 0)
            }
        
        return monthly_summaries
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {}


def get_department_summary(db: Session, month: int = None, year: int = None) -> list:
    """Get department summary with headcount breakdown, distribution ratio, and total disbursed. Only includes departments that exist in payroll_cost_owner."""
    
    try:
        # First, get total payroll headcount for distribution ratio calculation
        total_headcount_data = get_total_payroll_headcount(db, month=month, year=year)
        total_payroll_headcount = total_headcount_data.get("total_headcount", 1)  # Use 1 to avoid division by zero
        
        # Build the query to get department summaries grouped by department
        # Use unit_head from payroll_cost_owner as cost_owner
        query = """
        SELECT 
            pco.id_department as dept_id,
            pco.department_name,
            pco.unit_head as cost_owner,
            COUNT(DISTINCT pd.id_karyawan) as total_headcount,
            COUNT(DISTINCT CASE WHEN pd.status_kontrak = 1 THEN pd.id_karyawan END) as pkwtt_headcount,
            COUNT(DISTINCT CASE WHEN pd.status_kontrak = 2 THEN pd.id_karyawan END) as pkwt_headcount,
            COUNT(DISTINCT CASE WHEN pd.status_kontrak = 3 THEN pd.id_karyawan END) as mitra_headcount,
            SUM(pd.take_home_pay) as total_disbursed
        FROM payroll_detail pd
        INNER JOIN payroll_header ph ON pd.payroll_id = ph.payroll_id
        INNER JOIN payroll_cost_owner pco ON ph.dept_id = pco.id_department
        WHERE 1=1
        """
        
        params = {}
        
        # Add month filter if provided
        if month is not None:
            query += " AND pd.month = :month"
            params['month'] = month
        
        # Add year filter if provided
        if year is not None:
            query += " AND pd.year = :year"
            params['year'] = year
        
        # Group by department
        query += " GROUP BY pco.id_department, pco.department_name, pco.unit_head"
        # Order by department name
        query += " ORDER BY pco.department_name"
        
        # Execute the query
        result = db.execute(text(query), params)
        records = result.fetchall()
        
        # Convert to list of dictionaries and calculate distribution ratio
        departments = []
        for record in records:
            dept_id = record[0] if record[0] is not None else None
            raw_department_name = record[1] if record[1] is not None else None
            department_name = format_department_name(raw_department_name) if raw_department_name else None
            cost_owner = record[2] if record[2] is not None else None
            dept_total_headcount = record[3] if record[3] is not None else 0
            pkwtt_headcount = record[4] if record[4] is not None else 0
            pkwt_headcount = record[5] if record[5] is not None else 0
            mitra_headcount = record[6] if record[6] is not None else 0
            total_disbursed = record[7] if record[7] is not None else 0
            
            # Calculate distribution ratio
            distribution_ratio = dept_total_headcount / total_payroll_headcount if total_payroll_headcount > 0 else 0
            
            departments.append({
                "dept_id": dept_id,
                "department_name": department_name,
                "cost_owner": cost_owner,
                "total_headcount": dept_total_headcount,
                "pkwtt_headcount": pkwtt_headcount,
                "pkwt_headcount": pkwt_headcount,
                "mitra_headcount": mitra_headcount,
                "distribution_ratio": distribution_ratio,
                "total_disbursed": total_disbursed
            })
        
        return departments
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return []


def get_cost_owner_summary(db: Session, month: int = None, year: int = None) -> list:
    """Get cost owner summary with headcount breakdown, distribution ratio, and total disbursed. Only includes departments that exist in payroll_cost_owner."""
    
    try:
        # First, get total payroll headcount for distribution ratio calculation
        total_headcount_data = get_total_payroll_headcount(db, month=month, year=year)
        total_payroll_headcount = total_headcount_data.get("total_headcount", 1)  # Use 1 to avoid division by zero
        
        # Build the query to get cost owner summaries grouped by unit_head
        query = """
        SELECT 
            pco.unit_head as cost_owner,
            COUNT(DISTINCT pd.id_karyawan) as total_headcount,
            COUNT(DISTINCT CASE WHEN pd.status_kontrak = 1 THEN pd.id_karyawan END) as pkwtt_headcount,
            COUNT(DISTINCT CASE WHEN pd.status_kontrak = 2 THEN pd.id_karyawan END) as pkwt_headcount,
            COUNT(DISTINCT CASE WHEN pd.status_kontrak = 3 THEN pd.id_karyawan END) as mitra_headcount,
            SUM(pd.take_home_pay) as total_disbursed
        FROM payroll_detail pd
        INNER JOIN payroll_header ph ON pd.payroll_id = ph.payroll_id
        INNER JOIN payroll_cost_owner pco ON ph.dept_id = pco.id_department
        WHERE 1=1
        """
        
        params = {}
        
        # Add month filter if provided
        if month is not None:
            query += " AND pd.month = :month"
            params['month'] = month
        
        # Add year filter if provided
        if year is not None:
            query += " AND pd.year = :year"
            params['year'] = year
        
        # Group by cost owner (unit_head)
        query += " GROUP BY pco.unit_head"
        # Order by cost owner name
        query += " ORDER BY pco.unit_head"
        
        # Execute the query
        result = db.execute(text(query), params)
        records = result.fetchall()
        
        # Convert to list of dictionaries and calculate distribution ratio
        cost_owners = []
        for record in records:
            cost_owner = record[0] if record[0] is not None else None
            owner_total_headcount = record[1] if record[1] is not None else 0
            pkwtt_headcount = record[2] if record[2] is not None else 0
            pkwt_headcount = record[3] if record[3] is not None else 0
            mitra_headcount = record[4] if record[4] is not None else 0
            total_disbursed = record[5] if record[5] is not None else 0
            
            # Calculate distribution ratio
            distribution_ratio = owner_total_headcount / total_payroll_headcount if total_payroll_headcount > 0 else 0
            
            cost_owners.append({
                "cost_owner": cost_owner,
                "total_headcount": owner_total_headcount,
                "pkwtt_headcount": pkwtt_headcount,
                "pkwt_headcount": pkwt_headcount,
                "mitra_headcount": mitra_headcount,
                "distribution_ratio": distribution_ratio,
                "total_disbursed": total_disbursed
            })
        
        return cost_owners
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return []
