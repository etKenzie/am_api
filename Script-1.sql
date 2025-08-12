-- Employer
SELECT 
    tk.placement,
    tbl_gmc.keterangan 
FROM tbl_gmc
LEFT JOIN td_karyawan tk 
    ON tk.valdo_inc = tbl_gmc.kode_gmc
WHERE tbl_gmc.group_gmc = 'sub_client'
  AND tbl_gmc.aktif = 'Yes'
  AND tbl_gmc.keterangan3 = 1
GROUP BY tbl_gmc.kode_gmc;

-- Sourced to
SELECT 
    tk.placement,
    tbl_gmc.keterangan 
FROM tbl_gmc
LEFT JOIN td_karyawan tk 
    ON tk.placement = tbl_gmc.kode_gmc
WHERE tbl_gmc.group_gmc = 'placement_client'
  AND tbl_gmc.aktif = 'Yes'
  AND tbl_gmc.keterangan3 = 1
GROUP BY tbl_gmc.kode_gmc;

-- Project
SELECT 
    tk.placement,
    tbl_gmc.keterangan 
FROM tbl_gmc
LEFT JOIN td_karyawan tk 
    ON tk.project  = tbl_gmc.kode_gmc
WHERE tbl_gmc.group_gmc = 'client_project'
  AND tbl_gmc.aktif = 'Yes'
  AND tbl_gmc.keterangan3 = 1
GROUP BY tbl_gmc.kode_gmc;