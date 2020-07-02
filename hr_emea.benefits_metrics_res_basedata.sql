create or replace view hr_emea.benefits_metrics_res_basedata as


WITH LOAD AS(
  SELECT MAX(DW_LAST_UPDATED) AS "MAXDATE" FROM land_zn.o_remedy_tickets
  )
,resolvers AS(
  SELECT * FROM hr_emea.global_hrsd_mapping where report_switch = 1 and hrsd = 'Y'
  )
   ,rootcauses AS (SELECT * FROM hr_emea.hrssd_global_rootcauses)

  ,emp AS(
   SELECT 
      rank() over (partition by employee_login order by hr_begin_dt desc) as the_rank, date_trunc('mon',hr_begin_dt) as dd_dt
      ,lower(employee_login) as login
      ,department_name as dept_name
      ,department_id as dept_id
      ,company_country_code as country
      ,job_title_name as job_title_nm
      ,job_level_name as job_lvl_nm
      ,department_org_level1 as dept_org_level1
      ,department_org_level2 as dept_org_level2
      ,reports_to_supervisor_employee_name as reports_to_supervisor
      ,upper(reports_to_level_2_employee_login) as reports_to_level2_login
      ,upper(reports_to_level_3_employee_login) as reports_to_level3_login
      ,upper(reports_to_level_4_employee_login) as reports_to_level4_login
      ,upper(reports_to_level_5_employee_login) as reports_to_level5_login
      ,upper(reports_to_level_6_employee_login) as reports_to_level6_login
      ,upper(reports_to_level_7_employee_login) as reports_to_level7_login
      ,upper(reports_to_level_8_employee_login) as reports_to_level8_login
      ,upper(reports_to_level_9_employee_login) as reports_to_level9_login
  FROM land_zn.phoenix_total_headcount
  where employee_login IS NOT NULL
  )
,c AS(
  SELECT 
    distinct ticket_case_id
      , concerning_login 
  FROM hr_finance_crm.c_login_daily 
  WHERE create_date >= TO_DATE ('2018/10/01','YYYY/MM/DD'))
,sub_a AS (
      SELECT
    audit.case_id
    ,SUM(CASE WHEN (audit.TYPE = 261 AND LOWER(audit.TO_STRING) IN ('four eye check completed - document approved','4a')) THEN 1 ELSE 0 END) AS "four_a"
    ,SUM(CASE WHEN (audit.TYPE = 261 AND LOWER(audit.TO_STRING) IN ('four eye check completed - document rejected','4r')) THEN 1 ELSE 0 END) AS "four_r"
    ,SUM(CASE WHEN audit.description = 'Assigned Group' and audit.from_string IS NOT NULL THEN 1 ELSE 0 END) AS routed
    ,SUM(CASE WHEN audit.description = 'Pending Reason' THEN 1 ELSE 0 END) AS pending
    ,SUM(CASE WHEN audit.TYPE = 184 THEN 1 ELSE 0 END) AS reopened
    ,SUM(case when audit.TYPE = 261 and lower(audit.to_string) like '%audit%' then 1 else 0 end) as audit_root
    ,SUM(CASE WHEN audit.TYPE = 370 THEN 1 ELSE 0 END) AS created_from_quicklink
    ,MAX(CASE WHEN audit.TYPE = 370 THEN audit.TO_STRING ELSE NULL END) AS QUICKLINK_ID
      --,SUM(CASE WHEN audit.TYPE = 179 THEN 1 ELSE 0 END) AS correspondence_count
     ,MIN(CASE WHEN audit.TYPE = 261 and LOWER(audit.TO_STRING) IN ('2day','2days','2 days') THEN audit.modified_date ELSE TO_DATE('2099/01/01','YYYY/MM/DD') end) as two_days
    ,MIN(CASE WHEN audit.TYPE = 261 and LOWER(audit.TO_STRING) IN ('5day','5days','5 days') THEN audit.modified_date ELSE TO_DATE('2099/01/01','YYYY/MM/DD') end) as five_days
    ,SUM(CASE WHEN (audit.TYPE = 261 AND LOWER(audit.TO_STRING) IN ('four eye check required','4c')) THEN 1 ELSE 0 END) AS "four_c"
,SUM(CASE WHEN (audit.TYPE = 261 AND LOWER(audit.TO_STRING) IN ('4f')) THEN 1 ELSE 0 END) AS "four_f"
,SUM(CASE WHEN (audit.TYPE = 261 AND LOWER(audit.TO_STRING) IN ('4na')) THEN 1 ELSE 0 END) AS "four_na"
  ,MAX(CASE 
        WHEN(
          audit.TYPE = 261 
          AND (audit.TO_STRING between 1 and 9999) 
          and lower(audit.to_string) NOT IN ('4f','4r','4a','4c','4na','4ex') 
          and audit.created_by <> 'AR_ESCALATOR' and LENGTH(audit.TO_STRING) < 4) THEN TO_NUMBER(audit.TO_STRING,'9999') ELSE NULL 
          END 
          ) AS case_count

    --,MAX(CASE   
       -- WHEN(
          --audit.TYPE = 261 
         -- AND (audit.TO_STRING between 1 and 9999) 
          --and lower(audit.to_string) NOT IN ('4f','4r','4a','4c','4na','4ex') 
          --and audit.created_by <> 'AR_ESCALATOR' and LENGTH(audit.TO_STRING) < 4) THEN 1 ELSE NULL 
         -- END 
        --) AS case_count_hubs

  ,SUM(CASE WHEN audit.type = 20 AND FROM_STRING IS NOT NULL AND LEFT(TO_STRING,1) < LEFT(FROM_STRING,1) THEN 1 ELSE 0 END) AS escalated
  FROM land_zn.o_remedy_audittrail audit
  WHERE audit.create_day > TO_DATE('2018/01/01','YYYY/MM/DD')
  AND (audit.type IN ('261','184') OR audit.description in ('Assigned Group','Pending Reason'))
  GROUP BY audit.case_id
  )    
   ,tags as 
(with t1 as(
select t.case_id from land_zn.o_remedy_tickets t
LEFT JOIN sub_a
ON t.case_id = sub_a.case_id
LEFT JOIN resolvers res
ON t.assigned_to_group = res.resolver_group
LEFT JOIN hr_emea.emea_remedy_ctiprocess proc
ON t.category = proc.category
AND t.type = proc.type
LEFT JOIN emp AS emp_ass
ON LOWER(emp_ass.login) = lower(t.assigned_to_individual)
AND emp_ass.login IS NOT NULL
and emp_ass.the_rank = 1
and emp_ass.reports_to_level2_login = 'GALETTIB'
LEFT JOIN emp AS emp_res
ON LOWER(emp_res.login) = lower(t.resolved_by)
AND emp_res.login IS NOT NULL
and emp_res.the_rank = 1
and emp_res.reports_to_level2_login = 'GALETTIB'
WHERE 
--(lower(dash_process) = 'learning services' or (lower(dash_process) = 'all' and lower(proc.global_process) = 'learning services'))
--AND trunc(t.resolved_date)  = trunc(sysdate) - interval '1 day'
t.resolved_date >= TO_DATE('10/01/2018', 'MM/DD/YYYY'))     
--AND ((CASE WHEN DATEPART(dw,t.RESOLVED_DATE) = 0 THEN DATEPART(week,t.RESOLVED_DATE) +1 ELSE DATEPART(week,t.RESOLVED_DATE)
  --END BETWEEN (DATEPART(week,getdate()) - 1) AND (DATEPART(week,getdate()) -1)) AND (Datepart(year,t.RESOLVED_DATE) = Datepart(year,getdate())))                     
--and (emp_ass.reports_to_level6_login IN ('ARIJBASU') or emp_res.reports_to_level6_login IN ('ARIJBASU') or t.resolved_by = 'flx-HRDMIVS'))
SELECT
row_number() over (partition by a.case_id  order by modified_date desc) as row_num
,f.case_id
,a.to_string as root_cause_tag FROM land_zn.o_remedy_audittrail a
left join t1 f
on a.case_id = f.case_id
where
a.create_day >= to_date('2018/10/01','yyyy/mm/dd')
and a.type = 261
and f.case_id is not null) 
,routed as(
select case_id,
assigned_to,
from_string as routed_from,
to_string as routed_to,
create_date as routed_date,
create_date as modified_date,
rank() over (partition by case_id order by create_date desc) as the_rank
 from land_zn.o_remedy_audittrail c
where to_string IN (SELECT resolver_group FROM hr_emea.global_hrsd_mapping where dash_process in ('Benefits'))
and from_string is not null and from_string not in ('DM-Automation-Queue')
and create_date >= to_date('2018/01/01','yyyy/mm/dd')
 and type = 1

)
,reopened as(
select case_id,
case when created_by = 'flx-term' then 'Y' else 'N' end as reopened_by_system,
create_date as last_reopened_date,
rank() over (partition by case_id order by create_date desc) as the_rank
 from land_zn.o_remedy_audittrail c
where assigned_to IN (SELECT resolver_group FROM hr_emea.global_hrsd_mapping where dash_process in ('Exits'))
--and from_string is not null
and create_date >= to_date('2018/01/01','yyyy/mm/dd')
 --   'APAC Data Management','APAC Letter Generation','Costa Rica ERC Data Management','EMEA ERC Data Management','ERC DM HR for HR','ERC Data Management',,'ERC NAFC Seasonal','ROI ERC Data Management','UK ERC Data Management',
   -- 'erc india dm'
 and type = 184

)
,tx AS (SELECT
    distinct x.case_id
    ,x.assigned_to_group
    ,x.resolved_date
    ,(case when x.status IN ('Resolved','Closed') then (
       case when res_tp.location = 'DXB10' then networkdays_dxb(x.create_date,x.resolved_date) --- Need it
            when res_tp.location IN ('PEK2','PEK10','PEK','HND','HND11','JPN','CHN') then networkdays(zone_pek(x.create_date),zone_pek(x.resolved_date)) --- Need it
            when res_tp.days_week = 6 then networkdays_six(x.create_date,x.resolved_date) --- Need it
            when res_tp.days_week = 7 then networkdays_seven(x.create_date,x.resolved_date) --- Need it
            else networkdays(x.create_date,x.resolved_date) --- Need it
            end)
    else (
        case when res_tp.location = 'DXB10' then networkdays_dxb(x.create_date,(SELECT MAXDATE FROM LOAD))
            when res_tp.location IN ('PEK2','PEK10','PEK','HND','HND11','JPN','CHN') then networkdays(zone_pek(x.create_date),zone_pek((SELECT MAXDATE FROM LOAD)))
            when res_tp.days_week = 6 then networkdays_six(x.create_date,(SELECT MAXDATE FROM LOAD))
            when res_tp.days_week = 7 then networkdays_seven(x.create_date,(SELECT MAXDATE FROM LOAD))
            else networkdays(x.create_date,(SELECT MAXDATE FROM LOAD)) end)
            end)
     AS TTR
    FROM land_zn.o_remedy_tickets x
LEFT JOIN resolvers res_tp
ON x.assigned_to_group = res_tp.resolver_group
LEFT JOIN hr_emea.opr_benefits_tt_mapping_v3 p
ON x.assigned_to_group = p.assigned_to_group
AND lower(x.category) = lower(p.category)
AND lower(x.type) = lower(p.type)
AND lower(x.item) = lower(p.item)

    WHERE 
    --res_tp.resolver_group IS NOT NULL and res_tp.report_switch = 1 and hrsd = 'Y'
      (x.assigned_to_group+x.category+x.type+x.item in (select distinct grouping from hr_emea.opr_benefits_tt_mapping_v3 p))
  AND (x.resolved_date >= TO_DATE('2018/01/01','YYYY/MM/DD'))
  and x.status IN('Resolved','Closed')
  AND NVL(x.root_cause,'x') NOT IN ('OAA/ALERT LIST WORKFLOW TICKET', 
                    'OAA/WORKFLOW TICKET', 'Test - Do not include in metrics'
                    , 'Test Ticket - Exclude from metrics'
                    , 'Test Ticket - Remove from metrics'
                    ,'Test Ticket - remove from metrics'))
,tp AS (
  SELECT *, PERCENTILE_CONT(0.9) WITHIN GROUP(ORDER BY TTR) OVER (PARTITION BY tx.assigned_to_group, DATE_TRUNC('month',tx.resolved_date)) AS TP90onTTR
  ,PERCENTILE_CONT(0.9) WITHIN GROUP(ORDER BY TTR) OVER (PARTITION BY tx.assigned_to_group, DATE_TRUNC('week',tx.resolved_date)) AS W_TP90onTTR
  ,PERCENTILE_CONT(0.75) WITHIN GROUP(ORDER BY TTR) OVER (PARTITION BY tx.assigned_to_group, DATE_TRUNC('week',tx.resolved_date)) AS W_TP75onTTR
  ,PERCENTILE_CONT(0.50) WITHIN GROUP(ORDER BY TTR) OVER (PARTITION BY tx.assigned_to_group, DATE_TRUNC('week',tx.resolved_date)) AS W_TP50onTTR
  ,PERCENTILE_CONT(0.25) WITHIN GROUP(ORDER BY TTR) OVER (PARTITION BY tx.assigned_to_group, DATE_TRUNC('week',tx.resolved_date)) AS W_TP25onTTR
  FROM tx)        
select 
    distinct t.case_id
    ,t.assigned_to_group as assigned_to_group
    ,res.super_region AS superregion

    ,CASE
  WHEN lower(t.assigned_to_group) IN ('benefits cz corp - hr4hr', 'benefits emea prg eng', 'benefits emea prg hr4hr', 'apac mnl ctk-jpn','emea erc data management','emea erc loa','emea erc onboarding','hrs recruiting support','apac loa','emea erc terminations','hrs recruiting emea apac','apac letter generation','apac data management','apac extended onboarding','eu corp employee referrals','apac terminations','apac payroll prep','apac benefits - hyd',
  'apac mnl hrs data mgmt green', 'apac mnl hrs time attendance','apac mnl hrs data management','apac mnl hrs letter management','apac mnl hrs immigration','apac mnl hrs onboarding','apac mnl hrs payroll prep','apac mnl hrs benefit','apac mnl hrs hr for hr','apac mnl hrs leave of absence','apac mnl hrs terminations'
  ) and proc.country IS NOT NULL THEN proc.country
  when t.category = 'Human Resources - Belgium' then 'BEL'
  when t.category = 'Human Resources - Sweden' then 'SWE'
  when t.category = 'Human Resources - Norway' then 'NOR'
  when res.country IS NOT NULL THEN res.country
  when emp_con.country IS NOT NULL then emp_con.country
  when emp_req.country IS NOT NULL then emp_req.country
  ELSE 'Unmapped'
  END AS resolver_country

    ,case when p.transaction_type is null then 'Other' else p.transaction_type end as transaction_type
    ,t.category
    ,t.type
    ,t.item
    ,t.closure_code
    ,aud.correspondence_count
    ,t.requester_login
    ,t.submitted_by
    ,t.assigned_to_individual
    ,t.resolved_by
    ,t.resolution
   --  ,tp.W_TP90onTTR
     --,tp.W_TP75onTTR
     --,tp.W_TP50onTTR
     --,tp.W_TP25onTTR
    ,case when aud.correspondence_count BETWEEN 0 and 4 THEN '1 to 3'
    when  aud.correspondence_count BETWEEN 4 and 10 THEN '4 to 10'
    when aud.correspondence_count BETWEEN 11 and 26 THEN '11 to 25'
    when aud.correspondence_count > 25 THEN '>25' END as Correspondence_bucket
  ,TO_CHAR(t.CREATE_DATE,'YYYY-MM-DD HH24:MI:SS') AS create_date
    ,TO_CHAR(t.resolved_DATE,'YYYY-MM-DD HH24:MI:SS') AS resolved_date_time
    ,trunc(t.resolved_date) as resolved_date
    ,cast(datepart(YEAR,t.resolved_date) as varchar)+cast('-' as varchar)+cast(datepart(Month,t.resolved_date) as varchar) as resolved_Month
    ,cast(datepart(YEAR,t.resolved_date) as varchar)+cast('-' as varchar)+cast(case when datepart(dw, t.resolved_date)= 0 then (datepart(week, t.resolved_date))+1
 else datepart(week, t.resolved_date) end as varchar) as resolved_week
  ,abs(case when t.status IN ('Resolved','Closed') then (
        case when res.location = 'DXB10' then networkdays_dxb(t.create_date,t.resolved_date)
            when res.location IN ('PEK2','PEK10','PEK','HND','HND11','JPN','CHN','MNL') then networkdays(zone_pek(t.create_date),zone_pek(t.resolved_date))
            when res.days_week = 6 then networkdays_six(t.create_date,t.resolved_date)
            when res.days_week = 7 then networkdays_seven(t.create_date,t.resolved_date)
            else networkdays(t.create_date,t.resolved_date)
            end)
    else (
        case when res.location = 'DXB10' then networkdays_dxb(t.create_date,(SELECT MAXDATE FROM LOAD))
            when res.location IN ('PEK2','PEK10','PEK','HND','HND11','JPN','CHN','MNL') then networkdays(zone_pek(t.create_date),zone_pek((SELECT MAXDATE FROM LOAD)))
            when res.days_week = 6 then networkdays_six(t.create_date,(SELECT MAXDATE FROM LOAD))
            when res.days_week = 7 then networkdays_seven(t.create_date,(SELECT MAXDATE FROM LOAD))
            else networkdays(t.create_date,(SELECT MAXDATE FROM LOAD)) end)
            end)
    as TTR
    ,tp.TP90onTTR

,CASE
  WHEN p.transaction_type = 'Benefit Support Order' and t.item in ('Waiting Period (90 Days)') then '-'
  when p.transaction_type = 'Benefit Support Order' and t.item in ('Employee Not Enrolled') and networkdays(t.create_date,t.resolved_date)*24.00 <= 960.00 then 'Y'
    
  when p.transaction_type = 'Benefit Support Order' and t.item in ('Employee Not Enrolled') and t.resolution IN 
  ('"Duplicate - 2nd Order/Non Compliance letter"','"Duplicate - True Duplicate"','"Other – General Query"','"Others - Insurance Verification form"','Others - Insurance Verification form'
  ,'"Others - Not an NMSN/QMCSO "','"Release - Duplicate"','Duplicate - 2nd Order/Non Compliance letter','Duplicate - Duplicate - 2nd Order/Non Compliance letter'
    ,'Duplicate - True Duplicate','Others','Others - General queries','Others - general queries','Other -Refund Requested') THEN '-'
  
  when p.transaction_type = 'Benefit Support Order' and t.item in ('Employee Not Enrolled') 
  and t.resolution IN ('"Enrolled -Enrolled Dependent"','Enrolled -Enrolled Dependent','Enrolled Dependent','"Enrolled Dependent"') and networkdays(t.create_date,t.resolved_date)*24.00 <= 960.00
  then 'Y'

  when p.transaction_type = 'Benefit Support Order' and t.item in ('Benefit Support Order') and t.resolution IN 
  ('"Did not Enroll - Employee Terminated"','"Did not Enroll - Employee has other garnishment"','"Did not Enroll - Employee not eligible"'
  ,'"Did not Enroll - Employee not found"','"Did not Enroll - Group ID not eligible"','"Did not Enroll - Missing Information in Court Order"'
  ,'"Did not enroll - Not eligible"','"Did not enroll-Employee Terminated"','"Release - Dropped QMSCO Flags in WTW"','"Release – Refund Request"',
    'Did not Enroll - Employee Terminated','Did not Enroll - Employee has other garnishment','Did not Enroll - Employee not found',
    'Did not Enroll - Employee on LOA','Did not Enroll - Group ID not eligible','"Did not Enroll - Employee has other garnishments"',
    'Did not Enroll - Employee has other garnishments','Release - QMCSO flags dropped in WTW',
    '"Did not Enroll - Release received before processing NMSN"','"Did not Enroll - Noncompliance - Original Order Missing"','Did not Enroll - Noncompliance - Original Order Missing'
    ,'"Release - Dependents not enrolled"','"Release - Employee not found"','"Release - Employee Terminated"',
    'Release - Employee Terminated','Release - Employee not found','Did not Enroll - Release received before processing NMSN'
    ,'"Release - QMCSO flags dropped in WTW"')
  and networkdays(t.create_date,t.resolved_date)*24.00 <= 480.00 then 'Y'

  when p.transaction_type = 'Benefit Support Order' and t.item in ('Benefit Support Order') and t.resolution IS NULL and networkdays(t.create_date,t.resolved_date)*24.00 <= 480.00 then 'Y' 

  when p.transaction_type = 'Benefit Support Order' and t.item in ('Benefit Support Order') and t.resolution IN 
  ('"Duplicate - 2nd Order/Non Compliance letter"','"Duplicate - True Duplicate"','"Other – General Query"','"Others - Insurance Verification form"'
  ,'"Others - Not an NMSN/QMCSO "','"Release - Duplicate"','Duplicate - 2nd Order/Non Compliance letter','Duplicate - Duplicate - 2nd Order/Non Compliance letter'
    ,'Duplicate - True Duplicate','Others','Others - General queries','Others - general queries') THEN '-'
  
  when p.transaction_type = 'Benefit Support Order' and t.item in ('Benefit Support Order') and t.resolution IN ('"Enrolled -Enrolled Dependent"','Enrolled -Enrolled Dependent','Enrolled Dependent','"Enrolled Dependent"')
  and networkdays(t.create_date,t.resolved_date)*24.00 <= 960.00 then 'Y'
  
  WHEN t.assigned_to_group = 'ERC Extended Onboarding' and emp_req.reports_to_level5_login = ('JMACDOUG') then '-'
  WHEN (sla.sla_target = -5 or res.sla_target = -5 or def.sla_target = -5)  and (sub_a.five_days = TO_DATE('2099/01/01','YYYY/MM/DD') 
        or sub_a.five_days IS NULL) and (sub_a.two_days = TO_DATE('2099/01/01','YYYY/MM/DD') or sub_a.two_days IS NULL) then '-'
  WHEN (sla.sla_target = -5 or res.sla_target = -5 or def.sla_target = -5)  and networkdays(t.create_date,sub_a.two_days)*24.00 <= 48.00
        and networkdays(sub_a.two_days,sub_a.five_days)*24.00 <= 120.00 then 'Y'
  WHEN (sla.sla_target = -5 or res.sla_target = -5 or def.sla_target = -5)  and (networkdays(t.create_date,sub_a.two_days)*24.00 > 48.00
       or networkdays(sub_a.two_days,sub_a.five_days)*24.00 > 120.00) then 'N'
  WHEN t.assigned_to_group IN ('Costa Rica ERC Data Management','ERC Data Management','ERC DM HR for HR') and  (lower(t.root_cause) like '%audit%' OR sub_a.audit_root >0) then '-'
  WHEN def.sla_target = -1 then '-'
  WHEN sla.sla_target = -1 then '-'
  WHEN t.assigned_to_group = 'ERC Extended Onboarding' and t.submitted_by IN (select distinct t.assigned_to_individual FROM land_zn.o_remedy_tickets t
                                                                                   where t.assigned_to_group = 'ERC Extended Onboarding'
                                                                                   AND    (LEFT (t.resolved_date,11) BETWEEN (TO_DATE((Getdate() - 30),'YYYY-MM-DD')) AND (TO_DATE((Getdate() - 1),'YYYY-MM-DD')))
                                                                                   ) then '-'
       WHEN t.assigned_to_group IN ('ERC Onboarding','EMEA ERC Onboarding','Internal Transfers') and t.submitted_by IN (select distinct t.assigned_to_individual FROM land_zn.o_remedy_tickets t
                                                                                   where t.assigned_to_group IN ('ERC Onboarding','EMEA ERC Onboarding','Internal Transfers')
                                                                                   AND    (LEFT (t.resolved_date,11) BETWEEN (TO_DATE((Getdate() - 30),'YYYY-MM-DD')) AND (TO_DATE((Getdate() - 1),'YYYY-MM-DD')))
                                                                                   ) then '-'  
      WHEN  t.assigned_to_group IN ('ERC Onboarding','EMEA ERC Onboarding','Internal Transfers','ERC Extended Onboarding','erc india onboarding') and t.closure_code = 'Duplicate' THEN '-'                                                                                                                                                                
      WHEN lower(dash_process) = 'time and attendance/ctk' and emp_int.reports_to_level7_login = 'SIDHARTT' THEN '-'
      WHEN t.assigned_to_group = 'ERC Onboarding' and t.item not in ('Benefit Support Order', 'Internal Transfer - International',  'Merge Records (ERC Use Only)', 
                              'New Hire Paperwork',  'Temp/Vendor/Contractor', 'US - Corporate', 'US - Customer Service', 
                              'US - NAFC 4+', 'US - NAFC Tier 1 to Tier 3','Bonus/Pay/Offer Letter Issue', 'Conversion', 'Direct Hires',     
                              'General Questions', 'Badge Scan Failure', 'BR-Corporate',  'MEX - Corporate', 'Corporate Offer Extension') THEN '-'
  WHEN (res.sla_target_exists = 'N' and sla.sla_target IS NULL and def.sla_target IS NULL) THEN '-'
  ELSE 
  (
      CASE
            SIGN(
                (case when t.status IN ('Resolved','Closed') then (
                    case when res.location = 'DXB10' then networkdays_dxb(t.create_date,t.resolved_date)
                    when res.location IN ('PEK2','PEK10','PEK','HND','HND11','JPN','CHN') then networkdays(zone_pek(t.create_date),zone_pek(t.resolved_date))
                    when res.days_week = 6 then networkdays_six(t.create_date,t.resolved_date)
                    when res.days_week = 7 then networkdays_seven(t.create_date,t.resolved_date)
                    else networkdays(t.create_date,t.resolved_date)
                    end)
             else (
                    case when res.location = 'DXB10' then networkdays_dxb(t.create_date,(SELECT MAXDATE FROM LOAD))
                    when res.location IN ('PEK2','PEK10','PEK','HND','HND11','JPN','CHN') then networkdays(zone_pek(t.create_date),zone_pek((SELECT MAXDATE FROM LOAD)))
                    when res.days_week = 6 then networkdays_six(t.create_date,(SELECT MAXDATE FROM LOAD))
                    when res.days_week = 7 then networkdays_seven(t.create_date,(SELECT MAXDATE FROM LOAD))
                    else networkdays(t.create_date,(SELECT MAXDATE FROM LOAD))
            end)
    end
-
  (CASE
  WHEN t.assigned_to_group IN ('Executive Recruiting','Recruiting Sourcing Requests') and t.impact = 2 then 1
  WHEN (t.category = 'EMEA HRS Learning' and t.type = 'New enrollment' and t.item = 'Instructor-led training'
     and (t.root_cause='PRG Languages BN' OR t.root_cause_details ='PRG Languages BN' OR t.root_cause='PRG Languages CZ' OR t.root_cause_details = 'PRG Languages CZ')) then 7
  WHEN t.assigned_to_group IN ('UK ERC Data Management') and t.impact IN (2,3) then 1
  WHEN t.assigned_to_group IN ('TAO Team') and t.impact = 3 then 1
  WHEN t.assigned_to_group IN ('TAO Team') and t.impact = 4 then 2
  WHEN def.sla_target IS NOT NULL then def.sla_target
  WHEN sla.sla_target IS NOT NULL THEN sla.sla_target
  WHEN res.sla_target IS NOT NULL then res.sla_target
  WHEN t.IMPACT = 3 and res.location NOT IN ('IND','HYD') THEN 1
  ELSE 2 END)))
 WHEN -1 THEN 'Y'
    WHEN 0 THEN 'Y'
    WHEN 1 THEN 'N'
        END)
        
        END AS sla_target_met

    ----------------------
    -- sla calculation end
    ----------------------
 ,CASE
  WHEN p.transaction_type = 'Benefit Support Order' and t.item in ('Waiting Period (90 Days)') then -1
  when p.transaction_type = 'Benefit Support Order' and t.item in ('Employee Not Enrolled') then 40
  
  when p.transaction_type = 'Benefit Support Order' and t.item in ('Employee Not Enrolled') and t.resolution IN 
  ('"Duplicate - 2nd Order/Non Compliance letter"','"Duplicate - True Duplicate"','"Other – General Query"','"Others - Insurance Verification form"','Others - Insurance Verification form'
  ,'"Others - Not an NMSN/QMCSO "','"Release - Duplicate"','Duplicate - 2nd Order/Non Compliance letter','Duplicate - Duplicate - 2nd Order/Non Compliance letter'
    ,'Duplicate - True Duplicate','Others','Others - General queries','Others - general queries','Other -Refund Requested') THEN -1

  when p.transaction_type = 'Benefit Support Order' and t.item in ('Employee Not Enrolled') 
  and t.resolution IN ('"Enrolled -Enrolled Dependent"','Enrolled -Enrolled Dependent','Enrolled Dependent','"Enrolled Dependent"')
  then 60

  when p.transaction_type = 'Benefit Support Order' and t.item in ('Benefit Support Order') and t.resolution IN 
  ('"Did not Enroll - Employee Terminated"','"Did not Enroll - Employee has other garnishment"','"Did not Enroll - Employee not eligible"'
  ,'"Did not Enroll - Employee not found"','"Did not Enroll - Group ID not eligible"','"Did not Enroll - Missing Information in Court Order"'
  ,'"Did not enroll - Not eligible"','"Did not enroll-Employee Terminated"','"Release - Dropped QMSCO Flags in WTW"','"Release – Refund Request"'
    ,'Did not Enroll - Employee Terminated','Did not Enroll - Employee has other garnishment','Did not Enroll - Employee not found',
    'Did not Enroll - Employee on LOA','Did not Enroll - Group ID not eligible','"Did not Enroll - Employee has other garnishments"',
    'Did not Enroll - Employee has other garnishments','Release - QMCSO flags dropped in WTW',
    '"Did not Enroll - Release received before processing NMSN"','"Did not Enroll - Noncompliance - Original Order Missing"','Did not Enroll - Noncompliance - Original Order Missing'
    ,'"Release - Dependents not enrolled"','"Release - Employee not found"','"Release - Employee Terminated"',
    'Release - Employee Terminated','Release - Employee not found','Did not Enroll - Release received before processing NMSN'
    ,'"Release - QMCSO flags dropped in WTW"'
)
  then 20

  when p.transaction_type = 'Benefit Support Order' and t.item in ('Benefit Support Order') and t.resolution IS NULL then 20

  when p.transaction_type = 'Benefit Support Order' and t.item in ('Benefit Support Order') and t.resolution IN 
  ('"Duplicate - 2nd Order/Non Compliance letter"','"Duplicate - True Duplicate"','"Other – General Query"','"Others - Insurance Verification form"','Others - Insurance Verification form'
  ,'"Others - Not an NMSN/QMCSO "','"Release - Duplicate"','Duplicate - 2nd Order/Non Compliance letter','Duplicate - Duplicate - 2nd Order/Non Compliance letter'
    ,'Duplicate - True Duplicate','Others','Others - General queries','Others - general queries','Other -Refund Requested') THEN -1

  when p.transaction_type = 'Benefit Support Order' and t.item in ('Benefit Support Order') 
  and t.resolution IN ('"Enrolled -Enrolled Dependent"','Enrolled -Enrolled Dependent','Enrolled Dependent','"Enrolled Dependent"')
  then 40


  WHEN (sla.sla_target = -5 or res.sla_target = -5 or def.sla_target = -5)  and (sub_a.five_days = TO_DATE('2099/01/01','YYYY/MM/DD') 
    or sub_a.five_days IS NULL) and (sub_a.two_days = TO_DATE('2099/01/01','YYYY/MM/DD') or sub_a.two_days IS NULL) then -1
  WHEN (sla.sla_target = -5 or res.sla_target = -5 or def.sla_target = -5)  then -5
  WHEN t.assigned_to_group = 'ERC Extended Onboarding' and emp_req.reports_to_level5_login = ('JMACDOUG') then -1
  WHEN t.assigned_to_group IN ('UK ERC Data Management') and t.impact IN (2,3) then 1
  WHEN t.assigned_to_group IN ('Costa Rica ERC Data Management','ERC Data Management','ERC DM HR for HR') and  (lower(t.root_cause) like '%audit%' OR sub_a.audit_root >0) then -1
  WHEN t.assigned_to_group = 'ERC Extended Onboarding' and t.submitted_by IN (select distinct t.assigned_to_individual FROM land_zn.o_remedy_tickets t
                                                                                   where t.assigned_to_group = 'ERC Extended Onboarding'
                                                                                   AND    (LEFT (t.resolved_date,11) BETWEEN (TO_DATE((Getdate() - 30),'YYYY-MM-DD')) AND (TO_DATE((Getdate() - 1),'YYYY-MM-DD')))
                                                                                   ) then -1
       WHEN t.assigned_to_group IN ('ERC Onboarding','EMEA ERC Onboarding','Internal Transfers') and t.submitted_by IN (select distinct t.assigned_to_individual FROM land_zn.o_remedy_tickets t
                                                                                   where t.assigned_to_group IN ('ERC Onboarding','EMEA ERC Onboarding','Internal Transfers')
                                                                                   AND    (LEFT (t.resolved_date,11) BETWEEN (TO_DATE((Getdate() - 30),'YYYY-MM-DD')) AND (TO_DATE((Getdate() - 1),'YYYY-MM-DD')))
                                                                                   ) then -1  
      WHEN  t.assigned_to_group IN ('ERC Onboarding','EMEA ERC Onboarding','Internal Transfers','ERC Extended Onboarding','erc india onboarding') and t.closure_code = 'Duplicate' THEN -1                                                                                                                                                              
      WHEN lower(dash_process) = 'time and attendance/ctk' and emp_int.reports_to_level7_login = 'SIDHARTT' THEN -1
      WHEN t.assigned_to_group = 'ERC Onboarding' and t.item not in ('Benefit Support Order', 'Internal Transfer - International',  'Merge Records (ERC Use Only)', 
                              'New Hire Paperwork',  'Temp/Vendor/Contractor', 'US - Corporate', 'US - Customer Service', 
                              'US - NAFC 4+', 'US - NAFC Tier 1 to Tier 3','Bonus/Pay/Offer Letter Issue', 'Conversion', 'Direct Hires',     
                              'General Questions', 'Badge Scan Failure', 'BR-Corporate',  'MEX - Corporate', 'Corporate Offer Extension') THEN -1
  WHEN t.assigned_to_group IN ('Executive Recruiting','Recruiting Sourcing Requests') and t.impact = 2 then 1
  WHEN (t.category = 'EMEA HRS Learning' and t.type = 'New enrollment' and t.item = 'Instructor-led training'
     and (t.root_cause='PRG Languages BN' OR t.root_cause_details ='PRG Languages BN' OR t.root_cause='PRG Languages CZ' OR t.root_cause_details = 'PRG Languages CZ')) then 7
  WHEN t.assigned_to_group IN ('TAO Team') and t.impact = 3 then 1
  WHEN t.assigned_to_group IN ('TAO Team') and t.impact = 4 then 2
  WHEN def.sla_target IS NOT NULL then def.sla_target
  WHEN sla.sla_target IS NOT NULL THEN sla.sla_target
  WHEN res.sla_target_exists = 'N' THEN -1
  WHEN res.sla_target IS NOT NULL then res.sla_target
  WHEN t.IMPACT = 3 and res.location NOT IN ('IND','HYD') THEN 1
  ELSE 2 
  END AS sla_taget_value
    -------------------------
    -- Sla target calculation
    -------------------------
   ,CASE
  WHEN p.transaction_type = 'Benefit Support Order' and t.item in ('Waiting Period (90 Days)') then 'N'
  when p.transaction_type = 'Benefit Support Order' and t.item in ('Employee Not Enrolled') then 'Y'
  when p.transaction_type = 'Benefit Support Order' and t.item in ('Employee Not Enrolled') and t.resolution IN 
  ('"Duplicate - 2nd Order/Non Compliance letter"','"Duplicate - True Duplicate"','"Other – General Query"','"Others - Insurance Verification form"','Others - Insurance Verification form'
  ,'"Others - Not an NMSN/QMCSO "','"Release - Duplicate"','Duplicate - 2nd Order/Non Compliance letter','Duplicate - Duplicate - 2nd Order/Non Compliance letter'
    ,'Duplicate - True Duplicate','Others','Others - General queries','Others - general queries','Other -Refund Requested') THEN 'N'

  when p.transaction_type = 'Benefit Support Order' and t.item in ('Employee Not Enrolled') 
  and t.resolution IN ('"Enrolled -Enrolled Dependent"','Enrolled -Enrolled Dependent','Enrolled Dependent','"Enrolled Dependent"')
  then 'Y'
  
    when p.transaction_type = 'Benefit Support Order' and t.item in ('Benefit Support Order') and t.resolution IN 
      ('"Did not Enroll - Employee Terminated"','"Did not Enroll - Employee has other garnishment"','"Did not Enroll - Employee not eligible"'
  ,'"Did not Enroll - Employee not found"','"Did not Enroll - Group ID not eligible"','"Did not Enroll - Missing Information in Court Order"'
  ,'"Did not enroll - Not eligible"','"Did not enroll-Employee Terminated"','"Release - Dropped QMSCO Flags in WTW"','"Release – Refund Request"'
    ,'Did not Enroll - Employee Terminated','Did not Enroll - Employee has other garnishment','Did not Enroll - Employee not found',
    'Did not Enroll - Employee on LOA','Did not Enroll - Group ID not eligible','"Did not Enroll - Employee has other garnishments"',
    'Did not Enroll - Employee has other garnishments','Release - QMCSO flags dropped in WTW',
    '"Did not Enroll - Release received before processing NMSN"','"Did not Enroll - Noncompliance - Original Order Missing"','Did not Enroll - Noncompliance - Original Order Missing'
    ,'"Release - Dependents not enrolled"','"Release - Employee not found"','"Release - Employee Terminated"',
    'Release - Employee Terminated','Release - Employee not found','Did not Enroll - Release received before processing NMSN'
    ,'"Release - QMCSO flags dropped in WTW"') then 'Y'

    when p.transaction_type = 'Benefit Support Order' and t.item in ('Benefit Support Order') and t.resolution IS NULL and networkdays(t.create_date,t.resolved_date)*24.00 <= 480.00 then 'Y'

    when p.transaction_type = 'Benefit Support Order' and t.item in ('Benefit Support Order') and t.resolution IN 
    ('"Did not Enroll - Noncompliance - Original Order Missing"','"Did not Enroll - Release received before processing NMSN"','"Duplicate - 2nd Order/Non Compliance letter"','"Duplicate - True Duplicate"','"Other – General Query"','"Others - Insurance Verification form"','"Others - Not an NMSN/QMCSO "','"Release - Dependents not enrolled"','"Release - Duplicate"','"Release - Employee Terminated"',
      '"Release - Employee not found"','Duplicate - 2nd Order/Non Compliance letter','Duplicate - Duplicate - 2nd Order/Non Compliance letter','Duplicate - True Duplicate','Others','Release - Employee Terminated','Release - Employee not found') THEN 'N'
    when p.transaction_type = 'Benefit Support Order' and t.item in ('Benefit Support Order') and t.resolution IN ('"Enrolled -Enrolled Dependent"','Enrolled -Enrolled Dependent') then 'Y'



WHEN (sla.sla_target = -5 or res.sla_target = -5 or def.sla_target = -5) and (sub_a.five_days = TO_DATE('2099/01/01','YYYY/MM/DD') 
    or sub_a.five_days IS NULL) and (sub_a.two_days = TO_DATE('2099/01/01','YYYY/MM/DD') or sub_a.two_days IS NULL) then 'N'
WHEN (sla.sla_target = -5 or res.sla_target = -5 or def.sla_target = -5) then 'Y'

WHEN (sla.sla_target = -1 OR def.sla_target = -1) THEN 'N'

WHEN t.assigned_to_group = 'ERC Extended Onboarding' and emp_req.reports_to_level5_login = ('JMACDOUG') then 'N'
WHEN t.assigned_to_group IN ('Costa Rica ERC Data Management','ERC Data Management','ERC DM HR for HR') and  (lower(t.root_cause) like '%audit%' OR sub_a.audit_root >0) then 'N'
WHEN t.assigned_to_group = 'ERC Extended Onboarding' and t.submitted_by IN (select distinct t.assigned_to_individual FROM land_zn.o_remedy_tickets t
                                                                                   where t.assigned_to_group = 'ERC Extended Onboarding'
                                                                                   AND    (LEFT (t.resolved_date,11) BETWEEN (TO_DATE((Getdate() - 30),'YYYY-MM-DD')) AND (TO_DATE((Getdate() - 1),'YYYY-MM-DD')))
                                                                                   ) then 'N'
       WHEN t.assigned_to_group IN ('ERC Onboarding','EMEA ERC Onboarding','Internal Transfers') and t.submitted_by IN (select distinct t.assigned_to_individual FROM land_zn.o_remedy_tickets t
                                                                                   where t.assigned_to_group IN ('ERC Onboarding','EMEA ERC Onboarding','Internal Transfers')
                                                                                   AND    (LEFT (t.resolved_date,11) BETWEEN (TO_DATE((Getdate() - 30),'YYYY-MM-DD')) AND (TO_DATE((Getdate() - 1),'YYYY-MM-DD')))
                                                                                   ) then 'N'  
      WHEN  t.assigned_to_group IN ('ERC Onboarding','EMEA ERC Onboarding','Internal Transfers','ERC Extended Onboarding','erc india onboarding') and t.closure_code = 'Duplicate' THEN 'N'                                                                                                                                                                
      WHEN lower(dash_process) = 'time and attendance/ctk' and emp_int.reports_to_level7_login = 'SIDHARTT' THEN 'N'
      WHEN t.assigned_to_group = 'ERC Onboarding' and t.item not in ('Benefit Support Order', 'Internal Transfer - International',  'Merge Records (ERC Use Only)', 
                              'New Hire Paperwork',  'Temp/Vendor/Contractor', 'US - Corporate', 'US - Customer Service', 
                              'US - NAFC 4+', 'US - NAFC Tier 1 to Tier 3','Bonus/Pay/Offer Letter Issue', 'Conversion', 'Direct Hires',     
                              'General Questions', 'Badge Scan Failure', 'BR-Corporate',  'MEX - Corporate', 'Corporate Offer Extension') THEN 'N'
WHEN (sla.sla_target > 0 or def.sla_target > 0) THEN 'Y'
WHEN res.sla_target_exists = 'N' then 'N'
ELSE 'Y'
END AS sla_target_exists

      ,CASE WHEN t.status not in ('Closed','Resolved') then null else DATE_TRUNC('mon', t.resolved_date) end as year_month
    -- ,res.super_region AS superregion
    -- ,count( * ) AS tt_count
       ,t.root_cause_details

    --,case when res.location IN ('IND','IND/CRI','CRI/IND') THEN
    --(TO_NUMBER(REGEXP_REPLACE(CASE WHEN (CASE WHEN Regexp_instr (t.root_cause_details,'[A-za-z]+|/s+|/+|-+|[.,\!$%\^&\*;:{}=\_`~()]+') > 0 THEN NULL ELSE t.root_cause_details END) = '0' THEN '1'
    --WHEN (CASE WHEN Regexp_instr (t.root_cause_details,'[A-za-z]+|/s+|/+|-+|[.,\!$%\^&\*;:{}=\_`~()]+') > 0 THEN NULL ELSE t.root_cause_details END) IS NULL THEN '1'
    --ELSE t.root_cause_details END,'[#]+'),'9999'))

  ,case --when res.location IN ('IND','IND/CRI','CRI/IND') and 
    when sub_a.case_count IS NULL THEN 1 
   -- when res.delivery_group IN (‘Manila Hubs’,’China Hubs’, ‘Japan Hubs’,'EMEA hubs') then sub_a.case_count_hubs
   -- when sub_a.case_count_hubs IS NULL THEN 1
    ELSE sub_a.case_count 
    END AS total_case_count

  , CASE
    WHEN (NVL(res.defect_scope,'x') <> 'Y') then 'Not Defect Scope'
    WHEN (NVL(res.defect_scope,'x') = 'Y' and t.resolved_date < res.defect_launch and res.defect_launch IS NOT NULL) then 'Not Defect Scope'
    WHEN (t.root_cause IS NULL or NVL(t.root_cause,'x') = 'x') THEN 'Blank'
    WHEN rootcauses.root_cause IS NULL then 'Other root cause'
    ELSE rootcauses.root_cause END as root_cause_defect
,case
    when res.delivery_group in ('Manila Hubs') and (emp_ass.country in ('PHL') OR emp_res.country in ('PHL')) THEN 'MNL'
    when res.delivery_group in ('Manila Hubs') and (emp_ass.country in ('CHN') OR emp_res.country in ('CHN')) THEN 'CHN'
    when res.delivery_group in ('Manila Hubs') and (emp_ass.country in ('JPN') OR emp_res.country in ('JPN')) THEN 'JPN'
    when res.location IN ('IND/CRI','CRI/IND') and (emp_ass.reports_to_level6_login IN ('EESCALAN','JMORAGA','JOSCAMA','ANAGUZA') or emp_res.reports_to_level6_login IN ('EESCALAN','JMORAGA','JOSCAMA','ANAGUZA')) then 'CRI'
    when res.location IN ('IND/CRI','CRI/IND') and (emp_ass.reports_to_level6_login IN ('ARIJBASU','SARAHPOW') or emp_res.reports_to_level6_login IN ('ARIJBASU','SARAHPOW')) then 'IND'
    when (t.assigned_to_individual = 'flx-HRDMIVS' or t.resolved_by = 'flx-HRDMIVS') then 'IND'
    when (t.assigned_to_individual IN ('annikt','edella','tgulbran','TAO Secondary')) then 'SEA'
    when res.location IN ('IND/CRI','CRI/IND') and (emp_ass.reports_to_level5_login IN ('SNYERM','ADITYASI','KRRAMSAY','JILLSLAT','WILKYL','OPHELIAG')) then 'SEA'
    when res.location IN ('IND/CRI','CRI/IND') and def.default_hub IS NOT NULL then def.default_hub
    when res.location IN ('IND/CRI','CRI/IND') then 'SEA'
    else res.location end as delivery_location    
 ,case
    when res.location IN ('IND/CRI','CRI/IND') and (emp_ass.reports_to_level6_login IN ('EESCALAN','JMORAGA','JOSCAMA','ANAGUZA') or emp_res.reports_to_level6_login IN ('EESCALAN','JMORAGA','JOSCAMA','ANAGUZA')) then 'Costa Rica Hub'
    when res.location IN ('IND/CRI','CRI/IND') and (emp_ass.reports_to_level6_login IN ('ARIJBASU') or emp_res.reports_to_level6_login IN ('ARIJBASU')) then 'India Hubs'
    when (t.assigned_to_individual = 'flx-HRDMIVS' or t.resolved_by = 'flx-HRDMIVS') then 'India Hubs'
    when res.location IN ('IND/CRI','CRI/IND') and def.def_hub_group IS NOT NULL then def.def_hub_group
    when res.location IN ('IND/CRI','CRI/IND') then 'Other'
    else res.delivery_group end AS delivery_group
    
,CASE 
    WHEN (sub_a.reopened > 0 OR sub_a.routed > 0 OR sub_a.pending > 0) THEN 'N' ELSE 'Y' END AS FCR
,CASE 
    WHEN sub_a.reopened > 0 THEN 'Y' ELSE 'N' END AS reopened
,CASE 
    WHEN sub_a.routed > 0 THEN 'Y' ELSE 'N' END AS routed
,CASE 
    WHEN sub_a.pending > 0 THEN 'Y' ELSE 'N' END AS pending

,case when t.assigned_to_group in ('APAC Extended Onboarding','ERC Extended Onboarding') then 'EOB'
          when res.super_region = 'EMEA' and t.item in ('Extended Onboarding') then 'EOB'
          when dash_process = 'ALL' and proc.global_process IS NOT NULL then proc.global_process
          when dash_process = 'ALL' and proc.global_process IS NULL THEN 'Other'
          ELSE dash_process end as process    

, case when emp_con.reports_to_level5_login = 'TOBIN' then 'CS'
          when emp_con.reports_to_level2_login in ('AJASSY','JBLACK','DAVELIMP','OLSAVSKY','CARNEY','DAVIDZ','GALETTIB') then 'CORP'
          when emp_con.reports_to_level2_login = 'WILKE' and emp_con.dept_org_level1 not in ('Ops & Cust Svc') then 'CORP'
          when emp_req.reports_to_level5_login = 'TOBIN' then 'CS'
          when emp_req.reports_to_level2_login in ('AJASSY','JBLACK','DAVELIMP','OLSAVSKY','CARNEY','DAVIDZ','GALETTIB') then 'CORP'
          when emp_req.reports_to_level2_login = 'WILKE' and emp_req.dept_org_level1 not in ('Ops & Cust Svc') then 'CORP'
    ELSE 'OPS' end as LOB1

, case when emp_con.reports_to_level5_login = 'TOBIN' then 'CS'
          when emp_con.reports_to_level2_login in ('AJASSY') then 'AWS'
          when emp_con.reports_to_level2_login in ('JBLACK') then 'BIZ_DEV'
          when emp_con.reports_to_level2_login in ('DAVELIMP') then 'DEVICES'
          when emp_con.reports_to_level2_login in ('OLSAVSKY') then 'FINANCE'
          when emp_con.reports_to_level2_login in ('CARNEY') then 'CORP_AFAIRS'
          when emp_con.reports_to_level2_login in ('DAVIDZ') then 'LEGAL'
          when emp_con.reports_to_level2_login in ('GALETTIB') then 'HR'
          when emp_con.reports_to_level2_login = 'WILKE' and emp_con.dept_org_level1 not in ('Ops & Cust Svc') then 'WW_CONSUMER'
          when emp_req.reports_to_level5_login = 'TOBIN' then 'CS'
          when emp_req.reports_to_level2_login in ('AJASSY') then 'AWS'
          when emp_req.reports_to_level2_login in ('JBLACK') then 'BIZ_DEV'
          when emp_req.reports_to_level2_login in ('DAVELIMP') then 'DEVICES'
          when emp_req.reports_to_level2_login in ('OLSAVSKY') then 'FINANCE'
          when emp_req.reports_to_level2_login in ('CARNEY') then 'CORP_AFAIRS'
          when emp_req.reports_to_level2_login in ('DAVIDZ') then 'LEGAL'
          when emp_req.reports_to_level2_login in ('GALETTIB') then 'HR'
          when emp_req.reports_to_level2_login = 'WILKE' and emp_req.dept_org_level1 not in ('Ops & Cust Svc') then 'WW_CONSUMER'
    ELSE 'OPS' end as LOB2

  ,res.country as country  
  ,t.assigned_date

  , CASE WHEN sub_a.escalated > 0 THEN 'Y' ELSE 'N' END AS escalated
,emp_ass.reports_to_supervisor as hrs_supervisor
, CASE WHEN sub_a.four_a > 0 THEN 'Y' ELSE 'N' END AS "4a"
, CASE WHEN sub_a.four_r > 0 THEN 'Y' ELSE 'N' END AS "4r"
, CASE WHEN sub_a.four_c > 0 THEN 'Y' ELSE 'N' END AS "4c"
, CASE WHEN sub_a.four_f > 0 THEN 'Y' ELSE 'N' END AS "4f"
, CASE
   -- WHEN (NVL(res.defect_scope,'x') <> 'Y') then 'Not Defect Scope'
   -- WHEN (NVL(res.defect_scope,'x') = 'Y' and t.resolved_date < res.defect_launch and res.defect_launch IS NOT NULL) then 'Not Defect Scope'
    WHEN r2.root_cause IS NOT NULL then r2.root_cause
    WHEN (res.dash_process = 'Transfers' and (t.resolution IS NULL or NVL(t.resolution,'x') = 'x')) THEN 'Blank'
    WHEN (t.root_cause IS NULL or NVL(t.root_cause,'x') = 'x') THEN 'Blank'
    WHEN (r1.root_cause IS NULL or (res.dash_process = 'Transfers' and r2.root_cause IS NULL)) then 'Other root cause'
    WHEN t.root_cause = 'No Defect' and (lower(t.root_cause_details) like '%escalation protocol%' or lower(t.root_cause_details) like '%other%' 
                                         or lower(t.root_cause_details) like '%pending%' or lower(t.root_cause_details) like '%waiting%' 
                                         or lower(t.root_cause_details) like '%follow%' or lower(t.root_cause_details) like '%HR%' 
                                         or lower(t.root_cause_details) like '%update%') then 'Missing Info'
    WHEN t.root_cause = 'No Defect' and (lower(t.root_cause_details) like '%future%' or lower(t.root_cause_details) like '%ldw%' 
                                         or lower(t.root_cause_details) like '%process%' or lower(t.root_cause_details) like '%last day of work%' 
                                         or lower(t.root_cause_details) like '%stop%' or lower(t.root_cause_details) like '%work%')
                                          then 'Process Dependency'                                         
    when t.root_cause = 'No Defect' and networkdays_seven(t.create_date,rtd.modified_date) > sla_taget_value then 'Routed_After_SLA_Lapse'
        when t.root_cause = 'No Defect' and networkdays_seven(t.create_date,ro.last_reopened_date) > sla_taget_value and ro.reopened_by_system = 'Y' then 'Reopened_Sys_After_SLA_Lapse'
          when t.root_cause = 'No Defect' and networkdays_seven(t.create_date,ro.last_reopened_date) > sla_taget_value and ro.reopened_by_system = 'N' then 'Reopened_Pers_After_SLA_Lapse'
          when t.root_cause = 'No Defect' and sub_a.pending > 0 then 'Missing Info'
          when t.root_cause = 'No Defect' then 'Lifecycle Defect'
          when r1.root_cause_group = 'HRS Defect' then 'Lifecycle Defect'
          when r1.root_cause_group = 'No Defect' then 'Lifecycle Defect'
    ELSE r1.root_cause_group END as root_cause_group

,CASE WHEN sub_a.created_from_quicklink > 0 THEN 'Y' ELSE 'N' 
 END AS created_from_quicklink
,LTRIM(sub_a.QUICKLINK_ID, 'htps:/.amzoncquikl') AS quicklink_id
, CASE WHEN t.root_cause = 'No Defect' THEN 'N' ELSE 'Y' END AS Defect

FROM land_zn.o_remedy_tickets t

LEFT JOIN tp
on t.case_id = tp.case_id

LEFT JOIN hr_emea.global_remedy_cti_slas sla
ON t.category = sla.category
AND t.type = sla.type
AND t.item = sla.item

LEFT JOIN rootcauses
ON lower(t.root_cause) = lower(rootcauses.root_cause)

LEFT JOIN rootcauses r1
ON lower(t.root_cause) = lower(r1.root_cause)

LEFT JOIN rootcauses r2
ON lower(t.resolution) = lower(r2.root_cause)

--resolver group mapping
LEFT JOIN resolvers res
ON t.assigned_to_group = res.resolver_group

--processes mapped on CTIs
LEFT JOIN hr_emea.emea_remedy_ctiprocess proc
ON t.category = proc.category
AND t.type = proc.type

LEFT JOIN tags
ON t.case_id = tags.case_id
and row_num=1
-- audit trail
LEFT JOIN sub_a
ON t.case_id = sub_a.case_id

--concerning login
LEFT JOIN c
ON t.case_id = c.ticket_case_id
AND c.concerning_login IS NOT NULL
AND lower(c.concerning_login) <> 'none'

--tp90

--employee data join on thousand columns
LEFT JOIN emp AS emp_req
ON LOWER(emp_req.login) = lower(t.requester_login)
AND emp_req.login IS NOT NULL
and emp_req.the_rank = 1
LEFT JOIN emp AS emp_ass
ON LOWER(emp_ass.login) = lower(t.assigned_to_individual)
AND emp_ass.login IS NOT NULL
and emp_ass.the_rank = 1
and emp_ass.reports_to_level2_login = 'GALETTIB'
LEFT JOIN emp AS emp_res
ON LOWER(emp_res.login) = lower(t.resolved_by)
AND emp_res.login IS NOT NULL
and emp_res.the_rank = 1
and emp_res.reports_to_level2_login = 'GALETTIB'
LEFT JOIN emp AS emp_int
ON LOWER(emp_int.login) = lower(t.submitted_by)
AND emp_int.login IS NOT NULL
and emp_int.the_rank = 1
and emp_int.reports_to_level2_login = 'GALETTIB'
LEFT JOIN emp AS emp_con
ON LOWER(emp_con.login) = lower(c.concerning_login)
AND c.concerning_login IS NOT NULL
AND lower(c.concerning_login) <> 'none'
AND emp_con.login IS NOT NULL
and emp_con.the_rank = 1
LEFT JOIN hr_emea.default_hubs def
ON lower(t.category) = lower(def.category)
AND lower(t.type) = lower(def.type)
AND lower(t.item) = lower(def.item)
AND lower(t.assigned_to_group) = lower(def.resolver_group)
LEFT JOIN routed rtd
on t.case_id = rtd.case_id
and t.assigned_to_group = rtd.routed_to
and rtd.the_rank = 1

LEFT JOIN reopened ro
on t.case_id = ro.case_id
--and t.assigned_to_group = rtd.routed_to
and ro.the_rank = 1


LEFT JOIN hr_emea.opr_benefits_tt_mapping_v3 p
ON t.assigned_to_group = p.assigned_to_group
AND lower(t.category) = lower(p.category)
AND lower(t.type) = lower(p.type)
AND lower(t.item) = lower(p.item)
 left join
    (SELECT
    aud.case_id
    ,SUM(CASE WHEN aud.TYPE = 179 THEN 1 ELSE 0 END) AS correspondence_count   
   FROM land_zn.o_remedy_audittrail aud
     WHERE aud.create_day > TO_DATE('2017/12/01','YYYY/MM/DD')
  AND aud.type IN ('179')
  GROUP BY aud.case_id) aud
  on t.case_id = aud.case_id
WHERE 
--res.resolver_group IS NOT NULL and res.report_switch = 1 and hrsd = 'Y'
  (lower(dash_process) in ('benefits') or (lower(dash_process) = 'all' and lower(proc.global_process) in ('benefits')))
 --(t.assigned_to_group+t.category+t.type+t.item in (select distinct grouping from hr_emea.opr_benefits_tt_mapping_v3 p))
  AND (t.resolved_date >= TO_DATE('2018/10/01','YYYY/MM/DD'))
  --AND (t.resolved_date >= TO_DATE('2018/08/01','YYYY/MM/DD'))
  and t.status IN ('Resolved','Closed')
  AND NVL(t.root_cause,'x') NOT IN ('OAA/ALERT LIST WORKFLOW TICKET', 
                    'OAA/WORKFLOW TICKET', 'Test - Do not include in metrics'
                    , 'Test Ticket - Exclude from metrics'
                    , 'Test Ticket - Remove from metrics'
                    ,'Test Ticket - remove from metrics')

with no schema binding

