class StateStandardsQueries:

    @staticmethod
    def district_math(acc_id, year):
        return f"""
        CREATE OR REPLACE TABLE district_grade_math AS
        SELECT 
            results.stu_chron_grade_level as chron_grade_level,
            results.standard_reporting_group_id,
            results.standard_code,
            results.state_standards_name as state_standards,
            'Math' as subject,
            results.indicator_fit,
            results.applicable_grades,
            results.standard_display_text as standard_description,
            CASE WHEN indicator_fit = 'RELATED' THEN NULL 
                ELSE COUNT(CASE WHEN performance_result = 'MASTERED' THEN student_id END) 
            END as count_students_mastered,
            COUNT(CASE WHEN performance_result = 'PARTIAL_MASTERED' THEN student_id END) as count_students_partial_mastered,
            COUNT(CASE WHEN performance_result = 'NOT_MASTERED' THEN student_id END) as count_students_not_mastered
        FROM (
            SELECT f.student_id, f.account_id, f.academic_year, stu.stu_chron_grade_level, d.performance_result, 
                d1.standard_reporting_group_id, d1.indicator_fit, d1.standard_code, d1.state_standards_name,
                d1.standard_order, d1.parsed_group, d1.applicable_grades, d1.min_grade_level, d1.max_grade_level, d1.standard_display_text
            FROM (
                SELECT * FROM (
                    SELECT student_id, account_id, academic_year, al_score, no_score, ms_score, geo_score,
                        al_placement_grade_level, no_placement_grade_level, ms_placement_grade_level, geo_placement_grade_level,
                        ROW_NUMBER() OVER (PARTITION BY student_id ORDER BY completion_time DESC) as test_num
                    FROM canonical_staging.public.fact_student_assessment_diag_math
                    WHERE account_id = '{acc_id}' AND academic_year = '{year}' AND status = 2
                ) WHERE test_num = 1
            ) f
            RIGHT OUTER JOIN canonical_staging.public.dim_diagnostic_standard_performance_result_math d ON (
                d.state_standard_id IN (
                    SELECT s.state_standard_id FROM canonical_staging.public.dim_state_standard s 
                    JOIN canonical_staging.public.dim_account a ON a.state_abbreviation = s.state_abbreviation
                    WHERE s.subject = 'math' AND a.id = '{acc_id}'
                )
                AND f.al_score BETWEEN d.al_min_score AND d.al_max_score
                AND f.no_score BETWEEN d.no_min_score AND d.no_max_score
                AND f.ms_score BETWEEN d.ms_min_score AND d.ms_max_score
                AND f.geo_score BETWEEN d.geo_min_score AND d.geo_max_score
                AND f.al_placement_grade_level BETWEEN d.al_min_placement_grade_level AND d.al_max_placement_grade_level
                AND f.no_placement_grade_level BETWEEN d.no_min_placement_grade_level AND d.no_max_placement_grade_level
                AND f.ms_placement_grade_level BETWEEN d.ms_min_placement_grade_level AND d.ms_max_placement_grade_level
                AND f.geo_placement_grade_level BETWEEN d.geo_min_placement_grade_level AND d.geo_max_placement_grade_level
            )
            INNER JOIN (
                SELECT ds.*, x.state_standards_name 
                FROM canonical_staging.public.dim_diagnostic_standard ds
                JOIN (
                    SELECT state_standard_id, display_name as state_standards_name
                    FROM canonical_staging.public.dim_state_standard s
                    JOIN canonical_staging.public.dim_account a ON a.state_abbreviation = s.state_abbreviation
                    WHERE s.subject = 'math' AND a.id = '{acc_id}'
                ) x ON x.state_standard_id = ds.state_standard_id
            ) d1 ON d1.standard_reporting_group_id = d.standard_reporting_group_id
            INNER JOIN (
                SELECT id, account_id, academic_year, CAST(grade_level AS INT) as stu_chron_grade_level
                FROM canonical_staging.public.dim_student_no_pii
                WHERE account_id = '{acc_id}' AND academic_year = '{year}'
            ) stu ON f.student_id = stu.id AND f.account_id = stu.account_id AND f.academic_year = stu.academic_year
            AND d1.min_grade_level <= stu.stu_chron_grade_level + 1 
            AND stu.stu_chron_grade_level - 2 <= d1.max_grade_level
            INNER JOIN canonical_staging.public.fact_student_product_enrollment pe ON pe.student_id = stu.id 
            AND pe.subject = 'math' AND pe.eligible_diagnostic = 1 AND pe.account_id = '{acc_id}'
        ) results
        GROUP BY 1,2,3,4,5,6,7,8
        ORDER BY 1,4,3
        """
    
    @staticmethod
    def school_math(acc_id, year):
        return f"""
        CREATE OR REPLACE TABLE school_grade_math AS
        SELECT
            results.school_id, results.school_name, results.stu_chron_grade_level as chron_grade_level,
            results.standard_reporting_group_id, results.standard_code, results.state_standards_name as state_standards,
            'Math' as subject, results.indicator_fit, results.applicable_grades, results.standard_display_text as standard_description,
            CASE WHEN indicator_fit = 'RELATED' THEN NULL ELSE COUNT(CASE WHEN performance_result = 'MASTERED' THEN student_id END) END as count_students_mastered,
            COUNT(CASE WHEN performance_result = 'PARTIAL_MASTERED' THEN student_id END) as count_students_partial_mastered,
            COUNT(CASE WHEN performance_result = 'NOT_MASTERED' THEN student_id END) as count_students_not_mastered
        FROM (
            SELECT school_enr.school_id, school_enr.school_name, f.student_id, f.account_id, f.academic_year, stu.stu_chron_grade_level, d.performance_result, d1.*
            FROM (
                SELECT * FROM (
                    SELECT student_id, account_id, academic_year, al_score, no_score, ms_score, geo_score, al_placement_grade_level, no_placement_grade_level, ms_placement_grade_level, geo_placement_grade_level,
                        ROW_NUMBER() OVER (PARTITION BY student_id ORDER BY completion_time DESC) as test_num
                    FROM canonical_staging.public.fact_student_assessment_diag_math
                    WHERE account_id = '{acc_id}' AND academic_year = '{year}' AND status = 2
                ) WHERE test_num = 1
            ) f
            RIGHT OUTER JOIN canonical_staging.public.dim_diagnostic_standard_performance_result_math d ON (
                d.state_standard_id IN (SELECT s.state_standard_id FROM canonical_staging.public.dim_state_standard s JOIN canonical_staging.public.dim_account a ON a.state_abbreviation = s.state_abbreviation WHERE s.subject = 'math' AND a.id = '{acc_id}')
                AND f.al_score BETWEEN d.al_min_score AND d.al_max_score AND f.no_score BETWEEN d.no_min_score AND d.no_max_score AND f.ms_score BETWEEN d.ms_min_score AND d.ms_max_score AND f.geo_score BETWEEN d.geo_min_score AND d.geo_max_score
                AND f.al_placement_grade_level BETWEEN d.al_min_placement_grade_level AND d.al_max_placement_grade_level AND f.no_placement_grade_level BETWEEN d.no_min_placement_grade_level AND d.no_max_placement_grade_level AND f.ms_placement_grade_level BETWEEN d.ms_min_placement_grade_level AND d.ms_max_placement_grade_level AND f.geo_placement_grade_level BETWEEN d.geo_min_placement_grade_level AND d.geo_max_placement_grade_level
            )
            INNER JOIN (
                SELECT ds.*, x.state_standards_name FROM canonical_staging.public.dim_diagnostic_standard ds
                JOIN (SELECT state_standard_id, display_name as state_standards_name FROM canonical_staging.public.dim_state_standard s JOIN canonical_staging.public.dim_account a ON a.state_abbreviation = s.state_abbreviation WHERE s.subject = 'math' AND a.id = '{acc_id}') x ON x.state_standard_id = ds.state_standard_id
            ) d1 ON d1.standard_reporting_group_id = d.standard_reporting_group_id
            INNER JOIN (SELECT id, account_id, academic_year, CAST(grade_level AS INT) as stu_chron_grade_level FROM canonical_staging.public.dim_student_no_pii WHERE account_id = '{acc_id}' AND academic_year = '{year}') stu 
                ON f.student_id = stu.id AND f.account_id = stu.account_id AND f.academic_year = stu.academic_year AND d1.min_grade_level <= stu.stu_chron_grade_level + 1 AND stu.stu_chron_grade_level - 2 <= d1.max_grade_level
            INNER JOIN canonical_staging.public.fact_student_product_enrollment pe ON pe.student_id = stu.id AND pe.subject = 'math' AND pe.eligible_diagnostic = 1 AND pe.account_id = '{acc_id}'
            INNER JOIN (
                SELECT e.student_id, e.account_id, e.academic_year, e.school_id, s.name as school_name
                FROM canonical_staging.public.fact_student_school_enrollment e
                JOIN canonical_staging.public.dim_school s ON s.id = e.school_id
            ) school_enr ON pe.student_id = school_enr.student_id AND pe.account_id = school_enr.account_id AND pe.academic_year = school_enr.academic_year
        ) results
        GROUP BY 1,2,3,4,5,6,7,8,9,10
        ORDER BY 2,3,6,5
        """
    
    @staticmethod
    def district_ela(acc_id, year):
        return f"""
        CREATE OR REPLACE TABLE district_grade_ela AS
        SELECT 
            results.stu_chron_grade_level as chron_grade_level,
            results.standard_reporting_group_id,
            results.standard_code,
            results.state_standards_name as state_standards,
            'Reading' as subject,
            results.indicator_fit,
            results.applicable_grades,
            results.standard_display_text as standard_description,
            CASE WHEN indicator_fit = 'RELATED' THEN NULL ELSE COUNT(CASE WHEN performance_result = 'MASTERED' THEN student_id END) END as count_students_mastered,
            COUNT(CASE WHEN performance_result = 'PARTIAL_MASTERED' THEN student_id END) as count_students_partial_mastered,
            COUNT(CASE WHEN performance_result = 'NOT_MASTERED' THEN student_id END) as count_students_not_mastered
        FROM (
            SELECT f.student_id, f.account_id, f.academic_year, stu.stu_chron_grade_level, d.performance_result, d1.*
            FROM (
                SELECT * FROM (
                    SELECT student_id, account_id, academic_year, pa_score, ph_score, hfw_score, voc_score, com_score, com_lit_score, com_info_score,
                        pa_placement_grade_level, ph_placement_grade_level, hfw_placement_grade_level, voc_placement_grade_level, com_placement_grade_level, com_info_placement_grade_level, com_lit_placement_grade_level,
                        ROW_NUMBER() OVER (PARTITION BY student_id ORDER BY completion_time DESC) as test_num
                    FROM canonical_staging.public.fact_student_assessment_diag_ela
                    WHERE account_id = '{acc_id}' AND academic_year = '{year}' AND status = 2
                ) WHERE test_num = 1
            ) f
            RIGHT OUTER JOIN canonical_staging.public.dim_diagnostic_standard_performance_result_ela d ON (
                d.state_standard_id IN (SELECT s.state_standard_id FROM canonical_staging.public.dim_state_standard s JOIN canonical_staging.public.dim_account a ON a.state_abbreviation = s.state_abbreviation WHERE s.subject = 'ela' AND a.id = '{acc_id}')
                AND f.pa_score BETWEEN d.pa_min_score AND d.pa_max_score AND f.ph_score BETWEEN d.ph_min_score AND d.ph_max_score AND f.hfw_score BETWEEN d.hfw_min_score AND d.hfw_max_score AND f.voc_score BETWEEN d.voc_min_score AND d.voc_max_score 
                AND f.com_score BETWEEN d.com_min_score AND d.com_max_score AND f.com_info_score BETWEEN d.com_info_min_score AND d.com_info_max_score AND f.com_lit_score BETWEEN d.com_lit_min_score AND d.com_lit_max_score
                AND f.pa_placement_grade_level BETWEEN d.pa_min_placement_grade_level AND d.pa_max_placement_grade_level AND f.ph_placement_grade_level BETWEEN d.ph_min_placement_grade_level AND d.ph_max_placement_grade_level AND f.hfw_placement_grade_level BETWEEN d.hfw_min_placement_grade_level AND d.hfw_max_placement_grade_level AND f.voc_placement_grade_level BETWEEN d.voc_min_placement_grade_level AND d.voc_max_placement_grade_level AND f.com_placement_grade_level BETWEEN d.com_min_placement_grade_level AND d.com_max_placement_grade_level AND f.com_lit_placement_grade_level BETWEEN d.com_lit_min_placement_grade_level AND d.com_lit_max_placement_grade_level AND f.com_info_placement_grade_level BETWEEN d.com_info_min_placement_grade_level AND d.com_info_max_placement_grade_level
            )
            INNER JOIN (
                SELECT ds.*, x.state_standards_name FROM canonical_staging.public.dim_diagnostic_standard ds
                JOIN (SELECT state_standard_id, display_name as state_standards_name FROM canonical_staging.public.dim_state_standard s JOIN canonical_staging.public.dim_account a ON a.state_abbreviation = s.state_abbreviation WHERE s.subject = 'ela' AND a.id = '{acc_id}') x ON x.state_standard_id = ds.state_standard_id
            ) d1 ON d1.standard_reporting_group_id = d.standard_reporting_group_id
            INNER JOIN (SELECT id, account_id, academic_year, CAST(grade_level AS INT) as stu_chron_grade_level FROM canonical_staging.public.dim_student_no_pii WHERE account_id = '{acc_id}' AND academic_year = '{year}') stu 
                ON f.student_id = stu.id AND f.account_id = stu.account_id AND f.academic_year = stu.academic_year AND d1.min_grade_level <= stu.stu_chron_grade_level + 1 AND stu.stu_chron_grade_level - 2 <= d1.max_grade_level
            INNER JOIN canonical_staging.public.fact_student_product_enrollment pe ON pe.student_id = stu.id AND pe.subject = 'ela' AND pe.eligible_diagnostic = 1 AND pe.account_id = '{acc_id}'
        ) results
        GROUP BY 1,2,3,4,5,6,7,8
        ORDER BY 1,4,3
        """

    @staticmethod
    def school_ela(acc_id, year):
        return f"""
        CREATE OR REPLACE TABLE school_grade_ela AS
        SELECT
            results.school_id, results.school_name, results.stu_chron_grade_level as chron_grade_level,
            results.standard_reporting_group_id, results.standard_code, results.state_standards_name as state_standards,
            'Reading' as subject, results.indicator_fit, results.applicable_grades, results.standard_display_text as standard_description,
            CASE WHEN indicator_fit = 'RELATED' THEN NULL ELSE COUNT(CASE WHEN performance_result = 'MASTERED' THEN student_id END) END as count_students_mastered,
            COUNT(CASE WHEN performance_result = 'PARTIAL_MASTERED' THEN student_id END) as count_students_partial_mastered,
            COUNT(CASE WHEN performance_result = 'NOT_MASTERED' THEN student_id END) as count_students_not_mastered
        FROM (
            SELECT school_enr.school_id, school_enr.school_name, f.student_id, f.account_id, f.academic_year, stu.stu_chron_grade_level, d.performance_result, d1.*
            FROM (
                SELECT * FROM (
                    SELECT student_id, account_id, academic_year, pa_score, ph_score, hfw_score, voc_score, com_score, com_lit_score, com_info_score,
                        pa_placement_grade_level, ph_placement_grade_level, hfw_placement_grade_level, voc_placement_grade_level, com_placement_grade_level, com_info_placement_grade_level, com_lit_placement_grade_level,
                        ROW_NUMBER() OVER (PARTITION BY student_id ORDER BY completion_time DESC) as test_num
                    FROM canonical_staging.public.fact_student_assessment_diag_ela
                    WHERE account_id = '{acc_id}' AND academic_year = '{year}' AND status = 2
                ) WHERE test_num = 1
            ) f
            RIGHT OUTER JOIN canonical_staging.public.dim_diagnostic_standard_performance_result_ela d ON (
                d.state_standard_id IN (SELECT s.state_standard_id FROM canonical_staging.public.dim_state_standard s JOIN canonical_staging.public.dim_account a ON a.state_abbreviation = s.state_abbreviation WHERE s.subject = 'ela' AND a.id = '{acc_id}')
                AND f.pa_score BETWEEN d.pa_min_score AND d.pa_max_score AND f.ph_score BETWEEN d.ph_min_score AND d.ph_max_score AND f.hfw_score BETWEEN d.hfw_min_score AND d.hfw_max_score AND f.voc_score BETWEEN d.voc_min_score AND d.voc_max_score 
                AND f.com_score BETWEEN d.com_min_score AND d.com_max_score AND f.com_info_score BETWEEN d.com_info_min_score AND d.com_info_max_score AND f.com_lit_score BETWEEN d.com_lit_min_score AND d.com_lit_max_score
                AND f.pa_placement_grade_level BETWEEN d.pa_min_placement_grade_level AND d.pa_max_placement_grade_level AND f.ph_placement_grade_level BETWEEN d.ph_min_placement_grade_level AND d.ph_max_placement_grade_level AND f.hfw_placement_grade_level BETWEEN d.hfw_min_placement_grade_level AND d.hfw_max_placement_grade_level AND f.voc_placement_grade_level BETWEEN d.voc_min_placement_grade_level AND d.voc_max_placement_grade_level AND f.com_placement_grade_level BETWEEN d.com_min_placement_grade_level AND d.com_max_placement_grade_level AND f.com_lit_placement_grade_level BETWEEN d.com_lit_min_placement_grade_level AND d.com_lit_max_placement_grade_level AND f.com_info_placement_grade_level BETWEEN d.com_info_min_placement_grade_level AND d.com_info_max_placement_grade_level
            )
            INNER JOIN (
                SELECT ds.*, x.state_standards_name FROM canonical_staging.public.dim_diagnostic_standard ds
                JOIN (SELECT state_standard_id, display_name as state_standards_name FROM canonical_staging.public.dim_state_standard s JOIN canonical_staging.public.dim_account a ON a.state_abbreviation = s.state_abbreviation WHERE s.subject = 'ela' AND a.id = '{acc_id}') x ON x.state_standard_id = ds.state_standard_id
            ) d1 ON d1.standard_reporting_group_id = d.standard_reporting_group_id
            INNER JOIN (SELECT id, account_id, academic_year, CAST(grade_level AS INT) as stu_chron_grade_level FROM canonical_staging.public.dim_student_no_pii WHERE account_id = '{acc_id}' AND academic_year = '{year}') stu 
                ON f.student_id = stu.id AND f.account_id = stu.account_id AND f.academic_year = stu.academic_year AND d1.min_grade_level <= stu.stu_chron_grade_level + 1 AND stu.stu_chron_grade_level - 2 <= d1.max_grade_level
            INNER JOIN canonical_staging.public.fact_student_product_enrollment pe ON pe.student_id = stu.id AND pe.subject = 'ela' AND pe.eligible_diagnostic = 1 AND pe.account_id = '{acc_id}'
            INNER JOIN (
                SELECT e.student_id, e.account_id, e.academic_year, e.school_id, s.name as school_name
                FROM canonical_staging.public.fact_student_school_enrollment e
                JOIN canonical_staging.public.dim_school s ON s.id = e.school_id
            ) school_enr ON pe.student_id = school_enr.student_id AND pe.account_id = school_enr.account_id AND pe.academic_year = school_enr.academic_year
        ) results
        GROUP BY 1,2,3,4,5,6,7,8,9,10
        ORDER BY 2,3,6,5
        """

    @staticmethod
    def district_standards(subject, g_min, g_max):
        total_sql = "count_students_mastered + count_students_partial_mastered + count_students_not_mastered"
        return f"""
            SELECT
                subject AS "Subject",
                chron_grade_level AS "Grade",
                standard_code AS "Standard Code",
                standard_description AS "Standard Description",
                count_students_mastered AS "Students with Green Checks",
                count_students_partial_mastered AS "Students with White Checks",
                count_students_not_mastered AS "Students with No Checks",
                {total_sql} AS "Total Students Assessed"
            FROM district_grade_{subject}
            WHERE chron_grade_level BETWEEN {g_min} AND {g_max}
            ORDER BY chron_grade_level, standard_code 
        """
    
    @staticmethod
    def list_packages(language):
        return f"""
        SELECT *
        FROM information_schema.packages
        WHERE language = '{language}'
        """