-- drop table if exists department cascade;
-- drop table if exists courses cascade;
-- drop table if exists professor cascade;
-- drop table if exists valid_entry cascade;
-- drop table if exists course_offers cascade;
-- drop table if exists student cascade;
-- drop table if exists student_courses cascade;
-- drop table if exists student_dept_change cascade;
-- drop function if exists is_valid_course_id;
-- drop view if exists course_eval;

CREATE OR REPLACE FUNCTION is_valid_course_id(course_id text) RETURNS BOOLEAN AS $$
DECLARE
    v_dept_id text;
    course_number text;
BEGIN
    v_dept_id := SUBSTRING(course_id, 1, 3);
    course_number := SUBSTRING(course_id, 4, 3);

    IF NOT EXISTS (SELECT 1 FROM department WHERE dept_id = v_dept_id) THEN
        RETURN FALSE;
    END IF;

    IF course_number !~ '^[0-9]{3}$' THEN
        RETURN FALSE;
    END IF;

    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;


CREATE TABLE department (
    dept_id CHAR(3) PRIMARY KEY,
    dept_name VARCHAR(40) NOT NULL UNIQUE
);

CREATE TABLE student (
    first_name VARCHAR(40) NOT NULL,
    last_name VARCHAR(40),
    student_id CHAR(11) PRIMARY KEY,
    address VARCHAR(100),
    contact_number CHAR(10) UNIQUE NOT NULL,
    email_id VARCHAR(50) UNIQUE,
    tot_credits INT NOT NULL CHECK (tot_credits >= 0),
	dept_id CHAR(3),
    FOREIGN KEY (dept_id) REFERENCES department(dept_id) ON UPDATE CASCADE
);

CREATE TABLE courses (
    course_id CHAR(6) PRIMARY KEY,
    course_name VARCHAR(20) NOT NULL UNIQUE,
    course_desc TEXT,
    credits NUMERIC NOT NULL CHECK (credits > 0),
    dept_id CHAR(3),
    FOREIGN KEY (dept_id) REFERENCES department(dept_id) ON UPDATE CASCADE,
    CONSTRAINT check_course_id CHECK (is_valid_course_id(course_id))
);

CREATE TABLE professor (
    professor_id VARCHAR(10) PRIMARY KEY,
    professor_first_name VARCHAR(40) NOT NULL,
    professor_last_name VARCHAR(40) NOT NULL,
    office_number VARCHAR(20),
    contact_number CHAR(10) NOT NULL,
    start_year INT,
    resign_year INT,
    dept_id CHAR(3),
    FOREIGN KEY (dept_id) REFERENCES department(dept_id) ON UPDATE CASCADE,
    CONSTRAINT CHK_StartResignYear CHECK (start_year <= resign_year)
);

CREATE TABLE course_offers (
    course_id CHAR(6),
    FOREIGN KEY (course_id) REFERENCES courses(course_id) ON UPDATE CASCADE,
    session VARCHAR(9),
    semester INT NOT NULL CHECK (semester IN (1,2)),
    professor_id VARCHAR(10),
    FOREIGN KEY (professor_id) REFERENCES professor(professor_id) ON UPDATE CASCADE,
    capacity INT,
    enrollments INT,
    PRIMARY KEY (course_id, session, semester)
);

CREATE TABLE student_courses (
    student_id CHAR(11),
    FOREIGN KEY (student_id) REFERENCES student(student_id) ON UPDATE CASCADE,
    course_id CHAR(6),
    session VARCHAR(9),
    semester INT NOT NULL CHECK (semester IN (1,2)),
    grade NUMERIC NOT NULL CHECK (grade >= 0 AND grade <= 10),
    FOREIGN KEY (course_id, session, semester) REFERENCES course_offers(course_id, session, semester) ON UPDATE CASCADE
);


CREATE TABLE valid_entry (
    dept_id CHAR(3),
    FOREIGN KEY (dept_id) REFERENCES department(dept_id) ON UPDATE CASCADE,
    entry_year INT NOT NULL,
    seq_number INT NOT NULL
);

-- 2.1.1 create trigger for validating student_id
CREATE OR REPLACE FUNCTION validate_student_id() 
RETURNS TRIGGER AS $$
DECLARE
    v_entry_year INT;
	v_dept_id CHAR(3);
	v_seq_num INT;
BEGIN
    -- Retrieve the next sequence number for the department
	v_entry_year := cast(SUBSTRING(NEW.student_id,1,4) as INT);
	v_dept_id := SUBSTRING(NEW.student_id,5,3);
	v_seq_num := cast(SUBSTRING(NEW.student_id,8,3) as INT);
    IF EXISTS (
		SELECT 1 from valid_entry
		WHERE v_entry_year = entry_year and v_dept_id = dept_id
		and v_seq_num = seq_number
	) THEN
		RETURN NEW;
	END IF;
	RAISE EXCEPTION 'invalid';
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER validate_student_id
BEFORE INSERT ON student
FOR EACH ROW
EXECUTE FUNCTION validate_student_id();


-- 2.1.2 create trigger for updating sequence number for a valid student entry
CREATE OR REPLACE FUNCTION update_seq_number()
RETURNS TRIGGER AS $$
DECLARE
    v_entry_year INT;
    v_dept_id CHAR(3);
BEGIN
    v_entry_year := cast(SUBSTRING(NEW.student_id,1,4) as INT);
    v_dept_id := SUBSTRING(NEW.student_id,5,3);
    UPDATE valid_entry
    SET seq_number = seq_number + 1
    WHERE dept_id = v_dept_id AND entry_year = v_entry_year;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_seq_number
AFTER INSERT ON student
FOR EACH ROW
EXECUTE FUNCTION update_seq_number();

-- 2.1.3 create trigger for validating student email
CREATE OR REPLACE FUNCTION validate_student_email()
RETURNS TRIGGER AS $$
DECLARE
    v_student_id CHAR(11);
    v_dept_id CHAR(3);
    v_email_id VARCHAR(50);
    dept_id CHAR(3);
BEGIN
    -- Retrieve the student id and email id
    v_student_id := NEW.student_id;
    v_email_id := NEW.email_id;
    v_dept_id := SUBSTRING(v_student_id, 5, 3);
    dept_id := NEW.dept_id;
    -- Check if the email id is valid
    IF NOT ((v_dept_id = dept_id) and (v_email_id ~* ('^' || v_student_id || '@' || v_dept_id || '.iitd.ac.in$'))) THEN
        RAISE EXCEPTION 'invalid';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER validate_student_email
BEFORE INSERT ON student
FOR EACH ROW
EXECUTE FUNCTION validate_student_email();

-- 2.1.4 creating a table to store the student department change
CREATE TABLE student_dept_change (
    old_student_id CHAR(11) NOT NULL,
    old_dept_id CHAR(3) NOT NULL,
    new_dept_id CHAR(3) NOT NULL,
    new_student_id CHAR(11) NOT NULL,
    FOREIGN KEY (old_dept_id) REFERENCES department(dept_id),
    FOREIGN KEY (new_dept_id) REFERENCES department(dept_id)
);

CREATE OR REPLACE FUNCTION log_student_dept_change()
RETURNS TRIGGER AS $$
DECLARE
    v_new_student_id CHAR(11);
    v_year INTEGER;
    v_new_email_id VARCHAR(50);
    v_seq INTEGER;
BEGIN
    IF NEW.dept_id != OLD.dept_id and NEW.student_id = OLD.student_id THEN
        SELECT INTO v_year CAST(SUBSTRING(OLD.student_id FROM 1 FOR 4) AS INTEGER);
        IF EXISTS (SELECT * FROM student_dept_change WHERE new_student_id = OLD.student_id) THEN
            RAISE EXCEPTION 'Department can be changed only once';
        ELSEIF (v_year < 2022) THEN
            RAISE EXCEPTION 'Entry year must be >= 2022';
        ELSEIF (SELECT AVG(grade) FROM student_courses WHERE student_id = OLD.student_id) <= 8.5 THEN
            RAISE EXCEPTION 'Low Grade';
        END IF;
        SELECT seq_number INTO v_seq FROM valid_entry WHERE dept_id = NEW.dept_id and entry_year = v_year;
        v_new_student_id := CONCAT(v_year, NEW.dept_id, LPAD(v_seq::text, 3, '0'));
        v_new_email_id := CONCAT(v_new_student_id, '@', CAST(NEW.dept_id as CHAR), '.iitd.ac.in');
        
        NEW.student_id = v_new_student_id;
        NEW.email_id = v_new_email_id;
        UPDATE valid_entry SET seq_number = seq_number + 1 WHERE dept_id = NEW.dept_id;
        INSERT INTO student_dept_change VALUES (OLD.student_id, OLD.dept_id, NEW.dept_id, v_new_student_id);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- DROP TRIGGER IF EXISTS log_student_dept_change ON student;
CREATE TRIGGER log_student_dept_change
BEFORE UPDATE ON student
FOR EACH ROW
EXECUTE FUNCTION log_student_dept_change();


-- 2.2.1 creating the view named course_eval for student_course table
-- need to see it again, need to ensure this view is up to date some how
-- by creating materialized view or using triggers
CREATE MATERIALIZED VIEW course_eval AS
SELECT 
    sc.course_id AS course_id,
    sc.session AS session,
    sc.semester AS semester,
    COUNT(sc.student_id) AS number_of_students,
    AVG(sc.grade) AS average_grade,
    MAX(sc.grade) AS max_grade,
    MIN(sc.grade) AS min_grade
FROM 
    student_courses sc
JOIN 
    courses c ON sc.course_id = c.course_id
GROUP BY 
    sc.course_id, sc.session, sc.semester;

-- writing a trigger for refreshing the materialized view course_eval
CREATE OR REPLACE FUNCTION refresh_course_eval()
RETURNS TRIGGER AS $$
BEGIN
    REFRESH MATERIALIZED VIEW course_eval;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER refresh_course_eval
AFTER INSERT OR DELETE OR UPDATE ON student_courses
FOR EACH ROW
EXECUTE FUNCTION refresh_course_eval();


-- 2.2.2 creating trigger for updating tot_credits of student

CREATE OR REPLACE FUNCTION update_tot_credits()
RETURNS TRIGGER AS $$
DECLARE
    v_course_id CHAR(6);
    v_credits INT;
BEGIN
    v_course_id := NEW.course_id;
    SELECT credits INTO v_credits FROM courses WHERE course_id = v_course_id;
    UPDATE student
    SET tot_credits = tot_credits + v_credits
    WHERE student_id = NEW.student_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_tot_credits
AFTER INSERT ON student_courses
FOR EACH ROW
EXECUTE FUNCTION update_tot_credits();


-- 2.2.3 trigger for checking if the student is enrolled in more than 5 courses simultaneously and credit limit
-- is not exceeded
CREATE OR REPLACE FUNCTION validate_student_courses()
RETURNS TRIGGER AS $$
DECLARE
    v_student_id CHAR(11);
    v_session VARCHAR(9);
    v_semester INT;
    v_tot_credits INT;
    v_credits INT;
    v_count INT;
BEGIN
    v_student_id := NEW.student_id;
    v_session := NEW.session;
    v_semester := NEW.semester;
    v_credits := (SELECT credits from courses where course_id = NEW.course_id);
    v_tot_credits := (SELECT tot_credits from student where student_id = v_student_id);
    v_count := (SELECT count(*) from student_courses where student_id = v_student_id and session = v_session and semester = v_semester);
    IF v_count >= 5 THEN
        RAISE EXCEPTION 'invalid';
    END IF;
    IF v_tot_credits + v_credits > 60 THEN
        RAISE EXCEPTION 'invalid';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER validate_student_courses
BEFORE INSERT ON student_courses
FOR EACH ROW
EXECUTE FUNCTION validate_student_courses();


-- 2.2.4 creating trigger for checking 5 credit course is only taken in first year
CREATE OR REPLACE FUNCTION five_credit_course_first_year()
RETURNS TRIGGER AS $$
DECLARE
    v_student_id CHAR(11);
    v_course_id CHAR(6);
    v_session VARCHAR(9);
    v_credits INT;
    v_current_year TEXT;
    v_first_year TEXT;
BEGIN
    v_student_id := NEW.student_id;
    v_course_id := NEW.course_id;
    v_session := NEW.session;
    v_credits := (SELECT credits from courses where course_id = v_course_id);
    v_current_year := SUBSTRING(v_session, 1, 4);
    v_first_year := SUBSTRING(v_student_id, 1, 4);
    IF v_credits = 5 AND v_current_year != v_first_year THEN
        RAISE EXCEPTION 'invalid';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER five_credit_course_first_year
BEFORE INSERT ON student_courses
FOR EACH ROW
EXECUTE FUNCTION five_credit_course_first_year();

-- 2.2.5 creating trigger for updating sgpa in student_semester_summary


CREATE MATERIALIZED VIEW student_semester_summary AS
SELECT 
    student_courses.student_id,
    student_courses.session,
    student_courses.semester,
    SUM(student_courses.grade * courses.credits) / SUM(courses.credits) AS sgpa,
    SUM(courses.credits) AS credits
FROM student_courses
JOIN courses ON student_courses.course_id = courses.course_id
WHERE student_courses.grade >= 5
GROUP BY student_courses.student_id, student_courses.session, student_courses.semester;


CREATE OR REPLACE FUNCTION update_student_semester_summary() 
RETURNS TRIGGER AS $$
DECLARE 
    v_credits INT;
BEGIN
    v_credits := (SELECT SUM(courses.credits) FROM student_courses 
    JOIN courses ON student_courses.course_id = courses.course_id 
    WHERE student_courses.student_id = NEW.student_id AND student_courses.session = NEW.session AND student_courses.semester = NEW.semester
    GROUP BY student_courses.student_id, student_courses.session, student_courses.semester);
    IF v_credits + (SELECT credits from courses where course_id = NEW.course_id) >= 26 THEN
        RAISE EXCEPTION 'invalid';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_student_semester_summary
BEFORE INSERT ON student_courses
FOR EACH ROW
EXECUTE FUNCTION update_student_semester_summary();

-- writing a trigger for refreshing the materialized view student_semester_summary
CREATE OR REPLACE FUNCTION refresh_student_semester_summary()
RETURNS TRIGGER AS $$
BEGIN
    REFRESH MATERIALIZED VIEW student_semester_summary;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER refresh_student_semester_summary
AFTER INSERT OR UPDATE OR DELETE ON student_courses
FOR EACH ROW
EXECUTE FUNCTION refresh_student_semester_summary();

-- 2.2.6 creating trigger for checking if the course is full and updating enrollments
CREATE OR REPLACE FUNCTION course_full()
RETURNS TRIGGER AS $$
DECLARE
    v_course_id CHAR(6);
    v_session VARCHAR(9);
    v_semester INT;
    v_count INT;
    v_capacity INT;
BEGIN
    v_course_id := NEW.course_id;
    v_session := NEW.session;
    v_semester := NEW.semester;
    v_count := (SELECT enrollments from course_offers where course_id = v_course_id and session = v_session and semester = v_semester);
    v_capacity := (SELECT capacity from course_offers where course_id = v_course_id and session = v_session and semester = v_semester);
    IF v_count >= v_capacity THEN
        RAISE EXCEPTION 'course is full';
    END IF;
    UPDATE course_offers 
    SET enrollments = v_count + 1 
    WHERE course_id = v_course_id and session = v_session and semester = v_semester;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER course_full
BEFORE INSERT ON student_courses
FOR EACH ROW
EXECUTE FUNCTION course_full();

-- 2.3.1 trigger for removing student courses when course is removed from course offers and also
-- course is added to course offers
CREATE OR REPLACE FUNCTION modify_student_courses()
RETURNS TRIGGER AS $$
DECLARE
    v_course_id CHAR(6);
    v_professor_id VARCHAR(10);
    v_session VARCHAR(9);
    v_semester INT;
    v_count INT;
    -- v_tot_credits INT;
    -- v_student_id CHAR(11);
    v_credits INT;
BEGIN
    IF TG_OP = 'DELETE' THEN
        v_course_id := OLD.course_id;
        v_session := OLD.session;
        v_semester := OLD.semester;
        v_credits := (SELECT credits from courses where course_id = v_course_id);
        UPDATE student
        SET tot_credits = tot_credits - v_credits
        WHERE student_id IN (SELECT student_id from student_courses where course_id = v_course_id and session = v_session and semester = v_semester);
        v_count := (SELECT count(*) from student_courses where course_id = v_course_id and session = v_session and semester = v_semester);
        IF v_count > 0 THEN
            DELETE FROM student_courses WHERE course_id = v_course_id and session = v_session and semester = v_semester;
        END IF;
        RETURN OLD;
    END IF;
    IF TG_OP = 'INSERT' THEN
        v_course_id := NEW.course_id;
        v_professor_id := NEW.professor_id;
        IF NOT EXISTS (SELECT * FROM courses WHERE course_id = v_course_id) THEN
            RAISE EXCEPTION 'course id not present in courses table';
        END IF;
        IF NOT EXISTS (SELECT * FROM professor WHERE professor_id = v_professor_id) THEN
            RAISE EXCEPTION 'professor id not present in professor table';
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- DROP TRIGGER IF EXISTS modify_student_courses ON course_offers;
CREATE TRIGGER modify_student_courses
BEFORE DELETE OR INSERT ON course_offers
FOR EACH ROW
EXECUTE FUNCTION modify_student_courses();

-- 2.3.2 trigger for professor not teaching more than 4 courses in a session and 
-- course is being offered before the associated professor resigns

CREATE OR REPLACE FUNCTION prof_course_offers()
RETURNS TRIGGER AS $$
DECLARE
    v_session INTEGER;

BEGIN
    v_session := cast(SUBSTRING(NEW.session, 1, 4) as INTEGER);
    IF (SELECT COUNT(*) FROM course_offers 
        WHERE professor_id = NEW.professor_id AND session = NEW.session) >= 4 THEN
        RAISE EXCEPTION 'invalid';
    END IF;
    IF (SELECT resign_year FROM professor WHERE professor_id = NEW.professor_id) < v_session THEN
        RAISE EXCEPTION 'invalid';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- DROP TRIGGER IF EXISTS validate_prof_course_offers ON course_offers;
CREATE TRIGGER validate_prof_course_offers
BEFORE INSERT ON course_offers
FOR EACH ROW
EXECUTE FUNCTION prof_course_offers();


-- 2.4 creating trigger for updating the department id in all the tables and updating relevant course ids in all related tables
-- and deleting the department if there are no students in the department and deleting related courses and professors

CREATE OR REPLACE FUNCTION update_dept_id()
RETURNS TRIGGER AS $$
DECLARE 
    dum text;
BEGIN
    IF TG_OP = 'UPDATE' THEN
        dum := OLD.dept_name;
        IF NEW.dept_id != OLD.dept_id THEN
            INSERT INTO department VALUES (NEW.dept_id, 'dummy');
            UPDATE student 
            SET dept_id = NEW.dept_id, student_id = substring(student_id from 1 for 4) || NEW.dept_id || substring(student_id from 8),
            email_id = substring(student_id from 1 for 4) || NEW.dept_id || substring(student_id from 8) || '@' || NEW.dept_id || '.iitd.ac.in'
            WHERE dept_id = OLD.dept_id;
            UPDATE courses 
            SET course_id = NEW.dept_id || substring(course_id from 4), dept_id = NEW.dept_id 
            WHERE dept_id = OLD.dept_id;
            UPDATE professor SET dept_id = NEW.dept_id WHERE dept_id = OLD.dept_id;
            UPDATE course_offers SET course_id = NEW.dept_id || substring(course_id from 4) WHERE substring(course_id, 1, 3) = OLD.dept_id;
            UPDATE student_courses SET course_id = NEW.dept_id || substring(course_id from 4) WHERE substring(course_id, 1, 3) = OLD.dept_id;
            UPDATE valid_entry SET dept_id = NEW.dept_id WHERE dept_id = OLD.dept_id;

            UPDATE student_dept_change
            SET old_dept_id = NEW.dept_id,old_student_id = SUBSTRING(old_student_id, 1, 4) || NEW.dept_id || SUBSTRING(old_student_id from 8)
            WHERE old_dept_id = OLD.dept_id;

            UPDATE student_dept_change
            SET new_dept_id = NEW.dept_id,new_student_id = SUBSTRING(new_student_id, 1, 4) || NEW.dept_id || SUBSTRING(new_student_id FROM 8)
            WHERE new_dept_id = OLD.dept_id;

            DELETE FROM department WHERE dept_Id = OLd.dept_id;
            UPDATE department SET dept_name = dum WHERE dept_id = NEW.dept_id;
            RETURN NULL;
        END IF;
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        IF EXISTS (SELECT * FROM student WHERE dept_id = OLD.dept_id) THEN
            RAISE EXCEPTION 'Department has students';
        ELSE
            DELETE FROM course_offers WHERE substring(course_id, 1, 3) = OLD.dept_id;
            DELETE FROM courses WHERE dept_id = OLD.dept_id;
            DELETE FROM professor WHERE dept_id = OLD.dept_id;
            DELETE FROM student_courses WHERE substring(course_id, 1, 3) = OLD.dept_id;
            DELETE FROM valid_entry WHERE dept_id = OLD.dept_id;
            DELETE FROM student_dept_change WHERE old_dept_id = OLD.dept_id;
            DELETE FROM student_dept_change WHERE new_dept_id = OLD.dept_id;
        END IF;
        RETURN OLD;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- DROP TRIGGER IF EXISTS update_dept_id ON department;
CREATE TRIGGER update_dept_id
BEFORE UPDATE OR DELETE ON department
FOR EACH ROW
EXECUTE FUNCTION update_dept_id();


