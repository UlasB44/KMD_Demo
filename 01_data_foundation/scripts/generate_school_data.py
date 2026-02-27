"""
KMD Denmark - Synthetic School Data Generator
Generates realistic Danish municipality school data for workshop demo
"""

import csv
import random
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
import json

MUNICIPALITIES = {
    "COPENHAGEN": {"code": "101", "name": "Kobenhavn", "population": 650000, "num_schools": 12},
    "AARHUS": {"code": "751", "name": "Aarhus", "population": 350000, "num_schools": 8},
    "ODENSE": {"code": "461", "name": "Odense", "population": 205000, "num_schools": 6},
    "AALBORG": {"code": "851", "name": "Aalborg", "population": 120000, "num_schools": 5},
    "ESBJERG": {"code": "561", "name": "Esbjerg", "population": 72000, "num_schools": 4},
}

DANISH_FIRST_NAMES_MALE = [
    "Magnus", "Oliver", "William", "Victor", "Noah", "Lucas", "Oscar", "Carl",
    "Frederik", "Emil", "Mikkel", "Sebastian", "Alexander", "Christian", "Mathias",
    "Jonas", "Mads", "Rasmus", "Simon", "Jakob", "Nikolaj", "Kasper", "Tobias"
]

DANISH_FIRST_NAMES_FEMALE = [
    "Emma", "Ida", "Sofia", "Freja", "Clara", "Laura", "Anna", "Mathilde",
    "Alma", "Josefine", "Caroline", "Emilie", "Victoria", "Isabella", "Maja",
    "Nanna", "Marie", "Sofie", "Sarah", "Line", "Katrine", "Julie", "Camilla"
]

DANISH_LAST_NAMES = [
    "Jensen", "Nielsen", "Hansen", "Pedersen", "Andersen", "Christensen",
    "Larsen", "Sorensen", "Rasmussen", "Jorgensen", "Petersen", "Madsen",
    "Kristensen", "Olsen", "Thomsen", "Poulsen", "Johansen", "Knudsen",
    "Mortensen", "Moller", "Jakobsen", "Lund", "Berg", "Eriksen"
]

SCHOOL_PREFIXES = [
    "Nordre", "Sondre", "Ostre", "Vestre", "Centrum", "Strand", "Park",
    "Skov", "Have", "Bakke", "Eng", "Mark", "Kirke", "Slot", "Borg"
]

SCHOOL_SUFFIXES = ["Skole", "Folkeskole", "Privatskole", "Friskole"]

SUBJECTS = [
    "Dansk", "Matematik", "Engelsk", "Tysk", "Fysik", "Kemi", "Biologi",
    "Historie", "Geografi", "Samfundsfag", "Musik", "Billedkunst", "Idraet"
]

SPECIAL_NEEDS_CATEGORIES = [
    "Dysleksi", "ADHD", "Autisme", "Horenedsaettelse", "Synsvanskeligheder",
    "Motoriske vanskeligheder", "Sprogvanskeligheder", "Ingen"
]


def generate_cpr(birth_date, gender):
    """Generate a realistic-looking Danish CPR number (masked for demo)"""
    day = birth_date.strftime("%d")
    month = birth_date.strftime("%m")
    year = birth_date.strftime("%y")
    seq = random.randint(1000, 9999)
    if gender == "M":
        seq = seq if seq % 2 == 1 else seq + 1
    else:
        seq = seq if seq % 2 == 0 else seq + 1
    return f"{day}{month}{year}-{seq}"


def mask_cpr(cpr):
    """Mask CPR number for PII protection demo"""
    return f"{cpr[:6]}-XXXX"


def generate_school_name(prefix_used):
    """Generate a Danish school name"""
    available_prefixes = [p for p in SCHOOL_PREFIXES if p not in prefix_used]
    if not available_prefixes:
        available_prefixes = SCHOOL_PREFIXES
    prefix = random.choice(available_prefixes)
    prefix_used.add(prefix)
    suffix = random.choice(SCHOOL_SUFFIXES)
    return f"{prefix} {suffix}", prefix_used


def generate_schools(municipality_code, municipality_name, num_schools, output_dir):
    """Generate school data for a municipality"""
    schools = []
    prefix_used = set()
    
    for i in range(num_schools):
        school_name, prefix_used = generate_school_name(prefix_used)
        school_id = f"{municipality_code}-SCH-{str(i+1).zfill(3)}"
        
        school = {
            "school_id": school_id,
            "municipality_code": municipality_code,
            "school_name": school_name,
            "school_type": random.choice(["Folkeskole", "Privatskole", "Friskole"]),
            "address": f"{random.choice(SCHOOL_PREFIXES)}vej {random.randint(1, 200)}",
            "postal_code": f"{random.randint(1000, 9999)}",
            "city": municipality_name,
            "phone": f"+45 {random.randint(20, 99)} {random.randint(10, 99)} {random.randint(10, 99)} {random.randint(10, 99)}",
            "email": f"kontor@{school_name.lower().replace(' ', '')}.dk",
            "founded_year": random.randint(1850, 2015),
            "student_capacity": random.randint(200, 800),
            "is_active": True,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        }
        schools.append(school)
    
    return schools


def generate_teachers(schools, output_dir):
    """Generate teacher data"""
    teachers = []
    teacher_counter = 1
    
    for school in schools:
        num_teachers = random.randint(15, 40)
        
        for _ in range(num_teachers):
            gender = random.choice(["M", "F"])
            first_name = random.choice(DANISH_FIRST_NAMES_MALE if gender == "M" else DANISH_FIRST_NAMES_FEMALE)
            last_name = random.choice(DANISH_LAST_NAMES)
            birth_year = random.randint(1960, 1995)
            birth_date = datetime(birth_year, random.randint(1, 12), random.randint(1, 28))
            hire_year = max(birth_year + 25, random.randint(2000, 2023))
            
            teacher = {
                "teacher_id": f"TCH-{str(teacher_counter).zfill(5)}",
                "school_id": school["school_id"],
                "municipality_code": school["municipality_code"],
                "cpr_number": generate_cpr(birth_date, gender),
                "cpr_masked": mask_cpr(generate_cpr(birth_date, gender)),
                "first_name": first_name,
                "last_name": last_name,
                "gender": gender,
                "birth_date": birth_date.strftime("%Y-%m-%d"),
                "email": f"{first_name.lower()}.{last_name.lower()}@{school['school_name'].lower().replace(' ', '')}.dk",
                "phone": f"+45 {random.randint(20, 99)} {random.randint(10, 99)} {random.randint(10, 99)} {random.randint(10, 99)}",
                "hire_date": f"{hire_year}-{random.randint(1, 12):02d}-01",
                "subjects": ",".join(random.sample(SUBJECTS, random.randint(1, 3))),
                "salary_band": random.choice(["A", "B", "C", "D"]),
                "is_active": random.random() > 0.05,
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat()
            }
            teachers.append(teacher)
            teacher_counter += 1
    
    return teachers


def generate_classes(schools, output_dir):
    """Generate class data"""
    classes = []
    class_counter = 1
    
    grades = ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]
    sections = ["A", "B", "C"]
    
    for school in schools:
        for grade in grades:
            num_sections = random.randint(1, 3)
            for section in sections[:num_sections]:
                class_record = {
                    "class_id": f"CLS-{str(class_counter).zfill(5)}",
                    "school_id": school["school_id"],
                    "municipality_code": school["municipality_code"],
                    "grade": grade,
                    "section": section,
                    "class_name": f"{grade}.{section}",
                    "academic_year": "2024-2025",
                    "max_students": random.randint(20, 28),
                    "classroom_number": f"L{random.randint(100, 350)}",
                    "is_active": True,
                    "created_at": datetime.now().isoformat(),
                    "updated_at": datetime.now().isoformat()
                }
                classes.append(class_record)
                class_counter += 1
    
    return classes


def generate_students(classes, output_dir):
    """Generate student data"""
    students = []
    student_counter = 1
    
    for class_record in classes:
        num_students = random.randint(18, int(class_record["max_students"]))
        grade = int(class_record["grade"])
        
        for _ in range(num_students):
            gender = random.choice(["M", "F"])
            first_name = random.choice(DANISH_FIRST_NAMES_MALE if gender == "M" else DANISH_FIRST_NAMES_FEMALE)
            last_name = random.choice(DANISH_LAST_NAMES)
            birth_year = 2024 - 6 - grade
            birth_date = datetime(birth_year, random.randint(1, 12), random.randint(1, 28))
            
            student = {
                "student_id": f"STU-{str(student_counter).zfill(6)}",
                "class_id": class_record["class_id"],
                "school_id": class_record["school_id"],
                "municipality_code": class_record["municipality_code"],
                "cpr_number": generate_cpr(birth_date, gender),
                "cpr_masked": mask_cpr(generate_cpr(birth_date, gender)),
                "first_name": first_name,
                "last_name": last_name,
                "gender": gender,
                "birth_date": birth_date.strftime("%Y-%m-%d"),
                "enrollment_date": f"2024-08-{random.randint(1, 15):02d}",
                "guardian_name": f"{random.choice(DANISH_FIRST_NAMES_MALE if random.random() > 0.5 else DANISH_FIRST_NAMES_FEMALE)} {last_name}",
                "guardian_phone": f"+45 {random.randint(20, 99)} {random.randint(10, 99)} {random.randint(10, 99)} {random.randint(10, 99)}",
                "guardian_email": f"{last_name.lower()}familie@email.dk",
                "address": f"{random.choice(SCHOOL_PREFIXES)}vej {random.randint(1, 200)}",
                "postal_code": f"{random.randint(1000, 9999)}",
                "special_needs": random.choices(SPECIAL_NEEDS_CATEGORIES, weights=[5, 8, 6, 2, 2, 3, 4, 70])[0],
                "is_active": True,
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat()
            }
            students.append(student)
            student_counter += 1
    
    return students


def generate_grades(students, output_dir):
    """Generate grade records"""
    grades = []
    grade_counter = 1
    
    grade_scale = ["12", "10", "7", "4", "02", "00", "-3"]
    grade_weights = [10, 20, 35, 20, 10, 4, 1]
    
    for student in students:
        student_grade = int(student["class_id"].split("-")[1][:2]) if student["class_id"] else 0
        if student_grade >= 3:
            for subject in random.sample(SUBJECTS, random.randint(6, 10)):
                for term in ["Q1", "Q2", "Q3", "Q4"]:
                    grade_record = {
                        "grade_record_id": f"GRD-{str(grade_counter).zfill(8)}",
                        "student_id": student["student_id"],
                        "class_id": student["class_id"],
                        "school_id": student["school_id"],
                        "municipality_code": student["municipality_code"],
                        "subject": subject,
                        "academic_year": "2024-2025",
                        "term": term,
                        "grade_value": random.choices(grade_scale, weights=grade_weights)[0],
                        "grade_date": f"2024-{['03', '06', '10', '12'][['Q1', 'Q2', 'Q3', 'Q4'].index(term)]}-15",
                        "teacher_comment": random.choice([
                            "God indsats", "Kan forbedres", "Fremragende", 
                            "Arbejder flittigt", "Viser fremgang", ""
                        ]),
                        "is_final": term == "Q4",
                        "created_at": datetime.now().isoformat()
                    }
                    grades.append(grade_record)
                    grade_counter += 1
    
    return grades


def generate_attendance(students, output_dir):
    """Generate attendance records"""
    attendance = []
    attendance_counter = 1
    
    start_date = datetime(2024, 8, 12)
    end_date = datetime(2024, 12, 20)
    current_date = start_date
    
    while current_date <= end_date:
        if current_date.weekday() < 5:
            for student in random.sample(students, min(len(students), 500)):
                status = random.choices(
                    ["Present", "Absent_Sick", "Absent_Excused", "Absent_Unexcused", "Late"],
                    weights=[85, 8, 3, 2, 2]
                )[0]
                
                attendance_record = {
                    "attendance_id": f"ATT-{str(attendance_counter).zfill(10)}",
                    "student_id": student["student_id"],
                    "class_id": student["class_id"],
                    "school_id": student["school_id"],
                    "municipality_code": student["municipality_code"],
                    "attendance_date": current_date.strftime("%Y-%m-%d"),
                    "status": status,
                    "check_in_time": "08:00:00" if status in ["Present", "Late"] else None,
                    "check_out_time": "14:00:00" if status == "Present" else None,
                    "minutes_late": random.randint(5, 30) if status == "Late" else 0,
                    "notes": "" if status == "Present" else random.choice(["", "Sygdom", "Laege", "Familie"]),
                    "created_at": datetime.now().isoformat()
                }
                attendance.append(attendance_record)
                attendance_counter += 1
        
        current_date += timedelta(days=1)
    
    return attendance


def generate_budgets(schools, output_dir):
    """Generate school budget data"""
    budgets = []
    budget_counter = 1
    
    budget_categories = [
        "Personale", "Undervisningsmaterialer", "IT_udstyr", "Bygningsvedligehold",
        "Transport", "Kantinedrift", "Specialundervisning", "Fritidsaktiviteter",
        "Administration", "Rengoring"
    ]
    
    for school in schools:
        total_budget = school["student_capacity"] * random.randint(50000, 70000)
        
        for category in budget_categories:
            if category == "Personale":
                category_budget = total_budget * random.uniform(0.55, 0.65)
            elif category == "Undervisningsmaterialer":
                category_budget = total_budget * random.uniform(0.08, 0.12)
            elif category == "IT_udstyr":
                category_budget = total_budget * random.uniform(0.05, 0.08)
            else:
                category_budget = total_budget * random.uniform(0.02, 0.05)
            
            budget_record = {
                "budget_id": f"BUD-{str(budget_counter).zfill(6)}",
                "school_id": school["school_id"],
                "municipality_code": school["municipality_code"],
                "fiscal_year": 2024,
                "category": category,
                "budgeted_amount": round(category_budget, 2),
                "spent_amount": round(category_budget * random.uniform(0.75, 0.95), 2),
                "currency": "DKK",
                "approved_date": "2024-01-15",
                "approved_by": f"{random.choice(DANISH_FIRST_NAMES_MALE)} {random.choice(DANISH_LAST_NAMES)}",
                "notes": "",
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat()
            }
            budgets.append(budget_record)
            budget_counter += 1
    
    return budgets


def generate_wellness(students, output_dir):
    """Generate student wellness data"""
    wellness = []
    wellness_counter = 1
    
    for student in random.sample(students, min(len(students), 1000)):
        wellness_record = {
            "wellness_id": f"WEL-{str(wellness_counter).zfill(6)}",
            "student_id": student["student_id"],
            "school_id": student["school_id"],
            "municipality_code": student["municipality_code"],
            "assessment_date": f"2024-{random.randint(9, 12):02d}-{random.randint(1, 28):02d}",
            "academic_wellbeing_score": random.randint(1, 10),
            "social_wellbeing_score": random.randint(1, 10),
            "physical_wellbeing_score": random.randint(1, 10),
            "overall_satisfaction": random.randint(1, 10),
            "bullying_experienced": random.choices([True, False], weights=[5, 95])[0],
            "support_needed": random.choices([True, False], weights=[15, 85])[0],
            "counselor_notes": random.choice([
                "", "Folger op", "Samtale planlagt", "Ingen bekymringer",
                "Kontakt til foraeldere", "Henvisning til PPR"
            ]),
            "assessed_by": f"{random.choice(DANISH_FIRST_NAMES_FEMALE)} {random.choice(DANISH_LAST_NAMES)}",
            "created_at": datetime.now().isoformat()
        }
        wellness.append(wellness_record)
        wellness_counter += 1
    
    return wellness


def write_csv(data, filename, output_dir):
    """Write data to CSV file"""
    if not data:
        return
    
    filepath = output_dir / filename
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    
    print(f"  Written {len(data)} records to {filename}")


def main():
    """Main function to generate all data"""
    base_dir = Path(__file__).parent.parent / "data"
    base_dir.mkdir(parents=True, exist_ok=True)
    
    print("=" * 60)
    print("KMD Denmark - School Data Generator")
    print("=" * 60)
    
    all_schools = []
    all_teachers = []
    all_classes = []
    all_students = []
    all_grades = []
    all_attendance = []
    all_budgets = []
    all_wellness = []
    
    for municipality_key, municipality_info in MUNICIPALITIES.items():
        print(f"\nGenerating data for {municipality_info['name']}...")
        
        muni_dir = base_dir / municipality_key.lower()
        muni_dir.mkdir(parents=True, exist_ok=True)
        
        schools = generate_schools(
            municipality_info["code"],
            municipality_info["name"],
            municipality_info["num_schools"],
            muni_dir
        )
        all_schools.extend(schools)
        write_csv(schools, "dim_schools.csv", muni_dir)
        
        teachers = generate_teachers(schools, muni_dir)
        all_teachers.extend(teachers)
        write_csv(teachers, "dim_teachers.csv", muni_dir)
        
        classes = generate_classes(schools, muni_dir)
        all_classes.extend(classes)
        write_csv(classes, "dim_classes.csv", muni_dir)
        
        students = generate_students(classes, muni_dir)
        all_students.extend(students)
        write_csv(students, "dim_students.csv", muni_dir)
        
        grades_data = generate_grades(students, muni_dir)
        all_grades.extend(grades_data)
        write_csv(grades_data, "fact_grades.csv", muni_dir)
        
        budgets = generate_budgets(schools, muni_dir)
        all_budgets.extend(budgets)
        write_csv(budgets, "fact_budgets.csv", muni_dir)
        
        wellness = generate_wellness(students, muni_dir)
        all_wellness.extend(wellness)
        write_csv(wellness, "fact_wellness.csv", muni_dir)
    
    print("\n" + "=" * 60)
    print("Generating combined attendance data (sampled)...")
    all_attendance = generate_attendance(all_students, base_dir)
    
    combined_dir = base_dir / "combined"
    combined_dir.mkdir(parents=True, exist_ok=True)
    
    print("\nWriting combined files...")
    write_csv(all_schools, "dim_schools_all.csv", combined_dir)
    write_csv(all_teachers, "dim_teachers_all.csv", combined_dir)
    write_csv(all_classes, "dim_classes_all.csv", combined_dir)
    write_csv(all_students, "dim_students_all.csv", combined_dir)
    write_csv(all_grades, "fact_grades_all.csv", combined_dir)
    write_csv(all_attendance[:50000], "fact_attendance_sample.csv", combined_dir)
    write_csv(all_budgets, "fact_budgets_all.csv", combined_dir)
    write_csv(all_wellness, "fact_wellness_all.csv", combined_dir)
    
    print("\n" + "=" * 60)
    print("Summary:")
    print(f"  Schools:    {len(all_schools)}")
    print(f"  Teachers:   {len(all_teachers)}")
    print(f"  Classes:    {len(all_classes)}")
    print(f"  Students:   {len(all_students)}")
    print(f"  Grades:     {len(all_grades)}")
    print(f"  Attendance: {len(all_attendance)} (50k sample written)")
    print(f"  Budgets:    {len(all_budgets)}")
    print(f"  Wellness:   {len(all_wellness)}")
    print("=" * 60)
    print("\nData generation complete!")


if __name__ == "__main__":
    main()
