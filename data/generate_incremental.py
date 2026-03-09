import csv
import random
from datetime import date, timedelta
from pathlib import Path

DANISH_FIRST_NAMES_M = ["Magnus", "Oliver", "William", "Noah", "Oscar", "Lucas", "Victor", "Malthe", "Alfred", "Emil", "Frederik", "Mikkel", "Rasmus", "Christian", "Sebastian"]
DANISH_FIRST_NAMES_F = ["Emma", "Ida", "Clara", "Freja", "Alma", "Sofia", "Ella", "Anna", "Laura", "Mathilde", "Sofie", "Isabella", "Lærke", "Victoria", "Karla"]
DANISH_LAST_NAMES = ["Jensen", "Nielsen", "Hansen", "Pedersen", "Andersen", "Christensen", "Larsen", "Sørensen", "Rasmussen", "Jørgensen", "Petersen", "Madsen", "Kristensen", "Olsen", "Thomsen"]
SUBJECTS = ["Dansk", "Matematik", "Engelsk", "Historie", "Geografi", "Biologi", "Fysik", "Kemi", "Musik", "Idræt", "Billedkunst"]
STREETS = ["Hovedgade", "Strandvej", "Skovvej", "Parkvej", "Kirkevej", "Nørregade", "Søndergade", "Vestergade", "Østergade", "Møllevej"]

MUNICIPALITIES = {
    101: {"name": "copenhagen", "city": "København", "schools": ["101-SCH-001", "101-SCH-002", "101-SCH-003", "101-SCH-004", "101-SCH-005", "101-SCH-006", "101-SCH-007"]},
    751: {"name": "aarhus", "city": "Aarhus", "schools": ["751-SCH-001", "751-SCH-002", "751-SCH-003", "751-SCH-004", "751-SCH-005", "751-SCH-006", "751-SCH-007"]},
    461: {"name": "odense", "city": "Odense", "schools": ["461-SCH-001", "461-SCH-002", "461-SCH-003", "461-SCH-004", "461-SCH-005", "461-SCH-006", "461-SCH-007"]},
    851: {"name": "aalborg", "city": "Aalborg", "schools": ["851-SCH-001", "851-SCH-002", "851-SCH-003", "851-SCH-004", "851-SCH-005"]},
    561: {"name": "esbjerg", "city": "Esbjerg", "schools": ["561-SCH-001", "561-SCH-002", "561-SCH-003", "561-SCH-004", "561-SCH-005", "561-SCH-006", "561-SCH-007", "561-SCH-008", "561-SCH-009"]}
}

# Baseline counts per municipality (approximate from existing data)
BASELINE_COUNTS = {
    101: {"students": 2900, "teachers": 200, "classes": 140},
    751: {"students": 2900, "teachers": 200, "classes": 140},
    461: {"students": 2900, "teachers": 200, "classes": 140},
    851: {"students": 2900, "teachers": 200, "classes": 140},
    561: {"students": 3000, "teachers": 193, "classes": 139}
}

# New records per day per municipality
NEW_PER_DAY = {"students": 10, "teachers": 2, "classes": 2}

def generate_cpr(birth_date, gender):
    day = birth_date.strftime("%d%m%y")
    last4 = random.randint(1000, 9999)
    if gender == "M":
        last4 = last4 | 1
    else:
        last4 = last4 & ~1
    return f"{day}-{last4}"

def generate_phone():
    return f"+45 {random.randint(20,99)} {random.randint(10,99)} {random.randint(10,99)} {random.randint(10,99)}"

def generate_student(student_id, muni_code, muni_info, file_date):
    gender = random.choice(["M", "F"])
    first_name = random.choice(DANISH_FIRST_NAMES_M if gender == "M" else DANISH_FIRST_NAMES_F)
    last_name = random.choice(DANISH_LAST_NAMES)
    birth_date = date(2018, 1, 1) + timedelta(days=random.randint(0, 365*2))
    cpr = generate_cpr(birth_date, gender)
    school_id = random.choice(muni_info["schools"])
    class_id = f"CLS-{random.randint(1, 252):05d}"
    
    return {
        "student_id": f"STU-{muni_code}-{student_id:05d}",
        "class_id": class_id,
        "school_id": school_id,
        "municipality_code": muni_code,
        "cpr_number": cpr,
        "cpr_masked": cpr[:6] + "-XXXX",
        "first_name": first_name,
        "last_name": last_name,
        "gender": gender,
        "birth_date": birth_date.isoformat(),
        "enrollment_date": file_date.isoformat(),
        "guardian_name": f"{random.choice(DANISH_FIRST_NAMES_M)} {last_name}",
        "guardian_phone": generate_phone(),
        "guardian_email": f"{last_name.lower()}familie@email.dk",
        "address": f"{random.choice(STREETS)} {random.randint(1, 200)}",
        "postal_code": str(random.randint(1000, 9999)),
        "special_needs": random.choice(["Ingen", "Ingen", "Ingen", "Dysleksi", "ADHD", "Autisme"]),
        "is_active": "true",
        "created_at": f"{file_date} 08:00:00",
        "updated_at": f"{file_date} 08:00:00"
    }

def generate_teacher(teacher_id, muni_code, muni_info, file_date):
    gender = random.choice(["M", "F"])
    first_name = random.choice(DANISH_FIRST_NAMES_M if gender == "M" else DANISH_FIRST_NAMES_F)
    last_name = random.choice(DANISH_LAST_NAMES)
    birth_date = date(1970, 1, 1) + timedelta(days=random.randint(0, 365*30))
    cpr = generate_cpr(birth_date, gender)
    school_id = random.choice(muni_info["schools"])
    subjects = ",".join(random.sample(SUBJECTS, random.randint(1, 3)))
    
    return {
        "teacher_id": f"TCH-{muni_code}-{teacher_id:05d}",
        "school_id": school_id,
        "municipality_code": muni_code,
        "cpr_number": cpr,
        "cpr_masked": cpr[:6] + "-XXXX",
        "first_name": first_name,
        "last_name": last_name,
        "gender": gender,
        "birth_date": birth_date.isoformat(),
        "email": f"{first_name.lower()}.{last_name.lower()}@skole.dk",
        "phone": generate_phone(),
        "hire_date": file_date.isoformat(),
        "subjects": subjects,
        "salary_band": random.choice(["A", "B", "C"]),
        "is_active": "true",
        "created_at": f"{file_date} 08:00:00",
        "updated_at": f"{file_date} 08:00:00"
    }

def generate_class(class_id, muni_code, muni_info, file_date):
    school_id = random.choice(muni_info["schools"])
    grade = random.randint(0, 9)
    section = random.choice(["A", "B", "C"])
    
    return {
        "class_id": f"CLS-{muni_code}-{class_id:05d}",
        "school_id": school_id,
        "municipality_code": muni_code,
        "grade": grade,
        "section": section,
        "class_name": f"{grade}.{section}",
        "academic_year": "2024-2025",
        "max_students": random.randint(22, 28),
        "classroom_number": f"L{random.randint(100, 300)}",
        "is_active": "true",
        "created_at": f"{file_date} 08:00:00",
        "updated_at": f"{file_date} 08:00:00"
    }

def write_csv(filename, data, fieldnames):
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    print(f"Created: {filename} ({len(data)} records)")

# Field definitions
student_fields = ["student_id", "class_id", "school_id", "municipality_code", "cpr_number", "cpr_masked", 
                  "first_name", "last_name", "gender", "birth_date", "enrollment_date", "guardian_name",
                  "guardian_phone", "guardian_email", "address", "postal_code", "special_needs", 
                  "is_active", "created_at", "updated_at"]

teacher_fields = ["teacher_id", "school_id", "municipality_code", "cpr_number", "cpr_masked",
                  "first_name", "last_name", "gender", "birth_date", "email", "phone", "hire_date",
                  "subjects", "salary_band", "is_active", "created_at", "updated_at"]

class_fields = ["class_id", "school_id", "municipality_code", "grade", "section", "class_name",
                "academic_year", "max_students", "classroom_number", "is_active", "created_at", "updated_at"]

output_base = Path("/Users/ulasbulut/Desktop/CoCo/KMD/KMD_Demo/data/incremental")

# Generate for each municipality
for muni_code, muni_info in MUNICIPALITIES.items():
    muni_name = muni_info["name"]
    output_dir = output_base / muni_name
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"\n=== {muni_name.upper()} (code: {muni_code}) ===")
    
    # Initialize baseline data (simulating existing records)
    baseline = BASELINE_COUNTS[muni_code]
    
    # Generate baseline students for this municipality
    students = []
    for i in range(1, baseline["students"] + 1):
        students.append(generate_student(i, muni_code, muni_info, date(2026, 3, 9)))
    
    teachers = []
    for i in range(1, baseline["teachers"] + 1):
        teachers.append(generate_teacher(i, muni_code, muni_info, date(2026, 3, 9)))
    
    classes = []
    for i in range(1, baseline["classes"] + 1):
        classes.append(generate_class(i, muni_code, muni_info, date(2026, 3, 9)))
    
    # Write Day 1 (baseline - March 10, 2026)
    day1_date = date(2026, 3, 10)
    date_str = day1_date.strftime("%Y%m%d")
    write_csv(output_dir / f"dim_students_{date_str}.csv", students, student_fields)
    write_csv(output_dir / f"dim_teachers_{date_str}.csv", teachers, teacher_fields)
    write_csv(output_dir / f"dim_classes_{date_str}.csv", classes, class_fields)
    
    # Day 2: Full load = Day 1 + new records
    day2_date = date(2026, 3, 11)
    date_str = day2_date.strftime("%Y%m%d")
    
    # Add new students
    for i in range(NEW_PER_DAY["students"]):
        students.append(generate_student(baseline["students"] + i + 1, muni_code, muni_info, day2_date))
    
    # Add new teachers
    for i in range(NEW_PER_DAY["teachers"]):
        teachers.append(generate_teacher(baseline["teachers"] + i + 1, muni_code, muni_info, day2_date))
    
    # Add new classes
    for i in range(NEW_PER_DAY["classes"]):
        classes.append(generate_class(baseline["classes"] + i + 1, muni_code, muni_info, day2_date))
    
    write_csv(output_dir / f"dim_students_{date_str}.csv", students, student_fields)
    write_csv(output_dir / f"dim_teachers_{date_str}.csv", teachers, teacher_fields)
    write_csv(output_dir / f"dim_classes_{date_str}.csv", classes, class_fields)
    
    # Day 3: Full load = Day 2 + more new records
    day3_date = date(2026, 3, 12)
    date_str = day3_date.strftime("%Y%m%d")
    
    # Add more new students
    for i in range(NEW_PER_DAY["students"]):
        students.append(generate_student(baseline["students"] + NEW_PER_DAY["students"] + i + 1, muni_code, muni_info, day3_date))
    
    # Add more new teachers
    for i in range(NEW_PER_DAY["teachers"]):
        teachers.append(generate_teacher(baseline["teachers"] + NEW_PER_DAY["teachers"] + i + 1, muni_code, muni_info, day3_date))
    
    # Add more new classes
    for i in range(NEW_PER_DAY["classes"]):
        classes.append(generate_class(baseline["classes"] + NEW_PER_DAY["classes"] + i + 1, muni_code, muni_info, day3_date))
    
    write_csv(output_dir / f"dim_students_{date_str}.csv", students, student_fields)
    write_csv(output_dir / f"dim_teachers_{date_str}.csv", teachers, teacher_fields)
    write_csv(output_dir / f"dim_classes_{date_str}.csv", classes, class_fields)

print("\n" + "="*60)
print("✅ Generated full-load files for all 5 municipalities!")
print("Each day's file contains ALL records (previous + new)")
print("="*60)
