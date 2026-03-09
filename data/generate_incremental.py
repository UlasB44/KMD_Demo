import csv
import random
from datetime import date, timedelta
from pathlib import Path

DANISH_FIRST_NAMES_M = ["Magnus", "Oliver", "William", "Noah", "Oscar", "Lucas", "Victor", "Malthe", "Alfred", "Emil", "Frederik", "Mikkel", "Rasmus", "Christian", "Sebastian"]
DANISH_FIRST_NAMES_F = ["Emma", "Ida", "Clara", "Freja", "Alma", "Sofia", "Ella", "Anna", "Laura", "Mathilde", "Sofie", "Isabella", "Lærke", "Victoria", "Karla"]
DANISH_LAST_NAMES = ["Jensen", "Nielsen", "Hansen", "Pedersen", "Andersen", "Christensen", "Larsen", "Sørensen", "Rasmussen", "Jørgensen", "Petersen", "Madsen", "Kristensen", "Olsen", "Thomsen"]
SUBJECTS = ["Dansk", "Matematik", "Engelsk", "Historie", "Geografi", "Biologi", "Fysik", "Kemi", "Musik", "Idræt", "Billedkunst"]
STREETS = ["Hovedgade", "Strandvej", "Skovvej", "Parkvej", "Kirkevej", "Nørregade", "Søndergade", "Vestergade", "Østergade", "Møllevej"]
CITIES = {101: "København", 751: "Aarhus", 461: "Odense", 851: "Aalborg", 561: "Esbjerg"}
MUNICIPALITIES = [101, 751, 461, 851, 561]
SCHOOL_IDS = {
    101: ["101-SCH-001", "101-SCH-002", "101-SCH-003", "101-SCH-004", "101-SCH-005", "101-SCH-006", "101-SCH-007"],
    751: ["751-SCH-001", "751-SCH-002", "751-SCH-003", "751-SCH-004", "751-SCH-005", "751-SCH-006", "751-SCH-007"],
    461: ["461-SCH-001", "461-SCH-002", "461-SCH-003", "461-SCH-004", "461-SCH-005", "461-SCH-006", "461-SCH-007"],
    851: ["851-SCH-001", "851-SCH-002", "851-SCH-003", "851-SCH-004", "851-SCH-005"],
    561: ["561-SCH-001", "561-SCH-002", "561-SCH-003", "561-SCH-004", "561-SCH-005", "561-SCH-006", "561-SCH-007", "561-SCH-008", "561-SCH-009"]
}

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

def generate_students(day_num, start_id, count_per_day):
    students = []
    student_id = start_id
    file_date = date(2026, 3, 9) + timedelta(days=day_num)
    
    for muni in MUNICIPALITIES:
        for _ in range(count_per_day // len(MUNICIPALITIES)):
            gender = random.choice(["M", "F"])
            first_name = random.choice(DANISH_FIRST_NAMES_M if gender == "M" else DANISH_FIRST_NAMES_F)
            last_name = random.choice(DANISH_LAST_NAMES)
            birth_date = date(2018, 1, 1) + timedelta(days=random.randint(0, 365*2))
            cpr = generate_cpr(birth_date, gender)
            school_id = random.choice(SCHOOL_IDS[muni])
            class_id = f"CLS-{random.randint(1, 252):05d}"
            
            students.append({
                "student_id": f"STU-{student_id:06d}",
                "class_id": class_id,
                "school_id": school_id,
                "municipality_code": muni,
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
            })
            student_id += 1
    return students, student_id

def generate_teachers(day_num, start_id, count_per_day):
    teachers = []
    teacher_id = start_id
    file_date = date(2026, 3, 9) + timedelta(days=day_num)
    
    for muni in MUNICIPALITIES:
        for _ in range(count_per_day // len(MUNICIPALITIES)):
            gender = random.choice(["M", "F"])
            first_name = random.choice(DANISH_FIRST_NAMES_M if gender == "M" else DANISH_FIRST_NAMES_F)
            last_name = random.choice(DANISH_LAST_NAMES)
            birth_date = date(1970, 1, 1) + timedelta(days=random.randint(0, 365*30))
            cpr = generate_cpr(birth_date, gender)
            school_id = random.choice(SCHOOL_IDS[muni])
            subjects = ",".join(random.sample(SUBJECTS, random.randint(1, 3)))
            
            teachers.append({
                "teacher_id": f"TCH-{teacher_id:05d}",
                "school_id": school_id,
                "municipality_code": muni,
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
            })
            teacher_id += 1
    return teachers, teacher_id

def generate_classes(day_num, start_id, count_per_day):
    classes = []
    class_id = start_id
    file_date = date(2026, 3, 9) + timedelta(days=day_num)
    
    for muni in MUNICIPALITIES:
        for _ in range(count_per_day // len(MUNICIPALITIES)):
            school_id = random.choice(SCHOOL_IDS[muni])
            grade = random.randint(0, 9)
            section = random.choice(["A", "B", "C"])
            
            classes.append({
                "class_id": f"CLS-{class_id:05d}",
                "school_id": school_id,
                "municipality_code": muni,
                "grade": grade,
                "section": section,
                "class_name": f"{grade}.{section}",
                "academic_year": "2024-2025",
                "max_students": random.randint(22, 28),
                "classroom_number": f"L{random.randint(100, 300)}",
                "is_active": "true",
                "created_at": f"{file_date} 08:00:00",
                "updated_at": f"{file_date} 08:00:00"
            })
            class_id += 1
    return classes, class_id

def write_csv(filename, data, fieldnames):
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    print(f"Created: {filename} ({len(data)} records)")

output_dir = Path("/Users/ulasbulut/Desktop/CoCo/KMD/KMD_Demo/data/incremental")

student_fields = ["student_id", "class_id", "school_id", "municipality_code", "cpr_number", "cpr_masked", 
                  "first_name", "last_name", "gender", "birth_date", "enrollment_date", "guardian_name",
                  "guardian_phone", "guardian_email", "address", "postal_code", "special_needs", 
                  "is_active", "created_at", "updated_at"]

teacher_fields = ["teacher_id", "school_id", "municipality_code", "cpr_number", "cpr_masked",
                  "first_name", "last_name", "gender", "birth_date", "email", "phone", "hire_date",
                  "subjects", "salary_band", "is_active", "created_at", "updated_at"]

class_fields = ["class_id", "school_id", "municipality_code", "grade", "section", "class_name",
                "academic_year", "max_students", "classroom_number", "is_active", "created_at", "updated_at"]

student_start_id = 5251
teacher_start_id = 366
class_start_id = 253

for day in [1, 2, 3]:
    file_date = date(2026, 3, 9) + timedelta(days=day)
    date_str = file_date.strftime("%Y%m%d")
    
    students, student_start_id = generate_students(day, student_start_id, 50)
    write_csv(output_dir / f"dim_students_{date_str}.csv", students, student_fields)
    
    teachers, teacher_start_id = generate_teachers(day, teacher_start_id, 10)
    write_csv(output_dir / f"dim_teachers_{date_str}.csv", teachers, teacher_fields)
    
    classes, class_start_id = generate_classes(day, class_start_id, 10)
    write_csv(output_dir / f"dim_classes_{date_str}.csv", classes, class_fields)

print("\n✅ Generated 3 days of incremental data files!")
print(f"Total new students: {student_start_id - 5251}")
print(f"Total new teachers: {teacher_start_id - 366}")
print(f"Total new classes: {class_start_id - 253}")
