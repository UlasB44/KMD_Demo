from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.colors import HexColor
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.units import mm
import os

BASE_PATH = os.path.dirname(os.path.abspath(__file__))

DOCS = {
    "copenhagen": [
        ("copenhagen_digital_learning_strategy_2024.pdf", "Copenhagen Digital Learning Strategy 2024-2026", """
<h2>EXECUTIVE SUMMARY</h2>
<p>Copenhagen Municipality is committed to preparing students for the digital future. This strategy outlines our comprehensive approach to integrating technology across all grade levels while maintaining focus on fundamental learning outcomes.</p>

<h2>VISION</h2>
<p>Every student in Copenhagen schools will develop strong digital competencies alongside traditional academic skills, enabling them to thrive in an increasingly technology-driven society.</p>

<h2>KEY INITIATIVES</h2>
<p><b>One-to-One Device Program:</b> All students from grade 3 onwards will receive a personal tablet or laptop. Younger students will have access to shared devices in classroom learning stations. Devices will be refreshed every 3 years.</p>

<p><b>Teacher Digital Training:</b> All teachers must complete 40 hours of digital pedagogy training annually. Training covers educational software, online safety instruction, and digital assessment tools.</p>

<p><b>Coding Curriculum Integration:</b> Introduction to computational thinking begins in grade 0 through unplugged activities. Scratch programming introduced in grades 3-4. Python basics taught in grades 7-9.</p>

<p><b>Digital Citizenship Education:</b> Mandatory lessons for all grade levels covering online safety, privacy, cyberbullying prevention, and responsible social media use.</p>

<h2>BUDGET ALLOCATION</h2>
<p>Total investment: 45 million DKK over three years. Device procurement: 18M DKK. Infrastructure: 12M DKK. Training: 10M DKK. Software: 5M DKK.</p>

<h2>CONTACT</h2>
<p>Copenhagen Education Technology Office - Email: digital@kk.dk - Phone: +45 33 66 33 66</p>
"""),
        ("copenhagen_special_needs_support_guide.pdf", "Copenhagen Special Needs Support Guide", """
<h2>INTRODUCTION</h2>
<p>Copenhagen Municipality is dedicated to inclusive education. This guide outlines support services for students with special needs including ADHD, Dyslexia, Autism Spectrum Disorder, and other learning differences.</p>

<h2>ADHD SUPPORT</h2>
<p>Designated quiet spaces in every classroom for focused work. Movement breaks integrated into daily schedules. One-on-one aide support for students with severe ADHD. Medication management coordination with school nurses. Behavioral intervention specialists available.</p>

<h2>DYSLEXIA SUPPORT</h2>
<p>Specialized reading intervention using Orton-Gillingham methodology. Audio textbooks and read-aloud software provided. Extended time accommodations for assessments. Dyslexia-trained specialists at each school. Text-to-speech tools available.</p>

<h2>AUTISM SPECTRUM SUPPORT</h2>
<p>Structured daily schedules with visual supports. Social skills groups led by trained therapists. Sensory-friendly classroom modifications. Transition support for routine changes. Communication aids for non-verbal students.</p>

<h2>REFERRAL PROCESS</h2>
<p>Step 1: Teacher identifies need. Step 2: Parent consent obtained. Step 3: Psychologist assessment. Step 4: IEP developed. Step 5: Services implemented. Step 6: Annual review.</p>

<h2>CONTACT</h2>
<p>Copenhagen Special Needs Center - Email: specialneeds@kk.dk - Phone: +45 33 66 34 00</p>
"""),
        ("copenhagen_teacher_handbook_2024.pdf", "Copenhagen Teacher Handbook 2024", """
<h2>WELCOME</h2>
<p>Welcome to Copenhagen Municipal Schools. This handbook provides essential information for all teaching staff regarding policies, procedures, and expectations.</p>

<h2>PROFESSIONAL CONDUCT</h2>
<p>Teachers must maintain highest standards of professional conduct including punctuality, appropriate dress, respectful communication with students and colleagues, and commitment to continuous improvement.</p>

<h2>CLASSROOM MANAGEMENT</h2>
<p>Copenhagen schools follow a positive behavior support framework. Classroom rules should be clearly posted and consistently enforced. Physical discipline is strictly prohibited. Restorative practices are encouraged.</p>

<h2>WORKING CONDITIONS</h2>
<p>Standard teaching load: 25-28 lessons weekly. Preparation time: minimum 5 hours weekly. Meeting time: 2 hours weekly. Class sizes: Grades 0-3 max 22 students, Grades 4-6 max 24, Grades 7-9 max 26.</p>

<h2>PROFESSIONAL DEVELOPMENT</h2>
<p>40 hours PD required annually. At least 20 hours must be district-approved courses. Career advancement through department head, curriculum coordinator, and mentor teacher positions.</p>

<h2>CONTACT</h2>
<p>Copenhagen Teacher Services - Email: teachers@kk.dk - Phone: +45 33 66 35 00</p>
"""),
        ("copenhagen_student_wellness_program.pdf", "Copenhagen Student Wellness Program", """
<h2>OVERVIEW</h2>
<p>The Copenhagen Student Wellness Program takes a holistic approach to student health, addressing physical, mental, and social well-being across all grade levels.</p>

<h2>MENTAL HEALTH SERVICES</h2>
<p>Each school has access to a school psychologist offering individual counseling, group therapy, crisis intervention, and assessments. Drop-in counseling hours available daily. Mindfulness programs integrated into routines.</p>

<h2>PHYSICAL HEALTH</h2>
<p>Annual vision and hearing screenings. Healthy lunch options at subsidized prices. Free breakfast program available. Minimum 45 minutes daily physical activity. Before and after school sports programs.</p>

<h2>ANTI-BULLYING</h2>
<p>Zero tolerance policy for bullying. Anonymous reporting system. Peer mediation programs in grades 5-9. Annual awareness week. Restorative circles for conflict resolution.</p>

<h2>CRISIS SUPPORT</h2>
<p>24-Hour Support Line: +45 33 66 36 36 - Text: Send HELP to 1899 - Online: wellness.kk.dk</p>
""")
    ],
    "aarhus": [
        ("aarhus_stem_education_initiative.pdf", "Aarhus STEM Education Initiative", """
<h2>MISSION</h2>
<p>Aarhus aims to become Denmark's leading municipality for STEM education, preparing students for careers in science, technology, engineering, and mathematics.</p>

<h2>KEY PROGRAMS</h2>
<p><b>STEM Discovery Labs:</b> State-of-the-art labs in 8 schools with 3D printers, robotics kits, chemistry stations, and electronics workbenches. Mobile lab visits smaller schools quarterly.</p>

<p><b>Industry Partnerships:</b> Vestas Wind Systems provides wind energy modules. Arla Foods supports food science programs. Systematic sponsors coding clubs. Aarhus University faculty mentor students.</p>

<p><b>Girls in STEM:</b> Dedicated programs encouraging female participation. Female scientist mentors. Girls-only coding and robotics clubs.</p>

<h2>CURRICULUM</h2>
<p>Grade 0-3: Science exploration through play. Grade 4-6: Scientific method and basic engineering. Grade 7-9: Applied STEM projects with industry connections.</p>

<h2>CONTACT</h2>
<p>Aarhus STEM Office - Email: stem@aarhus.dk - Phone: +45 89 40 20 00</p>
"""),
        ("aarhus_parent_engagement_guide.pdf", "Aarhus Parent Engagement Guide", """
<h2>INTRODUCTION</h2>
<p>Parent involvement is one of the strongest predictors of student success. Aarhus is committed to building strong partnerships between schools and families.</p>

<h2>WAYS TO GET INVOLVED</h2>
<p><b>Classroom:</b> Volunteer as reading helper, assist with activities, share your profession, chaperone trips.</p>
<p><b>Governance:</b> Join school board, participate in parent council, serve on hiring committees.</p>
<p><b>Home:</b> Establish homework routines, read with your child daily, monitor screen time.</p>

<h2>COMMUNICATION</h2>
<p>AULA platform for grades, attendance, assignments, and teacher messages. Parent conferences twice yearly. Weekly class newsletters. Emergency SMS notifications.</p>

<h2>WORKSHOPS</h2>
<p>Supporting homework. Understanding Danish school system. Digital safety. Raising resilient children. Danish language courses for immigrant parents.</p>

<h2>CONTACT</h2>
<p>Aarhus Family Engagement - Email: families@aarhus.dk - Phone: +45 89 40 25 00</p>
"""),
        ("aarhus_environmental_education.pdf", "Aarhus Environmental Education Program", """
<h2>GREEN SCHOOLS INITIATIVE</h2>
<p>Aarhus schools serve as living laboratories for environmental education and sustainable practices.</p>

<h2>CURRICULUM</h2>
<p><b>Climate Change:</b> Age-appropriate climate science. Local impact studies. Carbon footprint projects. Solutions-focused approach.</p>
<p><b>Biodiversity:</b> School gardens in 75% of schools. Native plant identification. Insect hotels. Annual biodiversity surveys.</p>
<p><b>Waste Reduction:</b> Comprehensive recycling. Composting facilities. Plastic-free lunch initiatives. Upcycling art projects.</p>

<h2>OUTDOOR LEARNING</h2>
<p>Weekly forest school sessions with certified teachers. School vegetable gardens with harvest used in kitchens. Minimum 4 nature excursions annually. Partnerships with Mols Bjerge National Park.</p>

<h2>CERTIFICATION</h2>
<p>Bronze: Basic recycling. Silver: Active gardens. Gold: Carbon neutral. Current: 12 Gold, 23 Silver, 18 Bronze schools.</p>

<h2>CONTACT</h2>
<p>Aarhus Environmental Education - Email: green@aarhus.dk - Phone: +45 89 40 22 00</p>
""")
    ],
    "odense": [
        ("odense_arts_culture_curriculum.pdf", "Odense Arts and Culture Curriculum", """
<h2>THE ODENSE CREATIVE VISION</h2>
<p>As birthplace of Hans Christian Andersen, Odense has special responsibility to nurture creativity and artistic expression in students.</p>

<h2>ARTS EDUCATION</h2>
<p><b>Visual Arts:</b> Dedicated studios in all schools. Professional artist guest instructors. Annual exhibition at Brandts Museum. Digital art from grade 5.</p>
<p><b>Music:</b> Instruction from grade 0. Instrument lending library. School choirs and bands. Annual Music Festival. Symphony Orchestra partnerships.</p>
<p><b>Drama:</b> Integrated into Danish curriculum. Annual productions. Improvisation and public speaking. Theatre field trips.</p>

<h2>CULTURAL PARTNERSHIPS</h2>
<p>Every student visits HC Andersen birthplace. Free museum access for school groups. Artist residency programs. Community mural projects.</p>

<h2>CREATIVE SPACES</h2>
<p>Makerspaces in 60% of schools. Recording studios. Green screen production. 3D printing labs.</p>

<h2>CONTACT</h2>
<p>Odense Arts Education - Email: kunst@odense.dk - Phone: +45 65 51 25 00</p>
"""),
        ("odense_school_safety_protocols.pdf", "Odense School Safety Protocols", """
<h2>COMMITMENT TO SAFETY</h2>
<p>Student and staff safety is Odense Municipality's highest priority. These protocols ensure consistent emergency response.</p>

<h2>EMERGENCY PROCEDURES</h2>
<p><b>Fire:</b> Evacuate immediately using designated routes. Roll call within 3 minutes. Monthly drills required.</p>
<p><b>Medical:</b> First aid kits in every room. AED devices in all schools. School nurse on-site or on-call.</p>
<p><b>Lockdown:</b> Code word system. Secure classroom protocols practiced twice yearly. Police coordination.</p>

<h2>DAILY SAFETY</h2>
<p>All visitors check in at office. Photo ID required. Crossing guards at high-traffic schools. Daily playground inspections.</p>

<h2>HEALTH AND HYGIENE</h2>
<p>Handwashing protocols. Illness exclusion guidelines. Cleaning schedules. Ventilation monitoring. Pandemic response plans.</p>

<h2>EMERGENCY CONTACTS</h2>
<p>Emergency: 112 - Safety Hotline: +45 65 51 24 24 - Email: safety@odense.dk</p>
"""),
        ("odense_inclusion_diversity_policy.pdf", "Odense Inclusion and Diversity Policy", """
<h2>VISION</h2>
<p>Odense creates welcoming schools where every student belongs, regardless of background, ability, or identity.</p>

<h2>DIVERSITY IN ODENSE</h2>
<p>Students from 87 countries. 23% speak Danish as second language. 12% receive special needs support. Growing refugee populations.</p>

<h2>INCLUSIVE PRACTICES</h2>
<p><b>Culturally Responsive:</b> Cultural competence training. Diverse curriculum perspectives. Multilingual resources. Anti-bias training.</p>
<p><b>Language Support:</b> Danish as Second Language instruction. Mother tongue classes. Bilingual assistants. Translation services.</p>

<h2>REFUGEE SUPPORT</h2>
<p>Dedicated welcome classes. Buddy system with Danish students. Family orientation. Cultural liaisons. Trauma-informed approaches.</p>

<h2>ANTI-DISCRIMINATION</h2>
<p>Clear complaint process. Investigation within 10 days. Support for affected students. Appeal process available.</p>

<h2>CONTACT</h2>
<p>Odense Inclusion Office - Email: inclusion@odense.dk - Phone: +45 65 51 26 00</p>
""")
    ],
    "aalborg": [
        ("aalborg_mental_health_resources.pdf", "Aalborg Student Mental Health Resources", """
<h2>PRIORITIZING MENTAL HEALTH</h2>
<p>One in five young people experience mental health challenges. Schools play a crucial role in early identification and support.</p>

<h2>SERVICES</h2>
<p><b>Counseling:</b> Licensed counselors in every school. Drop-in hours daily. Individual and group options. Crisis intervention available.</p>
<p><b>Psychological:</b> Educational psychologists for assessments. Anxiety and depression screening. ADHD evaluation. Family consultation.</p>
<p><b>Peer Support:</b> Trained peer listeners grades 7-9. Anonymous hotline. Lunch buddy programs. Mental health ambassador clubs.</p>

<h2>CONCERNS ADDRESSED</h2>
<p>Anxiety interventions. Depression screening and support. Self-harm crisis protocols. Safety assessments. Return-to-school support.</p>

<h2>STRESS REDUCTION</h2>
<p>Homework-free weekends. Flexible deadlines. Mindfulness programs. Physical activity breaks. Nature-based relief.</p>

<h2>CRISIS RESOURCES</h2>
<p>24/7 Crisis: 70 201 201 - Text: UNG to 1899 - School: +45 99 31 31 31 - Email: mentalhealth@aalborg.dk</p>
"""),
        ("aalborg_professional_development.pdf", "Aalborg Teacher Professional Development", """
<h2>INVESTING IN TEACHERS</h2>
<p>Excellent teachers create excellent schools. Our comprehensive program ensures continuous growth for all educators.</p>

<h2>REQUIREMENTS</h2>
<p>Minimum 50 hours PD annually. 30 hours in district priorities. Documentation required. Completion linked to advancement.</p>

<h2>PRIORITY AREAS 2024-2025</h2>
<p><b>Differentiated Instruction:</b> Mixed-ability strategies. Assessment for learning. Flexible grouping. UDL principles.</p>
<p><b>Trauma-Informed Practice:</b> Understanding ACEs. Safe environments. De-escalation. Educator self-care.</p>
<p><b>Digital Pedagogy:</b> LMS use. Digital content creation. Online assessment. AI tools in education.</p>

<h2>CAREER PATHWAYS</h2>
<p>Leadership roles: Department heads, curriculum coordinators, instructional coaches. Certifications: Special ed, reading specialist, STEM, bilingual.</p>

<h2>CONTACT</h2>
<p>Aalborg Teacher Development - Email: pd@aalborg.dk - Phone: +45 99 31 32 00</p>
"""),
        ("aalborg_sports_physical_education.pdf", "Aalborg Sports and Physical Education Program", """
<h2>ACTIVE STUDENTS, HEALTHY FUTURES</h2>
<p>Aalborg PE develops lifelong healthy habits while building teamwork, resilience, and self-confidence.</p>

<h2>STANDARDS</h2>
<p>Minimum 90 minutes PE weekly. Daily movement breaks. Active recess. Before/after school options. Swimming required.</p>

<h2>CURRICULUM</h2>
<p>Grades 0-3: Fundamental movement. Grades 4-6: Team sports introduction. Grades 7-9: Lifetime fitness activities.</p>

<h2>SPORTS OFFERED</h2>
<p>Football, Handball, Basketball, Swimming, Gymnastics, Track and field, Badminton, Floor hockey, Dance, Outdoor education.</p>

<h2>INCLUSION</h2>
<p>Adapted PE for disabilities. Inclusive teams. Modified equipment. Peer buddy support. Specialized teacher training.</p>

<h2>SPECIAL PROGRAMS</h2>
<p>Aalborg School Olympics. Sport-for-all day. Professional athlete visits. Parent-child events. Teacher wellness.</p>

<h2>CONTACT</h2>
<p>Aalborg Sports Education - Email: sport@aalborg.dk - Phone: +45 99 31 33 00</p>
""")
    ],
    "esbjerg": [
        ("esbjerg_maritime_education_heritage.pdf", "Esbjerg Maritime Education Heritage Program", """
<h2>EMBRACING OUR COASTAL IDENTITY</h2>
<p>As Denmark's largest North Sea port city, Esbjerg integrates maritime heritage into education, preparing students for marine industries.</p>

<h2>MARITIME CURRICULUM</h2>
<p><b>Ocean Literacy:</b> Marine ecosystems. Ocean conservation. Climate impacts on coastal communities. Fisheries management. Renewable ocean energy.</p>
<p><b>History:</b> Esbjerg as fishing port. Danish shipping traditions. Offshore oil era. Transition to wind energy hub.</p>
<p><b>Practical Skills:</b> Sailing courses. Basic seamanship grades 5-7. Water safety certification. Navigation introduction. Knot tying.</p>

<h2>INDUSTRY CONNECTIONS</h2>
<p>Partnerships with Vestas and Siemens Gamesa. Turbine technology modules. Port visit field trips. Maritime career day. Internships for older students.</p>

<h2>SIGNATURE PROGRAMS</h2>
<p>Maritime Heritage Week. School sailing regatta. Beach art competition. Oral history from fishing families. Environmental monitoring of Esbjerg Bay.</p>

<h2>CONTACT</h2>
<p>Esbjerg Maritime Education - Email: maritime@esbjerg.dk - Phone: +45 76 16 16 00</p>
"""),
        ("esbjerg_inclusive_classroom_guide.pdf", "Esbjerg Inclusive Classroom Guidelines", """
<h2>INCLUSION PHILOSOPHY</h2>
<p>Every student belongs in their neighborhood school. Inclusive education benefits all students, teaching empathy and collaboration.</p>

<h2>STRATEGIES</h2>
<p><b>Universal Design:</b> Multiple means of engagement, representation, and expression. Flexible materials. Barrier-free environment.</p>
<p><b>Differentiated Instruction:</b> Tiered assignments. Flexible grouping. Choice in demonstrating learning. Varied pacing.</p>
<p><b>Co-Teaching:</b> Collaboration with special educators. Parallel teaching. Station rotations. Team teaching.</p>

<h2>SUPPORTING SPECIFIC NEEDS</h2>
<p><b>ADHD:</b> Preferential seating. Movement opportunities. Clear routines. Chunked assignments. Fidget tools.</p>
<p><b>Dyslexia:</b> Audio texts. Extended time. Dyslexia-friendly fonts. Graphic organizers.</p>
<p><b>Autism:</b> Visual schedules. Sensory considerations. Clear expectations. Calm space. Transition warnings.</p>

<h2>CONTACT</h2>
<p>Esbjerg Inclusion Support - Email: inclusion@esbjerg.dk - Phone: +45 76 16 17 00</p>
"""),
        ("esbjerg_family_school_partnership.pdf", "Esbjerg Family-School Partnership Framework", """
<h2>PARTNERSHIP PRINCIPLES</h2>
<p>Esbjerg views families as essential partners. Strong partnerships improve student outcomes, attendance, and well-being.</p>

<h2>COMMUNICATION</h2>
<p>Weekly class updates via AULA. Monthly principal newsletters. Quarterly progress reports. Translation in major languages. Home visits when helpful.</p>

<h2>ENGAGEMENT OPPORTUNITIES</h2>
<p><b>In-School:</b> Classroom volunteering. Library assistance. Event planning. Career presentations. Field trip chaperoning.</p>
<p><b>At-Home:</b> Homework guidelines. Reading resources. Educational activities. Summer learning packets.</p>
<p><b>Decision-Making:</b> School board. Parent council. Policy input. Hiring committees.</p>

<h2>SERVING DIVERSE FAMILIES</h2>
<p>Cultural liaisons for immigrants. Flexible meeting times for working parents. Video conferencing. Before/after school programs.</p>

<h2>CONTACT</h2>
<p>Esbjerg Family Partnership - Email: families@esbjerg.dk - Phone: +45 76 16 18 00</p>
"""),
        ("esbjerg_emergency_procedures.pdf", "Esbjerg School Emergency Procedures", """
<h2>EMERGENCY PREPAREDNESS</h2>
<p>Esbjerg's coastal location requires specialized emergency preparedness for student and staff safety.</p>

<h2>COASTAL EMERGENCIES</h2>
<p><b>Storm Surge:</b> Monitor DMI alerts. Early dismissal procedures. Inland evacuation routes. Family reunification at safe locations.</p>
<p><b>Severe Weather:</b> High wind protocols. Lightning safety. Flooding response. School closure decisions.</p>

<h2>STANDARD PROCEDURES</h2>
<p><b>Fire:</b> Immediate evacuation. Designated assembly areas. Accountability procedures. Fire department coordination.</p>
<p><b>Medical:</b> First aid response. Emergency services (112). Family notification. Hospital coordination.</p>
<p><b>Lockdown:</b> Initiation code. Classroom security. Communication silence. Police coordination.</p>

<h2>DRILLS</h2>
<p>Monthly fire drills. Twice-yearly lockdown. Annual evacuation. Weather emergency drills. Staff CPR certification.</p>

<h2>EMERGENCY CONTACTS</h2>
<p>Emergency: 112 - School Line: +45 76 16 19 99 - Storm Info: +45 76 16 19 50 - Email: safety@esbjerg.dk</p>
""")
    ]
}

def create_styles():
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(
        name='DocTitle',
        parent=styles['Heading1'],
        fontSize=18,
        textColor=HexColor('#003366'),
        spaceAfter=20
    ))
    styles.add(ParagraphStyle(
        name='SectionHead',
        parent=styles['Heading2'],
        fontSize=14,
        textColor=HexColor('#003366'),
        spaceBefore=15,
        spaceAfter=8
    ))
    styles.add(ParagraphStyle(
        name='DocBody',
        parent=styles['Normal'],
        fontSize=10,
        spaceBefore=4,
        spaceAfter=4
    ))
    return styles

def generate_pdf(filepath, title, content, styles):
    doc = SimpleDocTemplate(filepath, pagesize=A4, rightMargin=20*mm, leftMargin=20*mm, topMargin=20*mm, bottomMargin=20*mm)
    story = []
    
    story.append(Paragraph(title, styles['DocTitle']))
    story.append(Spacer(1, 10))
    
    for line in content.strip().split('\n'):
        line = line.strip()
        if not line:
            continue
        if line.startswith('<h2>'):
            text = line.replace('<h2>', '').replace('</h2>', '')
            story.append(Paragraph(text, styles['SectionHead']))
        elif line.startswith('<p>'):
            text = line.replace('<p>', '').replace('</p>', '')
            story.append(Paragraph(text, styles['DocBody']))
    
    doc.build(story)

def main():
    styles = create_styles()
    for municipality, docs in DOCS.items():
        folder = os.path.join(BASE_PATH, 'pdfs', municipality)
        os.makedirs(folder, exist_ok=True)
        for filename, title, content in docs:
            filepath = os.path.join(folder, filename)
            generate_pdf(filepath, title, content, styles)
            print(f"Created: {filepath}")
    print("\nAll PDFs generated!")

if __name__ == "__main__":
    main()
