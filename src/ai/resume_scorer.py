import asyncio
import json
import sys
from datetime import datetime
from dataclasses import dataclass
from typing import List
from pydantic import BaseModel, Field, field_validator
from openai import AsyncOpenAI
import os
import agents
from agents import (
    Agent, 
    Runner, 
    function_tool, 
    RunContextWrapper,
    GuardrailFunctionOutput,
    OutputGuardrailTripwireTriggered,
    output_guardrail
)

# Print environment information for debugging
print(f"Python version: {sys.version}")
print(f"Agents library version: {getattr(agents, '__version__', 'Unknown')}")
print(f"Runner type: {type(Runner)}")
print(f"Runner.run type: {type(Runner.run)}")
print(f"Runner.run callable: {callable(Runner.run)}")

# Test Runner.run behavior
try:
    print("Testing Runner.run behavior...")
    # Create a simple test to see what Runner.run returns
    test_agent = Agent(name="Test", instructions="Test", model="gpt-3.5-turbo")
    test_result = Runner.run(test_agent, "test input")
    print(f"Test Runner.run returned: {type(test_result)}")
    print(f"Test result has __await__: {hasattr(test_result, '__await__')}")
except Exception as e:
    print(f"Error testing Runner.run: {str(e)}")


# Load environment variables
# load_dotenv()

# Initialize OpenAI client
client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
MODEL = os.getenv("MODEL_CHOICE", "gpt-4o")

# --- Models for structured outputs ---

class ResumeExtractor(BaseModel):
    skills: List[str] = Field(description="List of skills found in the resume")
    experience: List[str] = Field(description="List of work experiences")
    education: List[str] = Field(description="List of educational qualifications")
    projects: List[str] = Field(description="List of projects")
    achievements: List[str] = Field(description="List of achievements")

class JobRequirements(BaseModel):
    required_skills: List[str] = Field(description="Required skills for the job")
    preferred_skills: List[str] = Field(description="Preferred skills for the job")
    experience_level: str = Field(description="Required experience level")
    education_requirements: List[str] = Field(description="Required education qualifications")

class SkillsFound(BaseModel):
    skills_found: List[str] = Field(description="List of skills that were present in the Resume")
    total_skills_checked: int = Field(description="Total number of skills that were checked against the resume")
    match_percentage: float = Field(description="Percentage of target skills found in the resume (0.0 to 1.0)")
    skill_context: List[str] = Field(description="Brief context of where/how each skill was found in the resume")
    skill_score: float = Field(description="Score for skills match (0.0 to 4.0)")

class ExperienceScore(BaseModel):
    experience_score: float = Field(description="Score for experience match (0.0 to 4.5)")
    years_experience: float = Field(description="Total years of relevant experience")
    relevant_roles: List[str] = Field(description="List of relevant job titles/roles found")
    experience_breakdown: str = Field(description="High-level summary of experience relevance")

class EducationScore(BaseModel):
    education_score: float = Field(description="Score for education match (0.0 to 1.0)")
    degree_match: str = Field(description="How well the degree matches requirements")
    certifications: List[str] = Field(description="List of relevant certifications")
    education_breakdown: str = Field(description="High-level summary of education relevance")

class ResumeScore(BaseModel):
    overall_score: float = Field(description="Overall score out of 10")
    skill_score: float = Field(description="Score for skills match (0.0 to 4.0)")
    experience_score: float = Field(description="Score for experience match (0.0 to 4.5)")
    education_score: float = Field(description="Score for education match (0.0 to 1.0)")
    other_factors_score: float = Field(description="Score for other factors like projects, achievements, additional qualifications (0.0 to 0.5 MAXIMUM)")
    strengths: List[str] = Field(description="List of candidate's strengths")
    weaknesses: List[str] = Field(description="List of candidate's weaknesses")
    breakdown: str = Field(description="High-level summary of overall assessment")
    summary: str = Field(description="Overall summary of the candidate's fit")
    
    @field_validator('other_factors_score')
    @classmethod
    def validate_other_factors_score(cls, v):
        if v > 0.5:
            return 0.5  # Cap at 0.5
        if v < 0.0:
            return 0.0  # Ensure non-negative
        return v

class ExperienceRelevance(BaseModel):
    relevance_explanation: str = Field(description="Penjelasan detail tentang bagaimana pengalaman kerja kandidat berkaitan dengan tujuan dan persyaratan pekerjaan (dalam bahasa Indonesia)")
    relevant_experiences: List[str] = Field(description="Daftar pengalaman spesifik yang paling relevan dengan posisi (dalam bahasa Indonesia)")
    experience_gaps: List[str] = Field(description="Daftar kesenjangan pengalaman atau area di mana kandidat kurang memiliki pengalaman yang relevan (dalam bahasa Indonesia)")

class EducationRelevance(BaseModel):
    relevance_explanation: str = Field(description="Penjelasan detail tentang bagaimana pendidikan kandidat berkaitan dengan tujuan dan persyaratan pekerjaan (dalam bahasa Indonesia)")
    relevant_qualifications: List[str] = Field(description="Daftar kualifikasi pendidikan yang paling relevan dengan posisi (dalam bahasa Indonesia)")
    education_gaps: List[str] = Field(description="Daftar kesenjangan pendidikan atau kualifikasi yang kurang dimiliki kandidat (dalam bahasa Indonesia)")

class Recommendation(BaseModel):
    should_hire: bool = Field(description="Rekomendasi boolean apakah kandidat harus dipekerjakan")
    reasoning: str = Field(description="Alasan detail untuk rekomendasi perekrutan (dalam bahasa Indonesia)")
    confidence_level: str = Field(description="Tingkat kepercayaan rekomendasi: 'Tinggi', 'Sedang', atau 'Rendah'")
    key_factors: List[str] = Field(description="Daftar faktor kunci yang mempengaruhi keputusan perekrutan (dalam bahasa Indonesia)")

class AlternativePositions(BaseModel):
    suggested_positions: List[str] = Field(description="Daftar posisi pekerjaan alternatif yang mungkin lebih cocok untuk kandidat (dalam bahasa Indonesia)")
    reasoning: str = Field(description="Penjelasan mengapa posisi alternatif ini akan lebih cocok untuk kandidat (dalam bahasa Indonesia)")
    fit_scores: List[float] = Field(description="Skor kecocokan untuk setiap posisi yang disarankan (0.0 hingga 10.0)")

class FinalOutput(BaseModel):
    experience_relevance: ExperienceRelevance
    education_relevance: EducationRelevance
    recommendation: Recommendation
    alternative_positions: AlternativePositions

# Guardrail Models
class IndonesianCheckOutput(BaseModel):
    is_indonesian: bool = Field(description="Whether the text is in Indonesian language")
    confidence: float = Field(description="Confidence level of the language detection (0.0 to 1.0)")
    detected_language: str = Field(description="The detected language of the text")
    reasoning: str = Field(description="Explanation of why the text is or isn't Indonesian")

# Job Enhancement Models
class EnhancedJobRequirements(BaseModel):
    enhanced_requirements: str = Field(description="Enhanced job requirements with bullet point formatting, maximum 500 characters, and in Indonesian language") 

# --- Helper Functions ---

async def safe_runner_run(agent, input_data):
    """
    Safely run an agent with proper async/await handling.
    Handles both sync and async Runner.run() implementations.
    """
    print(f"Running agent: {agent.name}")
    
    try:
        result = Runner.run(agent, input_data)
        print(f"Agent {agent.name} - Runner.run() returned: {type(result)}")
        
        # Check if result is a dictionary (sync implementation)
        if isinstance(result, dict):
            print(f"Agent {agent.name} - Result is dict (sync implementation)")
            # Create a mock result object that matches the expected structure
            class MockResult:
                def __init__(self, data):
                    self.final_output = data
            return MockResult(result)
        
        # Check if result has __await__ method (async implementation)
        elif hasattr(result, '__await__'):
            print(f"Agent {agent.name} - Result is awaitable (async implementation)")
            try:
                awaited_result = await result
                print(f"Agent {agent.name} - Successfully awaited result")
                return awaited_result
            except TypeError as te:
                if "object dict can't be used in 'await' expression" in str(te):
                    print(f"Agent {agent.name} - Caught await error: {str(te)}")
                    print(f"Agent {agent.name} - Treating as sync result")
                    return result
                else:
                    print(f"Agent {agent.name} - Different TypeError: {str(te)}")
                    raise te
        
        # If it's neither dict nor awaitable, return as-is
        else:
            print(f"Agent {agent.name} - Result is neither dict nor awaitable: {type(result)}")
            return result
            
    except Exception as e:
        print(f"Agent {agent.name} - Error in safe_runner_run: {str(e)}")
        print(f"Agent {agent.name} - Error type: {type(e).__name__}")
        raise

# --- Context Class ---

@dataclass
class ScoringContext:
    job_title: str
    industry: str
    session_start: datetime = None
    
    def __post_init__(self):
        if self.session_start is None:
            self.session_start = datetime.now()

# --- Tools ---

# @function_tool
# async def extract_text_from_pdf(pdf_path: str) -> str:
#     """Extract text from a PDF file."""
#     reader = PdfReader(pdf_path)
#     text = ""
#     for page in reader.pages:
#         text += page.extract_text()
#     return text

# @function_tool
# async def read_text_file(file_path: str) -> str:
#     """Read text from a text file."""
#     with open(file_path, 'r', encoding='utf-8') as file:
#         return file.read()

# --- Specialized Agents ---

# resume_extractor_agent = Agent(
#     name="Resume Extractor",
#     instructions="""
#     You are a resume analysis expert. Your task is to extract and structure information from resumes. What is given is the path to a resume. Help extract the text using your tools then
#     Focus on identifying:
#     - Technical and soft skills
#     - Work experience with details
#     - Educational background
#     - Projects and achievements
    
#     Provide the information in a structured format that can be easily compared with job requirements.
#     Make sure to follow the exact schema provided in the ResumeExtractor model.
#     The output MUST include all required fields: skills, experience, education, projects, and achievements.
#     Do not add any additional fields that are not in the schema.
#     """,
#     # tools=[extract_text_from_pdf, read_text_file],
#     output_type=ResumeExtractor,
#     model=MODEL
# )



skill_extractor_agent = Agent(
    name="Skill Extractor Agent",
    instructions="""
    You are a specialized Skill Extractor Agent designed to analyze resumes and identify specific skills that are present in the candidate's background.
    
    INPUT FORMAT:
    You will receive a JSON object with:
    - resume_path: Path to the resume file (PDF or text)
    - target_skills: array of strings containing the skills to check for (comma-separated or list format)
    
    YOUR TASK:
    1. Extract text from the provided resume using the resume_path
    2. Parse the target_skills string into a list of individual skills
    3. Analyze the resume content thoroughly to identify which target skills are present. 
    4. Calculate a skill score based on match percentage:
       - skill_score = match_percentage * 4.0
       - This ensures consistent scoring based on the actual percentage of skills found
       - Example: 75% match = 0.75 * 4.0 = 3.0 skill score
       - Max Points: 4.0 (when 100% of skills are found)
    5. Return detailed information about skill matches including context and statistics
    
    ANALYSIS APPROACH:
    - Look for skills in multiple sections: experience, education, projects, certifications
    - Consider variations and synonyms of skill names (e.g., "Python" vs "Python programming")
    - Check for skill mentions in experience, project descriptions, and technical sections
    - Be thorough but accurate - only include skills that are clearly demonstrated
    
    SKILL DETECTION RULES (BE CONSISTENT AND DETERMINISTIC):
    1. EXACT MATCHES: Direct mentions of the skill name (case-insensitive)
    2. SYNONYMS: Common variations (e.g., "JS" for "JavaScript", "React.js" for "React", "Node.js" for "Node")
    3. CONTEXT CLUES: Skills clearly implied through project descriptions, experience, or job responsibilities
    4. CERTIFICATIONS: Skills mentioned in certification names or descriptions
    5. CONSISTENCY RULES:
       - Always use the same criteria for skill detection
       - If a skill is mentioned in multiple places, count it only once
       - Be conservative - only count skills that are clearly demonstrated
       - Use exact skill names from the target_skills list when possible 
    
    OUTPUT REQUIREMENTS:
    - skills_found: List of skills that are actually present in the resume
    - total_skills_checked: Count of all target skills that were evaluated
    - match_percentage: Calculate as (len(skills_found) / total_skills_checked) with 2 decimal places
    - skill_context: For each found skill, provide brief context like "Found in work experience as Senior Developer" or "Mentioned in project description"
    - skill_score: ALWAYS calculate as match_percentage * 4.0 (0.0 to 4.0)
    
    DETERMINISTIC CALCULATION RULES:
    - match_percentage = number_of_skills_found / total_skills_checked
    - skill_score = match_percentage * 4.0
    - Example: 8 skills found out of 10 total = 0.8 * 4.0 = 3.2 skill_score
    - This ensures consistent scoring regardless of content complexity
    
    IMPORTANT: 
    - Be precise and conservative. Only include skills that are clearly demonstrated
    - Provide meaningful context for each skill found
    - Calculate match percentage accurately (0.0 to 1.0 scale)
    - ALWAYS use the formula: skill_score = match_percentage * 4.0
    - Follow the SkillsFound schema exactly with all required fields
    - Handle cases where target_skills might be empty or malformed
    """,
    # tools=[extract_text_from_pdf, read_text_file],
    output_type=SkillsFound,
    model=MODEL,
)

job_analyzer_agent = Agent(
    name="Job Requirements Analyzer",
    instructions="""
    You are a STRICT job requirements extraction expert. Your task is to EXTRACT ONLY EXPLICITLY MENTIONED requirements from job descriptions.
    
    RULES:
    1. Only extract information that is DIRECTLY STATED in the job description
    2. If a requirement is not mentioned, leave that field EMPTY
    3. Never infer or assume requirements that aren't explicitly stated
    4. Be precise and literal in your extraction
    
    
    EXAMPLES:
    GOOD (exact extraction):
    - Job says: "Must know Python and SQL" → required_skills: ["Python", "SQL"]
    - Job says nothing about education → education_requirements: []
    
    BAD (making assumptions):
    - Job says nothing about education but you add: ["Bachelor's degree"] ← WRONG
    - Job mentions "sales experience" but you add: ["2+ years experience"] ← WRONG unless exact number stated
    
    OUTPUT REQUIREMENTS:
    - Must follow the EXACT schema in JobRequirements model
    - Only include what's EXPLICITLY in the job description
    - Empty fields should be EMPTY LISTS, never null or placeholder text
    """,
    output_type=JobRequirements,
    model=MODEL,
)

experience_scoring_agent = Agent(
    name="Experience Scoring Specialist",
    instructions="""
    You are an experience scoring specialist. Your task is to evaluate the candidate's work experience against job requirements.
    If no experience listed determine by yourself based on the job title how relevant the work is. Be strict on how relevant a candidates experience is to their job description.
    
    IMPORTANT: All text output must be in Indonesian language (Bahasa Indonesia).
    
    BREAKDOWN STYLE REQUIREMENTS:
    - Provide high-level, abstract summary of experience relevance
    - Focus on overall fit and alignment with job requirements
    - Avoid detailed point calculations or scoring explanations
    - Keep it concise and professional
    
    SCORING METHODOLOGY (0.0 to 4.5 scale):
    1. YEARS OF EXPERIENCE:
       - Exact match to requirement: +2.0 points
       - Within 1 year of requirement: +1.5 points
       - Within 2 years of requirement: +1.0 points
       - More than 2 years below requirement: +0.5 points
       - Significantly more experience: +2.5 points
    
    2. RELEVANT ROLES:
       - Exact job title match: +1.0 point
       - Similar role in same industry: +0.8 points
       - Related role in different industry: +0.5 points
       - Unrelated roles: +0.2 points
    
    3. INDUSTRY ALIGNMENT:
       - Same industry experience: +0.5 points
       - Related industry: +0.3 points
       - Different industry: +0.1 points
    
    CALCULATION RULES (BE CONSISTENT AND DETERMINISTIC):
    - Start with base score of 0.0
    - Add points according to methodology above
    - Cap total score at 4.5
    - Use precise decimals (e.g., 3.2, 4.1)
    - ALWAYS apply the same criteria for similar experience levels
    - Be consistent in role relevance assessment
    - Use the same industry classification criteria
    
    OUTPUT REQUIREMENTS:
    - experience_score: Final calculated score (0.0 to 4.5)
    - years_experience: Total years of relevant experience
    - relevant_roles: List of relevant job titles/roles found (in Indonesian)
    - experience_breakdown: High-level summary of experience relevance (in Indonesian, no point details)
    
    Make sure to follow the exact schema provided in the ExperienceScore model.
    """,
    output_type=ExperienceScore,
    model=MODEL,
)

education_scoring_agent = Agent(
    name="Education Scoring Specialist",
    instructions="""
    You are an education scoring specialist. Your task is to evaluate the candidate's educational background against job requirements. If there is no education requirement listed 
    make a decision based on the job description if the education is relevant.
    
    IMPORTANT: All text output must be in Indonesian language (Bahasa Indonesia).
    
    BREAKDOWN STYLE REQUIREMENTS:
    - Provide high-level, abstract summary of education relevance
    - Focus on overall fit and alignment with job requirements
    - Avoid detailed point calculations or scoring explanations
    - Keep it concise and professional
    
    SCORING METHODOLOGY (0.0 to 1.0 scale):
    1. DEGREE MATCH:
       - Exact degree match: +0.6 points
       - Related degree field: +0.4 points
       - Unrelated degree: +0.2 points
       - No degree but relevant certifications: +0.3 points
    
    2. CERTIFICATIONS:
       - Relevant professional certifications: +0.2 points each (max +0.4)
       - Industry-specific certifications: +0.1 points each (max +0.2)
    
    3. ADDITIONAL QUALIFICATIONS:
       - Relevant courses/training: +0.1 points
       - Academic achievements: +0.1 points
    
    CALCULATION RULES (BE CONSISTENT AND DETERMINISTIC):
    - Start with base score of 0.0
    - Add points according to methodology above
    - Cap total score at 1.0
    - Use precise decimals (e.g., 0.8, 0.6)
    - ALWAYS apply the same criteria for similar degree types
    - Be consistent in certification relevance assessment
    - Use the same field classification criteria
    
    OUTPUT REQUIREMENTS:
    - education_score: Final calculated score (0.0 to 1.0)
    - degree_match: Description of how well the degree matches requirements (in Indonesian)
    - certifications: List of relevant certifications found (in Indonesian)
    - education_breakdown: High-level summary of education relevance (in Indonesian, no point details)
    
    Make sure to follow the exact schema provided in the EducationScore model.
    """,
    output_type=EducationScore,
    model=MODEL,
)

final_scoring_agent = Agent(
    name="Final Scoring Coordinator",
    instructions="""
    You are a final scoring coordinator. Your task is to combine all individual scores and provide a comprehensive evaluation.
    
    IMPORTANT: All text output must be in Indonesian language (Bahasa Indonesia).
    
    BREAKDOWN STYLE REQUIREMENTS:
    - Provide high-level, abstract summary of overall assessment
    - Focus on overall candidate fit and key factors
    - Avoid detailed point calculations or scoring explanations
    - Keep it concise and professional
    
    CONSISTENCY REQUIREMENTS:
    - Use the EXACT scores provided by the individual agents
    - Do NOT modify or recalculate the skill_score, experience_score, or education_score
    - Only calculate the other_factors_score and overall_score
    - Be consistent in your other_factors_score calculation
    
    MANDATORY OTHER_FACTORS_SCORE RULES:
    - other_factors_score MUST be between 0.0 and 0.5 (inclusive)
    - If your calculation results in more than 0.5, you MUST set it to exactly 0.5
    - This is a HARD LIMIT that cannot be exceeded under any circumstances
    - Double-check your calculation before outputting the final result
    
    INPUT:
    You will receive scores from:
    - Skill Extractor Agent (skill_score: 0.0 to 4.0)
    - Experience Scoring Agent (experience_score: 0.0 to 4.5)
    - Education Scoring Agent (education_score: 0.0 to 1.0)
    - Resume data and job requirements for context
    
    FINAL SCORING METHODOLOGY:
    - Overall Score = skill_score + experience_score + education_score + other_factors_score
    - Other factors score (0.0 to 0.5): Projects, achievements, additional qualifications
    - Calculate other_factors_score based on (CAP AT 0.5):
      * Relevant projects: +0.2 points for significant projects (max 1 project)
      * Notable achievements: +0.1 points for relevant achievements (max 1 achievement)
      * Additional qualifications: +0.1 points for relevant certifications/training (max 1 qualification)
      * Leadership experience: +0.1 points for relevant leadership roles (max 1 leadership role)
      * TOTAL CAP: 0.5 maximum (even if sum exceeds 0.5)
    - VALIDATION STEP: Before finalizing, check if other_factors_score > 0.5, if yes, set to 0.5
    - Maximum possible score: 10.0
    
    EVALUATION CRITERIA:
    1. EXCELLENT (8.0-10.0): Strong match across all categories
    2. GOOD (6.0-7.9): Good match with minor gaps
    3. AVERAGE (4.0-5.9): Moderate match with some gaps
    4. POOR (0.0-3.9): Significant mismatches or missing requirements
    
    OUTPUT REQUIREMENTS:
    - overall_score: Sum of all component scores (0.0 to 10.0)
    - skill_score: From skill extractor agent
    - experience_score: From experience scoring agent
    - education_score: From education scoring agent
    - other_factors_score: Score for projects, achievements, additional qualifications (0.0 to 0.5 MAXIMUM)
    - strengths: List of candidate's key strengths (in Indonesian)
    - weaknesses: List of candidate's key weaknesses (in Indonesian)
    - breakdown: High-level summary of overall assessment (in Indonesian, no point details)
    - summary: Overall assessment of candidate fit (in Indonesian)
    
    CRITICAL: other_factors_score MUST NEVER exceed 0.5. If calculated sum exceeds 0.5, cap it at 0.5.
    
    FINAL VALIDATION CHECKLIST:
    1. Is other_factors_score <= 0.5? If NO, set to 0.5
    2. Is overall_score = skill_score + experience_score + education_score + other_factors_score?
    3. Are all scores within their valid ranges?
    
    Make sure to follow the exact schema provided in the ResumeScore model.
    """,
    output_type=ResumeScore,
    model=MODEL,
)

# Indonesian Language Guardrail Agent
indonesian_check_agent = Agent(
    name="Indonesian Language Checker",
    instructions="""
    You are a language detection specialist. Your task is to analyze text and determine if it is written in Indonesian (Bahasa Indonesia).
    
    ANALYSIS CRITERIA:
    1. LANGUAGE DETECTION:
       - Look for Indonesian words, phrases, and grammatical structures
       - Check for common Indonesian words like: "dalam", "untuk", "dengan", "yang", "adalah", "akan", "telah", "sudah", "belum", "tidak", "bukan"
       - Look for Indonesian suffixes like: "-nya", "-kan", "-i", "-an"
       - Check for Indonesian sentence structure and word order
    
    2. CONFIDENCE ASSESSMENT:
       - High confidence (0.8-1.0): Clear Indonesian text with proper grammar and vocabulary
       - Medium confidence (0.5-0.7): Mostly Indonesian with some mixed content
       - Low confidence (0.0-0.4): Primarily non-Indonesian or unclear language
    
    3. DETECTION RULES:
       - If text contains primarily English words and structures → is_indonesian: false
       - If text contains primarily Indonesian words and structures → is_indonesian: true
       - If text is mixed or unclear → assess based on dominant language
    
    OUTPUT REQUIREMENTS:
    - is_indonesian: Boolean indicating if text is in Indonesian
    - confidence: Confidence level (0.0 to 1.0) of the detection
    - detected_language: The primary language detected (e.g., "Indonesian", "English", "Mixed")
    - reasoning: Explanation of the detection decision
    
    Be thorough and accurate in your language analysis.
    """,
    output_type=IndonesianCheckOutput,
    model=MODEL
)

@output_guardrail
async def indonesian_guardrail_job_enhancement(
    ctx: RunContextWrapper, agent: Agent, output: EnhancedJobRequirements
) -> GuardrailFunctionOutput:
    """
    Guardrail to ensure job enhancement output is in Indonesian language.
    """
    # Check if the enhanced requirements text is in Indonesian
    result = await Runner.run(indonesian_check_agent, output.enhanced_requirements, context=ctx.context)
    
    # Trip the guardrail if the text is NOT in Indonesian
    tripwire_triggered = not result.final_output.is_indonesian
    
    return GuardrailFunctionOutput(
        output_info=result.final_output,
        tripwire_triggered=tripwire_triggered,
    )

@output_guardrail
async def indonesian_guardrail(
    ctx: RunContextWrapper, agent: Agent, output: FinalOutput
) -> GuardrailFunctionOutput:
    """
    Guardrail to ensure all text output is in Indonesian language.
    """
    # Combine all text fields from the output for language checking
    text_to_check = ""
    
    # Add experience relevance text
    text_to_check += output.experience_relevance.relevance_explanation + " "
    text_to_check += " ".join(output.experience_relevance.relevant_experiences) + " "
    text_to_check += " ".join(output.experience_relevance.experience_gaps) + " "
    
    # Add education relevance text
    text_to_check += output.education_relevance.relevance_explanation + " "
    text_to_check += " ".join(output.education_relevance.relevant_qualifications) + " "
    text_to_check += " ".join(output.education_relevance.education_gaps) + " "
    
    # Add recommendation text
    text_to_check += output.recommendation.reasoning + " "
    text_to_check += " ".join(output.recommendation.key_factors) + " "
    
    # Add alternative positions text
    text_to_check += output.alternative_positions.reasoning + " "
    text_to_check += " ".join(output.alternative_positions.suggested_positions) + " "
    
    # Check if the combined text is in Indonesian
    result = await Runner.run(indonesian_check_agent, text_to_check, context=ctx.context)
    
    # Trip the guardrail if the text is NOT in Indonesian
    tripwire_triggered = not result.final_output.is_indonesian
    
    return GuardrailFunctionOutput(
        output_info=result.final_output,
        tripwire_triggered=tripwire_triggered,
    )

resume_scoring_agent = Agent(
    name="Resume Evaluation Specialist",
    instructions="""
    You are a comprehensive resume evaluation specialist. Your task is to provide detailed analysis and recommendations for candidate evaluation.
    
    IMPORTANT: All text output must be in Indonesian language (Bahasa Indonesia).
    
    SCORE-DRIVEN RECOMMENDATION PRINCIPLE:
    - Your hiring recommendation MUST be heavily influenced by the overall_score
    - Higher scores (6.0+) should generally result in hiring recommendations
    - Lower scores (below 5.0) should generally result in non-hiring recommendations
    - Use the scoring results as the primary basis for your decision
    
    EVALUATION COMPONENTS:
    
    1. EXPERIENCE RELEVANCE ANALYSIS:
       - Analyze how the candidate's work experience relates to the job requirements
       - Identify specific experiences that are most relevant to the position
       - Highlight experience gaps or areas where the candidate lacks relevant experience
       - Provide detailed explanations of experience alignment
    
    2. EDUCATION RELEVANCE ANALYSIS:
       - Evaluate how the candidate's education relates to the job requirements
       - Identify relevant educational qualifications for the position
       - Highlight education gaps or missing qualifications
       - Provide detailed explanations of education alignment
    
    3. HIRING RECOMMENDATION (SCORE-DRIVEN):
       - Make hiring decision based HEAVILY on overall_score:
         * overall_score >= 7.0: STRONGLY RECOMMEND (should_hire: true, confidence: Tinggi)
         * overall_score 6.0-6.9: RECOMMEND (should_hire: true, confidence: Sedang-Tinggi)
         * overall_score 5.0-5.9: CONSIDER (should_hire: true, confidence: Sedang)
         * overall_score 4.0-4.9: WEAK RECOMMENDATION (should_hire: false, confidence: Sedang)
         * overall_score < 4.0: DO NOT RECOMMEND (should_hire: false, confidence: Tinggi)
       - Provide reasoning that emphasizes the scoring results
       - Confidence level should reflect score strength
       - List key factors with focus on score components
    
    4. ALTERNATIVE POSITIONS:
       - Suggest alternative job positions that might be better suited for the candidate
       - Provide reasoning for why these positions would be better fits
       - Assign fit scores (0.0 to 10.0) for each suggested position
       - Consider the candidate's skills, experience, and education for alternative roles
    
    OUTPUT REQUIREMENTS:
    - All explanations should be detailed and specific (in Indonesian)
    - Use clear, professional language in Indonesian
    - Provide actionable insights for hiring decisions
    - Include specific examples from the resume and job description
    - Be objective and evidence-based in all assessments
    - Base hiring recommendations PRIMARILY on overall_score results
    - Reference specific score components (skill_score, experience_score, education_score) in reasoning
    
    IMPORTANT: All text content must be in Indonesian language (Bahasa Indonesia).
    """,
    model=MODEL,
    output_type=FinalOutput,
    output_guardrails=[indonesian_guardrail],
)

# Job Enhancement Agent
job_enhancement_agent = Agent(
    name="Job Requirements Enhancement Specialist",
    instructions="""
    You are a job requirements creation specialist. Your task is to create well-structured, clear, and professional job requirements using the provided context information.
    
    INPUT FORMAT:
    You will receive a JSON object with:
    - job_title: Job title (optional)
    - industry: Industry sector (optional)
    - job_skills: Required skills (optional)
    - gender: Gender preference (optional)
    - years_experience: Years of experience required (optional)
    - age: Age requirements (optional)
    - education: Education requirements (optional)
    - working_type: Working arrangement (optional)
    
    ENHANCEMENT REQUIREMENTS:
    
    1. FORMATTING:
       - Use bullet points (•) for each requirement on a new line
       - Make each bullet point clear and concise
       - Group related requirements logically
       - Use proper spacing and structure
    
    2. CONTENT CREATION:
       - Create job requirements from scratch using the provided context information
       - Use job_title, industry, and job_skills to generate relevant requirements
       - Add standard requirements based on the role and industry
       - Make language professional and specific
       - Ensure requirements are actionable and measurable
       - DO NOT restate or repeat the candidate criteria (age, education, gender, years_experience) in the output
       - Focus on what the job requires, not what the candidate should have
    
    3. CHARACTER LIMIT:
       - Maximum 500 characters total
       - Be concise but comprehensive
       - Prioritize the most important requirements
       - Use efficient language
    
    4. STRUCTURE:
       - Start with most critical requirements
       - Group by category (skills, experience, education, etc.)
       - Use consistent formatting throughout

    5. LANGUAGE REQUIREMENTS:
       - All text must be in Indonesian (Bahasa Indonesia)
       - Use proper Indonesian grammar and vocabulary
       - Use professional Indonesian terminology
       - Translate technical terms appropriately
    
    CONTEXT UTILIZATION:
    - If job_title is provided, use it to make requirements more specific to that role
    - If industry is provided, tailor requirements to that industry's standards
    - If job_skills are provided, incorporate them into the skills requirements
    - If working_type is provided, include working arrangement details
    - Use years_experience, age, education, and gender as context to better understand the role, but DO NOT include them in the output
    - Focus on what the job requires, not what the candidate should have
    
    OUTPUT REQUIREMENTS:
    - enhanced_requirements: Well-formatted job requirements with bullet points
    - Must be exactly 500 characters or less
    - Each requirement should be on a new line with bullet point
    - Professional and clear language
    - Comprehensive but concise
    - Enhanced with context information when available
    
    EXAMPLE FORMAT (Indonesian):
    • Gelar Sarjana Teknik Informatika atau bidang terkait
    • Pengalaman 3+ tahun dalam pengembangan Python
    • Pengalaman dengan framework Django/Flask
    • Kemampuan pemecahan masalah yang kuat
    • Pengalaman kolaborasi tim
    
    IMPORTANT: Keep the output under 500 characters and use bullet point formatting. Create requirements from scratch using all available context information.
    """,
    output_type=EnhancedJobRequirements,
    model=MODEL,
    output_guardrails=[indonesian_guardrail_job_enhancement]
)

async def enhance_job_requirements(
    job_requirements: str = None,
    job_title: str = None,
    industry: str = None,
    job_skills: str = None,
    gender: str = None,
    years_experience: str = None,
    age: str = None,
    education: str = None,
    working_type: str = None
) -> str:
    """
    Enhance job requirements with bullet point formatting and character limit.
    Uses additional context information when available to provide more complete requirements.
    """
    try:
        # Create input data with all available context (excluding job_requirements)
        input_data = {
            "job_title": job_title,
            "industry": industry,
            "job_skills": job_skills,
            "gender": gender,
            "years_experience": years_experience,
            "age": age,
            "education": education,
            "working_type": working_type
        }
        
        # Remove None values to keep the input clean
        input_data = {k: v for k, v in input_data.items() if v is not None}
        
        result = await safe_runner_run(
            job_enhancement_agent,
            json.dumps(input_data)
        )
        
        if not isinstance(result.final_output, EnhancedJobRequirements):
            raise TypeError("Job Enhancement Agent returned wrong type")
        
        return result.final_output.enhanced_requirements
        
    except Exception as e:
        print(f"\nError enhancing job requirements: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        raise

async def score_resume(
    resume_text: str,
    job_description: str,
    target_skills: List[str],
) -> dict:
    """Sequentially runs the pipeline and returns a dictionary of results."""
    try:
        # STEP 1: Run Resume Extractor Agent
        # resume_extraction_result = await Runner.run(
        #     resume_extractor_agent,
        #     resume_path
        # )

        # if not isinstance(resume_extraction_result.final_output, ResumeExtractor):
        #     raise TypeError("Resume Extractor returned wrong type")
    
        # print("\nResume Extraction Result:")
        # print(resume_extraction_result)

        # resume_data = resume_extraction_result.final_output

        # STEP 2: Run Skill Extractor Agent
        skill_extraction_input = json.dumps({
            "resume_text": resume_text,
            "target_skills": target_skills
        })

        skill_extraction_result = await safe_runner_run(
            skill_extractor_agent,
            skill_extraction_input
        )

        if not isinstance(skill_extraction_result.final_output, SkillsFound):
            raise TypeError("Skill Extractor returned wrong type")

        print("\nSkill Extraction Result:")
        print(skill_extraction_result)

        skills_found = skill_extraction_result.final_output

        # STEP 3: Run Job Analyzer Agent
        job_analysis_input = json.dumps({
            "job_description": job_description
        })


        # STEP 4: Run Experience Scoring Agent
        experience_input = json.dumps({
            "resume_text": resume_text,
            "job_description": job_description
        })

        experience_score_result = await safe_runner_run(
            experience_scoring_agent,
            experience_input
        )

        if not isinstance(experience_score_result.final_output, ExperienceScore):
            raise TypeError("Experience Scoring Agent returned wrong type")

        print("\nExperience Scoring Result:")
        print(experience_score_result)

        experience_score = experience_score_result.final_output

        # STEP 5: Run Education Scoring Agent
        education_input = json.dumps({
            "resume_text": resume_text,
            "job_description": job_description
        })

        education_score_result = await safe_runner_run(
            education_scoring_agent,
            education_input
        )

        if not isinstance(education_score_result.final_output, EducationScore):
            raise TypeError("Education Scoring Agent returned wrong type")

        print("\nEducation Scoring Result:")
        print(education_score_result)

        education_score = education_score_result.final_output

        # STEP 6: Run Final Scoring Agent
        final_scoring_input = json.dumps({
            "skill_score": skills_found.skill_score,
            "experience_score": experience_score.experience_score,
            "education_score": education_score.education_score,
            "resume_text": resume_text,
            "skills_found": skills_found.model_dump(),
            "job_description": job_description
        })

        final_score_result = await safe_runner_run(
            final_scoring_agent,
            final_scoring_input
        )

        if not isinstance(final_score_result.final_output, ResumeScore):
            raise TypeError("Final Scoring Agent returned wrong type")

        print("\nFinal Scoring Result:")
        print(final_score_result)

        result = final_score_result.final_output

        # STEP 7: Run Final Evaluation Agent
        evaluation_input = json.dumps({
            "result": result.model_dump(),
            "job_description": job_description,
            "resume_text": resume_text,
            "skills_found": skills_found.model_dump(),
            "experience_score": experience_score.model_dump(),
            "education_score": education_score.model_dump()
        })

        resume_evaluation = await safe_runner_run(
            resume_scoring_agent,
            evaluation_input
        )

        if not isinstance(resume_evaluation.final_output, FinalOutput):
            raise TypeError("Resume Scoring Coordinator returned wrong type")

        # Return a dictionary with all the serializable data
        return {
            "skills_found": skills_found.model_dump(),
            "experience_score": experience_score.model_dump(),
            "education_score": education_score.model_dump(),
            "scoring": result.model_dump(),
            "evaluation": resume_evaluation.final_output.model_dump()
        }

    except Exception as e:
        print(f"\nError scoring resume: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        raise 


async def score_pdf(
    resume_text: str,
    job_description: str,
    target_skills: List[str],
) -> dict:
    """Sequentially runs the pipeline and returns a dictionary of results."""
    try:
        # STEP 1: Run Resume Extractor Agent
        # resume_extraction_result = await Runner.run(
        #     resume_extractor_agent,
        #     resume_path
        # )

        # if not isinstance(resume_extraction_result.final_output, ResumeExtractor):
        #     raise TypeError("Resume Extractor returned wrong type")
    
        # print("\nResume Extraction Result:")
        # print(resume_extraction_result)

        # resume_data = resume_extraction_result.final_output

        # STEP 2: Run Skill Extractor Agent
        skill_extraction_input = json.dumps({
            "resume_text": resume_text,
            "target_skills": target_skills
        })

        skill_extraction_result = await safe_runner_run(
            skill_extractor_agent,
            skill_extraction_input
        )

        if not isinstance(skill_extraction_result.final_output, SkillsFound):
            raise TypeError("Skill Extractor returned wrong type")

        print("\nSkill Extraction Result:")
        print(skill_extraction_result)

        skills_found = skill_extraction_result.final_output

        # STEP 3: Run Job Analyzer Agent
        job_analysis_input = json.dumps({
            "job_description": job_description
        })


        # STEP 4: Run Experience Scoring Agent
        experience_input = json.dumps({
            "resume_text": resume_text,
            "job_description": job_description
        })

        experience_score_result = await safe_runner_run(
            experience_scoring_agent,
            experience_input
        )

        if not isinstance(experience_score_result.final_output, ExperienceScore):
            raise TypeError("Experience Scoring Agent returned wrong type")

        print("\nExperience Scoring Result:")
        print(experience_score_result)

        experience_score = experience_score_result.final_output

        # STEP 5: Run Education Scoring Agent
        education_input = json.dumps({
            "resume_text": resume_text,
            "job_description": job_description
        })

        education_score_result = await safe_runner_run(
            education_scoring_agent,
            education_input
        )

        if not isinstance(education_score_result.final_output, EducationScore):
            raise TypeError("Education Scoring Agent returned wrong type")

        print("\nEducation Scoring Result:")
        print(education_score_result)

        education_score = education_score_result.final_output

        # STEP 6: Run Final Scoring Agent
        final_scoring_input = json.dumps({
            "skill_score": skills_found.skill_score,
            "experience_score": experience_score.experience_score,
            "education_score": education_score.education_score,
            "resume_text": resume_text,
            "skills_found": skills_found.model_dump(),
            "job_description": job_description
        })

        final_score_result = await safe_runner_run(
            final_scoring_agent,
            final_scoring_input
        )

        if not isinstance(final_score_result.final_output, ResumeScore):
            raise TypeError("Final Scoring Agent returned wrong type")

        print("\nFinal Scoring Result:")
        print(final_score_result)

        result = final_score_result.final_output

        # STEP 7: Run Final Evaluation Agent
        evaluation_input = json.dumps({
            "result": result.model_dump(),
            "job_description": job_description,
            "resume_text": resume_text,
            "skills_found": skills_found.model_dump(),
            "experience_score": experience_score.model_dump(),
            "education_score": education_score.model_dump()
        })

        resume_evaluation = await safe_runner_run(
            resume_scoring_agent,
            evaluation_input
        )

        if not isinstance(resume_evaluation.final_output, FinalOutput):
            raise TypeError("Resume Scoring Coordinator returned wrong type")

        # Return a dictionary with all the serializable data
        return {
            "skills_found": skills_found.model_dump(),
            "experience_score": experience_score.model_dump(),
            "education_score": education_score.model_dump(),
            "scoring": result.model_dump(),
            "evaluation": resume_evaluation.final_output.model_dump()
        }

    except Exception as e:
        print(f"\nError scoring resume: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        raise