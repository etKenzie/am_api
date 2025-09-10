import asyncio
import json
import sys
from datetime import datetime
from dataclasses import dataclass
from typing import List
from pydantic import BaseModel, Field
from openai import AsyncOpenAI
import os
import agents
from agents import Agent, Runner, function_tool, RunContextWrapper

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
    experience_breakdown: str = Field(description="Detailed breakdown of experience scoring")

class EducationScore(BaseModel):
    education_score: float = Field(description="Score for education match (0.0 to 1.0)")
    degree_match: str = Field(description="How well the degree matches requirements")
    certifications: List[str] = Field(description="List of relevant certifications")
    education_breakdown: str = Field(description="Detailed breakdown of education scoring")

class ResumeScore(BaseModel):
    overall_score: float = Field(description="Overall score out of 10")
    skill_score: float = Field(description="Score for skills match (0.0 to 4.0)")
    experience_score: float = Field(description="Score for experience match (0.0 to 4.5)")
    education_score: float = Field(description="Score for education match (0.0 to 1.0)")
    strengths: List[str] = Field(description="List of candidate's strengths")
    weaknesses: List[str] = Field(description="List of candidate's weaknesses")
    breakdown: str = Field(description="Detailed breakdown of final scoring")
    summary: str = Field(description="Overall summary of the candidate's fit")

class FinalOutput(BaseModel):
    reasoning: str = Field(description="A reasoning of why the evaluation makes sense or not")
    score: ResumeScore 

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
    4. Calculate a skill score based on the following methodology:
       - Required skills: 2 points per match
       - Preferred skills: 1 point per match
       - Missing required skills: -3 points per miss
       - Calculate as: (Total skill points / Max possible skill points) * 4.0
       - Max Points: 4.0
    5. Return detailed information about skill matches including context and statistics
    
    ANALYSIS APPROACH:
    - Look for skills in multiple sections: experience, education, projects, certifications
    - Consider variations and synonyms of skill names (e.g., "Python" vs "Python programming")
    - Check for skill mentions in experience, project descriptions, and technical sections
    - Be thorough but accurate - only include skills that are clearly demonstrated
    
    SKILL DETECTION RULES:(SKILLS CAN BE FOUND OTHER THAN EXPLICITLY STATING THEM)
    1. EXACT MATCHES: Direct mentions of the skill name
    2. SYNONYMS: Common variations (e.g., "JS" for "JavaScript", "React.js" for "React")
    3. CONTEXT CLUES: Skills implied through project descriptions, experience, or job responsibilities
    4. CERTIFICATIONS: Skills mentioned in certification names or descriptions
    5. 
    
    OUTPUT REQUIREMENTS:
    - skills_found: List of skills that are actually present in the resume
    - total_skills_checked: Count of all target skills that were evaluated
    - match_percentage: Calculate as (len(skills_found) / total_skills_checked) with 2 decimal places
    - skill_context: For each found skill, provide brief context like "Found in work experience as Senior Developer" or "Mentioned in project description"
    - skill_score: Calculated score based on the methodology above (0.0 to 4.0)
    
    IMPORTANT: 
    - Be precise and conservative. Only include skills that are clearly demonstrated
    - Provide meaningful context for each skill found
    - Calculate match percentage accurately (0.0 to 1.0 scale)
    - Calculate skill score using the exact methodology specified
    - Follow the SkillsFound schema exactly with all required fields
    - Handle cases where target_skills might be empty or malformed
    """,
    # tools=[extract_text_from_pdf, read_text_file],
    output_type=SkillsFound,
    model=MODEL
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
    model=MODEL
)

experience_scoring_agent = Agent(
    name="Experience Scoring Specialist",
    instructions="""
    You are an experience scoring specialist. Your task is to evaluate the candidate's work experience against job requirements.
    If no expereince listed determine by yourself based on the job title how relevant the work is. Be strict on how relevant a candidates expereice is to their job description
    
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
    
    CALCULATION RULES:
    - Start with base score of 0.0
    - Add points according to methodology above
    - Cap total score at 4.5
    - Use precise decimals (e.g., 3.2, 4.1)
    
    OUTPUT REQUIREMENTS:
    - experience_score: Final calculated score (0.0 to 4.5)
    - years_experience: Total years of relevant experience
    - relevant_roles: List of relevant job titles/roles found
    - experience_breakdown: Detailed explanation of scoring
    
    Make sure to follow the exact schema provided in the ExperienceScore model.
    """,
    output_type=ExperienceScore,
    model=MODEL
)

education_scoring_agent = Agent(
    name="Education Scoring Specialist",
    instructions="""
    You are an education scoring specialist. Your task is to evaluate the candidate's educational background against job requirements. If there is no education requirement listed 
    make a decision based on the job description if the education is relevant
    
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
    
    CALCULATION RULES:
    - Start with base score of 0.0
    - Add points according to methodology above
    - Cap total score at 1.0
    - Use precise decimals (e.g., 0.8, 0.6)
    
    OUTPUT REQUIREMENTS:
    - education_score: Final calculated score (0.0 to 1.0)
    - degree_match: Description of how well the degree matches requirements
    - certifications: List of relevant certifications found
    - education_breakdown: Detailed explanation of scoring
    
    Make sure to follow the exact schema provided in the EducationScore model.
    """,
    output_type=EducationScore,
    model=MODEL
)

final_scoring_agent = Agent(
    name="Final Scoring Coordinator",
    instructions="""
    You are a final scoring coordinator. Your task is to combine all individual scores and provide a comprehensive evaluation.
    
    INPUT:
    You will receive scores from:
    - Skill Extractor Agent (skill_score: 0.0 to 4.0)
    - Experience Scoring Agent (experience_score: 0.0 to 4.5)
    - Education Scoring Agent (education_score: 0.0 to 1.0)
    - Resume data and job requirements for context
    
    FINAL SCORING METHODOLOGY:
    - Overall Score = skill_score + experience_score + education_score + other_factors
    - Other factors (0.0 to 0.5): Projects, achievements, additional qualifications
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
    - strengths: List of candidate's key strengths
    - weaknesses: List of candidate's key weaknesses
    - breakdown: Detailed explanation of final scoring
    - summary: Overall assessment of candidate fit
    
    Make sure to follow the exact schema provided in the ResumeScore model.
    """,
    output_type=ResumeScore,
    model=MODEL
)

resume_scoring_agent = Agent(
    name="Resume Scoring Checker",
    instructions="""
    You are a resume scoring auditor. Your tasks:
    
    1. VALIDATE SCORING LOGIC:
       - Verify scores follow the defined methodology
       - Check for appropriate decimal precision
       - Ensure irrelevant resumes get scores <4.0
    
    2. ANALYZE JOB FIT:
       - Recalculate key match percentages
       - Flag any generous scoring
       - Identify critical missing requirements
    
    3. ADJUST SCORES IF NEEDED:
       - Reduce inflated scores with justification
       - Add penalty points for major mismatches
    
    4. FINAL OUTPUT:
       - Provide detailed reasoning in STRICT INDONESIAN
       - Include exact mismatch percentages
       - Final score should accurately represent the candidate
    
    IMPORTANT: Be extremely critical of mismatches. If the job description is irrelevant, the score MUST be low and STRICTLY GIVE THE RESULT BACK IN INDONESIAN.
    """,
    model=MODEL,
    output_type=FinalOutput,
)

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