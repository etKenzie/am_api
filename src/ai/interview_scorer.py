import asyncio
import json
from typing import List, Optional, Tuple

from agents import Agent
from pydantic import BaseModel, Field

from .resume_scorer import MODEL, safe_runner_run

# --- Shared constants ---

SCORE_RUBRIC = """
RUBRIK SKOR (0.0 - 10.0):
9.0 - 10.0 (Excellent): Metrik terukur, kerangka jelas (STAR), selaras dengan parameter pekerjaan.
7.0 - 8.9 (Good): Menjawab langsung dengan pengalaman personal, kurang data granular.
5.0 - 6.9 (Marginal): Jawaban generik/teoritis ("saya akan..." bukan "saya pernah...").
Di bawah 5.0 (Poor): Menghindar, tidak menjawab, atau kontradiksi teknis.
"""

TRANSCRIPT_GUIDANCE = """
TRANSKRIP SPEECH-TO-TEXT:
- Abaikan filler ("ee", "uh", pengulangan).
- Jika struktur kalimat terfragmentasi karena error transkripsi otomatis, nilai maksud
  semantik dan logika kandidat, BUKAN tata bahasa mekanis.
- Bedakan komunikasi verbal buruk vs kualitas transkripsi buruk.
"""

WEIGHT_RELEVANCE = 0.25
WEIGHT_DEPTH = 0.25
WEIGHT_COMMUNICATION = 0.20
WEIGHT_JOB_FIT = 0.30
RED_FLAG_PENALTY_EACH = 0.5
MAX_RED_FLAG_PENALTY = 1.5


# --- Input / output schemas ---


class InterviewQAItem(BaseModel):
    question_number: int = Field(..., ge=1, description="Nomor urut pertanyaan, dimulai dari 1")
    question: str = Field(..., min_length=1, description="Teks pertanyaan dari pewawancara")
    answer: str = Field(
        default="",
        description="Teks jawaban kandidat (dari transkripsi). Gunakan string kosong jika tidak dijawab.",
    )


class QuestionEvaluation(BaseModel):
    question_number: int
    question: str
    observed_evidence: str = Field(
        description="Bukti nyata atau fakta konkret yang disebutkan kandidat dalam jawaban"
    )
    missing_elements: str = Field(
        description="Informasi penting yang seharusnya ada tetapi tidak disebutkan kandidat"
    )
    relevance_score: float = Field(
        description="Skor relevansi (0.0-10.0). Lihat rubrik."
    )
    depth_score: float = Field(description="Skor kedalaman (0.0-10.0). Lihat rubrik.")
    communication_score: float = Field(
        description="Skor komunikasi/kejelasan maksud (0.0-10.0). Lihat rubrik."
    )
    job_fit_score: float = Field(
        description="Skor kecocokan jawaban dengan posisi (0.0-10.0). Lihat rubrik."
    )
    feedback: str = Field(description="Umpan balik spesifik (Bahasa Indonesia)")
    red_flags: List[str] = Field(default_factory=list)


class SkippedQuestion(BaseModel):
    question_number: int
    question: str
    reason: str = Field(description="Alasan pertanyaan diabaikan (Bahasa Indonesia)")


class QuestionMicroResult(BaseModel):
    """Stage 1 output: skip decision OR per-question evaluation."""

    question_number: int
    question: str
    should_skip: bool
    skip_reason: Optional[str] = None
    evaluation: Optional[QuestionEvaluation] = None


class ConsistencyCheckResult(BaseModel):
    """Stage 2 output: isolated CV vs interview cross-reference."""

    consistency_score: float = Field(
        description="Skor konsistensi keseluruhan (0.0-10.0). 10 = sangat konsisten."
    )
    employment_date_discrepancies: List[str] = Field(default_factory=list)
    skill_exaggerations: List[str] = Field(default_factory=list)
    explicit_contradictions: List[str] = Field(default_factory=list)
    summary: str = Field(description="Ringkasan temuan konsistensi (Bahasa Indonesia)")


class InterviewRecommendation(BaseModel):
    should_proceed: bool
    confidence_level: str
    reasoning: str


class InterviewSynthesisResult(BaseModel):
    """Narrative synthesis only — no numeric scores."""

    strengths: List[str]
    weaknesses: List[str]
    red_flags: List[str]
    summary: str


class ScoreBreakdown(BaseModel):
    relevance_avg: float
    depth_avg: float
    communication_avg: float
    job_fit_avg: float
    base_weighted_score: float
    red_flag_count: int
    red_flag_penalty: float
    final_overall_score: float
    consistency_score: Optional[float] = None


class InterviewScoreResult(BaseModel):
    overall_score: float
    communication_score: float
    relevance_score: float
    depth_score: float
    job_fit_score: float
    consistency_score: Optional[float] = None
    scored_question_count: int
    skipped_questions: List[SkippedQuestion]
    question_evaluations: List[QuestionEvaluation]
    strengths: List[str]
    weaknesses: List[str]
    red_flags: List[str]
    summary: str
    recommendation: InterviewRecommendation
    score_breakdown: ScoreBreakdown
    consistency_details: Optional[ConsistencyCheckResult] = None


# --- Stage 1: per-question micro-evaluation agent ---

question_micro_agent = Agent(
    name="Interview Question Micro-Evaluator",
    instructions=f"""
    Anda mengevaluasi SATU pasangan pertanyaan-jawaban wawancara secara terisolasi.

    {TRANSCRIPT_GUIDANCE}
    {SCORE_RUBRIC}

    TUGAS:
    1. Putuskan apakah item ini harus DIABAIKAN (should_skip=true) atau DINILAI (should_skip=false).

    ABAIKAN (should_skip=true) jika:
    - Pembukaan/ucapan pewawancara tanpa jawaban kandidat substantif
    - Pertanyaan meta kesiapan tanpa jawaban ("Apakah Anda sudah siap?")
    - Bukan pertanyaan penilaian ke kandidat
    - Jawaban kosong/tidak ada untuk pertanyaan intro yang tidak substantif

    NILAI (should_skip=false) jika:
    - Pertanyaan substantif dengan jawaban kandidat (meski lemah/singkat)
    - Pertanyaan intro seperti perkenalan diri JIKA ada jawaban kandidat

    Jika DINILAI:
    - Isi observed_evidence dan missing_elements SEBELUM memberi skor (chain-of-thought)
    - Berikan relevance_score, depth_score, communication_score, job_fit_score
    - JANGAN hitung skor keseluruhan atau rata-rata — hanya skor baris ini
    - Semua teks dalam Bahasa Indonesia

    Jika DIABAIKAN:
    - should_skip=true, skip_reason jelas, evaluation=null
    """,
    model=MODEL,
    output_type=QuestionMicroResult,
)

# --- Stage 2: CV cross-reference agent ---

consistency_agent = Agent(
    name="Interview CV Consistency Checker",
    instructions=f"""
    Anda membandingkan transkrip wawancara dengan CV kandidat secara eksplisit.

    CHECKLIST WAJIB:
    1. Bandingkan tanggal/urutan pengalaman kerja di wawancara vs CV.
    2. Cross-check keahlian/teknis yang diklaim di jawaban vs durasi dan peran di CV.
    3. Identifikasi diskrepansi eksplisit atau exaggeration.

    OUTPUT:
    - consistency_score (0.0-10.0): 10=sangat konsisten, 0=banyak kontradiksi
    - employment_date_discrepancies: daftar ketidaksesuaian timeline
    - skill_exaggerations: klaim berlebihan di wawancara vs CV
    - explicit_contradictions: kontradiksi langsung
    - summary: ringkasan Bahasa Indonesia

    JANGAN hitung skor wawancara lainnya. Fokus hanya konsistensi CV vs wawancara.
    """,
    model=MODEL,
    output_type=ConsistencyCheckResult,
)

# --- Stage 3b: narrative synthesis (scores provided, no math) ---

synthesis_agent = Agent(
    name="Interview Synthesis Writer",
    instructions=f"""
    Anda menulis ringkasan naratif penilaian wawancara berdasarkan data yang DIBERIKAN.

    PENTING:
    - JANGAN menghitung atau mengubah skor — skor sudah dihitung oleh sistem
    - Gunakan skor dan evaluasi per-pertanyaan yang diberikan sebagai dasar
    - Semua output Bahasa Indonesia
    - strengths: kekuatan kandidat yang terbukti dari jawaban
    - weaknesses: kelemahan spesifik
    - red_flags: masalah serius (kontradiksi, jawaban kosong, exaggeration, dll.)
    - summary: ringkasan profesional 2-4 kalimat
    """,
    model=MODEL,
    output_type=InterviewSynthesisResult,
)


# --- Deterministic reducer (Stage 3) ---


def _clamp_score(value: float) -> float:
    return max(0.0, min(10.0, value))


def _normalize_llm_score(raw: float, question_number: int, dimension: str) -> float:
    if raw < 0.0 or raw > 10.0:
        print(
            f"WARNING: Q{question_number} {dimension} out of range ({raw}), clamping"
        )
    return _clamp_score(raw)


def compute_scores(
    evaluations: List[QuestionEvaluation],
    consistency_score: Optional[float] = None,
) -> Tuple[ScoreBreakdown, List[QuestionEvaluation], float, float, float, float, float]:
    if not evaluations:
        breakdown = ScoreBreakdown(
            relevance_avg=0.0,
            depth_avg=0.0,
            communication_avg=0.0,
            job_fit_avg=0.0,
            base_weighted_score=0.0,
            red_flag_count=0,
            red_flag_penalty=0.0,
            final_overall_score=0.0,
            consistency_score=consistency_score,
        )
        return breakdown, [], 0.0, 0.0, 0.0, 0.0, 0.0

    normalized: List[QuestionEvaluation] = []
    for evaluation in evaluations:
        normalized.append(
            QuestionEvaluation(
                question_number=evaluation.question_number,
                question=evaluation.question,
                observed_evidence=evaluation.observed_evidence,
                missing_elements=evaluation.missing_elements,
                relevance_score=_normalize_llm_score(
                    evaluation.relevance_score, evaluation.question_number, "relevance"
                ),
                depth_score=_normalize_llm_score(
                    evaluation.depth_score, evaluation.question_number, "depth"
                ),
                communication_score=_normalize_llm_score(
                    evaluation.communication_score,
                    evaluation.question_number,
                    "communication",
                ),
                job_fit_score=_normalize_llm_score(
                    evaluation.job_fit_score, evaluation.question_number, "job_fit"
                ),
                feedback=evaluation.feedback,
                red_flags=evaluation.red_flags,
            )
        )

    count = len(normalized)
    relevance = sum(e.relevance_score for e in normalized) / count
    depth = sum(e.depth_score for e in normalized) / count
    communication = sum(e.communication_score for e in normalized) / count
    job_fit = sum(e.job_fit_score for e in normalized) / count

    base_score = (
        relevance * WEIGHT_RELEVANCE
        + depth * WEIGHT_DEPTH
        + communication * WEIGHT_COMMUNICATION
        + job_fit * WEIGHT_JOB_FIT
    )

    red_flag_count = sum(len(e.red_flags) for e in normalized)
    red_flag_penalty = min(MAX_RED_FLAG_PENALTY, red_flag_count * RED_FLAG_PENALTY_EACH)
    final_overall = _clamp_score(base_score - red_flag_penalty)

    breakdown = ScoreBreakdown(
        relevance_avg=round(relevance, 2),
        depth_avg=round(depth, 2),
        communication_avg=round(communication, 2),
        job_fit_avg=round(job_fit, 2),
        base_weighted_score=round(base_score, 2),
        red_flag_count=red_flag_count,
        red_flag_penalty=round(red_flag_penalty, 2),
        final_overall_score=round(final_overall, 2),
        consistency_score=consistency_score,
    )
    return (
        breakdown,
        normalized,
        final_overall,
        relevance,
        depth,
        communication,
        job_fit,
    )


def build_recommendation(overall_score: float) -> InterviewRecommendation:
    if overall_score >= 7.0:
        return InterviewRecommendation(
            should_proceed=True,
            confidence_level="Tinggi",
            reasoning=(
                f"Skor keseluruhan {overall_score:.1f}/10 menunjukkan kandidat memenuhi "
                "ekspektasi untuk lanjut ke tahap berikutnya."
            ),
        )
    if overall_score >= 5.5:
        return InterviewRecommendation(
            should_proceed=True,
            confidence_level="Sedang",
            reasoning=(
                f"Skor keseluruhan {overall_score:.1f}/10 cukup untuk dipertimbangkan, "
                "dengan beberapa area yang perlu dievaluasi lebih lanjut."
            ),
        )
    if overall_score >= 4.0:
        return InterviewRecommendation(
            should_proceed=False,
            confidence_level="Sedang",
            reasoning=(
                f"Skor keseluruhan {overall_score:.1f}/10 di bawah ambang rekomendasi kuat. "
                "Kandidat belum menunjukkan kecocokan yang memadai."
            ),
        )
    return InterviewRecommendation(
        should_proceed=False,
        confidence_level="Tinggi",
        reasoning=(
            f"Skor keseluruhan {overall_score:.1f}/10 terlalu rendah untuk dilanjutkan."
        ),
    )


# --- Pipeline stages ---


async def _evaluate_question(
    qa: InterviewQAItem,
    job_description: str,
    job_title: Optional[str],
    target_skills: List[str],
) -> QuestionMicroResult:
    payload = {
        "question_number": qa.question_number,
        "question": qa.question,
        "answer": qa.answer if qa.answer.strip() else "(Tidak ada jawaban)",
        "job_title": job_title,
        "job_description": job_description,
        "target_skills": target_skills,
    }
    payload = {k: v for k, v in payload.items() if v is not None}

    result = await safe_runner_run(
        question_micro_agent, json.dumps(payload, ensure_ascii=False)
    )
    if not isinstance(result.final_output, QuestionMicroResult):
        raise TypeError("Question Micro-Evaluator returned wrong type")
    return result.final_output


async def _check_consistency(
    resume_text: str,
    evaluations: List[QuestionEvaluation],
    job_description: str,
) -> ConsistencyCheckResult:
    interview_claims = [
        {
            "question_number": e.question_number,
            "question": e.question,
            "observed_evidence": e.observed_evidence,
            "red_flags": e.red_flags,
        }
        for e in evaluations
    ]
    payload = {
        "resume_text": resume_text,
        "interview_claims": interview_claims,
        "job_description": job_description,
    }
    result = await safe_runner_run(
        consistency_agent, json.dumps(payload, ensure_ascii=False)
    )
    if not isinstance(result.final_output, ConsistencyCheckResult):
        raise TypeError("Consistency Checker returned wrong type")
    return result.final_output


async def _synthesize_narrative(
    evaluations: List[QuestionEvaluation],
    skipped: List[SkippedQuestion],
    breakdown: ScoreBreakdown,
    consistency: Optional[ConsistencyCheckResult],
    job_title: Optional[str],
    job_description: str,
) -> InterviewSynthesisResult:
    payload = {
        "job_title": job_title,
        "job_description": job_description,
        "computed_scores": breakdown.model_dump(),
        "question_evaluations": [e.model_dump() for e in evaluations],
        "skipped_questions": [s.model_dump() for s in skipped],
        "consistency_check": consistency.model_dump() if consistency else None,
    }
    payload = {k: v for k, v in payload.items() if v is not None}

    result = await safe_runner_run(
        synthesis_agent, json.dumps(payload, ensure_ascii=False)
    )
    if not isinstance(result.final_output, InterviewSynthesisResult):
        raise TypeError("Synthesis Agent returned wrong type")
    return result.final_output


async def score_interview(
    qa_pairs: List[InterviewQAItem],
    job_description: str,
    job_title: Optional[str] = None,
    resume_text: Optional[str] = None,
    target_skills: Optional[List[str]] = None,
) -> dict:
    """
    3-stage interview scoring pipeline:
    1. Parallel per-question micro-evaluation (map)
    2. Isolated CV consistency check (optional)
    3. Deterministic Python score aggregation (reduce) + narrative synthesis
    """
    if not qa_pairs:
        raise ValueError("qa_pairs wajib diisi minimal 1 pertanyaan")

    skills = target_skills or []

    # Stage 1: parallel micro-evaluations
    micro_results = await asyncio.gather(
        *(_evaluate_question(qa, job_description, job_title, skills) for qa in qa_pairs)
    )

    skipped: List[SkippedQuestion] = []
    evaluations: List[QuestionEvaluation] = []

    for micro in micro_results:
        if micro.should_skip:
            skipped.append(
                SkippedQuestion(
                    question_number=micro.question_number,
                    question=micro.question,
                    reason=micro.skip_reason or "Pertanyaan diabaikan dari penilaian",
                )
            )
        elif micro.evaluation:
            evaluations.append(micro.evaluation)
        else:
            skipped.append(
                SkippedQuestion(
                    question_number=micro.question_number,
                    question=micro.question,
                    reason="Tidak ada evaluasi yang dihasilkan untuk pertanyaan ini",
                )
            )

    # Stage 2: CV cross-reference (optional)
    consistency: Optional[ConsistencyCheckResult] = None
    consistency_score: Optional[float] = None
    if resume_text and resume_text.strip() and evaluations:
        consistency = await _check_consistency(
            resume_text, evaluations, job_description
        )
        consistency_score = _normalize_llm_score(
            consistency.consistency_score, 0, "consistency"
        )

    # Stage 3: deterministic reducer
    (
        breakdown,
        evaluations,
        overall_score,
        relevance,
        depth,
        communication,
        job_fit,
    ) = compute_scores(evaluations, consistency_score)

    # Narrative synthesis (no score math)
    synthesis = await _synthesize_narrative(
        evaluations, skipped, breakdown, consistency, job_title, job_description
    )

    # Merge red flags from all sources
    all_red_flags = list(synthesis.red_flags)
    for evaluation in evaluations:
        all_red_flags.extend(evaluation.red_flags)
    if consistency:
        all_red_flags.extend(consistency.explicit_contradictions)
        all_red_flags.extend(consistency.skill_exaggerations)
    all_red_flags = list(dict.fromkeys(all_red_flags))  # dedupe, preserve order

    recommendation = build_recommendation(overall_score)

    result = InterviewScoreResult(
        overall_score=overall_score,
        communication_score=round(communication, 2),
        relevance_score=round(relevance, 2),
        depth_score=round(depth, 2),
        job_fit_score=round(job_fit, 2),
        consistency_score=consistency_score,
        scored_question_count=len(evaluations),
        skipped_questions=skipped,
        question_evaluations=evaluations,
        strengths=synthesis.strengths,
        weaknesses=synthesis.weaknesses,
        red_flags=all_red_flags,
        summary=synthesis.summary,
        recommendation=recommendation,
        score_breakdown=breakdown,
        consistency_details=consistency,
    )

    return {"scoring": result.model_dump()}
