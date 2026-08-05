import asyncio
import json
from typing import Dict, List, Optional, Tuple

from agents import Agent
from pydantic import BaseModel, Field

from .resume_scorer import MODEL, safe_runner_run

# --- Shared constants ---

SCORE_RUBRIC = """
RUBRIK SKOR (0.0 - 10.0):
9.0 - 10.0 (Excellent): Bukti kuat, spesifik, konsisten dengan aspek yang dinilai.
7.0 - 8.9 (Good): Menunjukkan aspek dengan contoh personal yang cukup jelas.
5.0 - 6.9 (Marginal): Jawaban generik/teoritis, bukti lemah atau tidak konsisten.
Di bawah 5.0 (Poor): Tidak menunjukkan aspek, menghindar, atau kontradiksi.
"""

TRANSCRIPT_GUIDANCE = """
TRANSKRIP SPEECH-TO-TEXT:
- Abaikan filler ("ee", "uh", pengulangan).
- Jika struktur kalimat terfragmentasi karena error transkripsi otomatis, nilai maksud
  semantik dan logika kandidat, BUKAN tata bahasa mekanis.
- Bedakan komunikasi verbal buruk vs kualitas transkripsi buruk.
"""

# Aspek penilaian (dari form "ASPEK YANG DINILAI") + bobot.
# Sikap & Penampilan dihapus: penilaian berbasis transkrip, tanpa video.
# Bobot: teknis + analisa paling berat.
ASPEK_DEFINITIONS: Dict[str, Dict[str, object]] = {
    "komunikasi": {
        "label": "Komunikasi",
        "weight": 0.18,
        "description": (
            "Cara kandidat mengungkapkan pikiran dan perasaannya secara lisan: "
            "kejelasan, struktur, kelancaran menyampaikan ide."
        ),
    },
    "analisa_logika": {
        "label": "Analisa & Logika",
        "weight": 0.20,
        "description": (
            "Kemampuan menganalisa dan menyimpulkan masalah dengan melihat "
            "faktor sebab-akibat sehingga kesimpulan masuk akal."
        ),
    },
    "kemampuan_teknis": {
        "label": "Kemampuan Teknis Dibidangnya",
        "weight": 0.22,
        "description": (
            "Pengetahuan dan keterampilan yang dimiliki kandidat sesuai "
            "dengan posisi yang dilamar."
        ),
    },
    "motivasi_kerja": {
        "label": "Motivasi Kerja",
        "weight": 0.15,
        "description": (
            "Besarnya dorongan dari dalam diri untuk mencapai suatu tujuan tertentu: "
            "semangat, alasan melamar, komitmen, ketekunan."
        ),
    },
    "wawasan_berpikir": {
        "label": "Wawasan Berpikir",
        "weight": 0.13,
        "description": (
            "Luasnya pengetahuan dan cara pandang kandidat dalam menghadapi masalah: "
            "perspektif, konteks industri, pemikiran terbuka."
        ),
    },
    "potensi_berkembang": {
        "label": "Potensi untuk Berkembang",
        "weight": 0.12,
        "description": (
            "Kemampuan dan keinginan pribadi untuk mengembangkan diri dengan "
            "mempelajari hal baru yang belum pernah diperoleh."
        ),
    },
}

ASPEK_KEYS = list(ASPEK_DEFINITIONS.keys())
WEIGHTS = {k: float(v["weight"]) for k, v in ASPEK_DEFINITIONS.items()}
assert abs(sum(WEIGHTS.values()) - 1.0) < 1e-9

RED_FLAG_PENALTY_EACH = 0.5
MAX_RED_FLAG_PENALTY = 1.5
# Calibration agents may nudge preliminary averages within this band.
MAX_CATEGORY_ADJUSTMENT = 1.5


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
    komunikasi_score: float = Field(description="Skor Komunikasi (0.0-10.0)")
    analisa_logika_score: float = Field(description="Skor Analisa & Logika (0.0-10.0)")
    kemampuan_teknis_score: float = Field(
        description="Skor Kemampuan Teknis Dibidangnya (0.0-10.0)"
    )
    motivasi_kerja_score: float = Field(description="Skor Motivasi Kerja (0.0-10.0)")
    wawasan_berpikir_score: float = Field(description="Skor Wawasan Berpikir (0.0-10.0)")
    potensi_berkembang_score: float = Field(
        description="Skor Potensi untuk Berkembang (0.0-10.0)"
    )
    feedback: str = Field(
        description="Umpan balik singkat 1-2 kalimat (Bahasa Indonesia), to the point"
    )
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


class CategoryCalibrationResult(BaseModel):
    """Stage 3b: specialist agent adjusts one aspek score."""

    category_key: str
    preliminary_score: float
    adjusted_score: float = Field(description="Skor akhir aspek setelah kalibrasi (0.0-10.0)")
    adjustment_delta: float = Field(
        description="Selisih adjusted - preliminary (boleh 0 jika tidak diubah)"
    )
    justification: str = Field(
        description="Alasan penyesuaian 1-2 kalimat maks, to the point (Bahasa Indonesia)"
    )
    evidence_highlights: List[str] = Field(
        default_factory=list,
        description="Maks 2 highlight singkat (frasa/kalimat pendek)",
    )


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


class CategoryScoreDetail(BaseModel):
    key: str
    label: str
    weight: float
    preliminary_avg: float
    adjusted_score: float
    adjustment_delta: float
    justification: str
    evidence_highlights: List[str] = Field(default_factory=list)


class ScoreBreakdown(BaseModel):
    category_scores: List[CategoryScoreDetail]
    base_weighted_score: float
    red_flag_count: int
    red_flag_penalty: float
    final_overall_score: float
    consistency_score: Optional[float] = None
    weights: Dict[str, float]


class InterviewScoreResult(BaseModel):
    overall_score: float
    komunikasi_score: float
    analisa_logika_score: float
    kemampuan_teknis_score: float
    motivasi_kerja_score: float
    wawasan_berpikir_score: float
    potensi_berkembang_score: float
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


def _aspek_rubric_block() -> str:
    lines = ["ASPEK YANG DINILAI (beri skor 0.0-10.0 untuk SETIAP aspek):"]
    for key, meta in ASPEK_DEFINITIONS.items():
        lines.append(
            f"- {key} ({meta['label']}, bobot {float(meta['weight']) * 100:.0f}%): "
            f"{meta['description']}"
        )
    return "\n".join(lines)


# --- Stage 1: per-question micro-evaluation agent ---

question_micro_agent = Agent(
    name="Interview Question Micro-Evaluator",
    instructions=f"""
    Anda mengevaluasi SATU pasangan pertanyaan-jawaban wawancara secara terisolasi.

    {TRANSCRIPT_GUIDANCE}
    {SCORE_RUBRIC}

    {_aspek_rubric_block()}

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
    - Isi observed_evidence dan missing_elements SEBELUM memberi skor (masing-masing 1-2 kalimat)
    - Berikan keenam skor aspek: komunikasi, analisa_logika, kemampuan_teknis,
      motivasi_kerja, wawasan_berpikir, potensi_berkembang
    - Jika suatu aspek kurang relevan untuk pertanyaan ini, beri skor netral ~5.0-6.0
      berdasarkan sinyal yang ada; jangan mengarang bukti
    - feedback: 1-2 kalimat, to the point
    - JANGAN hitung skor keseluruhan atau rata-rata — hanya skor baris ini
    - Semua teks dalam Bahasa Indonesia

    Jika DIABAIKAN:
    - should_skip=true, skip_reason singkat (1 kalimat), evaluation=null
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


def _build_category_calibration_agent(category_key: str) -> Agent:
    meta = ASPEK_DEFINITIONS[category_key]
    label = meta["label"]
    description = meta["description"]
    weight_pct = float(meta["weight"]) * 100

    return Agent(
        name=f"Interview Aspek Calibrator: {label}",
        instructions=f"""
        Anda adalah spesialis penilaian aspek "{label}" untuk wawancara kerja.

        DEFINISI ASPEK:
        {description}

        Bobot aspek ini dalam skor keseluruhan: {weight_pct:.0f}%.

        {TRANSCRIPT_GUIDANCE}
        {SCORE_RUBRIC}

        INPUT berisi:
        - preliminary_score: rata-rata skor aspek dari evaluasi per-pertanyaan
        - evidence: ringkasan bukti per jawaban
        - job_title / job_description / target_skills
        - consistency_summary (jika ada)

        TUGAS:
        1. Tinjau apakah preliminary_score sudah adil untuk aspek "{label}".
        2. Sesuaikan adjusted_score jika perlu (naik/turun), maksimal ±{MAX_CATEGORY_ADJUSTMENT}
           dari preliminary_score.
        3. Jika sudah tepat, set adjusted_score = preliminary_score dan adjustment_delta = 0.
        4. category_key HARUS "{category_key}".
        5. justification: WAJIB 1-2 kalimat maks, to the point, Bahasa Indonesia.
           Jangan bertele-tele; sebutkan alasan utama saja.
        6. evidence_highlights: maks 2 item, masing-masing frasa singkat.
        7. JANGAN menilai aspek lain. JANGAN menghitung skor keseluruhan.
        """,
        model=MODEL,
        output_type=CategoryCalibrationResult,
    )


category_calibration_agents: Dict[str, Agent] = {
    key: _build_category_calibration_agent(key) for key in ASPEK_KEYS
}

# --- Stage 3c: narrative synthesis (scores provided, no math) ---

synthesis_agent = Agent(
    name="Interview Synthesis Writer",
    instructions=f"""
    Anda menulis ringkasan naratif penilaian wawancara berdasarkan data yang DIBERIKAN.

    PENTING:
    - JANGAN menghitung atau mengubah skor — skor sudah dihitung oleh sistem
    - Gunakan skor aspek dan evaluasi per-pertanyaan yang diberikan sebagai dasar
    - Semua output Bahasa Indonesia
    - strengths / weaknesses: masing-masing item 1 kalimat, to the point
    - red_flags: masalah serius (kontradiksi, jawaban kosong, exaggeration, dll.)
    - summary: ringkasan profesional 2-3 kalimat
    """,
    model=MODEL,
    output_type=InterviewSynthesisResult,
)


# --- Deterministic reducer ---


def _clamp_score(value: float) -> float:
    return max(0.0, min(10.0, value))


def _normalize_llm_score(raw: float, question_number: int, dimension: str) -> float:
    if raw < 0.0 or raw > 10.0:
        print(
            f"WARNING: Q{question_number} {dimension} out of range ({raw}), clamping"
        )
    return _clamp_score(raw)


def _score_attr(key: str) -> str:
    return f"{key}_score"


def average_category_scores(
    evaluations: List[QuestionEvaluation],
) -> Dict[str, float]:
    if not evaluations:
        return {key: 0.0 for key in ASPEK_KEYS}

    count = len(evaluations)
    return {
        key: sum(getattr(e, _score_attr(key)) for e in evaluations) / count
        for key in ASPEK_KEYS
    }


def normalize_evaluations(
    evaluations: List[QuestionEvaluation],
) -> List[QuestionEvaluation]:
    normalized: List[QuestionEvaluation] = []
    for evaluation in evaluations:
        kwargs = {
            "question_number": evaluation.question_number,
            "question": evaluation.question,
            "observed_evidence": evaluation.observed_evidence,
            "missing_elements": evaluation.missing_elements,
            "feedback": evaluation.feedback,
            "red_flags": evaluation.red_flags,
        }
        for key in ASPEK_KEYS:
            attr = _score_attr(key)
            kwargs[attr] = _normalize_llm_score(
                getattr(evaluation, attr), evaluation.question_number, key
            )
        normalized.append(QuestionEvaluation(**kwargs))
    return normalized


def apply_category_calibrations(
    preliminary: Dict[str, float],
    calibrations: List[CategoryCalibrationResult],
) -> Dict[str, CategoryScoreDetail]:
    by_key = {c.category_key: c for c in calibrations}
    details: Dict[str, CategoryScoreDetail] = {}

    for key in ASPEK_KEYS:
        meta = ASPEK_DEFINITIONS[key]
        prelim = preliminary.get(key, 0.0)
        cal = by_key.get(key)

        if cal is None:
            adjusted = prelim
            delta = 0.0
            justification = "Tidak ada kalibrasi aspek; memakai rata-rata per-pertanyaan."
            highlights: List[str] = []
        else:
            # Enforce band around preliminary so agents can't wildly rewrite scores.
            capped = max(
                prelim - MAX_CATEGORY_ADJUSTMENT,
                min(prelim + MAX_CATEGORY_ADJUSTMENT, cal.adjusted_score),
            )
            adjusted = _clamp_score(capped)
            delta = round(adjusted - prelim, 2)
            justification = cal.justification
            highlights = cal.evidence_highlights

        details[key] = CategoryScoreDetail(
            key=key,
            label=str(meta["label"]),
            weight=float(meta["weight"]),
            preliminary_avg=round(prelim, 2),
            adjusted_score=round(adjusted, 2),
            adjustment_delta=delta,
            justification=justification,
            evidence_highlights=highlights,
        )

    return details


def compute_scores(
    evaluations: List[QuestionEvaluation],
    category_details: Dict[str, CategoryScoreDetail],
    consistency_score: Optional[float] = None,
) -> Tuple[ScoreBreakdown, List[QuestionEvaluation], float, Dict[str, float]]:
    if not evaluations:
        empty_details = [
            CategoryScoreDetail(
                key=key,
                label=str(meta["label"]),
                weight=float(meta["weight"]),
                preliminary_avg=0.0,
                adjusted_score=0.0,
                adjustment_delta=0.0,
                justification="Tidak ada pertanyaan yang dinilai.",
            )
            for key, meta in ASPEK_DEFINITIONS.items()
        ]
        breakdown = ScoreBreakdown(
            category_scores=empty_details,
            base_weighted_score=0.0,
            red_flag_count=0,
            red_flag_penalty=0.0,
            final_overall_score=0.0,
            consistency_score=consistency_score,
            weights=dict(WEIGHTS),
        )
        return breakdown, [], 0.0, {key: 0.0 for key in ASPEK_KEYS}

    normalized = normalize_evaluations(evaluations)
    adjusted_scores = {
        key: category_details[key].adjusted_score for key in ASPEK_KEYS
    }

    base_score = sum(adjusted_scores[key] * WEIGHTS[key] for key in ASPEK_KEYS)

    red_flag_count = sum(len(e.red_flags) for e in normalized)
    red_flag_penalty = min(MAX_RED_FLAG_PENALTY, red_flag_count * RED_FLAG_PENALTY_EACH)
    final_overall = _clamp_score(base_score - red_flag_penalty)

    breakdown = ScoreBreakdown(
        category_scores=[category_details[key] for key in ASPEK_KEYS],
        base_weighted_score=round(base_score, 2),
        red_flag_count=red_flag_count,
        red_flag_penalty=round(red_flag_penalty, 2),
        final_overall_score=round(final_overall, 2),
        consistency_score=consistency_score,
        weights=dict(WEIGHTS),
    )
    return breakdown, normalized, final_overall, adjusted_scores


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


async def _calibrate_category(
    category_key: str,
    preliminary_score: float,
    evaluations: List[QuestionEvaluation],
    job_description: str,
    job_title: Optional[str],
    target_skills: List[str],
    consistency: Optional[ConsistencyCheckResult],
) -> CategoryCalibrationResult:
    evidence = [
        {
            "question_number": e.question_number,
            "question": e.question,
            "observed_evidence": e.observed_evidence,
            "missing_elements": e.missing_elements,
            "category_score": getattr(e, _score_attr(category_key)),
            "feedback": e.feedback,
        }
        for e in evaluations
    ]
    meta = ASPEK_DEFINITIONS[category_key]
    payload = {
        "category_key": category_key,
        "category_label": meta["label"],
        "category_description": meta["description"],
        "weight": meta["weight"],
        "preliminary_score": round(preliminary_score, 2),
        "max_adjustment": MAX_CATEGORY_ADJUSTMENT,
        "job_title": job_title,
        "job_description": job_description,
        "target_skills": target_skills,
        "evidence": evidence,
        "consistency_summary": consistency.summary if consistency else None,
    }
    payload = {k: v for k, v in payload.items() if v is not None}

    agent = category_calibration_agents[category_key]
    result = await safe_runner_run(agent, json.dumps(payload, ensure_ascii=False))
    if not isinstance(result.final_output, CategoryCalibrationResult):
        raise TypeError(f"Category calibrator {category_key} returned wrong type")

    output = result.final_output
    # Ensure key is correct even if the model mislabels it.
    return CategoryCalibrationResult(
        category_key=category_key,
        preliminary_score=preliminary_score,
        adjusted_score=_clamp_score(output.adjusted_score),
        adjustment_delta=round(output.adjusted_score - preliminary_score, 2),
        justification=output.justification,
        evidence_highlights=output.evidence_highlights,
    )


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
    Interview scoring pipeline against 6 aspek penilaian:
    1. Parallel per-question micro-evaluation (6 category scores each)
    2. Optional CV consistency check
    3. Parallel per-aspek calibration agents (adjust preliminary averages)
    4. Deterministic weighted aggregation + narrative synthesis
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

    evaluations = normalize_evaluations(evaluations)

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

    # Stage 3a: preliminary averages
    preliminary = average_category_scores(evaluations)

    # Stage 3b: parallel aspek calibration agents
    calibrations: List[CategoryCalibrationResult] = []
    if evaluations:
        calibrations = list(
            await asyncio.gather(
                *(
                    _calibrate_category(
                        key,
                        preliminary[key],
                        evaluations,
                        job_description,
                        job_title,
                        skills,
                        consistency,
                    )
                    for key in ASPEK_KEYS
                )
            )
        )

    category_details = apply_category_calibrations(preliminary, calibrations)

    # Stage 3c: deterministic weighted reduce
    breakdown, evaluations, overall_score, adjusted = compute_scores(
        evaluations, category_details, consistency_score
    )

    # Narrative synthesis (no score math)
    synthesis = await _synthesize_narrative(
        evaluations, skipped, breakdown, consistency, job_title, job_description
    )

    all_red_flags = list(synthesis.red_flags)
    for evaluation in evaluations:
        all_red_flags.extend(evaluation.red_flags)
    if consistency:
        all_red_flags.extend(consistency.explicit_contradictions)
        all_red_flags.extend(consistency.skill_exaggerations)
    all_red_flags = list(dict.fromkeys(all_red_flags))

    recommendation = build_recommendation(overall_score)

    result = InterviewScoreResult(
        overall_score=round(overall_score, 2),
        komunikasi_score=adjusted["komunikasi"],
        analisa_logika_score=adjusted["analisa_logika"],
        kemampuan_teknis_score=adjusted["kemampuan_teknis"],
        motivasi_kerja_score=adjusted["motivasi_kerja"],
        wawasan_berpikir_score=adjusted["wawasan_berpikir"],
        potensi_berkembang_score=adjusted["potensi_berkembang"],
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
