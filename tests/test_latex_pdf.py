from __future__ import annotations

import subprocess

from feishu_agent_bot.reporting.latex_renderer import (
    LaTeXRenderer,
    TEMPLATE_VERSION,
)
from feishu_agent_bot.reporting.pdf_compiler import PDFCompiler
from feishu_agent_bot.reporting.report_ir import ReportIR, ReportSection


def test_latex_renderer_uses_fixed_template_escapes_input_and_builds_references():
    ir = ReportIR(
        job_id="J1",
        report_id="R1",
        title=r"价格_分析 \\input{forbidden-input} & 100%",
        sections=[
            ReportSection(
                "S1",
                "结论",
                r"正文 \\write18{blocked-command}",
                table_ids=["T1"],
                chart_ids=["chart_1"],
                evidence_ids=["E1"],
            )
        ],
        tables=[
            {
                "table_id": "T1",
                "columns": ["产品", "价格"],
                "rows": [{"产品": "A&B", "价格": 99}],
            }
        ],
        charts=[
            {
                "chart_id": "chart_1",
                "title": "价格趋势",
                "unit": "CNY",
            }
        ],
        evidence_references=[{"evidence_id": "E1", "source_id": "SRC1"}],
        sources=[
            {
                "source_id": "SRC1",
                "title": "官方_资料",
                "canonical_url": "https://example.com/a_b",
            }
        ],
        limitations=["样本 < 10"],
    )

    renderer = LaTeXRenderer()
    latex = renderer.render(ir)
    bibliography = renderer.render_bibliography(ir)

    assert r"\tableofcontents" in latex
    assert r"\documentclass[11pt,a4paper,UTF8,fontset=fandol]{ctexart}" in latex
    assert r"\usepackage{fontspec}" not in latex
    assert r"\renewcommand{\figurename}{图}" in latex
    assert r"\begin{longtable}" in latex
    assert r"charts/chart_1.png" in latex
    assert r"\ref{fig:chart_1}" in latex
    assert r"\label{fig:chart_1}" in latex
    assert r"\textbackslash{}input\{" in latex
    assert r"\textbackslash{}write18\{" in latex
    assert r"\input{" not in latex
    assert r"\write18{" not in latex
    assert TEMPLATE_VERSION.replace("_", r"\_") in latex
    assert "@misc{src_SRC1_" in bibliography
    assert r"title = {官方\_资料}" in bibliography
    assert r"\url{https://example.com/a_b}" in bibliography


def test_bibliography_escapes_real_world_title_special_characters():
    ir = ReportIR(
        job_id="J1",
        report_id="R1",
        title="AI短视频发展趋势",
        sections=[ReportSection("S1", "结论", "正文", evidence_ids=["E1"])],
        evidence_references=[{"evidence_id": "E1", "source_id": "SRC1"}],
        sources=[
            {
                "source_id": "SRC1",
                "title": (
                    "2026年AI视频生成工具实测：10款主流工具深度对比与选型指南_"
                    "人工智能_johnyjohny 100% A&B #1 $price ~trend ^growth"
                ),
                "canonical_url": "https://example.com/a_b?q=x&v=1",
            }
        ],
    )

    bibliography = LaTeXRenderer().render_bibliography(ir)

    assert r"指南\_人工智能\_johnyjohny" in bibliography
    assert r"100\% A\&B \#1 \$price" in bibliography
    assert r"\textasciitilde{}trend \textasciicircum{}growth" in bibliography


def test_pdf_compiler_command_sequence_disables_shell_escape():
    commands = PDFCompiler._commands(has_bibliography=True)

    assert [command[0] for command in commands[:4]] == [
        "xelatex",
        "xelatex",
        "bibtex",
        "xelatex",
    ]
    assert commands[-1][0] == "latexmk"
    assert all(
        "-no-shell-escape" in command
        for command in commands
        if command[0] in {"xelatex", "latexmk"}
    )
    assert all("-shell-escape" not in command for command in commands)


def test_pdf_compile_failure_keeps_existing_pdf_and_persists_log(
    tmp_path, monkeypatch
):
    target = tmp_path / "report.pdf"
    target.write_bytes(b"old-pdf")
    monkeypatch.setattr("shutil.which", lambda command: f"/usr/bin/{command}")

    def fail(*args, **kwargs):
        raise subprocess.CalledProcessError(
            1, args[0], output=b"! Emergency stop. controlled failure"
        )

    monkeypatch.setattr(subprocess, "run", fail)

    artifact = PDFCompiler().compile(
        r"\documentclass{article}\begin{document}x\end{document}", target
    )

    assert artifact.status == "failed"
    assert target.read_bytes() == b"old-pdf"
    assert "controlled failure" in target.with_suffix(".log").read_text(
        encoding="utf-8"
    )
