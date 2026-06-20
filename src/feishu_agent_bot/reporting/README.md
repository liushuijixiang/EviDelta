# Reporting

The reporting package turns validated claims, evidence, analysis results,
tables, and charts into versioned artifacts.

Main components:

- `ir_builder.py` builds the report intermediate representation.
- `chart_renderer.py` writes chart PNG/SVG/data/metadata files.
- `latex_renderer.py` renders controlled XeLaTeX input.
- `pdf_compiler.py` runs `latexmk -xelatex` without shell escape.
- `excel_renderer.py` writes macro-free XLSX output.
- `artifact_validator.py` scans generated files for schema errors, broken
  references, sensitive strings, and unsafe content.

Report prose may be polished by an LLM, but it must only use accepted claim IDs
and existing evidence. Unknown facts, uncited numbers, and new evidence are
rejected.
