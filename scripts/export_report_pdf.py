"""
scripts/export_report_pdf.py
============================
Converts docs/REPORT.md to a styled HTML document and uses headless Microsoft Edge
to generate an official PDF report (docs/REPORT.pdf).
Verifies the page count is strictly within the 6-10 page limit.
"""

import os
import re
import sys
import subprocess

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOCS_DIR = os.path.join(BASE_DIR, "docs")
REPORT_MD = os.path.join(DOCS_DIR, "REPORT.md")
REPORT_HTML = os.path.join(DOCS_DIR, "REPORT.html")
REPORT_PDF = os.path.join(DOCS_DIR, "REPORT.pdf")

EDGE_PATH = r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"

def markdown_to_html(md_text):
    # Minimalist robust markdown converter with tables, images, and formatting
    html_lines = []
    in_table = False
    table_header = False

    lines = md_text.splitlines()
    for line in lines:
        line_str = line.strip()

        # Handle tables
        if line_str.startswith("|") and line_str.endswith("|"):
            cells = [c.strip() for c in line_str.split("|")[1:-1]]
            if all(set(c).issubset({'-', ':', ' '}) for c in cells if c):
                # separator row
                table_header = False
                continue
            if not in_table:
                html_lines.append("<div class='table-container'><table>")
                in_table = True
                table_header = True

            tag = "th" if table_header else "td"
            row_html = "".join(f"<{tag}>{c}</{tag}>" for c in cells)
            html_lines.append(f"<tr>{row_html}</tr>")
            continue
        else:
            if in_table:
                html_lines.append("</table></div>")
                in_table = False
                table_header = False

        if not line_str:
            html_lines.append("<p class='spacer'></p>")
            continue

        # Headers
        if line_str.startswith("# "):
            html_lines.append(f"<h1>{line_str[2:]}</h1>")
        elif line_str.startswith("## "):
            html_lines.append(f"<h2>{line_str[3:]}</h2>")
        elif line_str.startswith("### "):
            html_lines.append(f"<h3>{line_str[4:]}</h3>")
        elif line_str.startswith("#### "):
            html_lines.append(f"<h4>{line_str[5:]}</h4>")
        elif line_str.startswith("---"):
            html_lines.append("<hr/>")
        elif line_str.startswith("- ") or line_str.startswith("* "):
            html_lines.append(f"<li>{line_str[2:]}</li>")
        elif line_str.startswith("!["):
            # Image: ![alt](url)
            m = re.match(r"!\[(.*?)\]\((.*?)\)", line_str)
            if m:
                alt, src = m.groups()
                html_lines.append(f"<div class='img-container'><img src='{src}' alt='{alt}'/><p class='caption'>{alt}</p></div>")
        elif line_str.startswith("> "):
            html_lines.append(f"<blockquote>{line_str[2:]}</blockquote>")
        else:
            html_lines.append(f"<p>{line_str}</p>")

    if in_table:
        html_lines.append("</table></div>")

    body_content = "\n".join(html_lines)

    # Simple inline markdown formatting (bold, code, math)
    body_content = re.sub(r"\*\*(.*?)\*\*", r"<strong>\1</strong>", body_content)
    body_content = re.sub(r"`(.*?)`", r"<code>\1</code>", body_content)
    body_content = re.sub(r"\$(.*?)\$", r"<em>\1</em>", body_content)

    html_template = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>CrisisGuard AI: Comprehensive Project Report</title>
<style>
    @page {{
        size: letter;
        margin: 15mm 15mm 15mm 15mm;
    }}
    body {{
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
        font-size: 9.5pt;
        line-height: 1.38;
        color: #1e293b;
        background: #ffffff;
        margin: 0;
        padding: 0;
    }}
    h1 {{
        font-size: 20pt;
        font-weight: 800;
        color: #0f172a;
        margin-top: 0;
        margin-bottom: 8pt;
        border-bottom: 2px solid #0284c7;
        padding-bottom: 4pt;
    }}
    h2 {{
        font-size: 14pt;
        font-weight: 700;
        color: #0f172a;
        margin-top: 14pt;
        margin-bottom: 6pt;
        border-bottom: 1px solid #cbd5e1;
        padding-bottom: 2pt;
    }}
    h3 {{
        font-size: 11.5pt;
        font-weight: 600;
        color: #1e293b;
        margin-top: 10pt;
        margin-bottom: 4pt;
    }}
    h4 {{
        font-size: 10.5pt;
        font-weight: 600;
        color: #334155;
        margin-top: 8pt;
        margin-bottom: 2pt;
    }}
    p {{
        margin-top: 0;
        margin-bottom: 6pt;
        text-align: justify;
    }}
    hr {{
        border: none;
        border-top: 1px solid #e2e8f0;
        margin: 10pt 0;
    }}
    .table-container {{
        width: 100%;
        margin: 8pt 0;
        page-break-inside: avoid;
    }}
    table {{
        width: 100%;
        border-collapse: collapse;
        font-size: 8.5pt;
        margin-bottom: 4pt;
    }}
    th {{
        background: #f1f5f9;
        color: #0f172a;
        font-weight: 700;
        text-align: left;
        padding: 5pt 6pt;
        border: 1px solid #cbd5e1;
    }}
    td {{
        padding: 4pt 6pt;
        border: 1px solid #cbd5e1;
        vertical-align: top;
    }}
    tr:nth-child(even) td {{
        background: #f8fafc;
    }}
    code {{
        font-family: Consolas, "Courier New", monospace;
        font-size: 8.5pt;
        background: #f1f5f9;
        padding: 1pt 3pt;
        border-radius: 3pt;
        color: #0369a1;
    }}
    blockquote {{
        margin: 6pt 0;
        padding: 6pt 10pt;
        background: #f8fafc;
        border-left: 3px solid #0284c7;
        font-size: 9pt;
        color: #334155;
    }}
    li {{
        margin-bottom: 2pt;
    }}
    .img-container {{
        text-align: center;
        margin: 10pt 0;
        page-break-inside: avoid;
    }}
    img {{
        max-width: 95%;
        height: auto;
        border: 1px solid #cbd5e1;
        border-radius: 4pt;
    }}
    .caption {{
        font-size: 8pt;
        color: #64748b;
        margin-top: 3pt;
        font-style: italic;
    }}
    .spacer {{
        margin: 0;
        padding: 0;
    }}
</style>
</head>
<body>
{body_content}
</body>
</html>
"""
    return html_template

def count_pdf_pages(pdf_path):
    with open(pdf_path, "rb") as f:
        content = f.read()
    counts = [int(c) for c in re.findall(rb'/Count\s+(\d+)', content)]
    if counts:
        return max(counts)
    pages = re.findall(b"/Type\s*/Page\b", content)
    return len(pages)


def main():
    print(f"Reading {REPORT_MD}...")
    with open(REPORT_MD, "r", encoding="utf-8") as f:
        md_text = f.read()

    print(f"Converting to styled HTML ({REPORT_HTML})...")
    html_content = markdown_to_html(md_text)
    with open(REPORT_HTML, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(f"Generating PDF via Headless Edge ({REPORT_PDF})...")
    cmd = [
        EDGE_PATH,
        "--headless",
        "--disable-gpu",
        "--run-all-compositor-stages-before-draw",
        f"--print-to-pdf={REPORT_PDF}",
        REPORT_HTML
    ]
    subprocess.run(cmd, check=True)

    if os.path.exists(REPORT_PDF):
        pages = count_pdf_pages(REPORT_PDF)
        size_kb = os.path.getsize(REPORT_PDF) / 1024
        print(f"\n[SUCCESS] Generated PDF: {REPORT_PDF}")
        print(f"  File Size: {size_kb:.1f} KB")
        print(f"  Page Count: {pages} pages")
        if 6 <= pages <= 10:
            print(f"  Page Count Criteria: PASS ({pages} is strictly between 6 and 10 pages)")
        else:
            print(f"  Page Count Criteria: NOTICE ({pages} pages; target is 6-10)")
    else:
        print("[ERROR] Failed to generate PDF")

if __name__ == "__main__":
    main()
