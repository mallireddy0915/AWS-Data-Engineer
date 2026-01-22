#!/usr/bin/env python3
"""
Generate PDF from architecture_overview.md
Uses fpdf2 with Unicode support for proper formatting
"""
import re
from pathlib import Path
from fpdf import FPDF
from fpdf.enums import XPos, YPos

# Paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
MD_FILE = PROJECT_ROOT / "docs" / "architecture_overview.md"
PDF_FILE = PROJECT_ROOT / "docs" / "architecture_overview.pdf"

# Colors
DARK_BLUE = (0, 51, 102)
MED_BLUE = (0, 76, 153)
LIGHT_BLUE = (0, 102, 204)
HEADER_BG = (0, 51, 102)
ROW_BG = (245, 245, 250)
CODE_BG = (248, 248, 248)
BLACK = (0, 0, 0)
GRAY = (100, 100, 100)


class ArchitecturePDF(FPDF):
    def __init__(self):
        super().__init__(orientation='P', unit='mm', format='A4')
        self.set_auto_page_break(auto=True, margin=20)
        self.set_margins(15, 25, 15)
        self.add_page()
        
    def header(self):
        self.set_font('Helvetica', 'B', 9)
        self.set_text_color(*GRAY)
        self.cell(0, 8, 'NYC Yellow Taxi Data Platform - Architecture Overview', align='C')
        self.ln(3)
        self.set_draw_color(200, 200, 200)
        self.line(15, self.get_y(), self.w - 15, self.get_y())
        self.ln(5)
        
    def footer(self):
        self.set_y(-15)
        self.set_font('Helvetica', 'I', 8)
        self.set_text_color(128, 128, 128)
        self.cell(0, 10, f'Page {self.page_no()}', align='C')

    def add_title_page(self, title, subtitle, author=None, github=None):
        """Add a professional title page"""
        self.set_y(80)
        self.set_font('Helvetica', 'B', 28)
        self.set_text_color(*DARK_BLUE)
        self.multi_cell(0, 12, title, align='C')
        self.ln(10)
        self.set_font('Helvetica', '', 14)
        self.set_text_color(*GRAY)
        self.multi_cell(0, 8, subtitle, align='C')
        self.ln(30)
        
        # Author name
        if author:
            self.set_font('Helvetica', '', 11)
            self.set_text_color(*GRAY)
            self.cell(0, 8, 'Author:', align='C')
            self.ln(5)
            self.set_font('Helvetica', 'B', 14)
            self.set_text_color(*DARK_BLUE)
            self.cell(0, 8, author, align='C')
            self.ln(8)
        
        # GitHub link
        if github:
            self.set_font('Helvetica', '', 10)
            self.set_text_color(*LIGHT_BLUE)
            self.cell(0, 6, github, align='C')
            self.ln(8)
        
        self.ln(15)
        self.set_font('Helvetica', '', 11)
        self.set_text_color(*GRAY)
        self.cell(0, 8, 'January 2026', align='C')
        self.add_page()

    def section_title(self, text, level=1):
        """Add section headers with proper styling"""
        text = self._clean(text)
        if level == 1:
            self.ln(8)
            self.set_font('Helvetica', 'B', 18)
            self.set_text_color(*DARK_BLUE)
        elif level == 2:
            self.ln(6)
            self.set_font('Helvetica', 'B', 14)
            self.set_text_color(*MED_BLUE)
        else:
            self.ln(4)
            self.set_font('Helvetica', 'B', 12)
            self.set_text_color(*LIGHT_BLUE)
        self.multi_cell(0, 7, text)
        self.ln(3)
        
    def paragraph(self, text):
        """Add body paragraph"""
        text = self._clean(text)
        self.set_font('Helvetica', '', 10)
        self.set_text_color(*BLACK)
        self.multi_cell(0, 5.5, text)
        self.ln(2)
        
    def bullet(self, text):
        """Add bullet point"""
        text = self._clean(text)
        self.set_font('Helvetica', '', 10)
        self.set_text_color(*BLACK)
        # Get current position and ensure we have room
        x = self.get_x()
        if x > self.w - 30:
            self.ln()  # New line if too far right
        # Use indented text with bullet prefix
        self.multi_cell(0, 5.5, '  - ' + text)
        
    def code(self, text):
        """Add code block with monospace font"""
        text = self._clean(text)
        self.set_font('Courier', '', 6.5)
        self.set_fill_color(*CODE_BG)
        self.set_text_color(40, 40, 40)
        
        self.ln(2)
        lines = text.split('\n')
        for line in lines:
            if line.strip():
                # Truncate very long lines
                if len(line) > 95:
                    line = line[:92] + '...'
                self.cell(0, 3.8, '  ' + line, fill=True, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.ln(3)
        
    def table(self, rows):
        """Add formatted table"""
        if not rows:
            return
        
        # Clean all cells first
        rows = [[self._clean(c) for c in row] for row in rows]
            
        self.ln(3)
        n_cols = len(rows[0])
        
        # Limit columns to 4 max for readability
        if n_cols > 4:
            rows = [r[:4] for r in rows]
            n_cols = 4
        
        # Calculate column widths
        page_width = self.w - 30
        col_width = page_width / n_cols
        
        # Skip if columns would be too narrow
        if col_width < 20:
            # Print as text instead
            self.set_font('Helvetica', '', 8)
            self.set_text_color(*BLACK)
            for row in rows:
                self.multi_cell(0, 5, ' | '.join(row))
            self.ln(2)
            return
        
        for i, row in enumerate(rows):
            is_header = (i == 0)
            
            if is_header:
                self.set_font('Helvetica', 'B', 8)
                self.set_fill_color(*HEADER_BG)
                self.set_text_color(255, 255, 255)
            else:
                self.set_font('Helvetica', '', 8)
                self.set_fill_color(*ROW_BG if i % 2 == 0 else (255, 255, 255))
                self.set_text_color(*BLACK)
            
            for j, cell_text in enumerate(row[:n_cols]):
                # Truncate based on column width (roughly 2 chars per mm)
                max_chars = max(8, int(col_width / 2))
                if len(cell_text) > max_chars:
                    cell_text = cell_text[:max_chars-2] + '..'
                self.cell(col_width, 6, cell_text, border=1, fill=True)
            self.ln()
        
        self.ln(3)

    def horizontal_rule(self):
        """Add horizontal separator"""
        self.ln(4)
        self.set_draw_color(180, 180, 180)
        self.line(15, self.get_y(), self.w - 15, self.get_y())
        self.ln(4)
        
    def _clean(self, text):
        """Clean text for PDF output - replace special chars and strip markdown"""
        if not text:
            return ''
        
        # Strip markdown formatting
        # Bold: **text** or __text__
        text = re.sub(r'\*\*([^*]+)\*\*', r'\1', text)
        text = re.sub(r'__([^_]+)__', r'\1', text)
        # Italic: *text* or _text_
        text = re.sub(r'\*([^*]+)\*', r'\1', text)
        text = re.sub(r'_([^_]+)_', r'\1', text)
        # Code: `text`
        text = re.sub(r'`([^`]+)`', r'\1', text)
        # Links: [text](url)
        text = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', text)
        
        # Replace common Unicode with ASCII
        replacements = {
            '\u2014': '-', '\u2013': '-',  # em/en dash
            '\u2192': '->', '\u2190': '<-',  # arrows
            '\u2022': '*',  # bullet
            '\u2713': '[x]', '\u2717': '[!]',  # check/x marks
            '\u2265': '>=', '\u2264': '<=',  # comparison
            '\u201c': '"', '\u201d': '"',  # quotes
            '\u2018': "'", '\u2019': "'",
            # Box drawing
            '\u250c': '+', '\u2510': '+', '\u2514': '+', '\u2518': '+',
            '\u251c': '+', '\u2524': '+', '\u252c': '+', '\u2534': '+', '\u253c': '+',
            '\u2500': '-', '\u2502': '|',
            '\u2550': '=', '\u2551': '|',
            '\u2554': '+', '\u2557': '+', '\u255a': '+', '\u255d': '+',
            '\u2560': '+', '\u2563': '+', '\u2566': '+', '\u2569': '+', '\u256c': '+',
            '\u25b6': '>', '\u25c0': '<', '\u25bc': 'v', '\u25b2': '^',
            '\u25cf': '*', '\u25cb': 'o', '\u25a0': '#', '\u25a1': '[]',
            '\u2026': '...',
        }
        for orig, repl in replacements.items():
            text = text.replace(orig, repl)
        # Encode to ASCII, replacing unknown chars
        return text.encode('ascii', 'replace').decode('ascii')


def parse_markdown(content):
    """Parse markdown into structured elements"""
    elements = []
    lines = content.split('\n')
    i = 0
    in_code = False
    code_lines = []
    in_table = False
    table_rows = []
    
    while i < len(lines):
        line = lines[i]
        
        # Code blocks
        if line.startswith('```'):
            if in_code:
                elements.append(('code', '\n'.join(code_lines)))
                code_lines = []
                in_code = False
            else:
                in_code = True
            i += 1
            continue
            
        if in_code:
            code_lines.append(line)
            i += 1
            continue
        
        # Tables
        if '|' in line and line.strip().startswith('|'):
            if not in_table:
                in_table = True
                table_rows = []
            # Skip separator rows (|---|---|)
            if not re.match(r'^[\s|:-]+$', line):
                cells = [c.strip() for c in line.split('|')[1:-1]]
                if cells:
                    table_rows.append(cells)
            i += 1
            continue
        elif in_table:
            if table_rows:
                elements.append(('table', table_rows))
            table_rows = []
            in_table = False
            
        # Headers
        if line.startswith('# '):
            elements.append(('h1', line[2:].strip()))
        elif line.startswith('## '):
            elements.append(('h2', line[3:].strip()))
        elif line.startswith('### '):
            elements.append(('h3', line[4:].strip()))
        # Bullets
        elif line.strip().startswith('- ') or line.strip().startswith('* '):
            bullet_text = line.strip()[2:]
            elements.append(('bullet', bullet_text))
        # Numbered lists
        elif re.match(r'^\s*\d+\.\s', line):
            list_text = re.sub(r'^\s*\d+\.\s*', '', line)
            elements.append(('bullet', list_text))
        # Horizontal rules
        elif line.strip() in ('---', '***', '___'):
            elements.append(('hr', None))
        # Regular text
        elif line.strip():
            elements.append(('text', line.strip()))
            
        i += 1
    
    # Handle any remaining
    if in_code and code_lines:
        elements.append(('code', '\n'.join(code_lines)))
    if in_table and table_rows:
        elements.append(('table', table_rows))
        
    return elements


def generate_pdf():
    """Generate PDF from markdown"""
    print(f"Reading {MD_FILE}...")
    content = MD_FILE.read_text(encoding='utf-8')
    
    print("Parsing markdown...")
    elements = parse_markdown(content)
    
    print("Generating PDF...")
    pdf = ArchitecturePDF()
    
    # Add title page
    pdf.add_title_page(
        "NYC Yellow Taxi Data Platform",
        "Architecture Overview\nEnd-to-End Governed Data Engineering Solution",
        author="Mallikarjun Reddy",
        github="https://github.com/mallireddy0915/AWS-Data-Engineer"
    )
    
    for elem_type, elem_content in elements:
        try:
            if elem_type == 'h1':
                pdf.section_title(elem_content, 1)
            elif elem_type == 'h2':
                pdf.section_title(elem_content, 2)
            elif elem_type == 'h3':
                pdf.section_title(elem_content, 3)
            elif elem_type == 'text':
                pdf.paragraph(elem_content)
            elif elem_type == 'bullet':
                pdf.bullet(elem_content)
            elif elem_type == 'code':
                pdf.code(elem_content)
            elif elem_type == 'table':
                pdf.table(elem_content)
            elif elem_type == 'hr':
                pdf.horizontal_rule()
        except Exception as e:
            preview = str(elem_content)[:50] if elem_content else 'None'
            print(f"Warning: Skipping {elem_type} ({preview}...): {e}")
            continue
    
    print(f"Writing PDF to {PDF_FILE}...")
    pdf.output(str(PDF_FILE))
    
    size_kb = PDF_FILE.stat().st_size / 1024
    print(f"\n{'='*50}")
    print(f"PDF Generated Successfully!")
    print(f"{'='*50}")
    print(f"  File: {PDF_FILE}")
    print(f"  Size: {size_kb:.1f} KB")
    print(f"  Pages: {pdf.page_no()}")


if __name__ == "__main__":
    generate_pdf()
