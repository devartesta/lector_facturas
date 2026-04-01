"""
Generate iPostal invoice PDFs using reportlab.
Data comes directly from the CSV order history.
"""
from pathlib import Path
from decimal import Decimal
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.lib.colors import HexColor, black, white
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_RIGHT, TA_CENTER

OUTPUT_DIR = Path(r"C:\Users\AdriàSebastià\Downloads\ipostal_pdfs")
OUTPUT_DIR.mkdir(exist_ok=True)

# Colors matching iPostal style
BLUE = HexColor('#0d6efd')
LIGHT_GRAY = HexColor('#f8f9fa')
BORDER_GRAY = HexColor('#dee2e6')
TEXT_MUTED = HexColor('#6c757d')
TEXT_DARK = HexColor('#212529')

# Invoice data: (order_number, date_str_YYYYMMDD, display_date, service, amount_str, mail_ids, quantity_note)
INVOICES = [
    # Nov 2025
    (
        "33486018", "20251115", "Sat Nov 15, 2025 09:59 am",
        "Storage", "4.40",
        "M15697, M15713, M15714, M15735", None
    ),
    (
        "33644054", "20251122", "Sat Nov 22, 2025 10:12 am",
        "Storage", "2.20",
        "M15552, M15553", None
    ),
    (
        "33781546", "20251129", "Sat Nov 29, 2025 08:51 am",
        "Business Green Plan 30 Renewal", "14.99",
        None, None
    ),
    (
        "33795047", "20251129", "Sat Nov 29, 2025 12:00 pm",
        "Storage", "1.10",
        "M15613", None
    ),
    # Dec 2025
    (
        "34114236", "20251213", "Sat Dec 13, 2025 11:45 am",
        "Storage", "3.30",
        "M15697, M15713, M15714", None
    ),
    (
        "34166613", "20251215", "Mon Dec 15, 2025 18:18 pm",
        "Scan (up to 10 pages)", "2.25",
        "M15697", None
    ),
    # Mar 2026
    (
        "36541238", "20260325", "Wed Mar 25, 2026 22:01 pm",
        "5 Scan/Shred Bundle", "10.00",
        None, "5 Units"
    ),
    (
        "36584953", "20260328", "Sat Mar 28, 2026 07:36 am",
        "Business Green Plan 30 Renewal", "14.99",
        None, None
    ),
    (
        "36600953", "20260328", "Sat Mar 28, 2026 08:46 am",
        "Storage", "1.10",
        "M15613", None
    ),
]


def make_invoice_pdf(order_number, date_str, display_date, service, amount_str,
                     mail_ids, quantity_note, output_path):
    doc = SimpleDocTemplate(
        str(output_path),
        pagesize=A4,
        rightMargin=20*mm,
        leftMargin=20*mm,
        topMargin=20*mm,
        bottomMargin=20*mm,
    )

    styles = getSampleStyleSheet()

    # Custom styles
    title_style = ParagraphStyle(
        'Title', parent=styles['Normal'],
        fontSize=18, textColor=TEXT_DARK, spaceAfter=6,
        fontName='Helvetica-Bold'
    )
    label_style = ParagraphStyle(
        'Label', parent=styles['Normal'],
        fontSize=9, textColor=TEXT_MUTED, spaceBefore=0, spaceAfter=2,
        fontName='Helvetica'
    )
    value_style = ParagraphStyle(
        'Value', parent=styles['Normal'],
        fontSize=11, textColor=TEXT_DARK, spaceBefore=0, spaceAfter=8,
        fontName='Helvetica-Bold'
    )
    normal_style = ParagraphStyle(
        'Normal2', parent=styles['Normal'],
        fontSize=11, textColor=TEXT_DARK, spaceAfter=2,
        fontName='Helvetica'
    )
    small_style = ParagraphStyle(
        'Small', parent=styles['Normal'],
        fontSize=10, textColor=TEXT_DARK, spaceAfter=2,
        fontName='Helvetica'
    )

    story = []

    # Title
    story.append(Paragraph(f"Invoice #{order_number}", title_style))
    story.append(HRFlowable(width="100%", thickness=1, color=BORDER_GRAY, spaceAfter=10))

    # Transaction Date
    story.append(Paragraph("Transaction Date", label_style))
    story.append(Paragraph(display_date, value_style))

    # Two-column: Invoice For | Payable To
    invoice_for_lines = [
        "Artesta Inc.",
        "Artesta Inc.",
        "18 Campus Blvd Suite 100",
        "Newtown Square, Pennsylvania 19073",
        "United States",
    ]
    payable_to_lines = [
        "iPostal1",
        "10 West Road",
        "Newtown, PA 18940",
        "United States",
    ]

    def addr_cell(header, lines):
        parts = [f'<font color="#6c757d" size="9">{header}</font><br/>']
        for i, line in enumerate(lines):
            if i == 0:
                parts.append(f'<b>{line}</b><br/>')
            else:
                parts.append(f'{line}<br/>')
        return Paragraph(''.join(parts), normal_style)

    addr_table = Table(
        [[addr_cell("Invoice for", invoice_for_lines),
          addr_cell("Payable to", payable_to_lines)]],
        colWidths=[85*mm, 85*mm]
    )
    addr_table.setStyle(TableStyle([
        ('VALIGN', (0,0), (-1,-1), 'TOP'),
        ('LEFTPADDING', (0,0), (-1,-1), 0),
        ('RIGHTPADDING', (0,0), (-1,-1), 0),
        ('BOTTOMPADDING', (0,0), (-1,-1), 8),
    ]))
    story.append(addr_table)

    # Mail IDs (if applicable)
    if mail_ids:
        story.append(Paragraph("Pertains to Mail IDs", label_style))
        story.append(Paragraph(mail_ids, value_style))

    # Products table
    qty = quantity_note or "1"
    product_label = f"1 x {service}" if not quantity_note else service
    amount_display = f"${amount_str}"

    header_row = [
        Paragraph("Product", ParagraphStyle('th', fontName='Helvetica-Bold', fontSize=10, textColor=TEXT_DARK)),
        Paragraph("Cost", ParagraphStyle('th', fontName='Helvetica-Bold', fontSize=10, textColor=TEXT_DARK, alignment=TA_RIGHT)),
    ]
    data_row = [
        Paragraph(product_label, small_style),
        Paragraph(amount_display, ParagraphStyle('td_r', fontName='Helvetica', fontSize=10, textColor=TEXT_DARK, alignment=TA_RIGHT)),
    ]
    total_row = [
        Paragraph("Total", ParagraphStyle('total', fontName='Helvetica-Bold', fontSize=11, textColor=TEXT_DARK)),
        Paragraph(amount_display, ParagraphStyle('total_r', fontName='Helvetica-Bold', fontSize=11, textColor=TEXT_DARK, alignment=TA_RIGHT)),
    ]

    if quantity_note:
        qty_row = [
            Paragraph("", small_style),
            Paragraph(quantity_note, ParagraphStyle('qty_r', fontName='Helvetica', fontSize=9, textColor=TEXT_MUTED, alignment=TA_RIGHT)),
        ]
        table_data = [header_row, data_row, qty_row, total_row]
        row_count = 4
    else:
        table_data = [header_row, data_row, total_row]
        row_count = 3

    products_table = Table(table_data, colWidths=[130*mm, 40*mm])
    ts = TableStyle([
        ('LINEBELOW', (0,0), (-1,0), 1.5, BORDER_GRAY),  # header bottom
        ('LINEABOVE', (0, row_count-1), (-1, row_count-1), 1.5, BORDER_GRAY),  # total top
        ('ROWBACKGROUNDS', (0,0), (-1,0), [LIGHT_GRAY]),
        ('LEFTPADDING', (0,0), (-1,-1), 4),
        ('RIGHTPADDING', (0,0), (-1,-1), 4),
        ('TOPPADDING', (0,0), (-1,-1), 6),
        ('BOTTOMPADDING', (0,0), (-1,-1), 6),
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
    ])
    products_table.setStyle(ts)
    story.append(products_table)
    story.append(Spacer(1, 8*mm))

    # Payment info
    story.append(Paragraph("Payment method: Credit Card", small_style))
    story.append(Paragraph("Status: Paid", small_style))

    # Build
    doc.build(story)
    print(f"  Created: {output_path.name}")


def main():
    print(f"Generating {len(INVOICES)} PDFs in {OUTPUT_DIR}")
    for (order_number, date_str, display_date, service, amount_str, mail_ids, quantity_note) in INVOICES:
        fname = f"IPOSTAL_{date_str}_{order_number}.pdf"
        output_path = OUTPUT_DIR / fname
        print(f"\nInvoice #{order_number}:")
        make_invoice_pdf(
            order_number, date_str, display_date, service, amount_str,
            mail_ids, quantity_note, output_path
        )
    print(f"\nDone! PDFs saved to {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
