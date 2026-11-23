"""Isolated PDF worker: PDFium calls never share an application process.

PDFium forbids simultaneous calls across threads, even with distinct documents:
https://pypdfium2.readthedocs.io/en/stable/python_api.html#incompatibility-with-threading
No network access or application/credential initialization is needed here.
"""
import json
import sys

import pypdfium2 as pdfium


def extract(content):
    text = ''
    with pdfium.PdfDocument(content) as pdf:
        page_count = len(pdf)
        for index in range(min(page_count, 6)):
            page = pdf[index]
            try:
                textpage = page.get_textpage()
                try:
                    text += textpage.get_text_range() + '\n'
                finally:
                    textpage.close()
            finally:
                page.close()
    return {'text': text, 'page_count': page_count}


if __name__ == '__main__':
    if len(sys.argv) != 1:
        raise ValueError('The PDF worker only extracts native text; OCR uses Google Document AI.')
    content = sys.stdin.buffer.read(25 * 1024 * 1024 + 1)
    if not content.startswith(b'%PDF-') or len(content) > 25 * 1024 * 1024:
        raise ValueError('Invalid PDF input.')
    print(json.dumps(extract(content)))
