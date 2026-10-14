"""Isolated PDF worker: PDFium calls never share an application process.

PDFium forbids simultaneous calls across threads, even with distinct documents:
https://pypdfium2.readthedocs.io/en/stable/python_api.html#incompatibility-with-threading
No network access or application/credential initialization is needed here.
"""
import json
import subprocess
import sys
import tempfile
from pathlib import Path

import pypdfium2 as pdfium


def extract(content, ocr=False):
    text = ''
    with pdfium.PdfDocument(content) as pdf:
        for index in range(min(len(pdf), 2 if ocr else 6)):
            page = pdf[index]
            try:
                if ocr:
                    width, height = page.get_size()
                    if width * height * 9 > 24_000_000:
                        raise ValueError('OCR page exceeds its pixel limit.')
                    with tempfile.TemporaryDirectory(prefix='agm-accounting-ocr-') as directory:
                        image = Path(directory) / 'page.png'
                        bitmap = page.render(scale=3)
                        try:
                            bitmap.to_pil().save(image)
                        finally:
                            bitmap.close()
                        result = subprocess.run(['tesseract', str(image), 'stdout', '-l', 'eng', '--psm', '4'],
                                                capture_output=True, text=True, check=True, timeout=40)
                        text += result.stdout + '\n'
                else:
                    textpage = page.get_textpage()
                    try:
                        text += textpage.get_text_range() + '\n'
                    finally:
                        textpage.close()
            finally:
                page.close()
    return text


if __name__ == '__main__':
    content = sys.stdin.buffer.read(25 * 1024 * 1024 + 1)
    if not content.startswith(b'%PDF-') or len(content) > 25 * 1024 * 1024:
        raise ValueError('Invalid PDF input.')
    print(json.dumps({'text': extract(content, '--ocr' in sys.argv)}))
