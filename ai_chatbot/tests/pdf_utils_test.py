# ai_chatbot/tests/pdf_utils_test.py
"""
Tests for PDF processing functionality
"""

import unittest
import os
import shutil
from ..utils.pdf_utils import PDFProcessor
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

class TestPDFProcessor(unittest.TestCase):
    """Test cases for PDFProcessor"""
    
    def setUp(self):
        """Set up test environment"""
        self.test_dir = "./test_pdfs"
        self.test_index = f"{self.test_dir}/test_faiss_index"
        os.makedirs(self.test_dir, exist_ok=True)
        
        # Create test PDF processor
        self.processor = PDFProcessor(index_path=self.test_index)
        
        # Create a test PDF
        self.test_pdf_path = f"{self.test_dir}/test_document.pdf"
        self._create_test_pdf()
    
    def tearDown(self):
        """Clean up test environment"""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
    
    def _create_test_pdf(self):
        """Create a test PDF file"""
        c = canvas.Canvas(self.test_pdf_path, pagesize=letter)
        
        # Add test content
        test_content = [
            "Spotify Analytics Test Document",
            "",
            "This document contains information about music streaming.",
            "Track popularity is measured by various metrics.",
            "Artists like Bruno Mars have high engagement rates.",
            "Skip rates below 20% indicate good user engagement.",
            "",
            "Machine learning models can predict track success.",
            "Real-time data provides insights into listening patterns.",
            "Batch processing handles daily API updates."
        ]
        
        y = 750
        for line in test_content:
            c.drawString(50, y, line)
            y -= 20
        
        c.save()
    
    def test_process_pdf(self):
        """Test PDF processing"""
        # Process the test PDF
        result = self.processor.process_pdf(self.test_pdf_path)
        
        # Check success message
        self.assertIn("Successfully processed", result)
        self.assertIn("chunks", result)
        
        # Check vector store was created
        self.assertIsNotNone(self.processor.vector_store)
    
    def test_process_nonexistent_pdf(self):
        """Test processing non-existent PDF"""
        with self.assertRaises(FileNotFoundError):
            self.processor.process_pdf("nonexistent.pdf")
    
    def test_search_documents(self):
        """Test searching in processed PDFs"""
        # Process PDF first
        self.processor.process_pdf(self.test_pdf_path)
        
        # Search for content
        results = self.processor.search("Bruno Mars engagement", k=2)
        
        # Check results contain relevant content
        self.assertIn("Bruno Mars", results)
        self.assertTrue(len(results) > 0)
    
    def test_search_without_documents(self):
        """Test searching when no documents are loaded"""
        # Create new processor with no documents
        empty_processor = PDFProcessor(index_path="./empty_index")
        
        # Search should return empty string
        results = empty_processor.search("any query")
        self.assertEqual(results, "")
    
    def test_clear_index(self):
        """Test clearing the vector store"""
        # Process PDF
        self.processor.process_pdf(self.test_pdf_path)
        
        # Clear index
        self.processor.clear_index()
        
        # Check vector store is None
        self.assertIsNone(self.processor.vector_store)
        
        # Check index directory is removed
        self.assertFalse(os.path.exists(self.test_index))
    
    def test_multiple_pdfs(self):
        """Test processing multiple PDFs"""
        # Process first PDF
        self.processor.process_pdf(self.test_pdf_path)
        
        # Create and process second PDF
        second_pdf = f"{self.test_dir}/second_test.pdf"
        c = canvas.Canvas(second_pdf, pagesize=letter)
        c.drawString(50, 750, "Second document about Billie Eilish")
        c.drawString(50, 730, "Different content for testing")
        c.save()
        
        result = self.processor.process_pdf(second_pdf)
        self.assertIn("Successfully processed", result)
        
        # Search should find content from both PDFs
        results1 = self.processor.search("Bruno Mars")
        results2 = self.processor.search("Billie Eilish")
        
        self.assertIn("Bruno Mars", results1)
        self.assertIn("Billie Eilish", results2)

def run_tests():
    """Run all PDF processor tests"""
    unittest.main()

if __name__ == '__main__':
    run_tests()