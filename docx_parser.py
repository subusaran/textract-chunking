import docx

def parse_docx_to_chunks(file_path, document_id):
    """
    Parses a local DOCX file into chunks.
    """
    doc = docx.Document(file_path)
    chunks = []
    
    # We'll iterate through the document's block-level elements (paragraphs and tables)
    # in the order they appear. However, python-docx separates them.
    # A common approach to preserve order is to iterate through the element tree,
    # but for simplicity and robustness, we can process paragraphs and tables separately
    # or try to interleave them if order is strictly required. 
    # Given the "chunking" goal, processing paragraphs then tables (or vice versa) 
    # might lose context. 
    # Let's try to respect document order by iterating over `doc.element.body`.
    
    # Actually, a simpler robust way for RAG chunking often involves just text extraction
    # but preserving tables is key here.
    # Let's iterate over all block elements in order.
    
    def iter_block_items(parent):
        """
        Yield each paragraph and table child within *parent*, in document order.
        Each item will be an instance of either Table or Paragraph.
        """
        if isinstance(parent, docx.document.Document):
            parent_elm = parent.element.body
        elif isinstance(parent, docx.table._Cell):
            parent_elm = parent._tc
        else:
            raise ValueError("something's not right")

        for child in parent_elm.iterchildren():
            if isinstance(child, docx.oxml.text.paragraph.CT_P):
                yield docx.text.paragraph.Paragraph(child, parent)
            elif isinstance(child, docx.oxml.table.CT_Tbl):
                yield docx.table.Table(child, parent)

    current_text_block = []
    
    for block in iter_block_items(doc):
        if isinstance(block, docx.text.paragraph.Paragraph):
            text = block.text.strip()
            if text:
                # Heuristic: if it looks like a header (short, maybe bold?), treat as separate chunk?
                # For now, let's just accumulate text until a reasonable break or change in type.
                # But following the Textract logic, we might want to group paragraphs.
                # Let's group by simple paragraph breaks for now.
                chunks.append({
                    "text": text,
                    "metadata": {
                        "document_id": document_id,
                        "type": "text_block"
                        # Page numbers are hard in DOCX without rendering
                    }
                })
        
        elif isinstance(block, docx.table.Table):
            # Process Table
            # Convert to Markdown
            rows = block.rows
            if not rows:
                continue
                
            # Calculate max columns (handling merged cells is tricky in docx, 
            # but let's assume a simple grid for now or just take the max cells in a row)
            # python-docx handles merged cells by repeating the cell object.
            
            grid_data = []
            for row in rows:
                row_data = [cell.text.strip().replace('\n', ' ') for cell in row.cells]
                grid_data.append(row_data)
            
            if not grid_data:
                continue
                
            # Generate Markdown
            # We need to normalize column counts for a valid MD table
            max_cols = max(len(r) for r in grid_data)
            
            md_lines = []
            # Header
            header_cells = grid_data[0] + [''] * (max_cols - len(grid_data[0]))
            md_lines.append("| " + " | ".join(header_cells) + " |")
            # Separator
            md_lines.append("| " + " | ".join(['---'] * max_cols) + " |")
            # Body
            for r in grid_data[1:]:
                padded_row = r + [''] * (max_cols - len(r))
                md_lines.append("| " + " | ".join(padded_row) + " |")
            
            chunks.append({
                "text": "\n".join(md_lines),
                "metadata": {
                    "document_id": document_id,
                    "type": "table"
                }
            })

    return chunks
