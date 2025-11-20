import json
from chunk import parse_textract_layout_to_chunks

def test_merged_cells():
    # Mock Textract Response with a 2x2 table where the first row is merged across 2 columns
    # Row 1: "Header" (Spans 2 columns)
    # Row 2: "A", "B"
    
    mock_response = {
        'Blocks': [
            # TABLE Block
            {
                'Id': 'table-1',
                'BlockType': 'TABLE',
                'Relationships': [{'Type': 'CHILD', 'Ids': ['cell-1', 'cell-2', 'cell-3']}]
            },
            # Cell 1: "Header" (Row 1, Col 1, ColSpan 2)
            {
                'Id': 'cell-1',
                'BlockType': 'CELL',
                'RowIndex': 1,
                'ColumnIndex': 1,
                'RowSpan': 1,
                'ColumnSpan': 2,
                'Relationships': [{'Type': 'CHILD', 'Ids': ['word-1']}]
            },
            # Cell 2: "A" (Row 2, Col 1)
            {
                'Id': 'cell-2',
                'BlockType': 'CELL',
                'RowIndex': 2,
                'ColumnIndex': 1,
                'RowSpan': 1,
                'ColumnSpan': 1,
                'Relationships': [{'Type': 'CHILD', 'Ids': ['word-2']}]
            },
            # Cell 3: "B" (Row 2, Col 2)
            {
                'Id': 'cell-3',
                'BlockType': 'CELL',
                'RowIndex': 2,
                'ColumnIndex': 2,
                'RowSpan': 1,
                'ColumnSpan': 1,
                'Relationships': [{'Type': 'CHILD', 'Ids': ['word-3']}]
            },
            # Words
            {'Id': 'word-1', 'BlockType': 'WORD', 'Text': 'Header'},
            {'Id': 'word-2', 'BlockType': 'WORD', 'Text': 'A'},
            {'Id': 'word-3', 'BlockType': 'WORD', 'Text': 'B'}
        ]
    }

    print("Running Parser...")
    chunks = parse_textract_layout_to_chunks(mock_response, "test-doc")
    
    if not chunks:
        print("FAILED: No chunks returned.")
        return

    table_chunk = chunks[0]
    print("\n--- Generated Markdown Table ---")
    print(table_chunk['text'])
    
    # Expected Output:
    # | Header | Header |
    # | --- | --- |
    # | A | B |
    
    lines = table_chunk['text'].split('\n')
    
    # Assertions
    assert "| Header | Header |" in lines[0], f"Row 1 mismatch: {lines[0]}"
    assert "| A | B |" in lines[2], f"Row 2 mismatch: {lines[2]}"
    
    print("\nSUCCESS: Merged cell logic verified!")

if __name__ == "__main__":
    test_merged_cells()
