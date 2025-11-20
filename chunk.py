import boto3
import time
import json
import sys

# --- CONFIGURATION ---
BUCKET_NAME = "your-s3-bucket-name"
DOCUMENT_KEY = "folder/your-large-document.pdf" 
REGION = "us-east-1" # Change to your AWS region

# Initialize Client
textract_client = boto3.client('textract', region_name=REGION)

# ==========================================
# PART 1: The Parser (Layout-Aware Logic)
# ==========================================
def parse_textract_layout_to_chunks(textract_response, document_id):
    """
    Parses Textract JSON using Layout Analysis (if available).
    Injects 'document_id' into every chunk for Vector DB filtering.
    """
    blocks = textract_response['Blocks']
    block_map = {b['Id']: b for b in blocks}
    
    chunks = []
    consumed_word_ids = set() 

    # --- Step A: Extract Tables (High Value) ---
    for block in blocks:
        if block['BlockType'] == 'TABLE':
            if 'Relationships' not in block: continue
            
            cell_ids = [rel['Ids'] for rel in block['Relationships'] if rel['Type'] == 'CHILD'][0]
            cells_data = []
            
            for cell_id in cell_ids:
                cell = block_map[cell_id]
                cell_text_words = []
                if 'Relationships' in cell:
                    for rel in cell['Relationships']:
                        if rel['Type'] == 'CHILD':
                            for wid in rel['Ids']:
                                if block_map[wid]['BlockType'] == 'WORD':
                                    cell_text_words.append(block_map[wid]['Text'])
                                    consumed_word_ids.add(wid)
                
                cells_data.append({
                    'r': cell['RowIndex'], 
                    'c': cell['ColumnIndex'], 
                    'text': " ".join(cell_text_words)
                })

            if not cells_data: continue
            
            # Build Markdown Table
            max_row = max(c['r'] for c in cells_data)
            max_col = max(c['c'] for c in cells_data)
            grid = [['' for _ in range(max_col)] for _ in range(max_row)]
            for c in cells_data: grid[c['r']-1][c['c']-1] = c['text']
            
            md_lines = ["| " + " | ".join(grid[0]) + " |", "| " + " | ".join(['---'] * max_col) + " |"]
            for row in grid[1:]: md_lines.append("| " + " | ".join(row) + " |")
            
            chunks.append({
                "text": "\n".join(md_lines),
                "metadata": {
                    "document_id": document_id,
                    "page": block.get('Page', 1),
                    "type": "table"
                }
            })

    # --- Step B: Extract Layout Text ---
    layout_content_types = ['LAYOUT_TITLE', 'LAYOUT_HEADER', 'LAYOUT_SECTION_HEADER', 'LAYOUT_TEXT', 'LAYOUT_LIST']
    layout_blocks = [b for b in blocks if b['BlockType'] in layout_content_types]
    
    if layout_blocks:
        for block in layout_blocks:
            block_lines = []
            if 'Relationships' in block:
                for rel in block['Relationships']:
                    if rel['Type'] == 'CHILD':
                        for child_id in rel['Ids']:
                            child = block_map.get(child_id)
                            if child and child['BlockType'] == 'LINE':
                                block_lines.append(child)
            
            total_words = 0
            consumed_count = 0
            block_text_parts = []
            
            for line in block_lines:
                if 'Relationships' in line:
                    for rel in line['Relationships']:
                        if rel['Type'] == 'CHILD':
                            for wid in rel['Ids']:
                                total_words += 1
                                if wid in consumed_word_ids:
                                    consumed_count += 1
                                elif block_map.get(wid):
                                     block_text_parts.append(block_map[wid]['Text'])

            # Overlap check: if >50% of words are in a table, skip this block
            if total_words > 0 and (consumed_count / total_words) > 0.5:
                continue

            full_text = " ".join(block_text_parts)
            if full_text.strip():
                chunks.append({
                    "text": full_text,
                    "metadata": {
                        "document_id": document_id,
                        "page": block.get('Page', 1),
                        "type": block['BlockType'].lower()
                    }
                })

    # --- Step C: Fallback (Raw Lines) ---
    else:
        current_text = []
        last_top = 0
        current_page = 1
        
        for block in blocks:
            if block['BlockType'] == 'LINE':
                # Deduplication check
                if 'Relationships' in block:
                    wids = [id for rel in block['Relationships'] for id in rel['Ids']]
                    if sum(1 for w in wids if w in consumed_word_ids) > len(wids)/2:
                        continue
                
                top = block['Geometry']['BoundingBox']['Top']
                # Paragraph Break Heuristic (5% vertical gap)
                if current_text and (abs(top - last_top) > 0.05 or block.get('Page') != current_page):
                    chunks.append({
                        "text": " ".join(current_text), 
                        "metadata": {"document_id": document_id, "page": current_page, "type": "text_block"}
                    })
                    current_text = []
                
                current_text.append(block['Text'])
                last_top = top
                current_page = block.get('Page', 1)
        
        if current_text:
            chunks.append({"text": " ".join(current_text), "metadata": {"document_id": document_id, "page": current_page, "type": "text_block"}})

    return chunks

# ==========================================
# PART 2: Async Helper Functions
# ==========================================

def start_job(bucket, key):
    print(f"Starting Textract Job for: s3://{bucket}/{key}")
    response = textract_client.start_document_analysis(
        DocumentLocation={'S3Object': {'Bucket': bucket, 'Name': key}},
        FeatureTypes=['TABLES', 'LAYOUT'] # <--- Crucial for the parser to work best
    )
    return response['JobId']

def get_full_results(job_id):
    """
    Polls for completion and then paginates to get ALL blocks.
    """
    print(f"Waiting for Job {job_id} to complete...", end='')
    
    # 1. Polling Loop
    while True:
        response = textract_client.get_document_analysis(JobId=job_id)
        status = response['JobStatus']
        
        if status == 'SUCCEEDED':
            print("\nJob Succeeded! Fetching results...")
            break
        elif status == 'FAILED':
            print(f"\nJob Failed: {response}")
            sys.exit(1)
        else:
            print(".", end='', flush=True)
            time.sleep(5) # Check every 5 seconds

    # 2. Pagination Loop (The Aggregator)
    blocks = []
    next_token = None
    
    while True:
        # Only request MaxResults and NextToken if they exist
        kwargs = {'JobId': job_id, 'MaxResults': 1000}
        if next_token:
            kwargs['NextToken'] = next_token
            
        response = textract_client.get_document_analysis(**kwargs)
        blocks.extend(response['Blocks'])
        
        next_token = response.get('NextToken')
        if not next_token:
            break
            
    print(f"Retrieved {len(blocks)} total blocks from Textract.")
    return {'Blocks': blocks}

# ==========================================
# PART 3: Main Execution
# ==========================================
if __name__ == "__main__":
    # 1. Start
    try:
        job_id = start_job(BUCKET_NAME, DOCUMENT_KEY)
        
        # 2. Wait & Aggregate
        full_response = get_full_results(job_id)
        
        # 3. Parse
        # Use filename as ID, or generate a UUID here
        doc_id = DOCUMENT_KEY.split('/')[-1] 
        
        vector_ready_chunks = parse_textract_layout_to_chunks(full_response, doc_id)
        
        # 4. Output / Load to Vector DB
        print(f"\n--- Success! Created {len(vector_ready_chunks)} Chunks ---")
        
        # Print first 3 chunks to verify
        for i, chunk in enumerate(vector_ready_chunks[:3]):
            print(f"\n[Chunk {i+1}] Metadata: {chunk['metadata']}")
            print(f"Text Preview: {chunk['text'][:100]}...")

    except Exception as e:
        print(f"\nError: {str(e)}")
