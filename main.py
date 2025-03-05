import os
import json
import hashlib
from pathlib import Path

from fastapi import FastAPI, Request, HTTPException, Query, UploadFile, File
from fastapi.responses import Response

app = FastAPI(debug=True)

MB = 1024 * 1024
DEFAULT_CHUNK_SIZE = 5*MB
DEFALT_DOWNLOAD_SPEED_CHUNK_SIZE = 1 * MB

UPLOAD_DIR = Path("./data")
DOWNLOAD_DIR = Path('./library')

def verify_md5(target: str, chunk: bytes):
    md5 = hashlib.md5()
    md5.update(chunk)
    if md5.hexdigest() != target:
        return False
    return True

@app.get("/download")
async def get_library(chunk_number: int=Query(...), chunk_size: int=DEFAULT_CHUNK_SIZE, request: Request=None):
    library_path = DOWNLOAD_DIR / './AD8-300S-directDIA_Top6_Target_DecoyPsps100_SumNorm_Composed.npy'
    print(library_path)
    file_size = os.path.getsize(str(library_path))
    headers = {
        'Content-Length': file_size
    }
    if request.method == 'HEAD':
        return Response(headers=headers, status_code=200)
    
    start_byte = (chunk_number - 1)  * chunk_size
    end_byte = start_byte + chunk_size
    with open(str(library_path), 'rb') as f:
        f.seek(start_byte)
        chunk = f.read(chunk_size)
    
    headers = {
        'Content-Disposition': f'attachment; filename="spectrum_library"',
        'Content-Range': f'bytes {start_byte}-{end_byte}/{file_size}',
        'Content-Length': len(chunk),
    }
    return Response(content=chunk, status_code=206, headers=headers, media_type='application/octet-stream')

@app.post("/upload")
async def upload(md5: str, file: UploadFile=File(...), request: Request=None):
    chunk_number = request.headers.get("X-Chunk-Number")
    if chunk_number is None:
        raise HTTPException(status_code=400, detail="Missing X-Chunk-Number header")
    file_name = file.filename
    dir = UPLOAD_DIR / "file_name"
    dir.mkdir()
    chunk_data = await file.read()
    if not verify_md5(md5, chunk_data):
        raise HTTPException(status_code=400, detail="Chunk data md5 value not verify")

    temp_file_path = dir / f"{file_name}.part{chunk_number}"
    with open(temp_file_path, "wb") as f:
        f.write(chunk_data)
    
    return {"message": f"Chunk {chunk_number} received successfully"}

@app.post("/upload/{filename}")
async def complete_upload(filename: str):
    filename_dir = UPLOAD_DIR / filename
    if not filename_dir.exists():
        raise HTTPException(status_code=400, detail="No chunks found")

    chunk_numbers = [int(p.suffix[5:]) for p in filename_dir.glob(f"{filename}.part*")]
    if not chunk_numbers:
        raise HTTPException(status_code=400, detail="No chunks found")
    
    chunk_numbers = json.dumps({'chunk_number': chunk_numbers})

    return Response(content=chunk_numbers, status_code=200)

@app.post("/complete")
async def complete_upload(filename: str):
    filename_dir = UPLOAD_DIR / filename
    chunks = sorted([p for p in filename_dir.glob(f"{filename}.part*")], key=lambda x: int(x.suffix[5:]))
    if not chunks:
        raise HTTPException(status_code=400, detail="No chunks found")
    
    final_file_path = UPLOAD_DIR / filename
    
    with open(final_file_path, "wb") as outfile:
        for chunk in chunks:
            with open(chunk, "rb") as infile:
                outfile.write(infile.read())
            os.remove(chunk)
    
    return {"message": "File uploaded and merged successfully"}

@app.get("/download_speed")
async def download_speed():
    chunk = os.urandom(DEFALT_DOWNLOAD_SPEED_CHUNK_SIZE)
    return Response(content=chunk)

@app.post("/upload_speed")
async def upload_speed(file: UploadFile=File(...)):
    return Response(content="upload success")
