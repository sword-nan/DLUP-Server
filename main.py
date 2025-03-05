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
DOWNLOAD_DIR = Path('./resource')

def verify_md5(target: str, chunk: bytes):
    md5 = hashlib.md5()
    md5.update(chunk)
    if md5.hexdigest() != target:
        return False
    return True

@app.head("/download")
async def download(file_name: str=Query(...)):
    path = DOWNLOAD_DIR / file_name
    if file_name == 'library':
        path = DOWNLOAD_DIR / "AD8-300S-directDIA_Top6_Target_DecoyPsps100_SumNorm_Composed.npy"

    print(path)
    file_size = os.path.getsize(str(path))
    headers = {
        'Content-Length': str(file_size)
    }
    return Response(headers=headers, status_code=200)

@app.get("/download")
async def download(file_name: str=Query(...), chunk_id: int=Query(...), chunk_size: int=Query(...)):
    path = DOWNLOAD_DIR / file_name
    if file_name == 'library':
        path = DOWNLOAD_DIR / "AD8-300S-directDIA_Top6_Target_DecoyPsps100_SumNorm_Composed.npy"
    
    start_byte = chunk_id  * chunk_size
    with open(str(path), 'rb') as f:
        f.seek(start_byte)
        chunk = f.read(chunk_size)
    
    return Response(content=chunk, status_code=206, media_type='application/octet-stream')

@app.get("/launch")
async def launch(filename: str=Query(...)):
    # 不带后缀的文件名
    file_name = Path(filename).stem
    content = json.dumps({
            "peptide_quantification": "",
            "protein_quantification": ""
        }
    )
    return Response(content=content)

@app.post("/upload")
async def upload(md5: str, file: UploadFile=File(...), request: Request=None):
    chunk_number = request.headers.get("X-Chunk-Number")
    if chunk_number is None:
        raise HTTPException(status_code=400, detail="Missing X-Chunk-Number header")
    file_name = file.filename
    dir = UPLOAD_DIR / file_name
    dir.mkdir(exist_ok=True)
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
    chunk_ids = []
    if not filename_dir.exists():
        print("no chunk found")
    else:
        chunk_ids = [int(p.suffix[5:]) for p in filename_dir.glob(f"{filename}.part*")]
        if not chunk_ids:
            print("no chunk found")
    
    chunk_ids = json.dumps({'chunk_ids': chunk_ids})
    return Response(content=chunk_ids, status_code=200)

@app.post("/complete")
async def complete_upload(filename: str=Query(...), md5: str=Query(...), suffix: str=Query(...)):
    filename_dir = UPLOAD_DIR / filename
    chunks = sorted([p for p in filename_dir.glob(f"{filename}.part*")], key=lambda x: int(x.suffix[5:]))
    if not chunks:
        raise HTTPException(status_code=400, detail="No chunks found")
    
    final_file_path = UPLOAD_DIR / filename
    file_md5 = hashlib.md5()
    dir = ""
    with open(final_file_path.with_suffix(suffix), "wb") as outfile:
        for chunk in chunks:
            with open(chunk, "rb") as infile:
                bytes = infile.read()
                outfile.write(bytes)
                file_md5.update(bytes)
            os.remove(chunk)
        dir = chunk.parent

    if file_md5.hexdigest() != md5:
        os.remove(final_file_path)
        return Response(content="md5 value not verify", status_code=400)
    os.removedirs(dir)

    return {"message": "File uploaded and merged successfully"}

@app.get("/download_speed")
async def download_speed():
    chunk = os.urandom(DEFALT_DOWNLOAD_SPEED_CHUNK_SIZE)
    return Response(content=chunk)

@app.post("/upload_speed")
async def upload_speed():
    return Response(content="upload success")
