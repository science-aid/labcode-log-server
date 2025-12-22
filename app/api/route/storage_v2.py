"""Storage API v2

Hybrid Access Layer (HAL) を使用した新しいストレージAPI。
Run IDベースのアクセスで、S3/ローカル/DBデータを統一的に扱う。
"""

import logging
import os
import tempfile
import sqlite3
from typing import Optional
from fastapi import APIRouter, Depends, Query, HTTPException
from fastapi.responses import PlainTextResponse, FileResponse
from sqlalchemy.orm import Session

from define_db.database import get_db
from define_db.models import Run, Process, Operation, Edge, Port
from services.hal import HybridAccessLayer

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v2/storage", tags=["storage-v2"])


@router.get("/list/{run_id}")
def list_run_contents(
    run_id: int,
    prefix: str = Query("", description="仮想パスプレフィックス"),
    db: Session = Depends(get_db)
):
    """
    Run内のコンテンツ一覧を取得

    S3モード: S3ファイル一覧
    ローカルモード: DBデータを仮想ファイルとして表示
    """
    try:
        hal = HybridAccessLayer(db)
        items = hal.list_contents(run_id, prefix)
        return {
            "run_id": run_id,
            "prefix": prefix,
            "items": [item.to_dict() for item in items]
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        logger.error(f"Runtime error in list_run_contents: {e}")
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error in list_run_contents: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/content/{run_id}")
def load_content(
    run_id: int,
    path: str = Query(..., description="仮想パス"),
    db: Session = Depends(get_db)
):
    """
    コンテンツを取得（プレビュー用）

    テキストファイルの場合は文字列として返却
    """
    try:
        hal = HybridAccessLayer(db)
        content = hal.load_content(run_id, path)

        if content is None:
            raise HTTPException(status_code=404, detail="Content not found")

        # テキストとして返却
        try:
            text = content.decode('utf-8')
            return {"content": text, "encoding": "utf-8"}
        except UnicodeDecodeError:
            # バイナリの場合はBase64エンコード
            import base64
            return {"content": base64.b64encode(content).decode(), "encoding": "base64"}

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in load_content: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/download/{run_id}")
def get_download_url(
    run_id: int,
    path: str = Query(..., description="仮想パス"),
    db: Session = Depends(get_db)
):
    """ダウンロードURLを取得"""
    try:
        hal = HybridAccessLayer(db)
        url = hal.get_download_url(run_id, path)
        return {"url": url, "run_id": run_id, "path": path}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error in get_download_url: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/info/{run_id}")
def get_storage_info(
    run_id: int,
    db: Session = Depends(get_db)
):
    """Runのストレージ情報を取得"""
    try:
        hal = HybridAccessLayer(db)
        info = hal.get_storage_info(run_id)
        return info.to_dict()
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error in get_storage_info: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/db-content/{run_id}")
def get_db_content(
    run_id: int,
    path: str = Query(..., description="仮想パス"),
    op_id: Optional[int] = Query(None, description="Operation ID"),
    db: Session = Depends(get_db)
):
    """DBに保存されたコンテンツを直接取得"""
    # オペレーションログの取得
    if "operations/" in path and path.endswith("log.txt") and op_id:
        operation = db.query(Operation).filter(Operation.id == op_id).first()
        if operation and operation.log:
            return PlainTextResponse(
                content=operation.log,
                media_type="text/plain",
                headers={"Content-Disposition": f"attachment; filename=log_{op_id}.txt"}
            )

    raise HTTPException(status_code=404, detail="Content not found in database")


@router.get("/dump/{run_id}")
def download_sql_dump(
    run_id: int,
    db: Session = Depends(get_db)
):
    """
    Run関連データのSQLiteダンプをダウンロード

    ローカルモードのRunに対して、関連する全データを
    独立したSQLiteファイルとしてエクスポートする。

    含まれるデータ:
    - runs: 該当Run
    - processes: Run内のProcess
    - operations: Process内のOperation
    - edges: Run内のEdge
    - ports: Process内のPort
    """
    # Runの存在確認とモードチェック
    run = db.query(Run).filter(Run.id == run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    if run.storage_mode != 'local':
        raise HTTPException(
            status_code=400,
            detail=f"SQL dump is only available for local mode runs. This run uses '{run.storage_mode}' mode."
        )

    try:
        # 一時ファイルを作成
        temp_file = tempfile.NamedTemporaryFile(
            delete=False,
            suffix='.db',
            prefix=f'run_{run_id}_'
        )
        temp_path = temp_file.name
        temp_file.close()

        # 新しいSQLiteデータベースを作成
        conn = sqlite3.connect(temp_path)
        cursor = conn.cursor()

        # テーブル作成
        cursor.execute('''
            CREATE TABLE runs (
                id INTEGER PRIMARY KEY,
                project_id INTEGER,
                file_name TEXT,
                checksum TEXT,
                user_id INTEGER,
                added_at TEXT,
                started_at TEXT,
                finished_at TEXT,
                status TEXT,
                storage_address TEXT,
                storage_mode TEXT,
                deleted_at TEXT,
                display_visible INTEGER
            )
        ''')

        cursor.execute('''
            CREATE TABLE processes (
                id INTEGER PRIMARY KEY,
                name TEXT,
                run_id INTEGER,
                storage_address TEXT,
                process_type TEXT,
                FOREIGN KEY (run_id) REFERENCES runs(id)
            )
        ''')

        cursor.execute('''
            CREATE TABLE operations (
                id INTEGER PRIMARY KEY,
                process_id INTEGER,
                name TEXT,
                parent_id INTEGER,
                started_at TEXT,
                finished_at TEXT,
                status TEXT,
                storage_address TEXT,
                is_transport INTEGER,
                is_data INTEGER,
                log TEXT,
                FOREIGN KEY (process_id) REFERENCES processes(id)
            )
        ''')

        cursor.execute('''
            CREATE TABLE edges (
                id INTEGER PRIMARY KEY,
                run_id INTEGER,
                from_id INTEGER,
                to_id INTEGER,
                FOREIGN KEY (run_id) REFERENCES runs(id)
            )
        ''')

        cursor.execute('''
            CREATE TABLE ports (
                id INTEGER PRIMARY KEY,
                process_id INTEGER,
                port_name TEXT,
                port_type TEXT,
                data_type TEXT,
                position INTEGER,
                is_required INTEGER,
                default_value TEXT,
                description TEXT,
                FOREIGN KEY (process_id) REFERENCES processes(id)
            )
        ''')

        # Runデータを挿入
        cursor.execute('''
            INSERT INTO runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            run.id, run.project_id, run.file_name, run.checksum, run.user_id,
            run.added_at.isoformat() if run.added_at else None,
            run.started_at.isoformat() if run.started_at else None,
            run.finished_at.isoformat() if run.finished_at else None,
            run.status, run.storage_address, run.storage_mode,
            run.deleted_at.isoformat() if run.deleted_at else None,
            1 if run.display_visible else 0
        ))

        # Processデータを取得・挿入
        processes = db.query(Process).filter(Process.run_id == run_id).all()
        process_ids = [p.id for p in processes]

        for p in processes:
            cursor.execute('''
                INSERT INTO processes VALUES (?, ?, ?, ?, ?)
            ''', (p.id, p.name, p.run_id, p.storage_address, p.process_type))

        # Operationデータを挿入
        if process_ids:
            operations = db.query(Operation).filter(
                Operation.process_id.in_(process_ids)
            ).all()

            for op in operations:
                cursor.execute('''
                    INSERT INTO operations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    op.id, op.process_id, op.name, op.parent_id,
                    op.started_at.isoformat() if op.started_at else None,
                    op.finished_at.isoformat() if op.finished_at else None,
                    op.status, op.storage_address,
                    1 if op.is_transport else 0,
                    1 if op.is_data else 0,
                    op.log
                ))

            # Portデータを挿入
            ports = db.query(Port).filter(
                Port.process_id.in_(process_ids)
            ).all()

            for port in ports:
                cursor.execute('''
                    INSERT INTO ports VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    port.id, port.process_id, port.port_name, port.port_type,
                    port.data_type, port.position,
                    1 if port.is_required else 0,
                    getattr(port, 'default_value', None),
                    getattr(port, 'description', None)
                ))

        # Edgeデータを挿入
        edges = db.query(Edge).filter(Edge.run_id == run_id).all()
        for e in edges:
            cursor.execute('''
                INSERT INTO edges VALUES (?, ?, ?, ?)
            ''', (e.id, e.run_id, e.from_id, e.to_id))

        conn.commit()
        conn.close()

        # ファイルサイズをログ
        file_size = os.path.getsize(temp_path)
        logger.info(f"Created SQL dump for run {run_id}: {file_size} bytes")

        # FileResponseで返却（cleanup後に自動削除）
        return FileResponse(
            path=temp_path,
            filename=f"run_{run_id}_dump.db",
            media_type="application/x-sqlite3",
            background=None  # 同期的に処理
        )

    except Exception as e:
        logger.error(f"Error creating SQL dump for run {run_id}: {e}")
        # 一時ファイルがあれば削除
        if 'temp_path' in locals() and os.path.exists(temp_path):
            os.unlink(temp_path)
        raise HTTPException(status_code=500, detail=f"Failed to create SQL dump: {str(e)}")
